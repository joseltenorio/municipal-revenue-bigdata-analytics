"""Construccion de facts Gold para ingresos SIAF y estadisticas prediales.

Este modulo materializa solo las facts base del modelo Gold objetivo.
No construye marts, auditoria Gold, Power BI ni registros Hive.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.common.paths import GOLD_DIR, SILVER_DIR, get_source_silver_path
from src.common.spark_session import build_spark_session


GOLD_FACT_DATASETS = [
    "fact_siaf_income",
    "fact_predial_statistics",
]

FACT_SIAF_REQUIRED_COLUMNS = [
    "anio",
    "mes",
    "sec_ejec",
    "ubigeo6_ejecutora",
    "source_resource_key",
    "source_granularity",
    "monto_pia",
    "monto_pim",
    "monto_recaudado",
]

MAP_REQUIRED_COLUMNS = [
    "sec_ejec",
    "ubigeo6",
    "municipality_key",
    "match_status",
]

FACT_PREDIAL_REQUIRED_COLUMNS = [
    "sec_ejec",
    "ubigeo6",
    "anio_aplicacion",
    "periodo",
    "anio_estadistica",
    "mes_estadistica",
    "formulario_id",
    "monto_emision_predial_total",
    "monto_recaudacion_predial_total",
    "monto_saldo_predial_total",
    "numero_predios_total",
    "numero_contribuyentes_predio",
]

DECIMAL_AMOUNT_TYPE = DecimalType(18, 4)
DECIMAL_RATIO_TYPE = DecimalType(18, 8)


class GoldFactError(ValueError):
    """Error de contrato para facts Gold."""


@dataclass(frozen=True)
class GoldFactPaths:
    """Rutas fisicas de entrada Silver y salida Gold."""

    output_root: Path
    siaf_income_root: Path
    map_sec_ejec_ubigeo_path: Path
    sismepre_esat_path: Path
    dim_municipality_path: Path


def default_paths() -> GoldFactPaths:
    """Devuelve rutas vigentes para la construccion de facts Gold."""

    return GoldFactPaths(
        output_root=GOLD_DIR,
        siaf_income_root=get_source_silver_path("siaf_income"),
        map_sec_ejec_ubigeo_path=SILVER_DIR / "integrated" / "map_sec_ejec_ubigeo",
        sismepre_esat_path=get_source_silver_path("sismepre") / "resource_key=esat_estadistica_atm",
        dim_municipality_path=GOLD_DIR / "dim_municipality",
    )


def utc_now_iso() -> str:
    """Retorna timestamp UTC estable para metadata Gold."""

    return datetime.now(timezone.utc).isoformat()


def existing_columns(available_columns: list[str], desired_columns: list[str]) -> list[str]:
    """Conserva el orden deseado filtrando columnas existentes."""

    available = set(available_columns)
    return [column for column in desired_columns if column in available]


def missing_columns(available_columns: list[str], required_columns: list[str]) -> list[str]:
    """Retorna columnas faltantes de un DataFrame."""

    available = set(available_columns)
    return [column for column in required_columns if column not in available]


def require_columns(dataframe: DataFrame, required_columns: list[str], dataset_name: str) -> None:
    """Falla rapido cuando un contrato minimo no se cumple."""

    missing = missing_columns(dataframe.columns, required_columns)
    if missing:
        raise GoldFactError(f"{dataset_name} no tiene columnas requeridas: {missing}")


def validate_selected_datasets(selected_datasets: list[str] | None) -> list[str]:
    """Valida datasets fact seleccionados desde CLI."""

    if not selected_datasets:
        return GOLD_FACT_DATASETS

    unsupported = [
        dataset for dataset in selected_datasets if dataset not in GOLD_FACT_DATASETS
    ]
    if unsupported:
        supported = ", ".join(GOLD_FACT_DATASETS)
        raise GoldFactError(
            f"Datasets Gold no soportados: {unsupported}. Soportados: {supported}."
        )

    return selected_datasets


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta fisica de una fact Gold soportada."""

    validate_selected_datasets([dataset_name])
    return output_root / dataset_name


def normalize_string_code(column_name: str) -> Any:
    """Normaliza un codigo textual sin alterar su forma estable."""

    cleaned = F.trim(F.col(column_name).cast("string"))
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def normalize_sec_ejec(column_name: str = "sec_ejec") -> Any:
    """Normaliza sec_ejec como texto estable para cruces tecnicos."""

    return normalize_string_code(column_name)


def normalize_ubigeo6(column_name: str = "ubigeo6") -> Any:
    """Normaliza ubigeo6 como string de seis digitos cuando es numerico."""

    cleaned = normalize_string_code(column_name)
    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.rlike(r"^[0-9]+$"), F.lpad(cleaned, 6, "0"))
        .otherwise(F.lit(None))
    )


def is_valid_ubigeo6(column_name: str = "ubigeo6") -> Any:
    """Expresion Spark para ubigeo peruano de seis digitos."""

    return F.col(column_name).cast("string").rlike(r"^[0-9]{6}$")


def normalize_decimal_column(column_name: str, *, scale: int = 4) -> Any:
    """Normaliza montos Gold a decimal estable."""

    return F.col(column_name).cast(DecimalType(18, scale))


def safe_ratio_expression(numerator_column: str, denominator_column: str) -> Any:
    """Calcula un ratio evitando division por cero."""

    return (
        F.when(
            F.col(denominator_column).isNull() | (F.col(denominator_column) <= 0),
            F.lit(None),
        )
        .otherwise(
            (F.col(numerator_column) / F.col(denominator_column)).cast(DECIMAL_RATIO_TYPE)
        )
    )


def derive_date_key(dataframe: DataFrame) -> DataFrame:
    """Deriva date_key mensual usando mes observado o enero para recursos anuales."""

    resolved_month = (
        F.when(F.col("mes").cast("int").between(1, 12), F.col("mes").cast("int"))
        .when(
            F.lower(F.col("source_granularity").cast("string")) == F.lit("annual"),
            F.lit(1),
        )
        .otherwise(F.lit(None))
    )

    return dataframe.withColumn("resolved_month", resolved_month).withColumn(
        "date_key",
        F.when(
            F.col("anio").cast("int").isNotNull() & F.col("resolved_month").isNotNull(),
            (
                F.col("anio").cast("int") * F.lit(10000)
                + F.col("resolved_month") * F.lit(100)
                + F.lit(1)
            ).cast("int"),
        ).otherwise(F.lit(None).cast("int")),
    )


def derive_sismepre_period_key() -> Any:
    """Deriva una llave estable compatible con dim_sismepre_period."""

    return F.concat_ws(
        "_",
        F.col("anio_aplicacion").cast("int").cast("string"),
        F.format_string("%02d", F.col("periodo").cast("int")),
        F.col("anio_estadistica").cast("int").cast("string"),
        F.format_string("%02d", F.coalesce(F.col("mes_estadistica").cast("int"), F.lit(0))),
    )


def read_parquet_dataset(spark: Any, path: Path, limit: int | None = None) -> DataFrame:
    """Lee Parquet con limite opcional para pruebas locales."""

    dataframe = spark.read.parquet(str(path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def list_siaf_resource_paths(siaf_income_root: Path) -> list[Path]:
    """Lista carpetas resource_key disponibles para SIAF Silver."""

    if not siaf_income_root.exists():
        return []

    return sorted(
        path
        for path in siaf_income_root.iterdir()
        if path.is_dir() and path.name.startswith("resource_key=")
    )


def required_input_paths(paths: GoldFactPaths, datasets: list[str]) -> list[Path]:
    """Devuelve entradas requeridas para los datasets seleccionados."""

    required: list[Path] = []
    if "fact_siaf_income" in datasets:
        required.extend([paths.siaf_income_root, paths.map_sec_ejec_ubigeo_path, paths.dim_municipality_path])
    if "fact_predial_statistics" in datasets:
        required.append(paths.sismepre_esat_path)
    return sorted(set(required))


def validate_input_paths(paths: GoldFactPaths, datasets: list[str]) -> None:
    """Valida que las entradas minimas existan antes de leerlas."""

    missing = [path for path in required_input_paths(paths, datasets) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"No existen entradas Silver requeridas: {missing}")

    if "fact_siaf_income" in datasets and not list_siaf_resource_paths(paths.siaf_income_root):
        raise FileNotFoundError(
            f"No hay recursos SIAF Silver bajo {paths.siaf_income_root}."
        )


def build_siaf_resolution_map(map_sec_ejec_ubigeo: DataFrame) -> DataFrame:
    """Resume el mapa tecnico a una fila por sec_ejec sin duplicar la fact."""

    require_columns(map_sec_ejec_ubigeo, MAP_REQUIRED_COLUMNS, "map_sec_ejec_ubigeo")

    normalized = (
        map_sec_ejec_ubigeo.select(
            normalize_sec_ejec("sec_ejec").alias("sec_ejec"),
            normalize_ubigeo6("ubigeo6").alias("ubigeo6"),
            normalize_ubigeo6("municipality_key").alias("municipality_key"),
            F.trim(F.col("match_status").cast("string")).alias("match_status"),
        )
        .where(F.col("sec_ejec").isNotNull())
        .dropDuplicates(["sec_ejec", "municipality_key", "match_status"])
        .withColumn(
            "resolved_municipality_key",
            F.coalesce(F.col("municipality_key"), F.col("ubigeo6")),
        )
    )

    return (
        normalized.groupBy("sec_ejec")
        .agg(
            F.countDistinct("resolved_municipality_key").alias("distinct_municipality_count"),
            F.first("resolved_municipality_key", ignorenulls=True).alias("municipality_key"),
            F.max(F.when(F.col("match_status") == "ambiguous_sec_ejec", F.lit(1)).otherwise(F.lit(0))).alias("has_ambiguous_sec_ejec"),
            F.max(F.when(F.col("match_status") == "ambiguous_sec_ejec_ubigeo", F.lit(1)).otherwise(F.lit(0))).alias("has_ambiguous_sec_ejec_ubigeo"),
            F.max(F.when(F.col("match_status") == "invalid_ubigeo", F.lit(1)).otherwise(F.lit(0))).alias("has_invalid_ubigeo"),
            F.max(F.when(F.col("match_status") == "missing_renamu", F.lit(1)).otherwise(F.lit(0))).alias("has_missing_renamu"),
            F.max(F.when(F.col("match_status") == "missing_classification", F.lit(1)).otherwise(F.lit(0))).alias("has_missing_classification"),
            F.max(F.when(F.col("match_status") == "missing_sismepre", F.lit(1)).otherwise(F.lit(0))).alias("has_missing_sismepre"),
            F.max(F.when(F.col("match_status") == "missing_siaf", F.lit(1)).otherwise(F.lit(0))).alias("has_missing_siaf"),
            F.max(F.when(F.col("match_status") == "unmatched", F.lit(1)).otherwise(F.lit(0))).alias("has_unmatched"),
            F.max(F.when(F.col("match_status") == "matched", F.lit(1)).otherwise(F.lit(0))).alias("has_matched"),
        )
        .withColumn(
            "match_status",
            F.when(
                (F.col("distinct_municipality_count") > 1) | (F.col("has_ambiguous_sec_ejec") == 1),
                F.lit("ambiguous_sec_ejec"),
            )
            .when(F.col("has_ambiguous_sec_ejec_ubigeo") == 1, F.lit("ambiguous_sec_ejec_ubigeo"))
            .when(F.col("has_invalid_ubigeo") == 1, F.lit("invalid_ubigeo"))
            .when(F.col("has_missing_renamu") == 1, F.lit("missing_renamu"))
            .when(F.col("has_missing_classification") == 1, F.lit("missing_classification"))
            .when(F.col("has_missing_sismepre") == 1, F.lit("missing_sismepre"))
            .when(F.col("has_missing_siaf") == 1, F.lit("missing_siaf"))
            .when(F.col("has_unmatched") == 1, F.lit("unmatched"))
            .when(F.col("has_matched") == 1, F.lit("matched"))
            .otherwise(F.lit("unmatched")),
        )
        .withColumn(
            "has_municipality_match",
            (
                (F.col("distinct_municipality_count") == 1)
                & F.col("municipality_key").isNotNull()
                & ~F.col("match_status").isin(
                    "ambiguous_sec_ejec",
                    "invalid_ubigeo",
                    "unmatched",
                )
            ).cast("boolean"),
        )
        .withColumn(
            "municipality_key",
            F.when(F.col("has_municipality_match"), F.col("municipality_key")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .select("sec_ejec", "municipality_key", "has_municipality_match", "match_status")
    )


def build_fact_siaf_income(
    siaf_frames: list[DataFrame],
    map_sec_ejec_ubigeo: DataFrame,
    dim_municipality: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye la fact Gold de ingresos SIAF con municipality_key resuelto."""

    if not siaf_frames:
        raise GoldFactError("No hay datasets SIAF para construir fact_siaf_income.")

    processed_at = processed_at_utc or utc_now_iso()
    normalized_frames: list[DataFrame] = []
    for dataframe in siaf_frames:
        require_columns(dataframe, FACT_SIAF_REQUIRED_COLUMNS, "siaf_income")
        normalized_frames.append(
            dataframe.select(
                F.col("anio").cast("int").alias("anio"),
                F.col("mes").cast("int").alias("mes"),
                normalize_sec_ejec("sec_ejec").alias("sec_ejec"),
                normalize_ubigeo6("ubigeo6_ejecutora").alias("ubigeo6_ejecutora"),
                F.col("source_resource_key").cast("string").alias("source_resource_key"),
                F.col("source_granularity").cast("string").alias("source_granularity"),
                normalize_decimal_column("monto_pia").alias("monto_pia"),
                normalize_decimal_column("monto_pim").alias("monto_pim"),
                normalize_decimal_column("monto_recaudado").alias("monto_recaudado"),
            )
        )

    fact_base = normalized_frames[0]
    for dataframe in normalized_frames[1:]:
        fact_base = fact_base.unionByName(dataframe)

    # 1. Preparar la dimension municipal libre de duplicados para evitar cartesianos o montos inflados
    muni_lookup = dim_municipality.select(F.col("ubigeo6").alias("dim_ubigeo6")).dropDuplicates(["dim_ubigeo6"])

    # 2. Join inicial de validacion contra la dimension municipal por ubigeo6_ejecutora
    joined_dim = fact_base.join(muni_lookup, fact_base.ubigeo6_ejecutora == muni_lookup.dim_ubigeo6, how="left")

    # 3. Definir condicion principal de resolucion por ubigeo6 de la ejecutora
    is_primary_resolved = (
        F.col("ubigeo6_ejecutora").isNotNull()
        & is_valid_ubigeo6("ubigeo6_ejecutora")
        & F.col("dim_ubigeo6").isNotNull()
    )

    # 4. Join con el mapa tecnico de resolucion para fallback por sec_ejec
    resolution_map = build_siaf_resolution_map(map_sec_ejec_ubigeo)
    resolved = derive_date_key(joined_dim).join(resolution_map.alias("fallback"), on="sec_ejec", how="left")

    # 5. Resolver columnas de emparejamiento con prioridad en la ejecutora
    final_df = (
        resolved.withColumn(
            "municipality_key",
            F.when(is_primary_resolved, F.col("ubigeo6_ejecutora")).otherwise(F.col("fallback.municipality_key")),
        )
        .withColumn(
            "has_municipality_match",
            F.when(is_primary_resolved, F.lit(True)).otherwise(
                F.coalesce(F.col("fallback.has_municipality_match"), F.lit(False))
            ),
        )
        .withColumn(
            "match_status",
            F.when(is_primary_resolved, F.lit("matched")).otherwise(
                F.coalesce(F.col("fallback.match_status"), F.lit("missing_map"))
            ),
        )
    )

    return (
        final_df.filter(
            (F.col("has_municipality_match") == F.lit(True))
            & F.col("municipality_key").isNotNull()
            & (~F.col("match_status").isin(
                "missing_map",
                "ambiguous_sec_ejec",
                "unmatched",
                "invalid_ubigeo",
            ))
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(
            "municipality_key",
            "sec_ejec",
            "date_key",
            "source_resource_key",
            "source_granularity",
            "monto_pia",
            "monto_pim",
            "monto_recaudado",
            "has_municipality_match",
            "match_status",
            "gold_processed_at_utc",
        )
    )


def build_fact_predial_statistics(
    sismepre_esat: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye la fact Gold de estadisticas prediales SISMEPRE."""

    require_columns(sismepre_esat, FACT_PREDIAL_REQUIRED_COLUMNS, "esat_estadistica_atm")
    processed_at = processed_at_utc or utc_now_iso()

    normalized = sismepre_esat.select(
        normalize_sec_ejec("sec_ejec").alias("sec_ejec"),
        normalize_ubigeo6("ubigeo6").alias("ubigeo6"),
        F.col("anio_aplicacion").cast("int").alias("anio_aplicacion"),
        F.col("periodo").cast("int").alias("periodo"),
        F.col("anio_estadistica").cast("int").alias("anio_estadistica"),
        F.col("mes_estadistica").cast("int").alias("mes_estadistica"),
        F.col("formulario_id").cast("int").alias("formulario_id"),
        normalize_decimal_column("monto_emision_predial_total").alias(
            "monto_emision_predial_total"
        ),
        normalize_decimal_column("monto_recaudacion_predial_total").alias(
            "monto_recaudacion_predial_total"
        ),
        normalize_decimal_column("monto_saldo_predial_total").alias(
            "monto_saldo_predial_total"
        ),
        F.col("numero_predios_total").cast("int").alias("numero_predios_total"),
        F.col("numero_contribuyentes_predio").cast("int").alias(
            "numero_contribuyentes_predio"
        ),
    ).where(is_valid_ubigeo6("ubigeo6"))

    return (
        normalized.withColumn("municipality_key", F.col("ubigeo6"))
        .withColumn("sismepre_period_key", derive_sismepre_period_key())
        .withColumn(
            "ratio_recaudacion_emision",
            safe_ratio_expression(
                "monto_recaudacion_predial_total", "monto_emision_predial_total"
            ),
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(
            "municipality_key",
            "sismepre_period_key",
            "sec_ejec",
            "ubigeo6",
            "formulario_id",
            "monto_emision_predial_total",
            "monto_recaudacion_predial_total",
            "monto_saldo_predial_total",
            "ratio_recaudacion_emision",
            "numero_predios_total",
            "numero_contribuyentes_predio",
            "gold_processed_at_utc",
        )
    )


def write_dataset(dataframe: DataFrame, output_path: Path, overwrite: bool) -> None:
    """Escribe una fact Gold evitando sobrescritura accidental."""

    if output_path.exists():
        if not overwrite:
            raise GoldFactError(
                f"La salida ya existe: {output_path}. Use --overwrite para reemplazarla."
            )
        shutil.rmtree(output_path)

    dataframe.write.mode("overwrite").parquet(str(output_path))


def build_gold_revenue_predial_facts(
    *,
    paths: GoldFactPaths | None = None,
    selected_datasets: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Construye fisicamente las facts Gold seleccionadas."""

    resolved_paths = paths or default_paths()
    datasets = validate_selected_datasets(selected_datasets)
    validate_input_paths(resolved_paths, datasets)

    spark = build_spark_session(app_name="gold-revenue-predial-facts")
    outputs: dict[str, DataFrame] = {}
    try:
        if "fact_siaf_income" in datasets:
            siaf_frames = [
                read_parquet_dataset(spark, path, limit)
                for path in list_siaf_resource_paths(resolved_paths.siaf_income_root)
            ]
            map_dataframe = read_parquet_dataset(
                spark, resolved_paths.map_sec_ejec_ubigeo_path, limit
            )
            dim_municipality = read_parquet_dataset(
                spark, resolved_paths.dim_municipality_path, limit
            )
            outputs["fact_siaf_income"] = build_fact_siaf_income(
                siaf_frames, map_dataframe, dim_municipality
            )

        if "fact_predial_statistics" in datasets:
            sismepre_esat = read_parquet_dataset(
                spark, resolved_paths.sismepre_esat_path, limit
            )
            outputs["fact_predial_statistics"] = build_fact_predial_statistics(
                sismepre_esat
            )

        row_counts = {dataset: dataframe.count() for dataset, dataframe in outputs.items()}
        if dry_run:
            return row_counts

        resolved_paths.output_root.mkdir(parents=True, exist_ok=True)
        for dataset, dataframe in outputs.items():
            write_dataset(
                dataframe,
                output_dataset_path(resolved_paths.output_root, dataset),
                overwrite,
            )
        return row_counts
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI del builder Gold de facts."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=GOLD_FACT_DATASETS,
        help="Fact Gold a construir. Puede repetirse.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Reemplaza salidas Gold existentes.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Construye y cuenta DataFrames sin escribir salidas.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite opcional por dataset de entrada para pruebas locales.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    row_counts = build_gold_revenue_predial_facts(
        selected_datasets=args.dataset,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    for dataset, row_count in row_counts.items():
        print(f"{dataset}: {row_count} filas")


if __name__ == "__main__":
    main()
