"""Construcción de marts Gold para ingresos municipales.

Este módulo usa datasets Silver integrados para construir el primer conjunto de
marts analíticos orientados a ingresos municipales. No construye marts
prediales ni territoriales completos y no hace joins fila-a-fila entre fuentes
de distinta granularidad.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.common.logger import get_logger
from src.common.paths import SILVER_DIR, get_source_gold_path


SUBJECT_AREA = "municipal_revenue"
INTEGRATED_SUBDIR = "integrated"

INPUT_DATASETS = [
    "mef_municipal_amounts",
    "municipal_entity_bridge",
    "renamu_municipal_context",
    "integration_coverage",
]

GOLD_DATASETS = [
    "fact_municipal_income_execution",
    "dim_municipality",
    "dim_time",
    "mart_municipal_revenue_overview",
    "fact_revenue_integration_coverage",
]

MEF_GRAIN_COLUMNS = [
    "source_dataset",
    "silver_source_granularity",
    "anio",
    "mes",
    "nivel_gobierno",
    "sector",
    "pliego",
    "sec_ejec",
    "ejecutora",
    "fuente_financiamiento",
    "rubro",
    "tipo_recurso",
    "generica",
    "subgenerica",
    "subgenerica_det",
    "especifica",
    "especifica_det",
]


class GoldMartError(Exception):
    """Error controlado durante la construcción de marts Gold."""


@dataclass(frozen=True)
class GoldPaths:
    """Rutas de entrada Silver y salida Gold."""

    input_root: Path
    output_root: Path


def utc_now_iso() -> str:
    """Retorna fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Calcula una razón evitando división entre cero o valores nulos."""

    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def build_period_key(anio: int | None, mes: int | None) -> str | None:
    """Construye una llave de periodo `YYYYMM` cuando existe año."""

    if anio is None:
        return None
    normalized_mes = 0 if mes is None else int(mes)
    return f"{int(anio):04d}{normalized_mes:02d}"


def classify_integration_quality(
    *,
    has_municipal_bridge: bool,
    has_valid_ubigeo: bool,
    has_renamu_match: bool,
) -> str:
    """Clasifica cobertura territorial de integración para análisis Gold."""

    if has_renamu_match:
        return "matched_renamu"
    if has_valid_ubigeo:
        return "valid_ubigeo_without_renamu"
    if has_municipal_bridge:
        return "bridge_without_valid_ubigeo"
    return "without_bridge"


def existing_columns(columns: Iterable[str], desired_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas deseadas que existen en un dataset."""

    available = set(columns)
    return [column for column in desired_columns if column in available]


def missing_columns(columns: Iterable[str], required_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas requeridas faltantes."""

    available = set(columns)
    return [column for column in required_columns if column not in available]


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta Gold de un dataset soportado."""

    if dataset_name not in GOLD_DATASETS:
        raise GoldMartError(f"Dataset Gold no soportado: {dataset_name}")
    return output_root / dataset_name


def resolve_paths(output_subdir: str) -> GoldPaths:
    """Resuelve rutas de entrada Silver integrada y salida Gold."""

    return GoldPaths(
        input_root=SILVER_DIR / INTEGRATED_SUBDIR,
        output_root=get_source_gold_path(output_subdir),
    )


def validate_input_paths(paths: GoldPaths) -> None:
    """Valida que existan las entradas Silver integradas necesarias."""

    missing = [
        str(paths.input_root / dataset_name)
        for dataset_name in INPUT_DATASETS
        if not (paths.input_root / dataset_name).exists()
    ]
    if missing:
        raise GoldMartError(
            "Faltan datasets Silver integrados requeridos para Gold: "
            + ", ".join(missing)
        )


def validate_selected_datasets(selected: list[str] | None) -> list[str]:
    """Valida datasets Gold solicitados por CLI."""

    if not selected:
        return GOLD_DATASETS

    invalid = sorted(set(selected) - set(GOLD_DATASETS))
    if invalid:
        raise GoldMartError(
            f"Datasets Gold no soportados: {invalid}. Disponibles: {GOLD_DATASETS}."
        )
    return selected


def read_input_dataset(spark: Any, paths: GoldPaths, dataset_name: str, limit: int | None) -> Any:
    """Lee un dataset Silver integrado con límite opcional para pruebas."""

    dataframe = spark.read.parquet(str(paths.input_root / dataset_name))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def nonblank(column_name: str) -> Any:
    """Expresión Spark para validar texto no vacío."""

    from pyspark.sql import functions as F

    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def spark_safe_ratio(numerator_column: str, denominator_column: str) -> Any:
    """Construye expresión Spark de división segura."""

    from pyspark.sql import functions as F

    denominator = F.col(denominator_column)
    return F.when(
        denominator.isNotNull() & (denominator != 0),
        F.col(numerator_column) / denominator,
    )


def add_ratio_columns(dataframe: Any) -> Any:
    """Agrega métricas derivadas de ejecución con división segura."""

    return (
        dataframe.withColumn(
            "recaudacion_vs_pim_ratio",
            spark_safe_ratio("monto_recaudado_total", "monto_pim_total"),
        )
        .withColumn(
            "recaudacion_vs_pia_ratio",
            spark_safe_ratio("monto_recaudado_total", "monto_pia_total"),
        )
        .withColumn("pim_vs_pia_ratio", spark_safe_ratio("monto_pim_total", "monto_pia_total"))
    )


def classify_quality_column() -> Any:
    """Expresión Spark para clasificar cobertura de integración."""

    from pyspark.sql import functions as F

    return (
        F.when(F.col("has_renamu_match"), F.lit("matched_renamu"))
        .when(F.col("has_valid_ubigeo"), F.lit("valid_ubigeo_without_renamu"))
        .when(F.col("has_municipal_bridge"), F.lit("bridge_without_valid_ubigeo"))
        .otherwise(F.lit("without_bridge"))
    )


def build_bridge_coverage_by_sec_ejec(bridge: Any) -> Any:
    """Resume cobertura municipal por `sec_ejec` sin multiplicar hechos MEF."""

    from pyspark.sql import functions as F

    required = ["sec_ejec", "ubigeo", "is_valid_ubigeo", "has_renamu_match"]
    missing = missing_columns(bridge.columns, required)
    if missing:
        raise GoldMartError(f"El puente municipal no tiene columnas requeridas: {missing}")

    return (
        bridge.where(nonblank("sec_ejec"))
        .groupBy("sec_ejec")
        .agg(
            F.countDistinct("ubigeo").alias("bridge_ubigeo_count"),
            F.first("ubigeo", ignorenulls=True).alias("first_bridge_ubigeo"),
            F.max(F.col("is_valid_ubigeo").cast("int")).alias("has_valid_ubigeo_int"),
            F.max(F.col("has_renamu_match").cast("int")).alias("has_renamu_match_int"),
        )
        .withColumn("has_municipal_bridge", F.lit(True))
        .withColumn(
            "ubigeo",
            F.when(F.col("bridge_ubigeo_count") == 1, F.col("first_bridge_ubigeo")),
        )
        .withColumn("has_valid_ubigeo", F.col("has_valid_ubigeo_int") == 1)
        .withColumn("has_renamu_match", F.col("has_renamu_match_int") == 1)
        .drop("first_bridge_ubigeo", "has_valid_ubigeo_int", "has_renamu_match_int")
    )


def build_fact_municipal_income_execution(mef_amounts: Any, bridge: Any) -> Any:
    """Construye el hecho Gold de ejecución de ingresos municipales."""

    from pyspark.sql import functions as F

    required = [
        "anio",
        "mes",
        "sec_ejec",
        "monto_pia_total",
        "monto_pim_total",
        "monto_recaudado_total",
        "source_record_count",
    ]
    missing = missing_columns(mef_amounts.columns, required)
    if missing:
        raise GoldMartError(f"MEF integrado no tiene columnas requeridas: {missing}")

    bridge_coverage = build_bridge_coverage_by_sec_ejec(bridge)
    selected_columns = existing_columns(mef_amounts.columns, MEF_GRAIN_COLUMNS)

    fact = mef_amounts.select(
        *selected_columns,
        "monto_pia_total",
        "monto_pim_total",
        "monto_recaudado_total",
        "source_record_count",
    ).join(bridge_coverage, on="sec_ejec", how="left")

    fact = (
        fact.withColumn(
            "has_municipal_bridge",
            F.coalesce(F.col("has_municipal_bridge"), F.lit(False)),
        )
        .withColumn("has_valid_ubigeo", F.coalesce(F.col("has_valid_ubigeo"), F.lit(False)))
        .withColumn("has_renamu_match", F.coalesce(F.col("has_renamu_match"), F.lit(False)))
        .withColumn("integration_quality_status", classify_quality_column())
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn(
            "gold_grain",
            F.lit("source_dataset_anio_mes_sec_ejec_budget_classifier"),
        )
    )
    return add_ratio_columns(fact)


def build_dim_municipality(bridge: Any, renamu_context: Any) -> Any:
    """Construye dimensión municipal conservando granularidad `sec_ejec + ubigeo`."""

    from pyspark.sql import functions as F

    bridge_required = ["sec_ejec", "ubigeo", "is_valid_sec_ejec", "is_valid_ubigeo", "has_renamu_match"]
    renamu_required = ["ubigeo", "tipomuni", "tipomuni_int"]
    missing_bridge = missing_columns(bridge.columns, bridge_required)
    missing_renamu = missing_columns(renamu_context.columns, renamu_required)
    if missing_bridge:
        raise GoldMartError(f"El puente municipal no tiene columnas requeridas: {missing_bridge}")
    if missing_renamu:
        raise GoldMartError(f"RENAMU contexto no tiene columnas requeridas: {missing_renamu}")

    bridge_cols = bridge.select(
        "sec_ejec",
        "ubigeo",
        "departamento",
        "provincia",
        "distrito",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "municipalidad_nombre",
        "mapping_source",
        "is_valid_sec_ejec",
        "is_valid_ubigeo",
        "has_renamu_match",
    ).dropDuplicates(["sec_ejec", "ubigeo", "mapping_source"])

    renamu_cols = renamu_context.select(
        "ubigeo",
        "idmunici",
        "departamento_normalizado",
        "provincia_normalizada",
        "distrito_normalizado",
        "tipomuni",
        "tipomuni_int",
    ).dropDuplicates(["ubigeo"])

    return (
        bridge_cols.join(renamu_cols, on="ubigeo", how="left")
        .withColumn(
            "municipality_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("sec_ejec"), F.lit("")),
                    F.coalesce(F.col("ubigeo"), F.lit("")),
                    F.coalesce(F.col("mapping_source"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def build_dim_time(fact: Any) -> Any:
    """Construye dimensión temporal desde el hecho MEF Gold."""

    from pyspark.sql import functions as F

    return (
        fact.select("anio", "mes")
        .dropDuplicates()
        .where(F.col("anio").isNotNull())
        .withColumn("is_annual_record", F.col("mes").isNull() | (F.col("mes") == 0))
        .withColumn("normalized_mes", F.coalesce(F.col("mes"), F.lit(0)))
        .withColumn(
            "period_key",
            F.concat(
                F.format_string("%04d", F.col("anio")),
                F.format_string("%02d", F.col("normalized_mes")),
            ),
        )
        .withColumn(
            "year_month_key",
            F.when(
                F.col("normalized_mes") > 0,
                F.concat(
                    F.format_string("%04d", F.col("anio")),
                    F.lit("-"),
                    F.format_string("%02d", F.col("normalized_mes")),
                ),
            ),
        )
        .withColumn(
            "period_label",
            F.when(F.col("normalized_mes") > 0, F.col("year_month_key")).otherwise(
                F.concat(F.col("anio").cast("string"), F.lit(" anual"))
            ),
        )
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .drop("normalized_mes")
        .orderBy("anio", "mes")
    )


def build_mart_municipal_revenue_overview(fact: Any) -> Any:
    """Construye mart agregado para visión ejecutiva de ingresos municipales."""

    from pyspark.sql import functions as F

    group_columns = [
        "anio",
        "mes",
        "sec_ejec",
        "ubigeo",
        "has_municipal_bridge",
        "has_valid_ubigeo",
        "has_renamu_match",
        "integration_quality_status",
    ]

    mart = fact.groupBy(*group_columns).agg(
        F.sum("monto_pia_total").alias("monto_pia_total"),
        F.sum("monto_pim_total").alias("monto_pim_total"),
        F.sum("monto_recaudado_total").alias("monto_recaudado_total"),
        F.sum("source_record_count").alias("source_record_count"),
    )
    return (
        add_ratio_columns(mart)
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("gold_grain", F.lit("anio_mes_sec_ejec_coverage"))
    )


def build_fact_revenue_integration_coverage(integration_coverage: Any) -> Any:
    """Construye hecho técnico de cobertura de integración relevante para ingresos."""

    from pyspark.sql import functions as F

    required = ["metric_name", "numerator", "denominator", "coverage_percentage"]
    missing = missing_columns(integration_coverage.columns, required)
    if missing:
        raise GoldMartError(f"Integration coverage no tiene columnas requeridas: {missing}")

    return (
        integration_coverage.select(
            "metric_name",
            "numerator",
            "denominator",
            "coverage_percentage",
            "description",
        )
        .withColumn("coverage_ratio", spark_safe_ratio("numerator", "denominator"))
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("source_dataset", F.lit("silver.integration_coverage"))
    )


def write_dataset(dataframe: Any, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset Gold en Parquet Snappy."""

    mode = "overwrite" if overwrite else "errorifexists"
    dataframe.write.mode(mode).option("compression", "snappy").parquet(str(output_path))


def print_dry_run(paths: GoldPaths, datasets: list[str]) -> None:
    """Muestra plan de construcción Gold sin escribir Parquet."""

    print("=" * 80)
    print("Plan Gold de ingresos municipales")
    print(f"Entrada Silver integrada: {paths.input_root}")
    for dataset in INPUT_DATASETS:
        path = paths.input_root / dataset
        print(f"- input {dataset}: {path} | existe={path.exists()}")
    print(f"Salida Gold: {paths.output_root}")
    print("Datasets Gold a crear:")
    for dataset in datasets:
        print(f"- {dataset}: {output_dataset_path(paths.output_root, dataset)}")
    print("Dry-run finalizado. No se escribirá Parquet en data/gold.")


def print_schema_summary(spark: Any, paths: GoldPaths) -> None:
    """Muestra columnas clave disponibles en entradas Silver integradas."""

    key_candidates = {
        "mef_municipal_amounts": [
            "source_dataset",
            "anio",
            "mes",
            "sec_ejec",
            "monto_pia_total",
            "monto_pim_total",
            "monto_recaudado_total",
        ],
        "municipal_entity_bridge": [
            "sec_ejec",
            "ubigeo",
            "is_valid_sec_ejec",
            "is_valid_ubigeo",
            "has_renamu_match",
        ],
        "renamu_municipal_context": [
            "ubigeo",
            "idmunici",
            "departamento_normalizado",
            "provincia_normalizada",
            "distrito_normalizado",
            "tipomuni",
            "tipomuni_int",
        ],
        "integration_coverage": ["metric_name", "numerator", "denominator", "coverage_percentage"],
    }

    for dataset, candidates in key_candidates.items():
        path = paths.input_root / dataset
        if not path.exists():
            print(f"- {dataset}: ruta no existe")
            continue
        dataframe = spark.read.parquet(str(path))
        available = existing_columns(dataframe.columns, candidates)
        print(f"- {dataset}: filas={dataframe.count()} columnas={len(dataframe.columns)} claves={available}")


def build_gold_marts(
    *,
    dry_run: bool,
    overwrite: bool,
    selected_datasets: list[str] | None = None,
    limit: int | None = None,
    output_subdir: str = SUBJECT_AREA,
) -> dict[str, Any]:
    """Ejecuta o planifica la construcción Gold de ingresos municipales."""

    from src.common.spark_session import build_spark_session

    paths = resolve_paths(output_subdir)
    validate_input_paths(paths)
    datasets = validate_selected_datasets(selected_datasets)

    spark = build_spark_session(
        app_name="GoldMunicipalRevenueMarts",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "8"},
    )

    try:
        if dry_run:
            print_dry_run(paths, datasets)
            print_schema_summary(spark, paths)
            return {"datasets": datasets, "output_root": str(paths.output_root)}

        logger = get_logger(__name__)
        mef = read_input_dataset(spark, paths, "mef_municipal_amounts", limit)
        bridge = read_input_dataset(spark, paths, "municipal_entity_bridge", limit)
        renamu = read_input_dataset(spark, paths, "renamu_municipal_context", limit)
        coverage = read_input_dataset(spark, paths, "integration_coverage", None)

        fact = build_fact_municipal_income_execution(mef, bridge)
        dim_municipality = build_dim_municipality(bridge, renamu)
        dim_time = build_dim_time(fact)
        mart = build_mart_municipal_revenue_overview(fact)
        coverage_fact = build_fact_revenue_integration_coverage(coverage)

        built = {
            "fact_municipal_income_execution": fact,
            "dim_municipality": dim_municipality,
            "dim_time": dim_time,
            "mart_municipal_revenue_overview": mart,
            "fact_revenue_integration_coverage": coverage_fact,
        }

        outputs: dict[str, str] = {}
        counts: dict[str, int] = {}
        for dataset_name in datasets:
            output_path = output_dataset_path(paths.output_root, dataset_name)
            logger.info("Escribiendo dataset Gold %s en %s", dataset_name, output_path)
            write_dataset(built[dataset_name], output_path, overwrite=overwrite)
            outputs[dataset_name] = str(output_path)
            counts[dataset_name] = int(built[dataset_name].count())

        coverage_summary = (
            fact.select("sec_ejec", "has_municipal_bridge")
            .dropDuplicates()
            .groupBy("has_municipal_bridge")
            .count()
            .orderBy("has_municipal_bridge")
            .collect()
        )

        print("=" * 80)
        print("Gold ingresos municipales finalizado")
        for dataset_name in datasets:
            print(f"- {dataset_name}: filas={counts[dataset_name]} ruta={outputs[dataset_name]}")
        print("Cobertura MEF por puente municipal en sec_ejec distintos:")
        for row in coverage_summary:
            print(f"- has_municipal_bridge={row['has_municipal_bridge']}: {row['count']}")

        return {"datasets": outputs, "counts": counts}
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Construye marts Gold de ingresos municipales desde Silver integrado."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida entradas y muestra plan sin escribir Parquet Gold.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe salidas Gold existentes.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=None,
        help="Dataset Gold a construir. Puede repetirse.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite opcional de filas para pruebas locales.",
    )
    parser.add_argument(
        "--output-subdir",
        default=SUBJECT_AREA,
        help="Subcarpeta bajo data/gold para salidas.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise GoldMartError("--limit debe ser un entero positivo.")

    build_gold_marts(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        selected_datasets=args.dataset,
        limit=args.limit,
        output_subdir=args.output_subdir,
    )


if __name__ == "__main__":
    main()
