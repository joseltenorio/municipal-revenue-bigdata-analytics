"""Integración Silver focalizada en el mapa técnico `sec_ejec -> ubigeo6`.

Este módulo construye únicamente `data/silver/integrated/map_sec_ejec_ubigeo/`.
La salida no es Gold, no materializa hechos, no crea dimensiones de negocio y
no incluye nombres observados por fuente. Su función es resolver llaves y dejar
trazabilidad técnica mínima entre SIAF, SISMEPRE, RENAMU y la clasificación
municipal oficial.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.common.logger import get_logger
from src.common.paths import SILVER_DIR, get_source_silver_path


SOURCE_NAMES = {
    "siaf": "siaf_income",
    "sismepre": "sismepre",
    "renamu": "renamu",
    "classification": "municipal_classification",
}
SISMEPRE_RESOURCE_KEY = "esat_estadistica_atm"
RENAMU_RESOURCE_KEY = "municipal_context"
CLASSIFICATION_RESOURCE_KEY = "classification_2019"
OUTPUT_DATASET_NAME = "map_sec_ejec_ubigeo"
INTEGRATED_DATASETS = [OUTPUT_DATASET_NAME]


class SilverIntegrationError(Exception):
    """Error controlado durante la integración Silver."""


@dataclass(frozen=True)
class IntegrationPaths:
    """Rutas de entrada y salida para la integración Silver."""

    siaf_root: Path
    sismepre_path: Path
    renamu_path: Path
    classification_path: Path
    output_root: Path


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def existing_columns(columns: Iterable[str], desired_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas deseadas que existen en el dataset."""

    available = set(columns)
    return [column for column in desired_columns if column in available]


def missing_required_columns(
    columns: Iterable[str],
    required_columns: Iterable[str],
) -> list[str]:
    """Devuelve columnas requeridas faltantes."""

    available = set(columns)
    return [column for column in required_columns if column not in available]


def calculate_coverage_percentage(numerator: int, denominator: int) -> float:
    """Calcula porcentaje de cobertura evitando división entre cero."""

    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 4)


def normalize_metric_row(
    metric_name: str,
    numerator: int,
    denominator: int,
    description: str,
) -> dict[str, Any]:
    """Construye una fila de cobertura serializable."""

    return {
        "metric_name": metric_name,
        "numerator": int(numerator),
        "denominator": int(denominator),
        "coverage_percentage": calculate_coverage_percentage(numerator, denominator),
        "description": description,
    }


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta de salida del dataset integrado objetivo."""

    if dataset_name not in INTEGRATED_DATASETS:
        raise SilverIntegrationError(f"Dataset integrado no soportado: {dataset_name}")
    return output_root / dataset_name


def selected_dataset_names(selected_sources: list[str] | None) -> list[str]:
    """Resuelve datasets integrados seleccionados por CLI."""

    if not selected_sources:
        return INTEGRATED_DATASETS

    invalid = sorted(set(selected_sources) - set(INTEGRATED_DATASETS))
    if invalid:
        raise SilverIntegrationError(
            f"Dataset integrado no soportado: {invalid}. "
            f"Disponible: {INTEGRATED_DATASETS}."
        )
    return selected_sources


def resource_path(source_path: Path, resource_key: str) -> Path:
    """Construye ruta de un recurso Silver por `resource_key`."""

    return source_path / f"resource_key={resource_key}"


def list_resource_paths(source_path: Path) -> list[Path]:
    """Lista carpetas `resource_key=*` existentes bajo una fuente Silver."""

    return sorted(
        path
        for path in source_path.glob("resource_key=*")
        if path.is_dir()
    )


def read_parquet(spark: Any, path: Path, limit: int | None = None) -> Any:
    """Lee Parquet con límite opcional para pruebas."""

    dataframe = spark.read.parquet(str(path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def nonblank(column_name: str) -> Any:
    """Expresión Spark para texto no vacío."""

    from pyspark.sql import functions as F

    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def trim_string_columns(dataframe: Any) -> Any:
    """Aplica trim a todas las columnas string sin cambiar los nombres."""

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    selected_columns = []

    for field in dataframe.schema.fields:
        column = F.col(field.name)
        if isinstance(field.dataType, StringType):
            selected_columns.append(F.trim(column).alias(field.name))
        else:
            selected_columns.append(column)

    return dataframe.select(*selected_columns)


def normalize_string_label(column_name: str) -> Any:
    """Normaliza texto preservando nulos cuando el valor llega vacío."""

    from pyspark.sql import functions as F

    return F.when(
        F.trim(F.col(column_name).cast("string")) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name).cast("string")))


def normalize_string_code(column_name: str, *, width: int | None = None) -> Any:
    """Normaliza códigos como string preservando ceros a la izquierda."""

    from pyspark.sql import functions as F

    cleaned = normalize_string_label(column_name)
    if width is None:
        return cleaned

    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.rlike(r"^[0-9]+$"), F.lpad(cleaned, width, "0"))
        .otherwise(cleaned)
    )


def build_municipality_key(column_name: str) -> Any:
    """Municipality key estable para el modelo objetivo."""

    from pyspark.sql import functions as F

    return F.col(column_name)


def normalize_ubigeo6_from_sismepre(column_name: str) -> Any:
    """Normaliza `ubigeo6` garantizando seis dígitos cuando el código es numérico."""

    from pyspark.sql import functions as F

    cleaned = normalize_string_label(column_name)
    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.rlike(r"^[0-9]+$"), F.lpad(cleaned, 6, "0"))
        .otherwise(F.lit(None))
    )


def normalize_sec_ejec(column_name: str) -> Any:
    """Normaliza `sec_ejec` como texto estable para integraciones posteriores."""

    return normalize_string_code(column_name)


def add_silver_metadata(dataframe: Any, *, processed_at: str) -> Any:
    """Agrega metadata técnica Silver mínima al mapa técnico."""

    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("silver_source_name", F.lit("integrated"))
        .withColumn("silver_resource_key", F.lit(OUTPUT_DATASET_NAME))
        .withColumn("silver_processed_at_utc", F.lit(processed_at))
    )


def resolve_paths(output_subdir: str) -> IntegrationPaths:
    """Resuelve rutas Silver de entrada y salida."""

    return IntegrationPaths(
        siaf_root=get_source_silver_path(SOURCE_NAMES["siaf"]),
        sismepre_path=get_source_silver_path(SOURCE_NAMES["sismepre"])
        / f"resource_key={SISMEPRE_RESOURCE_KEY}",
        renamu_path=get_source_silver_path(SOURCE_NAMES["renamu"])
        / f"resource_key={RENAMU_RESOURCE_KEY}",
        classification_path=get_source_silver_path(SOURCE_NAMES["classification"])
        / f"resource_key={CLASSIFICATION_RESOURCE_KEY}",
        output_root=SILVER_DIR / output_subdir,
    )


def validate_input_paths(paths: IntegrationPaths) -> None:
    """Valida que existan las rutas Silver necesarias."""

    required_paths = [
        paths.siaf_root,
        paths.sismepre_path,
        paths.renamu_path,
        paths.classification_path,
    ]
    missing_paths = [str(path) for path in required_paths if not path.exists()]
    if missing_paths:
        raise SilverIntegrationError(
            "Faltan rutas Silver requeridas para construir el mapa técnico: "
            + ", ".join(missing_paths)
        )


def select_sec_ejec_from_siaf(spark: Any, siaf_root: Path, limit: int | None) -> Any:
    """Construye el universo de `sec_ejec` observado en SIAF Silver."""

    frames = []
    for path in list_resource_paths(siaf_root):
        dataframe = read_parquet(spark, path, limit)
        if "sec_ejec" not in dataframe.columns:
            continue
        frames.append(
            dataframe.select(
                normalize_sec_ejec("sec_ejec").alias("sec_ejec"),
            )
        )

    if not frames:
        raise SilverIntegrationError(
            "No se encontraron recursos SIAF Silver con columna `sec_ejec`."
        )

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)

    return combined.where(nonblank("sec_ejec")).dropDuplicates(["sec_ejec"])


def select_distinct_ubigeos(
    spark: Any,
    path: Path,
    *,
    limit: int | None,
    column_name: str = "ubigeo6",
) -> Any:
    """Selecciona ubigeos válidos desde una tabla Silver ya curada."""

    dataframe = read_parquet(spark, path, limit)
    if column_name not in dataframe.columns:
        raise SilverIntegrationError(
            f"La ruta Silver no tiene la columna requerida `{column_name}`: {path}"
        )

    return (
        dataframe.select(normalize_ubigeo6_from_sismepre(column_name).alias("ubigeo6"))
        .where(nonblank("ubigeo6"))
        .dropDuplicates(["ubigeo6"])
    )


def select_sismepre_base(
    spark: Any,
    path: Path,
    *,
    limit: int | None,
) -> Any:
    """Selecciona el vínculo principal `sec_ejec + ubigeo6` desde SISMEPRE."""

    required_columns = ["sec_ejec", "ubigeo6"]
    dataframe = read_parquet(spark, path, limit)
    missing = missing_required_columns(dataframe.columns, required_columns)
    if missing:
        raise SilverIntegrationError(
            "El recurso SISMEPRE `esat_estadistica_atm` no tiene columnas "
            f"requeridas para el mapa técnico: {missing}"
        )

    selected = dataframe.select(
        normalize_sec_ejec("sec_ejec").alias("sec_ejec"),
        normalize_ubigeo6_from_sismepre("ubigeo6").alias("ubigeo6"),
    )

    return selected.where(nonblank("sec_ejec") & nonblank("ubigeo6"))


def build_status_columns(dataframe: Any) -> Any:
    """Deriva flags y estado técnico final del mapa."""

    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("municipality_key", build_municipality_key("ubigeo6"))
        .withColumn("has_siaf_match", F.coalesce(F.col("has_siaf_match"), F.lit(False)))
        .withColumn(
            "has_sismepre_match",
            F.coalesce(F.col("has_sismepre_match"), F.lit(False)),
        )
        .withColumn("has_renamu_match", F.coalesce(F.col("has_renamu_match"), F.lit(False)))
        .withColumn(
            "has_classification_match",
            F.coalesce(F.col("has_classification_match"), F.lit(False)),
        )
        .withColumn(
            "match_status",
            F.when(F.col("is_invalid_ubigeo"), F.lit("invalid_ubigeo"))
            .when(F.col("is_ambiguous_sec_ejec_ubigeo"), F.lit("ambiguous_sec_ejec_ubigeo"))
            .when(F.col("is_ambiguous_sec_ejec"), F.lit("ambiguous_sec_ejec"))
            .when(~F.col("has_siaf_match"), F.lit("missing_siaf"))
            .when(~F.col("has_renamu_match"), F.lit("missing_renamu"))
            .when(~F.col("has_classification_match"), F.lit("missing_classification"))
            .when(~F.col("has_sismepre_match"), F.lit("missing_sismepre"))
            .otherwise(F.lit("matched")),
        )
        .withColumn(
            "confidence_level",
            F.when(F.col("match_status") == "matched", F.lit("high"))
            .when(
                F.col("match_status").isin(
                    "missing_siaf",
                    "missing_renamu",
                    "missing_classification",
                    "missing_sismepre",
                ),
                F.lit("medium"),
            )
            .otherwise(F.lit("low")),
        )
        .withColumn(
            "issue_reason",
            F.when(F.col("match_status") == "matched", F.lit("ok"))
            .when(
                F.col("match_status") == "missing_siaf",
                F.lit("sec_ejec_not_found_in_siaf"),
            )
            .when(
                F.col("match_status") == "missing_renamu",
                F.lit("ubigeo_not_found_in_renamu"),
            )
            .when(
                F.col("match_status") == "missing_classification",
                F.lit("ubigeo_not_found_in_classification"),
            )
            .when(
                F.col("match_status") == "missing_sismepre",
                F.lit("sec_ejec_not_found_in_sismepre"),
            )
            .when(
                F.col("match_status") == "ambiguous_sec_ejec",
                F.lit("sec_ejec_maps_to_multiple_ubigeo6"),
            )
            .when(
                F.col("match_status") == "ambiguous_sec_ejec_ubigeo",
                F.lit("duplicated_sec_ejec_ubigeo"),
            )
            .otherwise(F.lit("invalid_ubigeo_format")),
        )
    )


def build_map_sec_ejec_ubigeo_from_frames(
    *,
    sismepre: Any,
    siaf_sec_ejec: Any,
    renamu_ubigeos: Any,
    classification_ubigeos: Any,
    processed_at: str | None = None,
) -> Any:
    """Construye el mapa técnico a partir de DataFrames ya cargados."""

    from pyspark.sql import functions as F

    source_counts = (
        sismepre.groupBy("sec_ejec", "ubigeo6")
        .agg(F.count(F.lit(1)).alias("source_pair_count"))
    )
    sec_ejec_counts = (
        sismepre.groupBy("sec_ejec")
        .agg(F.countDistinct("ubigeo6").alias("distinct_ubigeo6_count"))
    )
    base = (
        sismepre.dropDuplicates(["sec_ejec", "ubigeo6"])
        .join(source_counts, on=["sec_ejec", "ubigeo6"], how="left")
        .join(sec_ejec_counts, on="sec_ejec", how="left")
        .withColumn("has_sismepre_match", F.lit(True))
        .join(siaf_sec_ejec.withColumn("has_siaf_match", F.lit(True)), on="sec_ejec", how="left")
        .join(
            renamu_ubigeos.withColumn("has_renamu_match", F.lit(True)),
            on="ubigeo6",
            how="left",
        )
        .join(
            classification_ubigeos.withColumn("has_classification_match", F.lit(True)),
            on="ubigeo6",
            how="left",
        )
        .withColumn(
            "is_invalid_ubigeo",
            F.col("ubigeo6").isNull() | ~F.col("ubigeo6").rlike(r"^[0-9]{6}$"),
        )
        .withColumn(
            "is_ambiguous_sec_ejec_ubigeo",
            F.coalesce(F.col("source_pair_count") > F.lit(1), F.lit(False)),
        )
        .withColumn(
            "is_ambiguous_sec_ejec",
            F.coalesce(F.col("distinct_ubigeo6_count") > F.lit(1), F.lit(False)),
        )
    )

    base = build_status_columns(base)

    final_columns = [
        "sec_ejec",
        "ubigeo6",
        "municipality_key",
        "has_siaf_match",
        "has_sismepre_match",
        "has_renamu_match",
        "has_classification_match",
        "match_status",
        "confidence_level",
        "issue_reason",
        "silver_source_name",
        "silver_resource_key",
        "silver_processed_at_utc",
    ]

    mapped = add_silver_metadata(
        base.select(*final_columns[:-3]),
        processed_at=processed_at or utc_now_iso(),
    )

    mapped = mapped.select(*final_columns)

    # El contrato objetivo exige ubigeo de 6 dígitos; los casos inválidos se
    # excluyen de la salida física y quedan fuera del mapa técnico.
    return mapped.where(
        F.col("ubigeo6").isNotNull() & F.col("ubigeo6").rlike(r"^[0-9]{6}$")
    )


def build_map_sec_ejec_ubigeo(
    spark: Any,
    paths: IntegrationPaths,
    limit: int | None,
) -> Any:
    """Construye el mapa técnico `sec_ejec -> ubigeo6 -> municipality_key`."""

    sismepre = select_sismepre_base(spark, paths.sismepre_path, limit=limit)
    siaf_sec_ejec = select_sec_ejec_from_siaf(spark, paths.siaf_root, limit)
    renamu_ubigeos = select_distinct_ubigeos(
        spark,
        paths.renamu_path,
        limit=limit,
    )
    classification_ubigeos = select_distinct_ubigeos(
        spark,
        paths.classification_path,
        limit=limit,
    )

    return build_map_sec_ejec_ubigeo_from_frames(
        sismepre=sismepre,
        siaf_sec_ejec=siaf_sec_ejec,
        renamu_ubigeos=renamu_ubigeos,
        classification_ubigeos=classification_ubigeos,
    )


def write_dataset(dataframe: Any, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset integrado en Parquet."""

    mode = "overwrite" if overwrite else "errorifexists"
    dataframe.write.mode(mode).option("compression", "snappy").parquet(str(output_path))


def build_dry_run_schema_summary(
    spark: Any,
    paths: IntegrationPaths,
    limit: int | None,
) -> dict[str, Any]:
    """Muestra columnas y conteos clave sin ejecutar la escritura."""

    summary: dict[str, Any] = {
        "siaf_root": str(paths.siaf_root),
        "sismepre_path": str(paths.sismepre_path),
        "renamu_path": str(paths.renamu_path),
        "classification_path": str(paths.classification_path),
        "output_root": str(paths.output_root),
        "output_dataset_path": str(output_dataset_path(paths.output_root, OUTPUT_DATASET_NAME)),
    }

    if paths.sismepre_path.exists():
        sismepre = read_parquet(spark, paths.sismepre_path, limit)
        summary["sismepre_row_count"] = sismepre.count()
        summary["sismepre_columns"] = existing_columns(
            sismepre.columns,
            ["sec_ejec", "ubigeo6", "anio_aplicacion", "periodo", "formulario_id"],
        )

    if paths.renamu_path.exists():
        renamu = read_parquet(spark, paths.renamu_path, limit)
        summary["renamu_columns"] = existing_columns(renamu.columns, ["ubigeo6", "idmunici"])

    if paths.classification_path.exists():
        classification = read_parquet(spark, paths.classification_path, limit)
        summary["classification_columns"] = existing_columns(
            classification.columns,
            ["ubigeo6", "tipo_clasificacion_municipal"],
        )

    if paths.siaf_root.exists():
        sec_ejec = select_sec_ejec_from_siaf(spark, paths.siaf_root, limit)
        summary["siaf_sec_ejec_count"] = sec_ejec.count()

    return summary


def run_integration(
    *,
    dry_run: bool,
    overwrite: bool,
    selected_sources: list[str] | None = None,
    limit: int | None = None,
    output_subdir: str = "integrated",
) -> dict[str, Any]:
    """Ejecuta o planifica la integración Silver del mapa técnico."""

    from src.common.spark_session import build_spark_session

    paths = resolve_paths(output_subdir)
    validate_input_paths(paths)
    datasets = selected_dataset_names(selected_sources)

    spark = build_spark_session(
        app_name="SilverSecEjecUbigeoMapping",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "4"},
    )

    try:
        if dry_run:
            from pprint import pformat

            summary = build_dry_run_schema_summary(spark, paths, limit)
            print("=" * 80)
            print("Plan de integración Silver map_sec_ejec_ubigeo")
            print(f"Salida integrada: {paths.output_root}")
            print(f"Dataset objetivo: {datasets[0]}")
            print(pformat(summary, sort_dicts=True))
            return {"datasets": datasets, "output_root": str(paths.output_root)}

        logger = get_logger(__name__)
        output_path = output_dataset_path(paths.output_root, OUTPUT_DATASET_NAME)
        dataframe = build_map_sec_ejec_ubigeo(spark, paths, limit)
        logger.info("Escribiendo mapa técnico en %s", output_path)
        write_dataset(dataframe, output_path, overwrite=overwrite)

        from pyspark.sql import functions as F

        coverage_rows = [
            row.asDict()
            for row in dataframe.groupBy("match_status")
            .agg(F.count(F.lit(1)).alias("row_count"))
            .orderBy("match_status")
            .collect()
        ]

        print("=" * 80)
        print("Integración Silver map_sec_ejec_ubigeo finalizada")
        print(f"- {OUTPUT_DATASET_NAME}: {output_path}")
        print("Distribución por match_status:")
        for row in coverage_rows:
            print(f"- {row['match_status']}: {row['row_count']}")

        return {
            "datasets": {OUTPUT_DATASET_NAME: str(output_path)},
            "coverage": coverage_rows,
        }
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Construye el mapa técnico sec_ejec -> ubigeo6 -> municipality_key."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida entradas y muestra plan sin escribir Parquet.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe la salida integrada existente.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Dataset integrado a crear. Solo se admite map_sec_ejec_ubigeo.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite opcional de filas por recurso para pruebas locales.",
    )
    parser.add_argument(
        "--output-subdir",
        default="integrated",
        help="Subcarpeta bajo data/silver para la salida integrada.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    run_integration(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        selected_sources=args.source,
        limit=args.limit,
        output_subdir=args.output_subdir,
    )


if __name__ == "__main__":
    main()
