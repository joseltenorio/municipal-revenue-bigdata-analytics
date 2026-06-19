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
OUTPUT_COVERAGE_DATASET_NAME = "integration_coverage"
INTEGRATED_DATASETS = [OUTPUT_DATASET_NAME, OUTPUT_COVERAGE_DATASET_NAME]


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


def metric_output_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta física de cualquier salida Silver integrada soportada."""

    return output_dataset_path(output_root, dataset_name)


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


def _metric_row(
    *,
    coverage_scope: str,
    source_name: str,
    metric_name: str,
    metric_value: float,
    total_records: int,
    matched_records: int,
    issue_count: int | None = None,
    processed_at: str,
) -> dict[str, Any]:
    """Construye una fila de cobertura con métricas consistentes."""

    unmatched_records = max(total_records - matched_records, 0)
    issue_count = unmatched_records if issue_count is None else issue_count
    match_rate = 0.0 if total_records == 0 else round(matched_records / total_records, 6)
    issue_rate = 0.0 if total_records == 0 else round(issue_count / total_records, 6)

    return {
        "coverage_scope": coverage_scope,
        "source_name": source_name,
        "metric_name": metric_name,
        "metric_value": float(metric_value),
        "total_records": int(total_records),
        "matched_records": int(matched_records),
        "unmatched_records": int(unmatched_records),
        "match_rate": float(match_rate),
        "issue_count": int(issue_count),
        "issue_rate": float(issue_rate),
        "silver_source_name": "integrated",
        "silver_resource_key": OUTPUT_COVERAGE_DATASET_NAME,
        "silver_processed_at_utc": processed_at,
    }


def _count_distinct_in_frame(dataframe: Any, *columns: str) -> int:
    """Cuenta combinaciones distintas en un DataFrame Spark."""

    if not columns:
        return 0
    return dataframe.select(*columns).dropDuplicates().count()


def build_integration_coverage_from_frames(
    *,
    map_dataframe: Any,
    siaf_sec_ejec: Any,
    renamu_ubigeos: Any,
    classification_ubigeos: Any,
    processed_at: str | None = None,
) -> Any:
    """Construye el resumen técnico de cobertura a partir del mapa Silver."""

    from pyspark.sql import functions as F

    processed_at_value = processed_at or utc_now_iso()

    total_map_records = map_dataframe.count()
    matched_map_records = map_dataframe.where(F.col("match_status") == "matched").count()
    missing_siaf = map_dataframe.where(F.col("match_status") == "missing_siaf").count()
    missing_sismepre = map_dataframe.where(F.col("match_status") == "missing_sismepre").count()
    missing_renamu = map_dataframe.where(F.col("match_status") == "missing_renamu").count()
    missing_classification = map_dataframe.where(
        F.col("match_status") == "missing_classification"
    ).count()
    invalid_ubigeo = map_dataframe.where(F.col("match_status") == "invalid_ubigeo").count()
    ambiguous_sec_ejec = map_dataframe.where(F.col("match_status") == "ambiguous_sec_ejec").count()
    ambiguous_sec_ejec_ubigeo = map_dataframe.where(
        F.col("match_status") == "ambiguous_sec_ejec_ubigeo"
    ).count()
    high_confidence_records = map_dataframe.where(
        F.col("confidence_level") == "high"
    ).count()
    medium_confidence_records = map_dataframe.where(
        F.col("confidence_level") == "medium"
    ).count()
    low_confidence_records = map_dataframe.where(F.col("confidence_level") == "low").count()
    distinct_sec_ejec = _count_distinct_in_frame(map_dataframe, "sec_ejec")
    distinct_ubigeo6 = _count_distinct_in_frame(map_dataframe, "ubigeo6")
    distinct_municipality_key = _count_distinct_in_frame(map_dataframe, "municipality_key")

    map_sec_ejec = map_dataframe.select("sec_ejec").dropDuplicates()
    siaf_total = siaf_sec_ejec.select("sec_ejec").dropDuplicates().count()
    siaf_matched = (
        0
        if siaf_total == 0
        else map_sec_ejec.join(
            siaf_sec_ejec.select("sec_ejec").dropDuplicates(),
            on="sec_ejec",
            how="inner",
        ).count()
    )
    renamu_distinct = renamu_ubigeos.select("ubigeo6").dropDuplicates()
    renamu_total = renamu_distinct.count()
    classification_total = classification_ubigeos.select("ubigeo6").dropDuplicates().count()
    classification_distinct = classification_ubigeos.select("ubigeo6").dropDuplicates()
    renamu_classification_shared = (
        0
        if renamu_total == 0 or classification_total == 0
        else renamu_distinct.join(
            classification_distinct,
            on="ubigeo6",
            how="inner",
        ).count()
    )

    rows = [
        _metric_row(
            coverage_scope="map_sec_ejec_ubigeo",
            source_name="integrated",
            metric_name="total_map_records",
            metric_value=total_map_records,
            total_records=total_map_records,
            matched_records=total_map_records,
            issue_count=0,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_sec_ejec_ubigeo",
            source_name="integrated",
            metric_name="matched_records",
            metric_value=matched_map_records,
            total_records=total_map_records,
            matched_records=matched_map_records,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_sec_ejec_ubigeo",
            source_name="integrated",
            metric_name="unmatched_records",
            metric_value=max(total_map_records - matched_map_records, 0),
            total_records=total_map_records,
            matched_records=matched_map_records,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_sec_ejec_ubigeo",
            source_name="integrated",
            metric_name="match_rate",
            metric_value=0.0 if total_map_records == 0 else matched_map_records / total_map_records,
            total_records=total_map_records,
            matched_records=matched_map_records,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="missing_siaf",
            metric_value=missing_siaf,
            total_records=total_map_records,
            matched_records=max(total_map_records - missing_siaf, 0),
            issue_count=missing_siaf,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="missing_sismepre",
            metric_value=missing_sismepre,
            total_records=total_map_records,
            matched_records=max(total_map_records - missing_sismepre, 0),
            issue_count=missing_sismepre,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="missing_renamu",
            metric_value=missing_renamu,
            total_records=total_map_records,
            matched_records=max(total_map_records - missing_renamu, 0),
            issue_count=missing_renamu,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="missing_classification",
            metric_value=missing_classification,
            total_records=total_map_records,
            matched_records=max(total_map_records - missing_classification, 0),
            issue_count=missing_classification,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="invalid_ubigeo",
            metric_value=invalid_ubigeo,
            total_records=total_map_records,
            matched_records=max(total_map_records - invalid_ubigeo, 0),
            issue_count=invalid_ubigeo,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="ambiguous_sec_ejec",
            metric_value=ambiguous_sec_ejec,
            total_records=total_map_records,
            matched_records=max(total_map_records - ambiguous_sec_ejec, 0),
            issue_count=ambiguous_sec_ejec,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="ambiguous_sec_ejec_ubigeo",
            metric_value=ambiguous_sec_ejec_ubigeo,
            total_records=total_map_records,
            matched_records=max(total_map_records - ambiguous_sec_ejec_ubigeo, 0),
            issue_count=ambiguous_sec_ejec_ubigeo,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="high_confidence_records",
            metric_value=high_confidence_records,
            total_records=total_map_records,
            matched_records=high_confidence_records,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="medium_confidence_records",
            metric_value=medium_confidence_records,
            total_records=total_map_records,
            matched_records=medium_confidence_records,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="low_confidence_records",
            metric_value=low_confidence_records,
            total_records=total_map_records,
            matched_records=low_confidence_records,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="distinct_sec_ejec",
            metric_value=distinct_sec_ejec,
            total_records=total_map_records,
            matched_records=distinct_sec_ejec,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="distinct_ubigeo6",
            metric_value=distinct_ubigeo6,
            total_records=total_map_records,
            matched_records=distinct_ubigeo6,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="map_quality",
            source_name="integrated",
            metric_name="distinct_municipality_key",
            metric_value=distinct_municipality_key,
            total_records=total_map_records,
            matched_records=distinct_municipality_key,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="siaf_to_sismepre",
            source_name="siaf_income",
            metric_name="missing_sismepre",
            metric_value=max(siaf_total - siaf_matched, 0),
            total_records=siaf_total,
            matched_records=siaf_matched,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="sismepre_to_renamu",
            source_name="sismepre",
            metric_name="missing_renamu",
            metric_value=missing_renamu,
            total_records=total_map_records,
            matched_records=max(total_map_records - missing_renamu, 0),
            issue_count=missing_renamu,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="sismepre_to_classification",
            source_name="sismepre",
            metric_name="missing_classification",
            metric_value=missing_classification,
            total_records=total_map_records,
            matched_records=max(total_map_records - missing_classification, 0),
            issue_count=missing_classification,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="renamu_to_classification",
            source_name="renamu",
            metric_name="missing_classification",
            metric_value=max(renamu_total - renamu_classification_shared, 0),
            total_records=renamu_total,
            matched_records=renamu_classification_shared,
            processed_at=processed_at_value,
        ),
        _metric_row(
            coverage_scope="classification_to_renamu",
            source_name="municipal_classification",
            metric_name="missing_renamu",
            metric_value=max(classification_total - renamu_classification_shared, 0),
            total_records=classification_total,
            matched_records=renamu_classification_shared,
            processed_at=processed_at_value,
        ),
    ]

    coverage = map_dataframe.sparkSession.createDataFrame(rows)
    return coverage.select(
        "coverage_scope",
        "source_name",
        "metric_name",
        "metric_value",
        "total_records",
        "matched_records",
        "unmatched_records",
        "match_rate",
        "issue_count",
        "issue_rate",
        "silver_source_name",
        "silver_resource_key",
        "silver_processed_at_utc",
    )


def build_integration_coverage(
    spark: Any,
    paths: IntegrationPaths,
    limit: int | None,
) -> Any:
    """Construye el resumen técnico de cobertura de integración."""

    map_dataframe = build_map_sec_ejec_ubigeo(spark, paths, limit)
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

    return build_integration_coverage_from_frames(
        map_dataframe=map_dataframe,
        siaf_sec_ejec=siaf_sec_ejec,
        renamu_ubigeos=renamu_ubigeos,
        classification_ubigeos=classification_ubigeos,
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
        "coverage_dataset_path": str(
            output_dataset_path(paths.output_root, OUTPUT_COVERAGE_DATASET_NAME)
        ),
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
            print(f"Datasets objetivo: {datasets}")
            print(pformat(summary, sort_dicts=True))
            return {"datasets": datasets, "output_root": str(paths.output_root)}

        logger = get_logger(__name__)
        outputs: dict[str, Any] = {}

        map_dataframe = build_map_sec_ejec_ubigeo(spark, paths, limit)
        if OUTPUT_DATASET_NAME in datasets:
            output_path = output_dataset_path(paths.output_root, OUTPUT_DATASET_NAME)
            logger.info("Escribiendo mapa técnico en %s", output_path)
            write_dataset(map_dataframe, output_path, overwrite=overwrite)
            outputs[OUTPUT_DATASET_NAME] = str(output_path)

        if OUTPUT_COVERAGE_DATASET_NAME in datasets:
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
            coverage_dataframe = build_integration_coverage_from_frames(
                map_dataframe=map_dataframe,
                siaf_sec_ejec=siaf_sec_ejec,
                renamu_ubigeos=renamu_ubigeos,
                classification_ubigeos=classification_ubigeos,
            )
            coverage_output_path = output_dataset_path(
                paths.output_root,
                OUTPUT_COVERAGE_DATASET_NAME,
            )
            logger.info("Escribiendo cobertura de integración en %s", coverage_output_path)
            write_dataset(coverage_dataframe, coverage_output_path, overwrite=overwrite)
            outputs[OUTPUT_COVERAGE_DATASET_NAME] = str(coverage_output_path)

        coverage_rows = [
            row.asDict()
            for row in map_dataframe.groupBy("match_status")
            .count()
            .orderBy("match_status")
            .collect()
        ]

        print("=" * 80)
        print("Integración Silver finalizada")
        for dataset_name, output_path in outputs.items():
            print(f"- {dataset_name}: {output_path}")
        print("Distribución por match_status:")
        for row in coverage_rows:
            print(f"- {row['match_status']}: {row['count']}")

        return {"datasets": outputs, "coverage": coverage_rows}
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
