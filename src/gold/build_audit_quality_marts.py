"""Construccion de marts Gold de auditoria y monitoreo de calidad.

Este modulo materializa tablas Gold separadas del modelo analitico principal.
Lee resultados JSONL de calidad cuando existen y puede probarse en memoria.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from src.common.paths import GOLD_DIR, QUALITY_DIR, SILVER_DIR
from src.common.spark_session import build_spark_session


GOLD_AUDIT_DATASETS = [
    "audit_quality_results",
    "audit_dataset_summary",
    "audit_integration_coverage",
]

VALID_AUDIT_STATUSES = {"PASS", "WARNING", "FAIL", "ERROR"}
QUALITY_RESULT_COLUMNS = [
    "quality_result_key",
    "layer_name",
    "dataset_name",
    "resource_key",
    "check_name",
    "rule_name",
    "rule_category",
    "severity",
    "status",
    "message",
    "metric_name",
    "metric_value",
    "expected_value",
    "actual_value",
    "checked_at_utc",
    "source_file_path",
    "gold_processed_at_utc",
]
DATASET_SUMMARY_COLUMNS = [
    "dataset_summary_key",
    "layer_name",
    "dataset_name",
    "resource_key",
    "total_checks",
    "pass_count",
    "warning_count",
    "fail_count",
    "error_count",
    "completeness_score",
    "validity_score",
    "conformity_score",
    "quality_score",
    "row_count",
    "null_percentage",
    "duplicate_rows",
    "last_checked_at_utc",
    "gold_processed_at_utc",
]
INTEGRATION_COVERAGE_COLUMNS = [
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
    "gold_processed_at_utc",
]
INTEGRATION_COVERAGE_REQUIRED_COLUMNS = [
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
]


class GoldAuditError(ValueError):
    """Error de contrato para marts Gold de auditoria."""


@dataclass(frozen=True)
class GoldAuditPaths:
    """Rutas fisicas para entradas de calidad y salidas Gold de auditoria."""

    output_root: Path
    bronze_quality_results_path: Path
    silver_quality_results_path: Path
    integration_coverage_path: Path


def default_paths() -> GoldAuditPaths:
    """Devuelve rutas vigentes para auditoria y monitoreo de calidad."""

    return GoldAuditPaths(
        output_root=GOLD_DIR,
        bronze_quality_results_path=QUALITY_DIR / "bronze_quality_results.jsonl",
        silver_quality_results_path=QUALITY_DIR / "silver_quality_results.jsonl",
        integration_coverage_path=SILVER_DIR / "integrated" / "integration_coverage",
    )


def utc_now_iso() -> str:
    """Retorna timestamp UTC estable para metadata Gold."""

    return datetime.now(timezone.utc).isoformat()


def existing_columns(available_columns: list[str], desired_columns: list[str]) -> list[str]:
    """Conserva el orden deseado filtrando columnas existentes."""

    available = set(available_columns)
    return [column for column in desired_columns if column in available]


def missing_columns(available_columns: list[str], required_columns: list[str]) -> list[str]:
    """Retorna columnas faltantes en un DataFrame."""

    available = set(available_columns)
    return [column for column in required_columns if column not in available]


def require_columns(dataframe: DataFrame, required_columns: list[str], dataset_name: str) -> None:
    """Falla rapido cuando un contrato minimo no se cumple."""

    missing = missing_columns(dataframe.columns, required_columns)
    if missing:
        raise GoldAuditError(f"{dataset_name} no tiene columnas requeridas: {missing}")


def validate_selected_datasets(selected_datasets: list[str] | None) -> list[str]:
    """Valida datasets de auditoria seleccionados desde CLI."""

    if not selected_datasets:
        return GOLD_AUDIT_DATASETS

    unsupported = [
        dataset for dataset in selected_datasets if dataset not in GOLD_AUDIT_DATASETS
    ]
    if unsupported:
        supported = ", ".join(GOLD_AUDIT_DATASETS)
        raise GoldAuditError(
            f"Datasets Gold no soportados: {unsupported}. Soportados: {supported}."
        )

    return selected_datasets


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta fisica de un dataset Gold de auditoria soportado."""

    validate_selected_datasets([dataset_name])
    return output_root / dataset_name


def read_jsonl_records(path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    """Lee un archivo JSONL de calidad como lista de registros."""

    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo JSONL: {path}")

    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                raise GoldAuditError(
                    f"JSONL invalido en {path} linea {line_number}: {exc}"
                ) from exc
            if not isinstance(record, dict):
                raise GoldAuditError(f"Registro JSONL invalido en {path} linea {line_number}.")
            records.append(record)
            if limit is not None and len(records) >= limit:
                break

    return records


def normalize_status(value: Any) -> str:
    """Normaliza estados de calidad permitiendo ERROR para auditoria Gold."""

    normalized = str(value or "ERROR").strip().upper()
    if normalized not in VALID_AUDIT_STATUSES:
        return "ERROR"
    return normalized


def normalize_text(value: Any) -> str | None:
    """Normaliza texto simple preservando nulos y evitando espacios vacios."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def serialize_value(value: Any) -> str | None:
    """Serializa valores complejos a texto estable para auditoria."""

    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def parse_metric_value(value: Any) -> float | None:
    """Convierte un valor potencialmente textual a metrica numerica."""

    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("%", "").replace(",", "")
    try:
        return float(normalized)
    except ValueError:
        return None


def infer_rule_category(rule_name: str | None, check_name: str | None = None) -> str:
    """Infere una categoria funcional simple para reglas de calidad."""

    reference = f"{rule_name or ''} {check_name or ''}".lower()
    if any(token in reference for token in ["null", "missing", "required", "row_count"]):
        return "completeness"
    if any(token in reference for token in ["format", "valid", "allowed", "range", "bounds"]):
        return "validity"
    if any(token in reference for token in ["contract", "schema", "type", "duplicate", "conform"]):
        return "conformity"
    if any(token in reference for token in ["path", "readable", "parquet", "exists"]):
        return "technical"
    return "monitoring"


def infer_metric_name(rule_name: str | None) -> str | None:
    """Infere nombre de metrica cuando la salida de calidad no lo trae explicito."""

    if not rule_name:
        return None
    lowered = rule_name.lower()
    if "row_count" in lowered:
        return "row_count"
    if "duplicate" in lowered:
        return "duplicate_rows"
    if "null_percentage" in lowered:
        return "null_percentage"
    if "null" in lowered:
        return "null_count"
    if "match_rate" in lowered:
        return "match_rate"
    if "issue_rate" in lowered:
        return "issue_rate"
    return None


def stable_hash(*parts: Any) -> str:
    """Construye un hash estable para llaves tecnicas Gold."""

    payload = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def normalize_bronze_quality_record(
    record: dict[str, Any],
    *,
    processed_at_utc: str,
) -> dict[str, Any]:
    """Normaliza un registro Bronze JSONL al contrato Gold de auditoria."""

    dataset_name = normalize_text(record.get("source_name"))
    resource_key = normalize_text(record.get("resource_key"))
    rule_name = normalize_text(record.get("rule_id"))
    check_name = normalize_text(record.get("rule_type"))
    status = normalize_status(record.get("status"))
    severity = normalize_status(record.get("severity"))
    actual_value = serialize_value(record.get("observed_value"))
    metric_name = infer_metric_name(rule_name)
    metric_value = parse_metric_value(record.get("observed_value")) if metric_name else None
    checked_at_utc = normalize_text(record.get("processed_at_utc"))

    return {
        "quality_result_key": stable_hash(
            "bronze", dataset_name, resource_key, rule_name, checked_at_utc
        ),
        "layer_name": "bronze",
        "dataset_name": dataset_name,
        "resource_key": resource_key,
        "check_name": check_name,
        "rule_name": rule_name,
        "rule_category": infer_rule_category(rule_name, check_name),
        "severity": severity,
        "status": status,
        "message": normalize_text(record.get("message")),
        "metric_name": metric_name,
        "metric_value": metric_value,
        "expected_value": serialize_value(record.get("expected_value")),
        "actual_value": actual_value,
        "checked_at_utc": checked_at_utc,
        "source_file_path": normalize_text(record.get("dataset_path")),
        "gold_processed_at_utc": processed_at_utc,
    }


def normalize_silver_quality_record(
    record: dict[str, Any],
    *,
    processed_at_utc: str,
) -> dict[str, Any]:
    """Normaliza un registro Silver JSONL al contrato Gold de auditoria."""

    details = record.get("details")
    details_dict = details if isinstance(details, dict) else {}
    dataset_name = normalize_text(record.get("source_name"))
    resource_key = normalize_text(record.get("resource_key"))
    rule_name = normalize_text(record.get("rule_name"))
    check_name = normalize_text(details_dict.get("check_name")) or rule_name
    rule_category = normalize_text(details_dict.get("rule_category")) or infer_rule_category(
        rule_name, check_name
    )
    status = normalize_status(record.get("status"))
    severity = normalize_status(record.get("severity"))
    metric_name = normalize_text(details_dict.get("metric_name")) or infer_metric_name(rule_name)
    actual_value = serialize_value(details_dict.get("actual_value"))
    if actual_value is None and "actual" in details_dict:
        actual_value = serialize_value(details_dict.get("actual"))
    if actual_value is None and metric_name:
        actual_value = serialize_value(details_dict.get("metric_value"))

    metric_value = parse_metric_value(details_dict.get("metric_value"))
    if metric_value is None and metric_name and actual_value is not None:
        metric_value = parse_metric_value(actual_value)

    checked_at_utc = normalize_text(record.get("checked_at_utc"))
    layer_name = normalize_text(record.get("layer")) or "silver"

    return {
        "quality_result_key": stable_hash(
            layer_name, dataset_name, resource_key, rule_name, checked_at_utc
        ),
        "layer_name": layer_name,
        "dataset_name": dataset_name,
        "resource_key": resource_key,
        "check_name": check_name,
        "rule_name": rule_name,
        "rule_category": rule_category,
        "severity": severity,
        "status": status,
        "message": normalize_text(record.get("message")),
        "metric_name": metric_name,
        "metric_value": metric_value,
        "expected_value": serialize_value(
            details_dict.get("expected_value", details_dict.get("expected"))
        ),
        "actual_value": actual_value,
        "checked_at_utc": checked_at_utc,
        "source_file_path": normalize_text(
            details_dict.get("source_file_path", details_dict.get("dataset_path"))
        ),
        "gold_processed_at_utc": processed_at_utc,
    }


def quality_results_schema() -> StructType:
    """Esquema estable para audit_quality_results."""

    return StructType(
        [
            StructField("quality_result_key", StringType(), False),
            StructField("layer_name", StringType(), True),
            StructField("dataset_name", StringType(), True),
            StructField("resource_key", StringType(), True),
            StructField("check_name", StringType(), True),
            StructField("rule_name", StringType(), True),
            StructField("rule_category", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("status", StringType(), True),
            StructField("message", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_value", DoubleType(), True),
            StructField("expected_value", StringType(), True),
            StructField("actual_value", StringType(), True),
            StructField("checked_at_utc", StringType(), True),
            StructField("source_file_path", StringType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )


def build_audit_quality_results(
    spark: Any,
    *,
    bronze_results: list[dict[str, Any]] | None = None,
    silver_results: list[dict[str, Any]] | None = None,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye audit_quality_results desde resultados Bronze/Silver existentes."""

    processed_at = processed_at_utc or utc_now_iso()
    normalized_records: list[dict[str, Any]] = []

    for record in bronze_results or []:
        normalized_records.append(
            normalize_bronze_quality_record(record, processed_at_utc=processed_at)
        )
    for record in silver_results or []:
        normalized_records.append(
            normalize_silver_quality_record(record, processed_at_utc=processed_at)
        )

    if not normalized_records:
        raise GoldAuditError(
            "No hay resultados de calidad para construir audit_quality_results."
        )

    return spark.createDataFrame(normalized_records, schema=quality_results_schema()).select(
        *QUALITY_RESULT_COLUMNS
    )


def build_audit_dataset_summary(
    audit_quality_results: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Agrega resultados de calidad por capa, dataset y recurso."""

    require_columns(audit_quality_results, QUALITY_RESULT_COLUMNS, "audit_quality_results")
    processed_at = processed_at_utc or utc_now_iso()

    base_group = audit_quality_results.groupBy("layer_name", "dataset_name", "resource_key")

    def category_score_expression(category_name: str) -> Any:
        total_category = F.sum(
            F.when(F.col("rule_category") == F.lit(category_name), F.lit(1)).otherwise(F.lit(0))
        )
        passed_category = F.sum(
            F.when(
                (F.col("rule_category") == F.lit(category_name))
                & (F.col("status") == F.lit("PASS")),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        return (
            F.when(total_category > 0, passed_category.cast("double") / total_category.cast("double"))
            .otherwise(F.lit(None).cast("double"))
            .alias(f"{category_name}_score")
        )

    summary = base_group.agg(
        F.count("*").cast("int").alias("total_checks"),
        F.sum(F.when(F.col("status") == F.lit("PASS"), F.lit(1)).otherwise(F.lit(0))).cast(
            "int"
        ).alias("pass_count"),
        F.sum(
            F.when(F.col("status") == F.lit("WARNING"), F.lit(1)).otherwise(F.lit(0))
        ).cast("int").alias("warning_count"),
        F.sum(F.when(F.col("status") == F.lit("FAIL"), F.lit(1)).otherwise(F.lit(0))).cast(
            "int"
        ).alias("fail_count"),
        F.sum(F.when(F.col("status") == F.lit("ERROR"), F.lit(1)).otherwise(F.lit(0))).cast(
            "int"
        ).alias("error_count"),
        category_score_expression("completeness"),
        category_score_expression("validity"),
        category_score_expression("conformity"),
        F.max(F.when(F.col("metric_name") == F.lit("row_count"), F.col("metric_value"))).alias(
            "row_count"
        ),
        F.max(
            F.when(F.col("metric_name") == F.lit("null_percentage"), F.col("metric_value"))
        ).alias("null_percentage"),
        F.max(
            F.when(F.col("metric_name") == F.lit("duplicate_rows"), F.col("metric_value"))
        ).alias("duplicate_rows"),
        F.max("checked_at_utc").alias("last_checked_at_utc"),
    )

    return (
        summary.withColumn(
            "quality_score",
            F.when(
                F.col("total_checks") > 0,
                F.col("pass_count").cast("double") / F.col("total_checks").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "dataset_summary_key",
            F.sha1(F.concat_ws("|", "layer_name", "dataset_name", "resource_key")),
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(*DATASET_SUMMARY_COLUMNS)
    )


def build_audit_integration_coverage(
    integration_coverage: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Curta integration_coverage para dashboard tecnico Gold separado."""

    require_columns(
        integration_coverage, INTEGRATION_COVERAGE_REQUIRED_COLUMNS, "integration_coverage"
    )
    processed_at = processed_at_utc or utc_now_iso()

    return (
        integration_coverage.select(
            F.col("coverage_scope").cast("string").alias("coverage_scope"),
            F.col("source_name").cast("string").alias("source_name"),
            F.col("metric_name").cast("string").alias("metric_name"),
            F.col("metric_value").cast("double").alias("metric_value"),
            F.col("total_records").cast("double").alias("total_records"),
            F.col("matched_records").cast("double").alias("matched_records"),
            F.col("unmatched_records").cast("double").alias("unmatched_records"),
            F.col("match_rate").cast("double").alias("match_rate"),
            F.col("issue_count").cast("double").alias("issue_count"),
            F.col("issue_rate").cast("double").alias("issue_rate"),
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(*INTEGRATION_COVERAGE_COLUMNS)
    )


def read_parquet_dataset(spark: Any, path: Path, limit: int | None = None) -> DataFrame:
    """Lee Parquet con limite opcional para pruebas locales."""

    dataframe = spark.read.parquet(str(path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def required_input_paths(paths: GoldAuditPaths, datasets: list[str]) -> list[Path]:
    """Devuelve entradas minimas requeridas por los datasets seleccionados."""

    required: list[Path] = []
    if "audit_quality_results" in datasets or "audit_dataset_summary" in datasets:
        required.extend([paths.bronze_quality_results_path, paths.silver_quality_results_path])
    if "audit_integration_coverage" in datasets:
        required.append(paths.integration_coverage_path)
    return required


def validate_input_paths(paths: GoldAuditPaths, datasets: list[str]) -> None:
    """Valida disponibilidad minima de entradas para auditoria Gold."""

    if "audit_quality_results" in datasets or "audit_dataset_summary" in datasets:
        quality_paths = [
            path
            for path in [paths.bronze_quality_results_path, paths.silver_quality_results_path]
            if path.exists()
        ]
        if not quality_paths:
            raise FileNotFoundError(
                "No existen archivos de resultados de calidad Bronze/Silver en data/quality."
            )

    if "audit_integration_coverage" in datasets and not paths.integration_coverage_path.exists():
        raise FileNotFoundError(
            f"No existe integration_coverage en {paths.integration_coverage_path}."
        )


def write_dataset(dataframe: DataFrame, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset Gold de auditoria evitando sobrescritura accidental."""

    if output_path.exists():
        if not overwrite:
            raise GoldAuditError(
                f"La salida ya existe: {output_path}. Use --overwrite para reemplazarla."
            )
        shutil.rmtree(output_path)

    dataframe.write.mode("overwrite").parquet(str(output_path))


def build_gold_audit_marts(
    *,
    paths: GoldAuditPaths | None = None,
    selected_datasets: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Construye fisicamente marts Gold de auditoria y monitoreo."""

    resolved_paths = paths or default_paths()
    datasets = validate_selected_datasets(selected_datasets)
    validate_input_paths(resolved_paths, datasets)

    spark = build_spark_session(app_name="gold-audit-quality-marts")
    outputs: dict[str, DataFrame] = {}
    try:
        if "audit_quality_results" in datasets or "audit_dataset_summary" in datasets:
            bronze_results = (
                read_jsonl_records(resolved_paths.bronze_quality_results_path, limit)
                if resolved_paths.bronze_quality_results_path.exists()
                else []
            )
            silver_results = (
                read_jsonl_records(resolved_paths.silver_quality_results_path, limit)
                if resolved_paths.silver_quality_results_path.exists()
                else []
            )
            outputs["audit_quality_results"] = build_audit_quality_results(
                spark,
                bronze_results=bronze_results,
                silver_results=silver_results,
            )
            if "audit_dataset_summary" in datasets:
                outputs["audit_dataset_summary"] = build_audit_dataset_summary(
                    outputs["audit_quality_results"]
                )

        if "audit_integration_coverage" in datasets:
            coverage = read_parquet_dataset(spark, resolved_paths.integration_coverage_path, limit)
            outputs["audit_integration_coverage"] = build_audit_integration_coverage(coverage)

        if "audit_quality_results" not in datasets and "audit_quality_results" in outputs:
            outputs.pop("audit_quality_results")

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
    """Parsea argumentos CLI del builder Gold de auditoria."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=GOLD_AUDIT_DATASETS,
        help="Dataset Gold de auditoria a construir. Puede repetirse.",
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
        help="Limite opcional por archivo de entrada para pruebas locales.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    row_counts = build_gold_audit_marts(
        selected_datasets=args.dataset,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    for dataset, row_count in row_counts.items():
        print(f"{dataset}: {row_count} filas")


if __name__ == "__main__":
    main()
