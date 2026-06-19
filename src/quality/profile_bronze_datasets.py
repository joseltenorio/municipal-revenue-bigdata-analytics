"""Profiling local de datasets Bronze para rediseño Silver/Gold.

Este módulo no limpia datos ni modifica Bronze. Lee datasets Parquet de Bronze,
calcula perfiles de columnas, valores nulos/vacíos, tipos candidatos, calidad de
texto y llaves candidatas para ayudar a decidir qué columnas conservar,
normalizar o descartar en Silver.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType

from src.common.config import get_config_value, load_sources_config
from src.common.logger import get_logger
from src.common.paths import BRONZE_DIR, QUALITY_DIR, REPORTS_DIR, get_source_bronze_path
from src.common.spark_session import build_spark_session

logger = get_logger(__name__)

NULL_LIKE_STRINGS = {
    "", " ", "null", "none", "nan", "na", "n/a", "s/d", "sd", "sin dato",
    "no aplica", "no corresponde", "-", "--", ".", "s/n",
}
BOOLEAN_LIKE_STRINGS = {"0", "1", "s", "n", "si", "sí", "no", "true", "false"}
TEXT_QUALITY_COLUMNS_HINTS = {
    "nombre", "departamento", "provincia", "distrito", "municipalidad",
    "ejecutora", "descripcion", "titulo", "categoria", "correo", "email", "url",
}

DATASET_SUMMARY_FIELDS = [
    "profile_run_id", "profiled_at_utc", "source_name", "resource_key", "dataset_path",
    "exists", "readable", "parquet_file_count", "row_count", "column_count",
    "exact_duplicate_rows", "error",
]
COLUMN_PROFILE_FIELDS = [
    "profile_run_id", "source_name", "resource_key", "column_name", "spark_type",
    "row_count", "non_null_count", "null_count", "null_percentage",
    "empty_string_count", "empty_string_percentage", "whitespace_only_count",
    "whitespace_only_percentage", "null_like_string_count", "null_like_string_percentage",
    "distinct_count_approx", "distinct_percentage_approx", "min_length", "max_length",
    "avg_length", "sample_values", "top_values",
]
TYPE_CANDIDATE_FIELDS = [
    "profile_run_id", "source_name", "resource_key", "column_name", "candidate_type",
    "confidence", "valid_count", "invalid_count", "details",
]
CANDIDATE_KEY_FIELDS = [
    "profile_run_id", "source_name", "resource_key", "candidate_key", "columns_present",
    "row_count", "complete_key_rows", "unique_key_count", "duplicate_key_rows",
    "uniqueness_percentage", "is_candidate_key",
]
TEXT_QUALITY_FIELDS = [
    "profile_run_id", "source_name", "resource_key", "column_name", "row_count",
    "non_empty_count", "leading_trailing_space_count", "repeated_space_count",
    "tab_or_newline_count", "control_character_count", "accented_character_count",
    "non_ascii_count", "mojibake_like_count", "email_like_count", "url_like_count",
    "normalized_distinct_count", "alias_group_count", "sample_alias_groups",
]
JOIN_KEY_FIELDS = [
    "profile_run_id", "source_name", "resource_key", "key_column", "row_count",
    "null_or_empty_count", "valid_format_count", "invalid_format_count",
    "distinct_count_approx", "sample_invalid_values",
]


@dataclass(frozen=True)
class BronzeResource:
    source_name: str
    resource_key: str
    dataset_path: Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_profile_run_id() -> str:
    return "profile_bronze_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def normalize_null_like_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def is_null_like_literal(value: Any) -> bool:
    return normalize_null_like_text(value) in NULL_LIKE_STRINGS


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("ascii", "ignore").decode("ascii")


def normalize_for_matching(value: Any) -> str:
    text = "" if value is None else str(value)
    text = html_unescape_safe(text)
    text = strip_accents(text).upper().strip()
    replacements = {
        "MUNICIPALIDAD DISTRITAL DE ": "",
        "MUNICIPALIDAD PROVINCIAL DE ": "",
        "MUNICIPALIDAD DISTRITAL ": "",
        "MUNICIPALIDAD PROVINCIAL ": "",
        "M. D. DE ": "",
        "M.D. DE ": "",
        "M D DE ": "",
        "M. P. DE ": "",
        "M.P. DE ": "",
        "M P DE ": "",
        "STGO.": "SANTIAGO",
        "STGO ": "SANTIAGO ",
        "STO.": "SANTO",
        "STO ": "SANTO ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def html_unescape_safe(value: str) -> str:
    # Evita dependencia directa en html para pruebas mínimas y mantiene comportamiento estable.
    return value.replace("&amp;", "&").replace("&#34;", '"').replace("&quot;", '"')


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            safe_row = {field: row.get(field, "") for field in fieldnames}
            writer.writerow(safe_row)


def load_expected_bronze_resources() -> list[BronzeResource]:
    config = load_sources_config()
    resources: list[BronzeResource] = []

    for source_name, source_config in sorted(config.get("sources", {}).items()):
        if source_config.get("enabled", True) is False:
            continue

        source_root = get_source_bronze_path(source_name)
        candidate_resources = source_config.get("candidate_resources", {})
        dataset_layout = str(source_config.get("bronze_dataset_layout") or "").lower()

        if dataset_layout == "direct":
            resources.append(
                BronzeResource(
                    source_name=source_name,
                    resource_key=str(source_config.get("name") or source_name),
                    dataset_path=source_root,
                )
            )
            continue

        if candidate_resources:
            for resource_key in sorted(candidate_resources):
                resources.append(
                    BronzeResource(
                        source_name=source_name,
                        resource_key=resource_key,
                        dataset_path=source_root / f"resource_key={resource_key}",
                    )
                )
            continue

        # Fuente manual simple o fuente sin candidate_resources explícito.
        default_resource = get_config_value(source_config, "resource_key") or source_config.get("name") or source_name
        resources.append(
            BronzeResource(
                source_name=source_name,
                resource_key=str(default_resource),
                dataset_path=source_root / f"resource_key={default_resource}",
            )
        )

    return resources


def list_existing_bronze_resources() -> list[BronzeResource]:
    resources: list[BronzeResource] = []
    if not BRONZE_DIR.exists():
        return resources

    for source_dir in sorted(path for path in BRONZE_DIR.iterdir() if path.is_dir()):
        if parquet_file_count(source_dir) > 0:
            resources.append(
                BronzeResource(
                    source_name=source_dir.name,
                    resource_key=source_dir.name,
                    dataset_path=source_dir,
                )
            )
            continue
        for resource_dir in sorted(source_dir.glob("resource_key=*")):
            if resource_dir.is_dir():
                resources.append(
                    BronzeResource(
                        source_name=source_dir.name,
                        resource_key=resource_dir.name.removeprefix("resource_key="),
                        dataset_path=resource_dir,
                    )
                )
    return resources


def parquet_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(list(path.rglob("*.parquet")))


def safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def column_as_text(column_name: str) -> F.Column:
    return F.col(column_name).cast("string")


def profile_column(df: DataFrame, profile_run_id: str, source_name: str, resource_key: str, column_name: str, row_count: int) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    col_text = column_as_text(column_name)
    trimmed = F.trim(col_text)
    lower_trimmed = F.lower(trimmed)
    null_like_values = list(NULL_LIKE_STRINGS)

    agg = df.agg(
        F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias("null_count"),
        F.sum(F.when((F.col(column_name).isNotNull()) & (trimmed == ""), 1).otherwise(0)).alias("empty_string_count"),
        F.sum(F.when((F.col(column_name).isNotNull()) & (col_text.rlike(r"^\s+$")), 1).otherwise(0)).alias("whitespace_only_count"),
        F.sum(F.when(lower_trimmed.isin(null_like_values), 1).otherwise(0)).alias("null_like_string_count"),
        F.approx_count_distinct(F.col(column_name)).alias("distinct_count_approx"),
        F.min(F.length(col_text)).alias("min_length"),
        F.max(F.length(col_text)).alias("max_length"),
        F.avg(F.length(col_text)).alias("avg_length"),
    ).collect()[0].asDict()

    null_count = int(agg.get("null_count") or 0)
    empty_count = int(agg.get("empty_string_count") or 0)
    whitespace_count = int(agg.get("whitespace_only_count") or 0)
    null_like_count = int(agg.get("null_like_string_count") or 0)
    distinct_count = int(agg.get("distinct_count_approx") or 0)
    non_null_count = row_count - null_count

    sample_values = [
        row[0]
        for row in df.select(F.col(column_name))
        .where(F.col(column_name).isNotNull() & (trimmed != ""))
        .dropDuplicates()
        .limit(8)
        .collect()
    ]
    top_values = [
        {"value": row[0], "count": row[1]}
        for row in df.groupBy(F.col(column_name)).count().orderBy(F.desc("count")).limit(8).collect()
    ]

    column_profile = {
        "profile_run_id": profile_run_id,
        "source_name": source_name,
        "resource_key": resource_key,
        "column_name": column_name,
        "spark_type": df.schema[column_name].dataType.simpleString(),
        "row_count": row_count,
        "non_null_count": non_null_count,
        "null_count": null_count,
        "null_percentage": round((null_count / row_count) * 100, 4) if row_count else 0,
        "empty_string_count": empty_count,
        "empty_string_percentage": round((empty_count / row_count) * 100, 4) if row_count else 0,
        "whitespace_only_count": whitespace_count,
        "whitespace_only_percentage": round((whitespace_count / row_count) * 100, 4) if row_count else 0,
        "null_like_string_count": null_like_count,
        "null_like_string_percentage": round((null_like_count / row_count) * 100, 4) if row_count else 0,
        "distinct_count_approx": distinct_count,
        "distinct_percentage_approx": round((distinct_count / row_count) * 100, 4) if row_count else 0,
        "min_length": agg.get("min_length"),
        "max_length": agg.get("max_length"),
        "avg_length": round(float(agg.get("avg_length") or 0), 4),
        "sample_values": safe_json(sample_values),
        "top_values": safe_json(top_values),
    }

    type_candidate = infer_type_candidate(df, profile_run_id, source_name, resource_key, column_name, row_count, non_null_count)
    text_quality = profile_text_column(df, profile_run_id, source_name, resource_key, column_name, row_count)

    return column_profile, type_candidate, text_quality


def infer_type_candidate(df: DataFrame, profile_run_id: str, source_name: str, resource_key: str, column_name: str, row_count: int, non_null_count: int) -> dict[str, Any]:
    data_type = df.schema[column_name].dataType
    if isinstance(data_type, NumericType):
        return {
            "profile_run_id": profile_run_id,
            "source_name": source_name,
            "resource_key": resource_key,
            "column_name": column_name,
            "candidate_type": "numeric",
            "confidence": 1.0,
            "valid_count": non_null_count,
            "invalid_count": 0,
            "details": "Spark numeric type",
        }

    text_col = F.trim(F.col(column_name).cast("string"))
    lower_col = F.lower(text_col)
    decimal_pattern = r"^[+-]?(\d+)?([\.,]\d+)?$"
    integer_pattern = r"^[+-]?\d+$"
    date_like_name = any(token in column_name.lower() for token in ["fecha", "date"])

    agg = df.where(F.col(column_name).isNotNull() & (text_col != "")).agg(
        F.count("*").alias("valid_base"),
        F.sum(F.when(text_col.rlike(integer_pattern), 1).otherwise(0)).alias("integer_count"),
        F.sum(F.when(text_col.rlike(decimal_pattern), 1).otherwise(0)).alias("decimal_count"),
        F.sum(F.when(lower_col.isin(list(BOOLEAN_LIKE_STRINGS)), 1).otherwise(0)).alias("boolean_count"),
        F.sum(F.when(text_col.rlike(r"^[0-9]{6}$"), 1).otherwise(0)).alias("ubigeo6_count"),
        F.sum(F.when(text_col.rlike(r"^[0-9]{5,6}$"), 1).otherwise(0)).alias("code_count"),
    ).collect()[0].asDict()

    base = int(agg.get("valid_base") or 0)
    if base == 0:
        return {
            "profile_run_id": profile_run_id,
            "source_name": source_name,
            "resource_key": resource_key,
            "column_name": column_name,
            "candidate_type": "empty_or_null",
            "confidence": 1.0 if row_count else 0,
            "valid_count": 0,
            "invalid_count": row_count,
            "details": "Sin valores no vacíos",
        }

    candidates = [
        ("integer", int(agg.get("integer_count") or 0)),
        ("decimal", int(agg.get("decimal_count") or 0)),
        ("boolean_code", int(agg.get("boolean_count") or 0)),
        ("ubigeo6", int(agg.get("ubigeo6_count") or 0)),
        ("identifier", int(agg.get("code_count") or 0)),
    ]
    candidate_type, valid_count = max(candidates, key=lambda item: item[1])
    confidence = valid_count / base if base else 0

    if date_like_name:
        candidate_type = "date_or_timestamp_text"
        valid_count = base
        confidence = 0.5
    elif confidence < 0.8:
        distinct_count = df.select(column_name).where(F.col(column_name).isNotNull()).distinct().limit(1001).count()
        candidate_type = "categorical" if distinct_count <= 100 else "free_text"
        valid_count = base
        confidence = 1.0

    return {
        "profile_run_id": profile_run_id,
        "source_name": source_name,
        "resource_key": resource_key,
        "column_name": column_name,
        "candidate_type": candidate_type,
        "confidence": round(confidence, 4),
        "valid_count": valid_count,
        "invalid_count": base - valid_count if confidence < 1 else 0,
        "details": safe_json(agg),
    }


def should_profile_text_column(column_name: str, data_type: Any) -> bool:
    if not isinstance(data_type, StringType):
        return False
    lowered = column_name.lower()
    return any(token in lowered for token in TEXT_QUALITY_COLUMNS_HINTS)


def profile_text_column(df: DataFrame, profile_run_id: str, source_name: str, resource_key: str, column_name: str, row_count: int) -> dict[str, Any] | None:
    if not should_profile_text_column(column_name, df.schema[column_name].dataType):
        return None

    text_col = F.col(column_name).cast("string")
    trimmed = F.trim(text_col)
    agg = df.agg(
        F.sum(F.when(F.col(column_name).isNotNull() & (trimmed != ""), 1).otherwise(0)).alias("non_empty_count"),
        F.sum(F.when(F.col(column_name).isNotNull() & (text_col != trimmed), 1).otherwise(0)).alias("leading_trailing_space_count"),
        F.sum(F.when(text_col.rlike(r" {2,}"), 1).otherwise(0)).alias("repeated_space_count"),
        F.sum(F.when(text_col.rlike(r"[\t\n\r]"), 1).otherwise(0)).alias("tab_or_newline_count"),
        F.sum(F.when(text_col.rlike(r"[\x00-\x1F\x7F]"), 1).otherwise(0)).alias("control_character_count"),
        F.sum(F.when(text_col.rlike(r"[ÁÉÍÓÚÜÑáéíóúüñ]"), 1).otherwise(0)).alias("accented_character_count"),
        F.sum(F.when(text_col.rlike(r"[^\x00-\x7F]"), 1).otherwise(0)).alias("non_ascii_count"),
        F.sum(F.when(text_col.rlike(r"Ã|Â|�"), 1).otherwise(0)).alias("mojibake_like_count"),
        F.sum(F.when(text_col.rlike(r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$"), 1).otherwise(0)).alias("email_like_count"),
        F.sum(F.when(text_col.rlike(r"(?i)(https?://|www\.)"), 1).otherwise(0)).alias("url_like_count"),
    ).collect()[0].asDict()

    samples = [row[0] for row in df.select(column_name).where(trimmed != "").dropDuplicates().limit(300).collect()]
    normalized_groups: dict[str, set[str]] = {}
    for value in samples:
        normalized = normalize_for_matching(value)
        if not normalized:
            continue
        normalized_groups.setdefault(normalized, set()).add(str(value))
    alias_groups = {
        key: sorted(values)[:5]
        for key, values in normalized_groups.items()
        if len(values) > 1
    }

    return {
        "profile_run_id": profile_run_id,
        "source_name": source_name,
        "resource_key": resource_key,
        "column_name": column_name,
        "row_count": row_count,
        "non_empty_count": int(agg.get("non_empty_count") or 0),
        "leading_trailing_space_count": int(agg.get("leading_trailing_space_count") or 0),
        "repeated_space_count": int(agg.get("repeated_space_count") or 0),
        "tab_or_newline_count": int(agg.get("tab_or_newline_count") or 0),
        "control_character_count": int(agg.get("control_character_count") or 0),
        "accented_character_count": int(agg.get("accented_character_count") or 0),
        "non_ascii_count": int(agg.get("non_ascii_count") or 0),
        "mojibake_like_count": int(agg.get("mojibake_like_count") or 0),
        "email_like_count": int(agg.get("email_like_count") or 0),
        "url_like_count": int(agg.get("url_like_count") or 0),
        "normalized_distinct_count": len(normalized_groups),
        "alias_group_count": len(alias_groups),
        "sample_alias_groups": safe_json(dict(list(alias_groups.items())[:10])),
    }


def candidate_keys_for_resource(source_name: str, resource_key: str) -> list[list[str]]:
    source = source_name.lower()
    resource = resource_key.lower()
    if source == "siaf_income":
        return [
            ["sec_ejec"],
            ["ano_doc", "mes_doc", "sec_ejec"],
            ["ano_doc", "mes_doc", "sec_ejec", "fuente_financiamiento", "rubro", "generica", "subgenerica", "especifica", "especifica_det"],
        ]
    if source == "sismepre":
        if "estadistica_atm" in resource or resource == "estadistica":
            return [
                ["sec_ejec"],
                ["ubigeo"],
                ["ano_aplicacion", "periodo", "sec_ejec"],
                ["ano_aplicacion", "periodo", "sec_ejec", "formulario_id", "ano_estadistica", "mes_estadistica"],
            ]
        if "respuesta" in resource:
            return [["sec_ejec", "ano_aplicacion", "periodo", "formulario_id", "pregunta_id", "respuesta_id"]]
        if "pregunta" in resource:
            return [["ano_aplicacion", "periodo", "formulario_id", "pregunta_id"]]
        if "formulario" in resource:
            return [["ano_aplicacion", "periodo", "formulario_id"]]
        return [["sec_ejec"], ["ano_aplicacion", "periodo"]]
    if source == "renamu":
        return [["ubigeo"], ["idmunici"], ["anio", "ubigeo"], ["ano", "ubigeo"], ["año", "ubigeo"]]
    if source == "municipal_classification":
        return [["ubigeo"], ["anio", "ubigeo"], ["tipo_clasificacion", "ubigeo"]]
    return []


def profile_candidate_keys(df: DataFrame, profile_run_id: str, source_name: str, resource_key: str, row_count: int) -> list[dict[str, Any]]:
    rows = []
    df_columns = set(df.columns)
    for key_columns in candidate_keys_for_resource(source_name, resource_key):
        present = all(column in df_columns for column in key_columns)
        result = {
            "profile_run_id": profile_run_id,
            "source_name": source_name,
            "resource_key": resource_key,
            "candidate_key": ",".join(key_columns),
            "columns_present": present,
            "row_count": row_count,
            "complete_key_rows": 0,
            "unique_key_count": 0,
            "duplicate_key_rows": 0,
            "uniqueness_percentage": 0,
            "is_candidate_key": False,
        }
        if not present:
            rows.append(result)
            continue

        condition = None
        for column in key_columns:
            current = F.col(column).isNotNull() & (F.trim(F.col(column).cast("string")) != "")
            condition = current if condition is None else condition & current
        complete_df = df.select(*key_columns).where(condition)
        complete_rows = complete_df.count()
        unique_rows = complete_df.dropDuplicates(key_columns).count()
        duplicate_rows = complete_rows - unique_rows
        uniqueness = round((unique_rows / complete_rows) * 100, 4) if complete_rows else 0

        result.update(
            {
                "complete_key_rows": complete_rows,
                "unique_key_count": unique_rows,
                "duplicate_key_rows": duplicate_rows,
                "uniqueness_percentage": uniqueness,
                "is_candidate_key": complete_rows > 0 and duplicate_rows == 0,
            }
        )
        rows.append(result)
    return rows


def profile_join_keys(df: DataFrame, profile_run_id: str, source_name: str, resource_key: str, row_count: int) -> list[dict[str, Any]]:
    formats = {
        "sec_ejec": r"^[0-9]{5,6}$",
        "ubigeo": r"^[0-9]{6}$",
        "ubigeo6": r"^[0-9]{6}$",
        "idmunici": r"^[0-9]{6}$",
        "ccdd": r"^[0-9]{2}$",
        "ccpp": r"^[0-9]{2}$",
        "ccdi": r"^[0-9]{2}$",
    }
    rows = []
    for key_column, pattern in formats.items():
        if key_column not in df.columns:
            continue
        text_col = F.trim(F.col(key_column).cast("string"))
        agg = df.agg(
            F.sum(F.when(F.col(key_column).isNull() | (text_col == ""), 1).otherwise(0)).alias("null_or_empty_count"),
            F.sum(F.when(text_col.rlike(pattern), 1).otherwise(0)).alias("valid_format_count"),
            F.sum(F.when((F.col(key_column).isNotNull()) & (text_col != "") & (~text_col.rlike(pattern)), 1).otherwise(0)).alias("invalid_format_count"),
            F.approx_count_distinct(F.col(key_column)).alias("distinct_count_approx"),
        ).collect()[0].asDict()
        invalid_samples = [
            r[0]
            for r in df.select(key_column)
            .where((F.col(key_column).isNotNull()) & (text_col != "") & (~text_col.rlike(pattern)))
            .dropDuplicates()
            .limit(10)
            .collect()
        ]
        rows.append(
            {
                "profile_run_id": profile_run_id,
                "source_name": source_name,
                "resource_key": resource_key,
                "key_column": key_column,
                "row_count": row_count,
                "null_or_empty_count": int(agg.get("null_or_empty_count") or 0),
                "valid_format_count": int(agg.get("valid_format_count") or 0),
                "invalid_format_count": int(agg.get("invalid_format_count") or 0),
                "distinct_count_approx": int(agg.get("distinct_count_approx") or 0),
                "sample_invalid_values": safe_json(invalid_samples),
            }
        )
    return rows


def profile_resource(spark: SparkSession, resource: BronzeResource, profile_run_id: str, profiled_at_utc: str, max_columns: int | None = None) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    summary = {
        "profile_run_id": profile_run_id,
        "profiled_at_utc": profiled_at_utc,
        "source_name": resource.source_name,
        "resource_key": resource.resource_key,
        "dataset_path": str(resource.dataset_path),
        "exists": resource.dataset_path.exists(),
        "readable": False,
        "parquet_file_count": parquet_file_count(resource.dataset_path),
        "row_count": 0,
        "column_count": 0,
        "exact_duplicate_rows": 0,
        "error": "",
    }

    if not resource.dataset_path.exists():
        summary["error"] = "Dataset path not found"
        return summary, [], [], [], [], []

    try:
        df = spark.read.parquet(str(resource.dataset_path))
        row_count = df.count()
        column_count = len(df.columns)
        exact_unique = df.dropDuplicates().count() if row_count else 0
        summary.update(
            {
                "readable": True,
                "row_count": row_count,
                "column_count": column_count,
                "exact_duplicate_rows": row_count - exact_unique,
            }
        )

        columns_to_profile = df.columns[:max_columns] if max_columns else df.columns
        column_rows = []
        type_rows = []
        text_rows = []
        for column_name in columns_to_profile:
            column_profile, type_candidate, text_quality = profile_column(
                df, profile_run_id, resource.source_name, resource.resource_key, column_name, row_count
            )
            column_rows.append(column_profile)
            type_rows.append(type_candidate)
            if text_quality:
                text_rows.append(text_quality)

        key_rows = profile_candidate_keys(df, profile_run_id, resource.source_name, resource.resource_key, row_count)
        join_key_rows = profile_join_keys(df, profile_run_id, resource.source_name, resource.resource_key, row_count)
        return summary, column_rows, type_rows, key_rows, text_rows, join_key_rows
    except Exception as exc:  # noqa: BLE001
        summary["error"] = str(exc)
        return summary, [], [], [], [], []


def generate_html_report(output_dir: Path, report_path: Path, summaries: list[dict[str, Any]]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    ok_count = sum(1 for item in summaries if item.get("readable"))
    html = [
        "<!doctype html>",
        "<html lang='es'><head><meta charset='utf-8'><title>Bronze Profiling Report</title>",
        "<style>body{font-family:Arial,sans-serif;margin:32px}table{border-collapse:collapse;width:100%;margin:16px 0}th,td{border:1px solid #ddd;padding:6px;font-size:13px}th{background:#f3f3f3}.bad{color:#b00020}.ok{color:#0a7a0a}</style>",
        "</head><body>",
        "<h1>Bronze Profiling Report</h1>",
        f"<p>Datasets legibles: <strong>{ok_count}</strong> / {len(summaries)}</p>",
        "<h2>Resumen de datasets</h2><table><tr><th>Fuente</th><th>Recurso</th><th>Filas</th><th>Columnas</th><th>Parquet</th><th>Estado</th><th>Error</th></tr>",
    ]
    for item in summaries:
        status = "OK" if item.get("readable") else "ERROR"
        cls = "ok" if item.get("readable") else "bad"
        html.append(
            "<tr>"
            f"<td>{item.get('source_name')}</td>"
            f"<td>{item.get('resource_key')}</td>"
            f"<td>{item.get('row_count')}</td>"
            f"<td>{item.get('column_count')}</td>"
            f"<td>{item.get('parquet_file_count')}</td>"
            f"<td class='{cls}'>{status}</td>"
            f"<td>{item.get('error') or ''}</td>"
            "</tr>"
        )
    html.extend(
        [
            "</table>",
            "<h2>Archivos generados</h2>",
            "<ul>",
            "<li>bronze_dataset_summary.csv</li>",
            "<li>bronze_column_profile.csv</li>",
            "<li>bronze_type_candidates.csv</li>",
            "<li>bronze_candidate_keys.csv</li>",
            "<li>bronze_text_quality.csv</li>",
            "<li>bronze_join_key_profile.csv</li>",
            "</ul>",
            f"<p>Directorio: <code>{output_dir}</code></p>",
            "</body></html>",
        ]
    )
    report_path.write_text("\n".join(html), encoding="utf-8")


def run_profile(max_columns: int | None = None, existing_only: bool = False) -> int:
    profile_run_id = create_profile_run_id()
    profiled_at_utc = utc_now_iso()
    output_dir = QUALITY_DIR / "profiling"
    output_dir.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session(app_name="BronzeProfiling")
    resources = list_existing_bronze_resources() if existing_only else load_expected_bronze_resources()

    summaries: list[dict[str, Any]] = []
    column_rows: list[dict[str, Any]] = []
    type_rows: list[dict[str, Any]] = []
    key_rows: list[dict[str, Any]] = []
    text_rows: list[dict[str, Any]] = []
    join_key_rows: list[dict[str, Any]] = []

    for resource in resources:
        logger.info("Perfilando Bronze: %s/%s", resource.source_name, resource.resource_key)
        summary, columns, types, keys, texts, join_keys = profile_resource(
            spark, resource, profile_run_id, profiled_at_utc, max_columns=max_columns
        )
        summaries.append(summary)
        column_rows.extend(columns)
        type_rows.extend(types)
        key_rows.extend(keys)
        text_rows.extend(texts)
        join_key_rows.extend(join_keys)

    write_csv(output_dir / "bronze_dataset_summary.csv", DATASET_SUMMARY_FIELDS, summaries)
    write_csv(output_dir / "bronze_column_profile.csv", COLUMN_PROFILE_FIELDS, column_rows)
    write_csv(output_dir / "bronze_type_candidates.csv", TYPE_CANDIDATE_FIELDS, type_rows)
    write_csv(output_dir / "bronze_candidate_keys.csv", CANDIDATE_KEY_FIELDS, key_rows)
    write_csv(output_dir / "bronze_text_quality.csv", TEXT_QUALITY_FIELDS, text_rows)
    write_csv(output_dir / "bronze_join_key_profile.csv", JOIN_KEY_FIELDS, join_key_rows)
    generate_html_report(output_dir, REPORTS_DIR / "bronze_profile_report.html", summaries)

    spark.stop()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Perfilado local de datasets Bronze Parquet.")
    parser.add_argument("--max-columns", type=int, default=0, help="Máximo de columnas por dataset. 0 perfila todas.")
    parser.add_argument("--existing-only", action="store_true", help="Perfilar solo recursos Bronze existentes, no los esperados por configuración.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_columns = args.max_columns if args.max_columns and args.max_columns > 0 else None
    raise SystemExit(run_profile(max_columns=max_columns, existing_only=args.existing_only))


if __name__ == "__main__":
    main()
