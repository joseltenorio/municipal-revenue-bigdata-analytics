"""Validaciones de calidad sobre datasets Silver Parquet.

Este módulo ejecuta reglas técnicas y de perfilado inicial sobre la capa
Silver. No transforma datos, no modifica Silver y mantiene la calidad Silver
separada del motor Bronze existente.
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pyspark.sql import functions as F

from src.common.config import get_config_value, load_quality_rules_config
from src.common.logger import get_logger
from src.common.paths import PROJECT_ROOT, QUALITY_DIR, get_source_silver_path


VALID_STATUSES = {"PASS", "WARNING", "FAIL"}
DEFAULT_OUTPUT_PATH = QUALITY_DIR / "silver_quality_results.jsonl"
BOOLEAN_TRUE_FALSE = {"true", "false"}


class SilverQualityCheckError(Exception):
    """Error controlado durante la ejecución de calidad Silver."""


@dataclass(frozen=True)
class SilverDataset:
    """Recurso Silver esperado por configuración."""

    source_name: str
    resource_key: str
    dataset_path: Path
    source_config: dict[str, Any]
    resource_config: dict[str, Any]


@dataclass(frozen=True)
class SilverQualityResult:
    """Resultado serializable de una regla Silver."""

    run_id: str
    layer: str
    source_name: str
    resource_key: str
    rule_name: str
    status: str
    severity: str
    evaluated: bool
    message: str
    details: dict[str, Any]
    checked_at_utc: str


def utc_now_iso() -> str:
    """Retorna fecha y hora UTC en formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def create_run_id() -> str:
    """Crea un identificador único para la corrida Silver."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    return f"quality_silver_{timestamp}_{suffix}"


def resolve_project_path(path_value: str | Path) -> Path:
    """Resuelve una ruta absoluta o relativa al proyecto."""

    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def build_silver_quality_result(
    *,
    run_id: str,
    dataset: SilverDataset,
    rule_name: str,
    status: str,
    severity: str,
    evaluated: bool,
    message: str,
    details: dict[str, Any] | None = None,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Construye un resultado validando estados permitidos."""

    if status not in VALID_STATUSES:
        raise SilverQualityCheckError(f"Estado no soportado: {status}")
    if severity not in VALID_STATUSES:
        raise SilverQualityCheckError(f"Severidad no soportada: {severity}")

    return SilverQualityResult(
        run_id=run_id,
        layer="silver",
        source_name=dataset.source_name,
        resource_key=dataset.resource_key,
        rule_name=rule_name,
        status=status,
        severity=severity,
        evaluated=evaluated,
        message=message,
        details=details or {},
        checked_at_utc=checked_at_utc,
    )


def evaluate_required_columns(
    existing_columns: Iterable[str],
    required_columns: Iterable[str],
) -> tuple[bool, list[str]]:
    """Evalúa si todas las columnas requeridas existen."""

    existing = set(existing_columns)
    missing = [column for column in required_columns if column not in existing]
    return len(missing) == 0, missing


def status_from_warning_count(count: int) -> str:
    """Devuelve WARNING cuando se detecta al menos un problema no bloqueante."""

    return "WARNING" if count > 0 else "PASS"


def invalid_boolean_flag_values(values: Iterable[Any]) -> list[Any]:
    """Retorna valores que no representan flags booleanos válidos."""

    invalid_values: list[Any] = []
    for value in values:
        if isinstance(value, bool):
            continue
        if value is None:
            invalid_values.append(value)
            continue
        if str(value).strip().lower() not in BOOLEAN_TRUE_FALSE:
            invalid_values.append(value)
    return invalid_values


def load_silver_quality_config() -> dict[str, Any]:
    """Carga y valida la sección quality.silver."""

    config = load_quality_rules_config()
    quality_config = config.get("quality", {})
    valid_statuses = set(quality_config.get("valid_statuses", []))

    if valid_statuses != VALID_STATUSES:
        raise SilverQualityCheckError(
            "quality.valid_statuses debe contener exactamente PASS, WARNING y FAIL."
        )

    silver_config = quality_config.get("silver", {})
    if not isinstance(silver_config, dict) or not silver_config:
        raise SilverQualityCheckError("quality.silver debe estar configurado.")

    sources = silver_config.get("sources", {})
    if not isinstance(sources, dict) or not sources:
        raise SilverQualityCheckError("quality.silver.sources debe definir fuentes.")

    return quality_config


def normalize_expected_resources(source_config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Normaliza expected_resources a diccionario por resource_key."""

    expected_resources = source_config.get("expected_resources", {})
    if isinstance(expected_resources, list):
        return {str(resource_key): {} for resource_key in expected_resources}
    if isinstance(expected_resources, dict):
        return {
            str(resource_key): resource_config or {}
            for resource_key, resource_config in expected_resources.items()
        }
    raise SilverQualityCheckError("expected_resources debe ser lista o diccionario.")


def build_expected_datasets(
    quality_config: dict[str, Any],
    selected_sources: Iterable[str] | None = None,
    selected_resources: Iterable[str] | None = None,
) -> list[SilverDataset]:
    """Construye recursos Silver esperados desde la configuración."""

    sources_filter = set(selected_sources or [])
    resources_filter = set(selected_resources or [])
    sources_config = get_config_value(quality_config, "silver.sources", {})
    datasets: list[SilverDataset] = []

    for source_name, source_config in sources_config.items():
        if sources_filter and source_name not in sources_filter:
            continue

        source_silver_path = get_source_silver_path(source_name)
        for resource_key, resource_config in normalize_expected_resources(
            source_config
        ).items():
            if resources_filter and resource_key not in resources_filter:
                continue
            datasets.append(
                SilverDataset(
                    source_name=source_name,
                    resource_key=resource_key,
                    dataset_path=source_silver_path / f"resource_key={resource_key}",
                    source_config=source_config,
                    resource_config=resource_config,
                )
            )

    if not datasets:
        raise SilverQualityCheckError(
            "No hay recursos Silver seleccionados para evaluar."
        )

    return datasets


def find_parquet_files(dataset_path: Path) -> list[Path]:
    """Lista archivos Parquet bajo un recurso Silver."""

    if not dataset_path.exists():
        return []
    return sorted(
        file_path
        for file_path in dataset_path.rglob("*.parquet")
        if file_path.is_file()
    )


def count_condition(dataframe: Any, condition: Any) -> int:
    """Cuenta filas que cumplen una condición Spark."""

    return int(dataframe.where(condition).count())


def is_string_type(dataframe: Any, column_name: str) -> bool:
    """Indica si una columna Spark es textual."""

    return dict(dataframe.dtypes).get(column_name, "").lower() == "string"


def blank_or_null_condition(dataframe: Any, column_name: str) -> Any:
    """Construye condición de nulo o texto vacío."""

    column = F.col(column_name)
    if is_string_type(dataframe, column_name):
        return column.isNull() | (F.trim(column) == "")
    return column.isNull()


def nonblank_condition(dataframe: Any, column_name: str) -> Any:
    """Construye condición de valor presente."""

    return ~blank_or_null_condition(dataframe, column_name)


def check_simple_path_rules(
    *, run_id: str, dataset: SilverDataset, checked_at_utc: str
) -> list[SilverQualityResult]:
    """Evalúa existencia de ruta y archivos Parquet."""

    path_exists = dataset.dataset_path.exists()
    parquet_count = len(find_parquet_files(dataset.dataset_path))
    return [
        build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name="dataset_path_exists",
            status="PASS" if path_exists else "FAIL",
            severity="FAIL",
            evaluated=True,
            message=(
                "La ruta del recurso Silver existe."
                if path_exists
                else "No existe la ruta esperada del recurso Silver."
            ),
            details={"dataset_path": str(dataset.dataset_path)},
            checked_at_utc=checked_at_utc,
        ),
        build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name="parquet_files_exist",
            status="PASS" if parquet_count > 0 else "FAIL",
            severity="FAIL",
            evaluated=True,
            message=(
                "Se encontraron archivos Parquet."
                if parquet_count > 0
                else "No se encontraron archivos Parquet."
            ),
            details={"parquet_file_count": parquet_count},
            checked_at_utc=checked_at_utc,
        ),
    ]


def check_dataset_readable(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any | None,
    read_error: str | None,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Valida lectura del recurso Silver con Spark."""

    readable = dataframe is not None and read_error is None
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="dataset_readable",
        status="PASS" if readable else "FAIL",
        severity="FAIL",
        evaluated=True,
        message=(
            "Spark pudo leer el dataset Silver."
            if readable
            else "Spark no pudo leer el dataset Silver."
        ),
        details={"read_error": read_error},
        checked_at_utc=checked_at_utc,
    )


def check_positive_counts(
    *,
    run_id: str,
    dataset: SilverDataset,
    row_count: int,
    column_count: int,
    checked_at_utc: str,
) -> list[SilverQualityResult]:
    """Valida conteos básicos del recurso."""

    results = [
        build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name="row_count_positive",
            status="PASS" if row_count > 0 else "FAIL",
            severity="FAIL",
            evaluated=True,
            message=(
                "El recurso Silver tiene filas."
                if row_count > 0
                else "El recurso Silver no tiene filas."
            ),
            details={"row_count": row_count},
            checked_at_utc=checked_at_utc,
        ),
        build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name="column_count_positive",
            status="PASS" if column_count > 0 else "FAIL",
            severity="FAIL",
            evaluated=True,
            message=(
                "El recurso Silver tiene columnas."
                if column_count > 0
                else "El recurso Silver no tiene columnas."
            ),
            details={"column_count": column_count},
            checked_at_utc=checked_at_utc,
        ),
    ]

    expected_row_count = dataset.resource_config.get("expected_row_count")
    if expected_row_count is not None:
        results.append(
            build_silver_quality_result(
                run_id=run_id,
                dataset=dataset,
                rule_name="expected_row_count",
                status="PASS" if row_count == int(expected_row_count) else "WARNING",
                severity="WARNING",
                evaluated=True,
                message=(
                    "El conteo de filas coincide con la referencia configurada."
                    if row_count == int(expected_row_count)
                    else "El conteo de filas difiere de la referencia configurada."
                ),
                details={
                    "row_count": row_count,
                    "expected_row_count": int(expected_row_count),
                },
                checked_at_utc=checked_at_utc,
            )
        )

    return results


def source_metadata_columns(dataset: SilverDataset, quality_config: dict[str, Any]) -> list[str]:
    """Obtiene metadata Silver común y específica de fuente."""

    common_metadata = get_config_value(
        quality_config,
        "silver.common_metadata_columns",
        [],
    )
    source_metadata = dataset.source_config.get("metadata_columns", [])
    return list(dict.fromkeys([*common_metadata, *source_metadata]))


def check_required_column_rule(
    *,
    run_id: str,
    dataset: SilverDataset,
    columns: list[str],
    required_columns: list[str],
    rule_name: str,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Valida presencia de columnas requeridas."""

    passed, missing = evaluate_required_columns(columns, required_columns)
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name=rule_name,
        status="PASS" if passed else "FAIL",
        severity="FAIL",
        evaluated=True,
        message=(
            "Todas las columnas requeridas están presentes."
            if passed
            else "Faltan columnas requeridas en el recurso Silver."
        ),
        details={"required_columns": required_columns, "missing_columns": missing},
        checked_at_utc=checked_at_utc,
    )


def check_critical_nulls(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Cuenta nulos o vacíos en columnas críticas configuradas."""

    configured_columns = dataset.resource_config.get(
        "critical_null_columns",
        dataset.source_config.get("critical_null_columns", []),
    )
    columns = [column for column in configured_columns if column in dataframe.columns]
    missing = [column for column in configured_columns if column not in dataframe.columns]

    if not configured_columns:
        return build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name="critical_nulls",
            status="PASS",
            severity="WARNING",
            evaluated=True,
            message="No hay columnas críticas configuradas para nulos.",
            details={},
            checked_at_utc=checked_at_utc,
        )

    null_counts = {
        column: count_condition(dataframe, blank_or_null_condition(dataframe, column))
        for column in columns
    }
    total_nulls = sum(null_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="critical_nulls",
        status=status_from_warning_count(total_nulls),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron nulos críticos."
            if total_nulls == 0
            else "Se detectaron nulos o vacíos en columnas críticas."
        ),
        details={
            "null_counts": null_counts,
            "missing_configured_columns": missing,
            "total_nulls": total_nulls,
        },
        checked_at_utc=checked_at_utc,
    )


def check_exact_duplicate_rows(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    row_count: int,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Detecta duplicados exactos."""

    distinct_count = int(dataframe.dropDuplicates().count())
    duplicate_count = row_count - distinct_count
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="exact_duplicate_rows",
        status=status_from_warning_count(duplicate_count),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron duplicados exactos."
            if duplicate_count == 0
            else "Se detectaron duplicados exactos."
        ),
        details={"duplicate_rows": duplicate_count, "distinct_rows": distinct_count},
        checked_at_utc=checked_at_utc,
    )


def candidate_key_columns(dataset: SilverDataset) -> list[str]:
    """Obtiene llave candidata configurada para el recurso."""

    if dataset.source_name == "siaf_income":
        candidate_key = dataset.source_config.get("candidate_key", {})
        return list(candidate_key.get("columns", []))
    return list(dataset.resource_config.get("candidate_key", []))


def check_candidate_key_duplicates(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    row_count: int,
    rule_name: str,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Detecta duplicados por llave candidata preliminar."""

    keys = candidate_key_columns(dataset)
    if not keys:
        return build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name=rule_name,
            status="PASS",
            severity="WARNING",
            evaluated=False,
            message="No hay llave candidata configurada para este recurso.",
            details={},
            checked_at_utc=checked_at_utc,
        )

    passed, missing = evaluate_required_columns(dataframe.columns, keys)
    if not passed:
        return build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name=rule_name,
            status="WARNING",
            severity="WARNING",
            evaluated=False,
            message="La llave candidata no se evaluó porque faltan columnas.",
            details={"candidate_key": keys, "missing_columns": missing},
            checked_at_utc=checked_at_utc,
        )

    distinct_key_count = int(dataframe.dropDuplicates(keys).count())
    duplicate_count = row_count - distinct_key_count
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name=rule_name,
        status=status_from_warning_count(duplicate_count),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron duplicados por llave candidata."
            if duplicate_count == 0
            else "Se detectaron duplicados por llave candidata preliminar."
        ),
        details={"candidate_key": keys, "duplicate_rows": duplicate_count},
        checked_at_utc=checked_at_utc,
    )


def check_invalid_boolean_flags(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    flags: list[str],
    checked_at_utc: str,
    rule_name: str = "invalid_boolean_flags",
) -> SilverQualityResult:
    """Detecta flags nulos o no booleanos."""

    existing_flags = [flag for flag in flags if flag in dataframe.columns]
    dtype_by_column = dict(dataframe.dtypes)
    invalid_counts: dict[str, int] = {}

    for flag in existing_flags:
        dtype = dtype_by_column.get(flag, "").lower()
        if dtype == "boolean":
            invalid_counts[flag] = count_condition(dataframe, F.col(flag).isNull())
        else:
            invalid_counts[flag] = count_condition(
                dataframe,
                F.col(flag).isNull()
                | ~F.lower(F.trim(F.col(flag).cast("string"))).isin(
                    sorted(BOOLEAN_TRUE_FALSE)
                ),
            )

    total_invalid = sum(invalid_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name=rule_name,
        status=status_from_warning_count(total_invalid),
        severity="WARNING",
        evaluated=True,
        message=(
            "Los flags evaluados tienen valores booleanos válidos."
            if total_invalid == 0
            else "Se detectaron flags nulos o con valores no booleanos."
        ),
        details={
            "evaluated_flags": existing_flags,
            "invalid_counts": invalid_counts,
            "total_invalid": total_invalid,
        },
        checked_at_utc=checked_at_utc,
    )


def check_dictionary_references(
    *,
    run_id: str,
    dataset: SilverDataset,
    quality_config: dict[str, Any],
    checked_at_utc: str,
) -> SilverQualityResult:
    """Verifica referencias semánticas locales opcionales."""

    source_refs = get_config_value(
        quality_config,
        f"silver.dictionary_references.{dataset.source_name}",
        {},
    )
    missing: list[str] = []
    found: list[str] = []

    for path_value in source_refs.get("paths", []):
        path = resolve_project_path(path_value)
        if path.exists():
            found.append(str(path))
        else:
            missing.append(str(path))

    for pattern in source_refs.get("glob_patterns", []):
        matches = sorted(PROJECT_ROOT.glob(pattern))
        if matches:
            found.extend(str(path) for path in matches)
        else:
            missing.append(pattern)

    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="dictionary_reference_missing",
        status="WARNING" if missing else "PASS",
        severity="WARNING",
        evaluated=True,
        message=(
            "Las referencias semánticas locales configuradas están disponibles."
            if not missing
            else "Falta al menos una referencia semántica local opcional."
        ),
        details={"found": found, "missing": missing},
        checked_at_utc=checked_at_utc,
    )


def check_negative_amounts(
    *, run_id: str, dataset: SilverDataset, dataframe: Any, checked_at_utc: str
) -> SilverQualityResult:
    """Detecta montos negativos en MEF."""

    amount_columns = [
        column
        for column in dataset.source_config.get("amount_columns", [])
        if column in dataframe.columns
    ]
    negative_counts = {
        column: count_condition(dataframe, F.col(column) < 0)
        for column in amount_columns
    }
    total_negative = sum(negative_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="negative_amounts",
        status=status_from_warning_count(total_negative),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron montos negativos."
            if total_negative == 0
            else "Se detectaron montos negativos en columnas MEF."
        ),
        details={"negative_counts": negative_counts, "total_negative": total_negative},
        checked_at_utc=checked_at_utc,
    )


def check_false_flags(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    flags: list[str],
    rule_name: str,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Cuenta flags técnicos en false."""

    false_counts = {
        flag: count_condition(dataframe, F.col(flag) == F.lit(False))
        for flag in flags
        if flag in dataframe.columns
    }
    total_false = sum(false_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name=rule_name,
        status=status_from_warning_count(total_false),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron flags inválidos."
            if total_false == 0
            else "Se detectaron flags técnicos en false."
        ),
        details={"false_counts": false_counts, "total_false": total_false},
        checked_at_utc=checked_at_utc,
    )


def check_predial_required_relationship_keys(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Detecta nulos en llaves relacionales candidatas de Predial."""

    keys = candidate_key_columns(dataset)
    existing_keys = [key for key in keys if key in dataframe.columns]
    missing_keys = [key for key in keys if key not in dataframe.columns]
    null_counts = {
        key: count_condition(dataframe, blank_or_null_condition(dataframe, key))
        for key in existing_keys
    }
    total_nulls = sum(null_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="predial_required_relationship_keys",
        status=status_from_warning_count(total_nulls + len(missing_keys)),
        severity="WARNING",
        evaluated=True,
        message=(
            "Las llaves relacionales candidatas están completas."
            if total_nulls == 0 and not missing_keys
            else "Se detectaron llaves relacionales candidatas faltantes o nulas."
        ),
        details={
            "candidate_key": keys,
            "missing_columns": missing_keys,
            "null_counts": null_counts,
        },
        checked_at_utc=checked_at_utc,
    )


def parse_failure_count(dataframe: Any, source_column: str, parsed_column: str) -> int:
    """Cuenta valores originales no vacíos que no se parsearon."""

    if source_column not in dataframe.columns or parsed_column not in dataframe.columns:
        return 0
    return count_condition(
        dataframe,
        nonblank_condition(dataframe, source_column) & F.col(parsed_column).isNull(),
    )


def check_predial_parse_failures(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Detecta fallas de parseo en auxiliares tipados Predial."""

    parse_pairs: dict[str, str] = {}
    known_pairs = {
        "respuesta_decimal": "respuesta_decimal_value",
        "respuesta_entero": "respuesta_entero_value",
        "respuesta_fecha": "respuesta_fecha_value",
    }
    for original, parsed in known_pairs.items():
        if parsed in dataframe.columns:
            parse_pairs[original] = parsed

    for column in dataframe.columns:
        if (column.startswith("mon_") or column.startswith("num_")) and column.endswith(
            "_decimal"
        ):
            parse_pairs[column.removesuffix("_decimal")] = column

    failure_counts = {
        parsed: parse_failure_count(dataframe, original, parsed)
        for original, parsed in parse_pairs.items()
    }
    total_failures = sum(failure_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="predial_parse_failures",
        status=status_from_warning_count(total_failures),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron fallas de parseo Predial."
            if total_failures == 0
            else "Se detectaron valores Predial no vacíos que no se parsearon."
        ),
        details={"failure_counts": failure_counts, "total_failures": total_failures},
        checked_at_utc=checked_at_utc,
    )


def check_renamu_territory_nulls(
    *, run_id: str, dataset: SilverDataset, dataframe: Any, checked_at_utc: str
) -> SilverQualityResult:
    """Detecta nulos territoriales en RENAMU."""

    territory_columns = [
        "departamento_normalizado",
        "provincia_normalizada",
        "distrito_normalizado",
    ]
    null_counts = {
        column: count_condition(dataframe, blank_or_null_condition(dataframe, column))
        for column in territory_columns
        if column in dataframe.columns
    }
    total_nulls = sum(null_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="renamu_territory_nulls",
        status=status_from_warning_count(total_nulls),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron nulos territoriales RENAMU."
            if total_nulls == 0
            else "Se detectaron nulos territoriales RENAMU."
        ),
        details={"null_counts": null_counts, "total_nulls": total_nulls},
        checked_at_utc=checked_at_utc,
    )


def check_duplicate_single_key(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    row_count: int,
    column_name: str,
    rule_name: str,
    checked_at_utc: str,
) -> SilverQualityResult:
    """Detecta duplicados por una columna."""

    if column_name not in dataframe.columns:
        return build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name=rule_name,
            status="WARNING",
            severity="WARNING",
            evaluated=False,
            message=f"No se evaluó la regla porque falta la columna {column_name}.",
            details={"missing_column": column_name},
            checked_at_utc=checked_at_utc,
        )

    distinct_count = int(dataframe.dropDuplicates([column_name]).count())
    duplicate_count = row_count - distinct_count
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name=rule_name,
        status=status_from_warning_count(duplicate_count),
        severity="WARNING",
        evaluated=True,
        message=(
            f"No se detectaron duplicados por {column_name}."
            if duplicate_count == 0
            else f"Se detectaron duplicados por {column_name}."
        ),
        details={"column": column_name, "duplicate_rows": duplicate_count},
        checked_at_utc=checked_at_utc,
    )


def check_renamu_tipomuni_invalid_values(
    *, run_id: str, dataset: SilverDataset, dataframe: Any, checked_at_utc: str
) -> SilverQualityResult:
    """Valida valores permitidos de tipomuni."""

    valid_values = set(dataset.resource_config.get("valid_tipomuni_values", []))
    if "tipomuni" not in dataframe.columns:
        return build_silver_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_name="renamu_tipomuni_invalid_values",
            status="WARNING",
            severity="WARNING",
            evaluated=False,
            message="No se evaluó tipomuni porque falta la columna original.",
            details={"valid_values": sorted(valid_values)},
            checked_at_utc=checked_at_utc,
        )

    invalid_count = count_condition(
        dataframe,
        nonblank_condition(dataframe, "tipomuni")
        & ~F.trim(F.col("tipomuni").cast("string")).isin(sorted(valid_values)),
    )
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="renamu_tipomuni_invalid_values",
        status=status_from_warning_count(invalid_count),
        severity="WARNING",
        evaluated=True,
        message=(
            "Los valores de tipomuni pertenecen a 1, 2 o 3."
            if invalid_count == 0
            else "Se detectaron valores de tipomuni fuera de 1, 2 o 3."
        ),
        details={"invalid_count": invalid_count, "valid_values": sorted(valid_values)},
        checked_at_utc=checked_at_utc,
    )


def check_renamu_financial_parse_failures(
    *, run_id: str, dataset: SilverDataset, dataframe: Any, checked_at_utc: str
) -> SilverQualityResult:
    """Detecta fallas de parseo en auxiliares decimales C96/C97."""

    prefixes = tuple(dataset.resource_config.get("financial_prefixes", []))
    failure_counts: dict[str, int] = {}
    for original in dataframe.columns:
        if not original.startswith(prefixes):
            continue
        parsed = f"{original}_decimal"
        if parsed in dataframe.columns:
            failure_counts[parsed] = parse_failure_count(dataframe, original, parsed)

    total_failures = sum(failure_counts.values())
    return build_silver_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_name="renamu_financial_parse_failures",
        status=status_from_warning_count(total_failures),
        severity="WARNING",
        evaluated=True,
        message=(
            "No se detectaron fallas de parseo financiero RENAMU."
            if total_failures == 0
            else "Se detectaron valores C96/C97 no vacíos que no se parsearon."
        ),
        details={
            "evaluated_decimal_columns": len(failure_counts),
            "failure_counts": failure_counts,
            "total_failures": total_failures,
        },
        checked_at_utc=checked_at_utc,
    )


def run_source_specific_checks(
    *,
    run_id: str,
    dataset: SilverDataset,
    dataframe: Any,
    row_count: int,
    checked_at_utc: str,
) -> list[SilverQualityResult]:
    """Ejecuta reglas específicas por fuente."""

    flags = dataset.resource_config.get(
        "expected_flags",
        dataset.source_config.get("expected_flags", []),
    )
    results: list[SilverQualityResult] = []

    if dataset.source_name == "siaf_income":
        results.extend(
            [
                check_negative_amounts(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    checked_at_utc=checked_at_utc,
                ),
                check_false_flags(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    flags=flags,
                    rule_name="invalid_mef_flags",
                    checked_at_utc=checked_at_utc,
                ),
                check_candidate_key_duplicates(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    row_count=row_count,
                    rule_name="mef_candidate_key_duplicates",
                    checked_at_utc=checked_at_utc,
                ),
            ]
        )

    if dataset.source_name == "sismepre":
        results.extend(
            [
                check_predial_required_relationship_keys(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    checked_at_utc=checked_at_utc,
                ),
                check_candidate_key_duplicates(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    row_count=row_count,
                    rule_name="predial_candidate_key_duplicates",
                    checked_at_utc=checked_at_utc,
                ),
                check_predial_parse_failures(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    checked_at_utc=checked_at_utc,
                ),
            ]
        )

    if dataset.source_name == "renamu":
        results.extend(
            [
                check_renamu_territory_nulls(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    checked_at_utc=checked_at_utc,
                ),
                check_duplicate_single_key(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    row_count=row_count,
                    column_name="ubigeo",
                    rule_name="renamu_ubigeo_duplicates",
                    checked_at_utc=checked_at_utc,
                ),
                check_duplicate_single_key(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    row_count=row_count,
                    column_name="idmunici",
                    rule_name="renamu_idmunici_duplicates",
                    checked_at_utc=checked_at_utc,
                ),
                check_renamu_tipomuni_invalid_values(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    checked_at_utc=checked_at_utc,
                ),
                check_renamu_financial_parse_failures(
                    run_id=run_id,
                    dataset=dataset,
                    dataframe=dataframe,
                    checked_at_utc=checked_at_utc,
                ),
            ]
        )

    return results


def run_checks_for_dataset(
    *,
    spark: Any,
    dataset: SilverDataset,
    quality_config: dict[str, Any],
    run_id: str,
    checked_at_utc: str,
    limit: int | None,
) -> list[SilverQualityResult]:
    """Ejecuta reglas Silver para un recurso."""

    results = check_simple_path_rules(
        run_id=run_id,
        dataset=dataset,
        checked_at_utc=checked_at_utc,
    )
    dataframe = None
    read_error = None

    try:
        if dataset.dataset_path.exists() and find_parquet_files(dataset.dataset_path):
            dataframe = spark.read.parquet(str(dataset.dataset_path))
            if limit is not None:
                dataframe = dataframe.limit(limit)
    except Exception as exc:  # noqa: BLE001
        read_error = f"{type(exc).__name__}: {exc}"

    results.append(
        check_dataset_readable(
            run_id=run_id,
            dataset=dataset,
            dataframe=dataframe,
            read_error=read_error,
            checked_at_utc=checked_at_utc,
        )
    )

    if dataframe is None:
        return results

    row_count = int(dataframe.count())
    column_count = len(dataframe.columns)
    typed_columns = dataset.resource_config.get(
        "typed_columns",
        dataset.source_config.get("typed_columns", []),
    )
    flags = dataset.resource_config.get(
        "expected_flags",
        dataset.source_config.get("expected_flags", []),
    )
    required_metadata = source_metadata_columns(dataset, quality_config)

    results.extend(
        [
            *check_positive_counts(
                run_id=run_id,
                dataset=dataset,
                row_count=row_count,
                column_count=column_count,
                checked_at_utc=checked_at_utc,
            ),
            check_required_column_rule(
                run_id=run_id,
                dataset=dataset,
                columns=dataframe.columns,
                required_columns=required_metadata,
                rule_name="silver_metadata_columns_present",
                checked_at_utc=checked_at_utc,
            ),
            check_required_column_rule(
                run_id=run_id,
                dataset=dataset,
                columns=dataframe.columns,
                required_columns=typed_columns,
                rule_name="expected_typed_columns_present",
                checked_at_utc=checked_at_utc,
            ),
            check_required_column_rule(
                run_id=run_id,
                dataset=dataset,
                columns=dataframe.columns,
                required_columns=flags,
                rule_name="expected_flags_present",
                checked_at_utc=checked_at_utc,
            ),
            check_critical_nulls(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                checked_at_utc=checked_at_utc,
            ),
            check_exact_duplicate_rows(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                row_count=row_count,
                checked_at_utc=checked_at_utc,
            ),
            check_candidate_key_duplicates(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                row_count=row_count,
                rule_name="candidate_key_duplicates",
                checked_at_utc=checked_at_utc,
            ),
            check_invalid_boolean_flags(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                flags=flags,
                checked_at_utc=checked_at_utc,
            ),
            check_dictionary_references(
                run_id=run_id,
                dataset=dataset,
                quality_config=quality_config,
                checked_at_utc=checked_at_utc,
            ),
            *run_source_specific_checks(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                row_count=row_count,
                checked_at_utc=checked_at_utc,
            ),
        ]
    )

    return results


def write_results_jsonl(results: list[SilverQualityResult], output_path: Path) -> Path:
    """Escribe resultados Silver como JSON Lines."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for result in results:
            file.write(json.dumps(asdict(result), ensure_ascii=False) + "\n")
    return output_path


def summarize_results(results: list[SilverQualityResult]) -> dict[str, int]:
    """Resume resultados por estado."""

    summary = {status: 0 for status in sorted(VALID_STATUSES)}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    return summary


def print_dry_run_plan(datasets: list[SilverDataset], output_path: Path) -> None:
    """Imprime el plan sin escribir resultados."""

    print("=" * 80)
    print("Plan de calidad Silver")
    print(f"Recursos esperados: {len(datasets)}")
    print(f"Salida configurada: {output_path}")

    for dataset in datasets:
        print(
            f"- {dataset.source_name} | {dataset.resource_key} | "
            f"ruta={dataset.dataset_path} | existe={dataset.dataset_path.exists()}"
        )

    print("Dry-run finalizado. No se escribió data/quality.")


def run_silver_quality_checks(
    *,
    dry_run: bool,
    output_path: Path | None = None,
    selected_sources: Iterable[str] | None = None,
    selected_resources: Iterable[str] | None = None,
    limit: int | None = None,
) -> list[SilverQualityResult]:
    """Ejecuta o planifica validaciones Silver."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(
        quality_config,
        selected_sources=selected_sources,
        selected_resources=selected_resources,
    )
    configured_output = get_config_value(
        quality_config,
        "silver.output.results_jsonl",
        DEFAULT_OUTPUT_PATH,
    )
    resolved_output_path = output_path or resolve_project_path(configured_output)

    if dry_run:
        print_dry_run_plan(datasets=datasets, output_path=resolved_output_path)
        return []

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    run_id = create_run_id()
    checked_at_utc = utc_now_iso()
    spark = build_spark_session(app_name="SilverQualityChecks")
    results: list[SilverQualityResult] = []

    try:
        for dataset in datasets:
            logger.info(
                "Ejecutando calidad Silver para %s/%s",
                dataset.source_name,
                dataset.resource_key,
            )
            results.extend(
                run_checks_for_dataset(
                    spark=spark,
                    dataset=dataset,
                    quality_config=quality_config,
                    run_id=run_id,
                    checked_at_utc=checked_at_utc,
                    limit=limit,
                )
            )
    finally:
        spark.stop()

    write_results_jsonl(results=results, output_path=resolved_output_path)
    print("=" * 80)
    print("Calidad Silver finalizada")
    print(f"Resultados generados: {len(results)}")
    print(f"Resumen: {summarize_results(results)}")
    print(f"Archivo JSONL: {resolved_output_path}")

    return results


def parse_args() -> argparse.Namespace:
    """Procesa argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description="Ejecuta reglas de calidad sobre datasets Silver Parquet."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida configuración y muestra plan sin escribir resultados.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Fuente Silver a evaluar. Puede repetirse.",
    )
    parser.add_argument(
        "--resource",
        action="append",
        default=None,
        help="Recurso Silver a evaluar. Puede repetirse.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=None,
        help="Ruta JSONL de salida.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite opcional de filas para pruebas locales.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    run_silver_quality_checks(
        dry_run=args.dry_run,
        output_path=args.output_path,
        selected_sources=args.source,
        selected_resources=args.resource,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
