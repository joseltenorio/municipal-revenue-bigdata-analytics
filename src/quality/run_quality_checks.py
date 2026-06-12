"""Motor inicial de calidad sobre datasets Bronze Parquet.

Este módulo ejecuta reglas técnicas y progresivas sobre recursos Bronze. No
limpia datos, no interpreta reglas de negocio y no modifica Landing ni Bronze.
En modo dry-run solo valida la configuración y muestra el plan de ejecución sin
leer Parquet ni escribir resultados.
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.common.config import get_config_value, load_quality_rules_config
from src.common.logger import get_logger
from src.common.paths import PROJECT_ROOT, QUALITY_DIR, get_source_bronze_path


VALID_STATUSES = {"PASS", "WARNING", "FAIL"}
DEFAULT_OUTPUT_PATH = QUALITY_DIR / "bronze_quality_results.jsonl"


class QualityCheckError(Exception):
    """Error controlado durante la ejecución de calidad."""


@dataclass(frozen=True)
class QualityResult:
    """Resultado estructurado de una regla de calidad."""

    run_id: str
    source_name: str
    resource_key: str
    dataset_path: str
    rule_id: str
    rule_type: str
    severity: str
    status: str
    evaluated: bool
    observed_value: str
    expected_value: str
    message: str
    processed_at_utc: str


@dataclass(frozen=True)
class BronzeDataset:
    """Recurso Bronze esperado por configuración."""

    source_name: str
    resource_key: str
    dataset_path: Path


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def create_run_id() -> str:
    """Crea un identificador único para una ejecución de calidad."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    return f"quality_bronze_{timestamp}_{suffix}"


def build_quality_result(
    *,
    run_id: str,
    dataset: BronzeDataset,
    rule_id: str,
    rule_type: str,
    severity: str,
    status: str,
    evaluated: bool,
    observed_value: Any,
    expected_value: Any,
    message: str,
    processed_at_utc: str,
) -> QualityResult:
    """Construye un resultado de calidad validando estados permitidos."""

    if severity not in VALID_STATUSES:
        raise QualityCheckError(f"Severidad no soportada: {severity}")

    if status not in VALID_STATUSES:
        raise QualityCheckError(f"Estado no soportado: {status}")

    return QualityResult(
        run_id=run_id,
        source_name=dataset.source_name,
        resource_key=dataset.resource_key,
        dataset_path=str(dataset.dataset_path),
        rule_id=rule_id,
        rule_type=rule_type,
        severity=severity,
        status=status,
        evaluated=evaluated,
        observed_value=str(observed_value),
        expected_value=str(expected_value),
        message=message,
        processed_at_utc=processed_at_utc,
    )


def not_evaluated_warning(
    *,
    run_id: str,
    dataset: BronzeDataset,
    rule_id: str,
    rule_type: str,
    expected_value: Any,
    message: str,
    processed_at_utc: str,
) -> QualityResult:
    """Crea un WARNING para reglas que no pueden evaluarse todavía."""

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id=rule_id,
        rule_type=rule_type,
        severity="WARNING",
        status="WARNING",
        evaluated=False,
        observed_value="no_evaluado",
        expected_value=expected_value,
        message=message,
        processed_at_utc=processed_at_utc,
    )


def resolve_project_path(path_value: str | Path) -> Path:
    """Resuelve una ruta relativa al proyecto."""

    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def load_quality_config() -> dict[str, Any]:
    """Carga y valida la estructura mínima de reglas de calidad."""

    config = load_quality_rules_config()
    quality_config = config.get("quality", {})
    valid_statuses = set(quality_config.get("valid_statuses", []))

    if valid_statuses != VALID_STATUSES:
        raise QualityCheckError(
            "quality.valid_statuses debe contener exactamente PASS, WARNING y FAIL."
        )

    bronze_config = quality_config.get("bronze", {})
    sources = bronze_config.get("sources", {})

    if not isinstance(sources, dict) or not sources:
        raise QualityCheckError("quality.bronze.sources debe definir fuentes Bronze.")

    return quality_config


def build_expected_datasets(quality_config: dict[str, Any]) -> list[BronzeDataset]:
    """Construye recursos Bronze esperados desde la configuración."""

    sources_config = get_config_value(quality_config, "bronze.sources", {})
    datasets: list[BronzeDataset] = []

    for source_name, source_config in sources_config.items():
        expected_resources = source_config.get("expected_resources", [])

        if not expected_resources:
            raise QualityCheckError(
                f"La fuente Bronze '{source_name}' no define expected_resources."
            )

        source_bronze_path = get_source_bronze_path(source_name)

        for resource_key in expected_resources:
            datasets.append(
                BronzeDataset(
                    source_name=source_name,
                    resource_key=str(resource_key),
                    dataset_path=source_bronze_path / f"resource_key={resource_key}",
                )
            )

    return datasets


def find_parquet_files(dataset_path: Path) -> list[Path]:
    """Lista archivos Parquet dentro de un recurso Bronze."""

    if not dataset_path.exists():
        return []

    return sorted(
        file_path
        for file_path in dataset_path.rglob("*.parquet")
        if file_path.is_file()
    )


def check_dataset_path_exists(
    *, run_id: str, dataset: BronzeDataset, processed_at_utc: str
) -> QualityResult:
    """Valida que exista la ruta esperada del recurso Bronze."""

    exists = dataset.dataset_path.exists()
    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="dataset_path_exists",
        rule_type="technical",
        severity="FAIL",
        status="PASS" if exists else "FAIL",
        evaluated=True,
        observed_value=exists,
        expected_value=True,
        message=(
            "La ruta del recurso Bronze existe."
            if exists
            else "No existe la ruta esperada del recurso Bronze."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_parquet_files_exist(
    *, run_id: str, dataset: BronzeDataset, processed_at_utc: str
) -> QualityResult:
    """Valida existencia de archivos Parquet dentro del recurso."""

    parquet_files = find_parquet_files(dataset.dataset_path)
    file_count = len(parquet_files)
    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="parquet_files_exist",
        rule_type="technical",
        severity="FAIL",
        status="PASS" if file_count > 0 else "FAIL",
        evaluated=True,
        observed_value=file_count,
        expected_value="> 0",
        message=(
            "Se encontraron archivos Parquet."
            if file_count > 0
            else "No se encontraron archivos Parquet en la ruta del recurso."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_dataset_readable(
    *,
    run_id: str,
    dataset: BronzeDataset,
    dataframe: Any | None,
    read_error: str | None,
    processed_at_utc: str,
) -> QualityResult:
    """Valida que Spark pueda leer el dataset."""

    readable = dataframe is not None and read_error is None
    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="dataset_readable",
        rule_type="technical",
        severity="FAIL",
        status="PASS" if readable else "FAIL",
        evaluated=True,
        observed_value="readable" if readable else read_error,
        expected_value="readable",
        message=(
            "Spark pudo leer el dataset Parquet."
            if readable
            else "Spark no pudo leer el dataset Parquet."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_row_count_positive(
    *, run_id: str, dataset: BronzeDataset, row_count: int, processed_at_utc: str
) -> QualityResult:
    """Valida que el dataset tenga filas."""

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="row_count_positive",
        rule_type="technical",
        severity="FAIL",
        status="PASS" if row_count > 0 else "FAIL",
        evaluated=True,
        observed_value=row_count,
        expected_value="> 0",
        message=(
            "El dataset tiene filas."
            if row_count > 0
            else "El dataset no tiene filas."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_column_count_positive(
    *, run_id: str, dataset: BronzeDataset, column_count: int, processed_at_utc: str
) -> QualityResult:
    """Valida que el dataset tenga columnas."""

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="column_count_positive",
        rule_type="technical",
        severity="FAIL",
        status="PASS" if column_count > 0 else "FAIL",
        evaluated=True,
        observed_value=column_count,
        expected_value="> 0",
        message=(
            "El dataset tiene columnas."
            if column_count > 0
            else "El dataset no tiene columnas."
        ),
        processed_at_utc=processed_at_utc,
    )


def evaluate_required_columns(
    existing_columns: Iterable[str],
    required_columns: Iterable[str],
) -> tuple[bool, list[str]]:
    """Evalúa presencia de columnas requeridas."""

    existing = set(existing_columns)
    missing = [column for column in required_columns if column not in existing]
    return len(missing) == 0, missing


def check_bronze_metadata_columns_present(
    *,
    run_id: str,
    dataset: BronzeDataset,
    columns: list[str],
    required_columns: list[str],
    processed_at_utc: str,
) -> QualityResult:
    """Valida columnas comunes de metadata técnica Bronze."""

    passed, missing = evaluate_required_columns(columns, required_columns)
    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="bronze_metadata_columns_present",
        rule_type="technical",
        severity="FAIL",
        status="PASS" if passed else "FAIL",
        evaluated=True,
        observed_value=f"faltantes={missing}",
        expected_value=required_columns,
        message=(
            "Todas las columnas comunes de metadata Bronze están presentes."
            if passed
            else "Faltan columnas comunes de metadata Bronze."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_fully_null_columns(
    *, run_id: str, dataset: BronzeDataset, dataframe: Any, row_count: int, processed_at_utc: str
) -> QualityResult:
    """Detecta columnas completamente nulas."""

    from pyspark.sql import functions as spark_functions

    if row_count == 0:
        return build_quality_result(
            run_id=run_id,
            dataset=dataset,
            rule_id="fully_null_columns",
            rule_type="completeness",
            severity="WARNING",
            status="WARNING",
            evaluated=True,
            observed_value="dataset_sin_filas",
            expected_value="sin columnas completamente nulas",
            message="No se puede descartar columnas completamente nulas porque el dataset no tiene filas.",
            processed_at_utc=processed_at_utc,
        )

    expressions = [
        spark_functions.count(spark_functions.col(column)).alias(column)
        for column in dataframe.columns
    ]
    counts = dataframe.select(expressions).collect()[0].asDict()
    fully_null = [column for column, non_null_count in counts.items() if non_null_count == 0]

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="fully_null_columns",
        rule_type="completeness",
        severity="WARNING",
        status="WARNING" if fully_null else "PASS",
        evaluated=True,
        observed_value=fully_null,
        expected_value="[]",
        message=(
            "Se detectaron columnas completamente nulas."
            if fully_null
            else "No se detectaron columnas completamente nulas."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_exact_duplicate_rows(
    *, run_id: str, dataset: BronzeDataset, dataframe: Any, row_count: int, processed_at_utc: str
) -> QualityResult:
    """Detecta filas duplicadas exactas."""

    distinct_count = dataframe.dropDuplicates().count() if row_count > 0 else 0
    duplicate_count = row_count - distinct_count

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="exact_duplicate_rows",
        rule_type="uniqueness",
        severity="WARNING",
        status="WARNING" if duplicate_count > 0 else "PASS",
        evaluated=True,
        observed_value=duplicate_count,
        expected_value=0,
        message=(
            "Se detectaron filas duplicadas exactas."
            if duplicate_count > 0
            else "No se detectaron filas duplicadas exactas."
        ),
        processed_at_utc=processed_at_utc,
    )


def find_first_existing_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    """Devuelve la primera columna candidata existente."""

    existing = set(columns)
    for candidate in candidates:
        if candidate in existing:
            return candidate
    return None


def validate_year_value(value: Any, min_year: int = 2010, max_year: int = 2030) -> bool:
    """Valida si un valor representa un año dentro del rango esperado."""

    if value is None:
        return False

    text_value = str(value).strip()
    if not text_value:
        return False

    try:
        year = int(float(text_value))
    except ValueError:
        return False

    return min_year <= year <= max_year


def validate_ubigeo_value(value: Any) -> bool:
    """Valida si un valor tiene formato de ubigeo peruano de seis dígitos."""

    if value is None:
        return False

    text_value = str(value).strip()
    return len(text_value) == 6 and text_value.isdigit()


def validate_percentage_value(value: Any, min_value: float = 0, max_value: float = 100) -> bool:
    """Valida si un valor representa un porcentaje dentro del rango esperado."""

    if value is None:
        return False

    text_value = str(value).strip().replace("%", "").replace(",", ".")
    if not text_value:
        return False

    try:
        number = float(text_value)
    except ValueError:
        return False

    return min_value <= number <= max_value


def check_invalid_year(
    *,
    run_id: str,
    dataset: BronzeDataset,
    dataframe: Any,
    rule_config: dict[str, Any],
    processed_at_utc: str,
) -> QualityResult:
    """Valida años inválidos si existe una columna candidata."""

    candidate_column = find_first_existing_column(
        dataframe.columns,
        rule_config.get("candidate_columns", ["anio", "ano", "año", "bronze_source_year"]),
    )

    if candidate_column is None:
        return not_evaluated_warning(
            run_id=run_id,
            dataset=dataset,
            rule_id="invalid_year",
            rule_type="validity",
            expected_value="columna de año existente",
            message="Regla no evaluada porque la columna de año no existe todavía en Bronze.",
            processed_at_utc=processed_at_utc,
        )

    from pyspark.sql import functions as spark_functions

    min_year = int(rule_config.get("min_year", 2010))
    max_year = int(rule_config.get("max_year", 2030))
    numeric_year = spark_functions.col(candidate_column).cast("int")
    invalid_count = dataframe.filter(
        spark_functions.col(candidate_column).isNull()
        | (spark_functions.trim(spark_functions.col(candidate_column)) == "")
        | numeric_year.isNull()
        | (numeric_year < min_year)
        | (numeric_year > max_year)
    ).count()

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="invalid_year",
        rule_type="validity",
        severity="WARNING",
        status="WARNING" if invalid_count > 0 else "PASS",
        evaluated=True,
        observed_value=f"{candidate_column}: {invalid_count}",
        expected_value=f"{min_year}-{max_year}",
        message=(
            "Se detectaron valores de año fuera del rango esperado."
            if invalid_count > 0
            else "Los valores de año evaluados están dentro del rango esperado."
        ),
        processed_at_utc=processed_at_utc,
    )


def check_invalid_ubigeo(
    *, run_id: str, dataset: BronzeDataset, dataframe: Any, processed_at_utc: str
) -> QualityResult:
    """Valida ubigeos inválidos si existe columna ubigeo."""

    candidate_column = find_first_existing_column(dataframe.columns, ["ubigeo"])

    if candidate_column is None:
        return not_evaluated_warning(
            run_id=run_id,
            dataset=dataset,
            rule_id="invalid_ubigeo",
            rule_type="validity",
            expected_value="columna ubigeo existente",
            message="Regla no evaluada porque la columna ubigeo no existe todavía en Bronze.",
            processed_at_utc=processed_at_utc,
        )

    from pyspark.sql import functions as spark_functions

    invalid_count = dataframe.filter(
        spark_functions.col(candidate_column).isNull()
        | (~spark_functions.trim(spark_functions.col(candidate_column)).rlike("^[0-9]{6}$"))
    ).count()

    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="invalid_ubigeo",
        rule_type="validity",
        severity="WARNING",
        status="WARNING" if invalid_count > 0 else "PASS",
        evaluated=True,
        observed_value=f"{candidate_column}: {invalid_count}",
        expected_value="seis dígitos",
        message=(
            "Se detectaron ubigeos vacíos o con formato inválido."
            if invalid_count > 0
            else "Los ubigeos evaluados tienen formato válido."
        ),
        processed_at_utc=processed_at_utc,
    )


def find_percentage_columns(columns: Iterable[str], patterns: Iterable[str]) -> list[str]:
    """Identifica columnas que sugieren porcentajes, avances o tasas."""

    normalized_patterns = [pattern.lower() for pattern in patterns]
    return [
        column
        for column in columns
        if any(pattern in column.lower() for pattern in normalized_patterns)
    ]


def check_invalid_percentage(
    *,
    run_id: str,
    dataset: BronzeDataset,
    dataframe: Any,
    rule_config: dict[str, Any],
    processed_at_utc: str,
) -> QualityResult:
    """Valida porcentajes inválidos si existen columnas candidatas."""

    percentage_columns = find_percentage_columns(
        dataframe.columns,
        rule_config.get(
            "column_name_patterns",
            ["porcentaje", "percent", "pct", "avance", "cumplimiento", "rate"],
        ),
    )

    if not percentage_columns:
        return not_evaluated_warning(
            run_id=run_id,
            dataset=dataset,
            rule_id="invalid_percentage",
            rule_type="validity",
            expected_value="columna de porcentaje, avance, cumplimiento o tasa existente",
            message="Regla no evaluada porque no existen columnas candidatas de porcentaje en Bronze.",
            processed_at_utc=processed_at_utc,
        )

    from pyspark.sql import functions as spark_functions

    min_value = float(rule_config.get("min_value", 0))
    max_value = float(rule_config.get("max_value", 100))
    invalid_counts: dict[str, int] = {}

    for column in percentage_columns:
        normalized_value = spark_functions.regexp_replace(
            spark_functions.regexp_replace(spark_functions.col(column), "%", ""),
            ",",
            ".",
        ).cast("double")
        invalid_count = dataframe.filter(
            spark_functions.col(column).isNull()
            | (spark_functions.trim(spark_functions.col(column)) == "")
            | normalized_value.isNull()
            | (normalized_value < min_value)
            | (normalized_value > max_value)
        ).count()
        invalid_counts[column] = invalid_count

    total_invalid = sum(invalid_counts.values())
    return build_quality_result(
        run_id=run_id,
        dataset=dataset,
        rule_id="invalid_percentage",
        rule_type="validity",
        severity="WARNING",
        status="WARNING" if total_invalid > 0 else "PASS",
        evaluated=True,
        observed_value=invalid_counts,
        expected_value=f"{min_value}-{max_value}",
        message=(
            "Se detectaron valores fuera de rango en columnas candidatas de porcentaje."
            if total_invalid > 0
            else "Las columnas candidatas de porcentaje están dentro del rango esperado."
        ),
        processed_at_utc=processed_at_utc,
    )


def run_checks_for_dataset(
    *,
    spark: Any,
    dataset: BronzeDataset,
    quality_config: dict[str, Any],
    run_id: str,
    processed_at_utc: str,
) -> list[QualityResult]:
    """Ejecuta reglas de calidad sobre un recurso Bronze."""

    results = [
        check_dataset_path_exists(
            run_id=run_id,
            dataset=dataset,
            processed_at_utc=processed_at_utc,
        ),
        check_parquet_files_exist(
            run_id=run_id,
            dataset=dataset,
            processed_at_utc=processed_at_utc,
        ),
    ]

    dataframe = None
    read_error = None

    try:
        if dataset.dataset_path.exists() and find_parquet_files(dataset.dataset_path):
            dataframe = spark.read.parquet(str(dataset.dataset_path))
    except Exception as exc:  # noqa: BLE001
        read_error = f"{type(exc).__name__}: {exc}"

    results.append(
        check_dataset_readable(
            run_id=run_id,
            dataset=dataset,
            dataframe=dataframe,
            read_error=read_error,
            processed_at_utc=processed_at_utc,
        )
    )

    if dataframe is None:
        return results

    row_count = dataframe.count()
    column_count = len(dataframe.columns)
    common_metadata = get_config_value(
        quality_config,
        "bronze.common_metadata_columns",
        [],
    )
    rules_config = get_config_value(quality_config, "bronze.rules", {})

    results.extend(
        [
            check_row_count_positive(
                run_id=run_id,
                dataset=dataset,
                row_count=row_count,
                processed_at_utc=processed_at_utc,
            ),
            check_column_count_positive(
                run_id=run_id,
                dataset=dataset,
                column_count=column_count,
                processed_at_utc=processed_at_utc,
            ),
            check_bronze_metadata_columns_present(
                run_id=run_id,
                dataset=dataset,
                columns=dataframe.columns,
                required_columns=common_metadata,
                processed_at_utc=processed_at_utc,
            ),
            check_fully_null_columns(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                row_count=row_count,
                processed_at_utc=processed_at_utc,
            ),
            check_exact_duplicate_rows(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                row_count=row_count,
                processed_at_utc=processed_at_utc,
            ),
            check_invalid_year(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                rule_config=rules_config.get("invalid_year", {}),
                processed_at_utc=processed_at_utc,
            ),
            check_invalid_ubigeo(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                processed_at_utc=processed_at_utc,
            ),
            check_invalid_percentage(
                run_id=run_id,
                dataset=dataset,
                dataframe=dataframe,
                rule_config=rules_config.get("invalid_percentage", {}),
                processed_at_utc=processed_at_utc,
            ),
        ]
    )

    return results


def write_results_jsonl(results: list[QualityResult], output_path: Path) -> Path:
    """Escribe resultados de calidad en JSON Lines."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        for result in results:
            file.write(json.dumps(asdict(result), ensure_ascii=False) + "\n")

    return output_path


def summarize_results(results: list[QualityResult]) -> dict[str, int]:
    """Resume resultados por estado."""

    summary = {status: 0 for status in sorted(VALID_STATUSES)}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    return summary


def print_dry_run_plan(datasets: list[BronzeDataset], output_path: Path) -> None:
    """Imprime el plan de ejecución sin leer Parquet ni escribir resultados."""

    print("=" * 80)
    print("Plan de calidad Bronze")
    print(f"Recursos esperados: {len(datasets)}")
    print(f"Salida configurada: {output_path}")

    for dataset in datasets:
        print(
            f"- {dataset.source_name} | {dataset.resource_key} | "
            f"ruta={dataset.dataset_path} | existe={dataset.dataset_path.exists()}"
        )

    print("Dry-run finalizado. No se leyó Parquet ni se escribió data/quality.")


def run_quality_checks(*, dry_run: bool, output_path: Path | None = None) -> list[QualityResult]:
    """Ejecuta o planifica validaciones de calidad Bronze."""

    quality_config = load_quality_config()
    datasets = build_expected_datasets(quality_config)
    configured_output = get_config_value(
        quality_config,
        "output.results_jsonl",
        DEFAULT_OUTPUT_PATH,
    )
    resolved_output_path = output_path or resolve_project_path(configured_output)

    if dry_run:
        print_dry_run_plan(datasets=datasets, output_path=resolved_output_path)
        return []

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    run_id = create_run_id()
    processed_at_utc = utc_now_iso()
    spark = build_spark_session(app_name="BronzeQualityChecks")
    results: list[QualityResult] = []

    try:
        for dataset in datasets:
            logger.info(
                "Ejecutando calidad Bronze para %s/%s",
                dataset.source_name,
                dataset.resource_key,
            )
            results.extend(
                run_checks_for_dataset(
                    spark=spark,
                    dataset=dataset,
                    quality_config=quality_config,
                    run_id=run_id,
                    processed_at_utc=processed_at_utc,
                )
            )
    finally:
        spark.stop()

    write_results_jsonl(results=results, output_path=resolved_output_path)
    print("=" * 80)
    print("Calidad Bronze finalizada")
    print(f"Resultados generados: {len(results)}")
    print(f"Resumen: {summarize_results(results)}")
    print(f"Archivo JSONL: {resolved_output_path}")

    return results


def parse_args() -> argparse.Namespace:
    """Procesa argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description="Ejecuta reglas de calidad sobre datasets Bronze Parquet."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida configuración y muestra plan sin leer Parquet ni escribir resultados.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ruta JSONL de salida. Por defecto usa data/quality/bronze_quality_results.jsonl.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    run_quality_checks(dry_run=args.dry_run, output_path=args.output)


if __name__ == "__main__":
    main()
