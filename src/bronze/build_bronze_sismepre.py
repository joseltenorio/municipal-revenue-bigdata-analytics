"""Construcción de archivos Parquet Bronze para la fuente de SISMEPRE.

Este módulo lee los CSV originales de `sismepre` desde Landing y escribe un
dataset Parquet por recurso bajo `data/bronze/sismepre`.

La capa Bronze conserva cada tabla predial por separado, mantiene los valores
como texto y aplica únicamente cambios técnicos: normalización de nombres de
columnas y metadata de procesamiento. No une tablas, no decide hechos o
dimensiones y no aplica reglas de negocio.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_sources_config, load_spark_config
from src.common.logger import get_logger
from src.common.paths import get_source_bronze_path, get_source_landing_path


SOURCE_NAME = "sismepre"


class BronzeBuildError(Exception):
    """Error controlado durante la construcción Bronze de SISMEPRE."""


@dataclass(frozen=True)
class BronzeResource:
    """Recurso predial seleccionado para conversión a Bronze."""

    resource_key: str
    file_name: str
    source_path: Path
    output_path: Path
    role: str
    priority: str | None


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def normalize_column_name(column_name: str) -> str:
    """Normaliza un nombre de columna de origen a snake_case técnico.

    Esta normalización sí convierte tildes y caracteres especiales a ASCII,
    porque el objetivo es generar nombres técnicos estables para Spark,
    Parquet y consultas posteriores.
    """

    without_accents = unicodedata.normalize("NFKD", column_name)
    ascii_name = without_accents.encode("ascii", "ignore").decode("ascii")
    normalized = ascii_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")

    if not normalized:
        normalized = "column"

    if normalized[0].isdigit():
        normalized = f"col_{normalized}"

    return normalized


def normalize_column_names(column_names: list[str]) -> list[str]:
    """Normaliza nombres de columnas y resuelve duplicados de forma determinística."""

    seen_names: dict[str, int] = {}
    normalized_names: list[str] = []

    for column_name in column_names:
        base_name = normalize_column_name(column_name)
        count = seen_names.get(base_name, 0)

        if count == 0:
            normalized_names.append(base_name)
        else:
            normalized_names.append(f"{base_name}_{count + 1}")

        seen_names[base_name] = count + 1

    return normalized_names


def load_sismepre_config() -> dict[str, Any]:
    """Carga la configuración de la fuente sismepre."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise BronzeBuildError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise BronzeBuildError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def is_bronze_source_table(resource: dict[str, Any]) -> bool:
    """Indica si un recurso predial debe convertirse como tabla fuente Bronze."""

    return (
        resource.get("format") == "csv"
        and resource.get("role") == "source_table"
        and bool(resource.get("use_for_ingestion", False))
    )


def select_bronze_resources(
    source_config: dict[str, Any],
    *,
    resource_keys: list[str] | None = None,
    landing_dir: Path | None = None,
    bronze_dir: Path | None = None,
) -> list[BronzeResource]:
    """Selecciona recursos prediales de datos para convertir a Bronze Parquet."""

    configured_resources = source_config.get("candidate_resources", {})

    if not isinstance(configured_resources, dict) or not configured_resources:
        raise BronzeBuildError("No existen recursos prediales configurados para Bronze.")

    landing_subdir = source_config.get("landing_subdir", SOURCE_NAME)
    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    resolved_landing_dir = landing_dir or get_source_landing_path(landing_subdir)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)

    selected_resources: list[BronzeResource] = []

    for resource_key, resource in configured_resources.items():
        if not isinstance(resource, dict) or not is_bronze_source_table(resource):
            continue

        if resource_keys and resource_key not in resource_keys:
            continue

        file_name = resource.get("file_name")

        if not file_name:
            raise BronzeBuildError(f"El recurso '{resource_key}' no tiene file_name.")

        selected_resources.append(
            BronzeResource(
                resource_key=resource_key,
                file_name=file_name,
                source_path=resolved_landing_dir / file_name,
                output_path=resolved_bronze_dir / f"resource_key={resource_key}",
                role=str(resource.get("role")),
                priority=resource.get("priority"),
            )
        )

    if resource_keys:
        found_keys = {resource.resource_key for resource in selected_resources}
        missing_keys = sorted(set(resource_keys) - found_keys)

        if missing_keys:
            available_keys = sorted(
                key
                for key, resource in configured_resources.items()
                if isinstance(resource, dict) and is_bronze_source_table(resource)
            )
            raise BronzeBuildError(
                f"Recursos prediales no válidos para Bronze: {missing_keys}. "
                f"Recursos disponibles: {available_keys}."
            )

    if not selected_resources:
        raise BronzeBuildError("No se seleccionó ningún recurso predial para Bronze.")

    return selected_resources


def validate_landing_inputs(resources: list[BronzeResource]) -> list[BronzeResource]:
    """Valida que los archivos de origen seleccionados existan en Landing."""

    missing_files = [
        str(resource.source_path)
        for resource in resources
        if not resource.source_path.exists()
    ]

    if missing_files:
        raise BronzeBuildError(
            "Faltan archivos prediales en Landing para construir Bronze: "
            + ", ".join(missing_files)
        )

    return resources


def build_dry_run_summary(resources: list[BronzeResource]) -> list[dict[str, Any]]:
    """Construye un resumen serializable de dry-run sin escribir datos Bronze."""

    return [
        {
            "resource_key": resource.resource_key,
            "file_name": resource.file_name,
            "role": resource.role,
            "priority": resource.priority,
            "source_path": str(resource.source_path),
            "output_path": str(resource.output_path),
            "source_exists": resource.source_path.exists(),
        }
        for resource in resources
    ]


def read_csv_as_strings(spark: Any, resource: BronzeResource) -> Any:
    """Lee un recurso CSV predial sin inferencia agresiva de tipos."""

    spark_config = load_spark_config()
    csv_options = get_config_value(spark_config, "spark.read_options.csv", {})

    reader = spark.read

    for key, value in csv_options.items():
        option_value = str(value).lower() if isinstance(value, bool) else value
        reader = reader.option(key, option_value)

    return reader.csv(str(resource.source_path))


def add_bronze_metadata(dataframe: Any, resource: BronzeResource, processed_at: str) -> Any:
    """Agrega columnas de metadata técnica Bronze."""

    from pyspark.sql import functions as spark_functions

    return (
        dataframe.withColumn("bronze_source_name", spark_functions.lit(SOURCE_NAME))
        .withColumn("bronze_resource_key", spark_functions.lit(resource.resource_key))
        .withColumn("bronze_source_file_name", spark_functions.lit(resource.file_name))
        .withColumn("bronze_source_file_path", spark_functions.lit(str(resource.source_path)))
        .withColumn("bronze_source_role", spark_functions.lit(resource.role))
        .withColumn("bronze_source_priority", spark_functions.lit(resource.priority))
        .withColumn("bronze_processed_at_utc", spark_functions.lit(processed_at))
    )


def build_resource_bronze(
    *,
    spark: Any,
    resource: BronzeResource,
    processed_at: str,
    overwrite: bool,
) -> None:
    """Convierte un recurso CSV predial a Parquet Bronze."""

    dataframe = read_csv_as_strings(spark=spark, resource=resource)
    normalized_columns = normalize_column_names(dataframe.columns)
    dataframe = dataframe.toDF(*normalized_columns)
    dataframe = add_bronze_metadata(
        dataframe=dataframe,
        resource=resource,
        processed_at=processed_at,
    )

    write_mode = "overwrite" if overwrite else "errorifexists"
    (
        dataframe.write.mode(write_mode)
        .option("compression", "snappy")
        .parquet(str(resource.output_path))
    )


def build_bronze_sismepre(
    *,
    resources: list[BronzeResource],
    dry_run: bool,
    overwrite: bool,
) -> list[dict[str, Any]]:
    """Construye Bronze predial o retorna un resumen de dry-run."""

    validate_landing_inputs(resources)
    summary = build_dry_run_summary(resources)

    if dry_run:
        return summary

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    processed_at = utc_now_iso()
    spark = build_spark_session(app_name="BronzePredialGoal")

    try:
        for resource in resources:
            logger.info(
                "Construyendo recurso Bronze predial %s desde %s",
                resource.resource_key,
                resource.source_path,
            )
            build_resource_bronze(
                spark=spark,
                resource=resource,
                processed_at=processed_at,
                overwrite=overwrite,
            )
    finally:
        spark.stop()

    return summary


def parse_args() -> argparse.Namespace:
    """Procesa los argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description="Convierte SISMEPRE desde Landing hacia Bronze Parquet."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help="Clave de recurso predial a convertir. Puede repetirse.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida entradas y muestra el plan sin escribir Parquet.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe el Parquet de salida si ya existe.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    source_config = load_sismepre_config()
    resources = select_bronze_resources(
        source_config=source_config,
        resource_keys=args.resources,
    )

    summary = build_bronze_sismepre(
        resources=resources,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
    )

    print("=" * 80)
    print("Bronze SISMEPRE")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recursos seleccionados: {len(summary)}")

    for item in summary:
        print(
            "- {resource_key} | {file_name} | rol={role} | "
            "prioridad={priority} | existe={source_exists}".format(**item)
        )
        print(f"  salida: {item['output_path']}")

    if args.dry_run:
        print("Dry-run finalizado. No se escribió Parquet ni se tocó data/bronze.")
    else:
        print("Conversión Bronze predial finalizada.")


if __name__ == "__main__":
    main()
