"""Construcción de archivos Parquet Bronze para la fuente MEF de ingresos.

Este módulo lee archivos CSV originales de MEF desde Landing y escribe un
dataset Parquet por recurso bajo data/bronze/mef_income.

La capa Bronze conserva la granularidad de origen y aplica únicamente cambios
técnicos: normalización de nombres de columnas y metadata de procesamiento.
No aplica limpieza fuerte de negocio ni integración analítica.
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


SOURCE_NAME = "mef_income"
DICTIONARY_FILE_NAME = "Ingresos_Diccionario.csv"


class BronzeBuildError(Exception):
    """Error controlado durante la construcción Bronze de MEF ingresos."""


@dataclass(frozen=True)
class BronzeResource:
    """Recurso MEF seleccionado para conversión a Bronze."""

    resource_key: str
    file_name: str
    source_path: Path
    output_path: Path
    year: int | None
    granularity: str
    role: str | None


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


def load_mef_income_config() -> dict[str, Any]:
    """Carga la configuración de la fuente MEF ingresos."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise BronzeBuildError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise BronzeBuildError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def is_fact_resource(resource: dict[str, Any]) -> bool:
    """Indica si un recurso configurado debe convertirse como entrada Bronze."""

    return (
        resource.get("format") == "csv"
        and resource.get("role") != "dictionary"
        and resource.get("file_name") != DICTIONARY_FILE_NAME
    )


def select_bronze_resources(
    source_config: dict[str, Any],
    *,
    resource_keys: list[str] | None = None,
    years: list[int] | None = None,
    granularities: list[str] | None = None,
    landing_dir: Path | None = None,
    bronze_dir: Path | None = None,
) -> list[BronzeResource]:
    """Selecciona los recursos CSV de MEF que se convertirán a Bronze Parquet."""

    configured_resources = source_config.get("candidate_resources", {})

    if not isinstance(configured_resources, dict) or not configured_resources:
        raise BronzeBuildError("No existen recursos MEF configurados para Bronze.")

    landing_subdir = source_config.get("landing_subdir", SOURCE_NAME)
    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    resolved_landing_dir = landing_dir or get_source_landing_path(landing_subdir)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)

    selected_resources: list[BronzeResource] = []

    for resource_key, resource in configured_resources.items():
        if not isinstance(resource, dict) or not is_fact_resource(resource):
            continue

        if resource_keys and resource_key not in resource_keys:
            continue

        resource_year = resource.get("year")
        resource_granularity = str(resource.get("granularity") or "unknown")

        if years and resource_year not in years:
            continue

        if granularities and resource_granularity not in granularities:
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
                year=resource_year if isinstance(resource_year, int) else None,
                granularity=resource_granularity,
                role=resource.get("role"),
            )
        )

    if resource_keys:
        found_keys = {resource.resource_key for resource in selected_resources}
        missing_keys = sorted(set(resource_keys) - found_keys)

        if missing_keys:
            available_keys = sorted(
                key
                for key, resource in configured_resources.items()
                if isinstance(resource, dict) and is_fact_resource(resource)
            )
            raise BronzeBuildError(
                f"Recursos MEF no válidos para Bronze: {missing_keys}. "
                f"Recursos disponibles: {available_keys}."
            )

    if not selected_resources:
        raise BronzeBuildError("No se seleccionó ningún recurso MEF para Bronze.")

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
            "Faltan archivos MEF en Landing para construir Bronze: "
            + ", ".join(missing_files)
        )

    return resources


def build_dry_run_summary(resources: list[BronzeResource]) -> list[dict[str, Any]]:
    """Construye un resumen serializable de dry-run sin escribir datos Bronze."""

    return [
        {
            "resource_key": resource.resource_key,
            "file_name": resource.file_name,
            "year": resource.year,
            "granularity": resource.granularity,
            "source_path": str(resource.source_path),
            "output_path": str(resource.output_path),
            "source_exists": resource.source_path.exists(),
        }
        for resource in resources
    ]


def read_csv_as_strings(spark: Any, resource: BronzeResource) -> Any:
    """Lee un recurso CSV de MEF sin inferencia agresiva de tipos."""

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
        .withColumn("bronze_source_year", spark_functions.lit(resource.year))
        .withColumn("bronze_source_granularity", spark_functions.lit(resource.granularity))
        .withColumn("bronze_processed_at_utc", spark_functions.lit(processed_at))
    )


def build_resource_bronze(
    *,
    spark: Any,
    resource: BronzeResource,
    processed_at: str,
    overwrite: bool,
) -> None:
    """Convierte un recurso CSV de MEF a Parquet Bronze."""

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


def build_bronze_mef_income(
    *,
    resources: list[BronzeResource],
    dry_run: bool,
    overwrite: bool,
) -> list[dict[str, Any]]:
    """Construye Bronze MEF ingresos o retorna un resumen de dry-run."""

    validate_landing_inputs(resources)
    summary = build_dry_run_summary(resources)

    if dry_run:
        return summary

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    processed_at = utc_now_iso()
    spark = build_spark_session(app_name="BronzeMEFIncome")

    try:
        for resource in resources:
            logger.info(
                "Construyendo recurso Bronze MEF %s desde %s",
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
        description="Convierte MEF ingresos desde Landing hacia Bronze Parquet."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help="Clave de recurso MEF a convertir. Puede repetirse.",
    )
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        dest="years",
        help="Año MEF a convertir. Puede repetirse.",
    )
    parser.add_argument(
        "--granularity",
        action="append",
        choices=["annual", "monthly", "daily"],
        dest="granularities",
        help="Granularidad MEF a convertir. Puede repetirse.",
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
    source_config = load_mef_income_config()
    resources = select_bronze_resources(
        source_config=source_config,
        resource_keys=args.resources,
        years=args.years,
        granularities=args.granularities,
    )

    summary = build_bronze_mef_income(
        resources=resources,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
    )

    print("=" * 80)
    print("Bronze MEF ingresos")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recursos seleccionados: {len(summary)}")

    for item in summary:
        print(
            "- {resource_key} | {file_name} | año={year} | "
            "granularidad={granularity} | existe={source_exists}".format(**item)
        )
        print(f"  salida: {item['output_path']}")

    if args.dry_run:
        print("Dry-run finalizado. No se escribió Parquet ni se tocó data/bronze.")
    else:
        print("Conversión Bronze MEF finalizada.")


if __name__ == "__main__":
    main()

