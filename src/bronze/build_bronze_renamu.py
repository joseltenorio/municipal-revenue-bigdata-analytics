"""Construcción de archivos Parquet Bronze para la fuente RENAMU 2022.

Este módulo convierte únicamente el CSV tabular principal de RENAMU extraído
desde Landing hacia un dataset Parquet bajo `data/bronze/renamu`.

La capa Bronze conserva el dataset completo como un recurso separado, mantiene
los valores como texto y aplica únicamente cambios técnicos: normalización de
nombres de columnas y metadata de procesamiento. No interpreta el cuestionario,
no selecciona variables analíticas y no intenta convertir PDFs o ZIP como
tablas.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_sources_config
from src.common.logger import get_logger
from src.common.paths import get_source_bronze_path, get_source_landing_path


SOURCE_NAME = "renamu"
RESOURCE_KEY = "base_renamu_2022"
SOURCE_YEAR = 2022
SOURCE_FILE_NAME = "Base_RENAMU_2022_f.csv"
SOURCE_RELATIVE_PATH = Path("extracted") / "783-Modulo1726" / SOURCE_FILE_NAME
CSV_SEPARATOR = ";"
CSV_ENCODING = "UTF-8"
SPARK_MAX_COLUMNS = 2000


class BronzeBuildError(Exception):
    """Error controlado durante la construcción Bronze de RENAMU."""


@dataclass(frozen=True)
class BronzeResource:
    """Recurso RENAMU seleccionado para conversión a Bronze."""

    resource_key: str
    file_name: str
    source_path: Path
    output_path: Path
    year: int


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


def load_renamu_config() -> dict[str, Any]:
    """Carga la configuración de la fuente RENAMU."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise BronzeBuildError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise BronzeBuildError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def build_renamu_resource(
    source_config: dict[str, Any],
    *,
    landing_dir: Path | None = None,
    bronze_dir: Path | None = None,
) -> BronzeResource:
    """Construye la definición del único recurso tabular RENAMU para Bronze."""

    landing_subdir = source_config.get("landing_subdir", SOURCE_NAME)
    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    resolved_landing_dir = landing_dir or get_source_landing_path(landing_subdir)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)

    return BronzeResource(
        resource_key=RESOURCE_KEY,
        file_name=SOURCE_FILE_NAME,
        source_path=resolved_landing_dir / SOURCE_RELATIVE_PATH,
        output_path=resolved_bronze_dir / f"resource_key={RESOURCE_KEY}",
        year=SOURCE_YEAR,
    )


def validate_landing_input(resource: BronzeResource) -> BronzeResource:
    """Valida que el CSV principal de RENAMU exista en Landing."""

    if not resource.source_path.exists():
        raise BronzeBuildError(
            "No existe el CSV principal de RENAMU en Landing: "
            f"{resource.source_path}"
        )

    if resource.source_path.suffix.lower() != ".csv":
        raise BronzeBuildError(
            f"El recurso RENAMU seleccionado no es CSV: {resource.source_path}"
        )

    return resource


def build_dry_run_summary(resource: BronzeResource) -> dict[str, Any]:
    """Construye un resumen serializable de dry-run sin escribir datos Bronze."""

    return {
        "resource_key": resource.resource_key,
        "file_name": resource.file_name,
        "year": resource.year,
        "source_path": str(resource.source_path),
        "output_path": str(resource.output_path),
        "source_exists": resource.source_path.exists(),
        "csv_separator": CSV_SEPARATOR,
        "csv_encoding": CSV_ENCODING,
    }


def read_csv_as_strings(spark: Any, resource: BronzeResource) -> Any:
    """Lee el CSV principal de RENAMU sin inferencia agresiva de tipos."""

    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("sep", CSV_SEPARATOR)
        .option("encoding", CSV_ENCODING)
        .option("multiLine", "false")
        .option("maxColumns", str(SPARK_MAX_COLUMNS))
        .csv(str(resource.source_path))
    )


def add_bronze_metadata(dataframe: Any, resource: BronzeResource, processed_at: str) -> Any:
    """Agrega columnas de metadata técnica Bronze."""

    from pyspark.sql import functions as spark_functions

    return (
        dataframe.withColumn("bronze_source_name", spark_functions.lit(SOURCE_NAME))
        .withColumn("bronze_resource_key", spark_functions.lit(resource.resource_key))
        .withColumn("bronze_source_file_name", spark_functions.lit(resource.file_name))
        .withColumn("bronze_source_file_path", spark_functions.lit(str(resource.source_path)))
        .withColumn("bronze_source_year", spark_functions.lit(resource.year))
        .withColumn("bronze_processed_at_utc", spark_functions.lit(processed_at))
    )


def build_resource_bronze(
    *,
    spark: Any,
    resource: BronzeResource,
    processed_at: str,
    overwrite: bool,
) -> None:
    """Convierte el CSV principal de RENAMU a Parquet Bronze."""

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


def build_bronze_renamu(
    *,
    resource: BronzeResource,
    dry_run: bool,
    overwrite: bool,
) -> dict[str, Any]:
    """Construye Bronze RENAMU o retorna un resumen de dry-run."""

    validate_landing_input(resource)
    summary = build_dry_run_summary(resource)

    if dry_run:
        return summary

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    processed_at = utc_now_iso()
    spark = build_spark_session(app_name="BronzeRENAMU")

    try:
        logger.info(
            "Construyendo recurso Bronze RENAMU %s desde %s",
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
        description="Convierte RENAMU 2022 desde Landing hacia Bronze Parquet."
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
    source_config = load_renamu_config()
    resource = build_renamu_resource(source_config)

    summary = build_bronze_renamu(
        resource=resource,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
    )

    print("=" * 80)
    print("Bronze RENAMU")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recurso seleccionado: {summary['resource_key']}")
    print(f"Archivo: {summary['file_name']}")
    print(f"Año: {summary['year']}")
    print(f"Separador CSV: {summary['csv_separator']}")
    print(f"Encoding CSV: {summary['csv_encoding']}")
    print(f"Existe en Landing: {summary['source_exists']}")
    print(f"Origen: {summary['source_path']}")
    print(f"Salida: {summary['output_path']}")

    if args.dry_run:
        print("Dry-run finalizado. No se escribió Parquet ni se tocó data/bronze.")
    else:
        print("Conversión Bronze RENAMU finalizada.")


if __name__ == "__main__":
    main()
