"""Limpieza y estandarización Silver para ingresos MEF.

Este módulo lee datasets Bronze Parquet de SIAF ingresos y escribe un dataset
Silver por recurso bajo ``data/silver/siaf_income``. La transformación aplica
limpieza técnica ligera, tipado semántico inicial y flags de calidad por fila.

No integra recursos, no elimina filas y no construye métricas analíticas finales.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_sources_config
from src.common.logger import get_logger
from src.common.paths import get_source_bronze_path, get_source_silver_path


SOURCE_NAME = "siaf_income"
DICTIONARY_FILE_NAME = "Ingresos_Diccionario.csv"

TYPED_COLUMNS = [
    "anio",
    "mes",
    "monto_pia_decimal",
    "monto_pim_decimal",
    "monto_recaudado_decimal",
    "bronze_processed_at_timestamp",
]

REQUIRED_BRONZE_COLUMNS = [
    "ano_doc",
    "mes_doc",
    "monto_pia",
    "monto_pim",
    "monto_recaudado",
    "departamento_ejecutora",
    "provincia_ejecutora",
    "distrito_ejecutora",
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_year",
    "bronze_source_granularity",
    "bronze_processed_at_utc",
]


class SilverTransformError(Exception):
    """Error controlado durante la transformación Silver de SIAF ingresos."""


@dataclass(frozen=True)
class SilverResource:
    """Recurso MEF seleccionado para transformación Silver."""

    resource_key: str
    bronze_path: Path
    silver_path: Path
    year: int | None
    granularity: str
    role: str | None


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_siaf_income_config() -> dict[str, Any]:
    """Carga la configuración de la fuente SIAF ingresos."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise SilverTransformError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise SilverTransformError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def is_transformable_resource(resource: dict[str, Any]) -> bool:
    """Indica si un recurso configurado corresponde a una tabla MEF transformable."""

    return (
        resource.get("format") == "csv"
        and resource.get("role") != "dictionary"
        and resource.get("file_name") != DICTIONARY_FILE_NAME
    )


def select_silver_resources(
    source_config: dict[str, Any],
    *,
    resource_keys: list[str] | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
) -> list[SilverResource]:
    """Selecciona recursos MEF Bronze que se transformarán hacia Silver."""

    configured_resources = source_config.get("candidate_resources", {})

    if not isinstance(configured_resources, dict) or not configured_resources:
        raise SilverTransformError("No existen recursos MEF configurados.")

    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    silver_subdir = source_config.get("silver_subdir", SOURCE_NAME)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)
    resolved_silver_dir = silver_dir or get_source_silver_path(silver_subdir)

    selected_resources: list[SilverResource] = []

    for resource_key, resource in configured_resources.items():
        if not isinstance(resource, dict) or not is_transformable_resource(resource):
            continue

        if resource_keys and resource_key not in resource_keys:
            continue

        resource_year = resource.get("year")
        resource_granularity = str(resource.get("granularity") or "unknown")

        selected_resources.append(
            SilverResource(
                resource_key=resource_key,
                bronze_path=resolved_bronze_dir / f"resource_key={resource_key}",
                silver_path=resolved_silver_dir / f"resource_key={resource_key}",
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
                if isinstance(resource, dict) and is_transformable_resource(resource)
            )
            raise SilverTransformError(
                f"Recursos MEF no válidos para Silver: {missing_keys}. "
                f"Recursos disponibles: {available_keys}."
            )

    if not selected_resources:
        raise SilverTransformError("No se seleccionó ningún recurso MEF para Silver.")

    return selected_resources


def validate_bronze_inputs(resources: list[SilverResource]) -> list[SilverResource]:
    """Valida que existan las rutas Bronze seleccionadas."""

    missing_paths = [
        str(resource.bronze_path)
        for resource in resources
        if not resource.bronze_path.exists()
    ]

    if missing_paths:
        raise SilverTransformError(
            "Faltan recursos MEF en Bronze para construir Silver: "
            + ", ".join(missing_paths)
        )

    return resources


def require_bronze_columns(columns: list[str], resource: SilverResource) -> None:
    """Valida que el recurso Bronze tenga las columnas requeridas para Silver."""

    missing_columns = sorted(set(REQUIRED_BRONZE_COLUMNS) - set(columns))

    if missing_columns:
        raise SilverTransformError(
            f"El recurso '{resource.resource_key}' no tiene columnas Bronze "
            f"requeridas para Silver: {missing_columns}."
        )


def build_dry_run_summary(
    *,
    spark: Any,
    resources: list[SilverResource],
    limit: int | None,
) -> list[dict[str, Any]]:
    """Construye un resumen de dry-run leyendo schema y conteo sin escribir datos."""

    summary: list[dict[str, Any]] = []

    for resource in resources:
        item: dict[str, Any] = {
            "resource_key": resource.resource_key,
            "year": resource.year,
            "granularity": resource.granularity,
            "bronze_path": str(resource.bronze_path),
            "silver_path": str(resource.silver_path),
            "bronze_exists": resource.bronze_path.exists(),
            "silver_exists": resource.silver_path.exists(),
            "typed_columns": TYPED_COLUMNS,
        }

        if resource.bronze_path.exists():
            try:
                dataframe = spark.read.parquet(str(resource.bronze_path))
                if limit is not None:
                    dataframe = dataframe.limit(limit)
                require_bronze_columns(dataframe.columns, resource)
                item["row_count"] = dataframe.count()
                item["column_count"] = len(dataframe.columns)
                item["columns_available"] = dataframe.columns
                item["readable"] = True
            except Exception as exc:  # pragma: no cover - depende del entorno Spark local.
                item["row_count"] = "no evaluado"
                item["column_count"] = "no evaluado"
                item["columns_available"] = []
                item["readable"] = False
                item["read_error"] = str(exc).splitlines()[0]

        summary.append(item)

    return summary


def trim_string_columns(dataframe: Any) -> Any:
    """Aplica trim a todas las columnas string sin cambiar los nombres."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql.types import StringType

    selected_columns = []

    for field in dataframe.schema.fields:
        column = spark_functions.col(field.name)
        if isinstance(field.dataType, StringType):
            selected_columns.append(spark_functions.trim(column).alias(field.name))
        else:
            selected_columns.append(column)

    return dataframe.select(*selected_columns)


def cast_decimal_column(column_name: str) -> Any:
    """Castea una columna de monto a decimal preservando precisión."""

    from pyspark.sql import functions as spark_functions
    from pyspark.sql.types import DecimalType

    normalized = spark_functions.regexp_replace(
        spark_functions.trim(spark_functions.col(column_name)),
        ",",
        "",
    )
    return normalized.cast(DecimalType(20, 4))


def is_parseable_or_blank(original_column: str, parsed_column: str) -> Any:
    """Construye flag de validez para campos opcionales casteados."""

    from pyspark.sql import functions as spark_functions

    original_value = spark_functions.trim(spark_functions.col(original_column))

    return (
        spark_functions.col(original_column).isNull()
        | (original_value == "")
        | spark_functions.col(parsed_column).isNotNull()
    )


def transform_resource_dataframe(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Aplica limpieza, tipado inicial, flags y metadata Silver a un recurso MEF."""

    from pyspark.sql import functions as spark_functions

    require_bronze_columns(dataframe.columns, resource)

    dataframe = trim_string_columns(dataframe)

    transformed = (
        dataframe.withColumn("anio", spark_functions.col("ano_doc").cast("int"))
        .withColumn("mes", spark_functions.col("mes_doc").cast("int"))
        .withColumn("monto_pia_decimal", cast_decimal_column("monto_pia"))
        .withColumn("monto_pim_decimal", cast_decimal_column("monto_pim"))
        .withColumn(
            "monto_recaudado_decimal",
            cast_decimal_column("monto_recaudado"),
        )
        .withColumn(
            "bronze_processed_at_timestamp",
            spark_functions.to_timestamp("bronze_processed_at_utc"),
        )
    )

    bronze_year = spark_functions.col("bronze_source_year").cast("int")

    transformed = (
        transformed.withColumn(
            "is_valid_anio",
            spark_functions.col("anio").isNotNull()
            & (bronze_year.isNull() | (spark_functions.col("anio") == bronze_year)),
        )
        .withColumn(
            "is_valid_mes",
            spark_functions.col("mes").between(1, 12),
        )
        .withColumn(
            "is_valid_monto_pia",
            is_parseable_or_blank("monto_pia", "monto_pia_decimal"),
        )
        .withColumn(
            "is_valid_monto_pim",
            is_parseable_or_blank("monto_pim", "monto_pim_decimal"),
        )
        .withColumn(
            "is_valid_monto_recaudado",
            is_parseable_or_blank("monto_recaudado", "monto_recaudado_decimal"),
        )
        .withColumn(
            "has_complete_executora_location",
            (spark_functions.col("departamento_ejecutora").isNotNull())
            & (spark_functions.trim("departamento_ejecutora") != "")
            & (spark_functions.col("provincia_ejecutora").isNotNull())
            & (spark_functions.trim("provincia_ejecutora") != "")
            & (spark_functions.col("distrito_ejecutora").isNotNull())
            & (spark_functions.trim("distrito_ejecutora") != ""),
        )
        .withColumn("silver_source_name", spark_functions.lit(SOURCE_NAME))
        .withColumn("silver_resource_key", spark_functions.lit(resource.resource_key))
        .withColumn("silver_source_year", spark_functions.lit(resource.year))
        .withColumn(
            "silver_source_granularity",
            spark_functions.lit(resource.granularity),
        )
        .withColumn("silver_processed_at_utc", spark_functions.lit(processed_at))
    )

    return transformed


def write_resource_silver(
    *,
    spark: Any,
    resource: SilverResource,
    processed_at: str,
    overwrite: bool,
    limit: int | None,
) -> None:
    """Transforma y escribe un recurso MEF en Parquet Silver."""

    dataframe = spark.read.parquet(str(resource.bronze_path))

    if limit is not None:
        dataframe = dataframe.limit(limit)

    transformed = transform_resource_dataframe(
        dataframe=dataframe,
        resource=resource,
        processed_at=processed_at,
    )

    write_mode = "overwrite" if overwrite else "errorifexists"
    (
        transformed.write.mode(write_mode)
        .option("compression", "snappy")
        .parquet(str(resource.silver_path))
    )


def transform_siaf_income(
    *,
    resources: list[SilverResource],
    dry_run: bool,
    overwrite: bool,
    limit: int | None,
) -> list[dict[str, Any]]:
    """Transforma SIAF ingresos hacia Silver o retorna un resumen de dry-run."""

    validate_bronze_inputs(resources)

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    spark = build_spark_session(app_name="SilverMEFIncome")

    try:
        if dry_run:
            return build_dry_run_summary(spark=spark, resources=resources, limit=limit)

        processed_at = utc_now_iso()
        summary: list[dict[str, Any]] = []

        for resource in resources:
            logger.info(
                "Transformando recurso Silver MEF %s desde %s",
                resource.resource_key,
                resource.bronze_path,
            )
            write_resource_silver(
                spark=spark,
                resource=resource,
                processed_at=processed_at,
                overwrite=overwrite,
                limit=limit,
            )
            summary.append(
                {
                    "resource_key": resource.resource_key,
                    "bronze_path": str(resource.bronze_path),
                    "silver_path": str(resource.silver_path),
                    "silver_written": True,
                }
            )

        return summary
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa los argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description="Limpia y estandariza SIAF ingresos desde Bronze hacia Silver."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help="Clave de recurso MEF a transformar. Puede repetirse.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida recursos, schema y conteos sin escribir Parquet Silver.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe el Parquet Silver de salida si ya existe.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita filas por recurso para pruebas locales. Por defecto procesa todo.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        raise SilverTransformError("--limit debe ser un entero positivo.")

    source_config = load_siaf_income_config()
    resources = select_silver_resources(
        source_config=source_config,
        resource_keys=args.resources,
    )

    summary = transform_siaf_income(
        resources=resources,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        limit=args.limit,
    )

    print("=" * 80)
    print("Silver SIAF ingresos")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recursos seleccionados: {len(summary)}")
    print(f"Columnas tipadas previstas: {', '.join(TYPED_COLUMNS)}")

    for item in summary:
        row_count = item.get("row_count", "n/a")
        column_count = item.get("column_count", "n/a")
        bronze_exists = item.get("bronze_exists", "n/a")
        silver_exists = item.get("silver_exists", "n/a")
        print(
            f"- {item['resource_key']} | filas={row_count} | "
            f"columnas={column_count} | bronze_existe={bronze_exists} | "
            f"silver_existe={silver_exists}"
        )
        print(f"  bronze: {item['bronze_path']}")
        print(f"  silver: {item['silver_path']}")
        if item.get("readable") is False:
            print(f"  lectura Spark: no evaluada ({item.get('read_error')})")

    if args.dry_run:
        print("Dry-run finalizado. No se escribió Parquet ni se tocó data/silver.")
    else:
        print("Transformación Silver MEF finalizada.")


if __name__ == "__main__":
    main()
