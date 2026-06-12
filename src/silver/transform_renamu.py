"""Limpieza y estandarización Silver para RENAMU 2022.

Este módulo lee el dataset Bronze Parquet principal de RENAMU 2022 y escribe un
dataset Silver ancho bajo ``data/silver/renamu``. La transformación conserva
todas las columnas originales, normaliza territorio, agrega auxiliares tipadas
mínimas y crea flags técnicos de validez.

No interpreta masivamente módulos del cuestionario, no integra con otras fuentes
y no construye modelo analítico final.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_sources_config
from src.common.logger import get_logger
from src.common.paths import get_source_bronze_path, get_source_silver_path


SOURCE_NAME = "renamu"
RESOURCE_KEY = "base_renamu_2022"
SOURCE_YEAR = 2022

REQUIRED_BRONZE_COLUMNS = [
    "ano",
    "idmunici",
    "ccdd",
    "ccpp",
    "ccdi",
    "ubigeo",
    "departamento",
    "provincia",
    "distrito",
    "tipomuni",
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_year",
    "bronze_processed_at_utc",
]


class SilverTransformError(Exception):
    """Error controlado durante la transformación Silver de RENAMU."""


@dataclass(frozen=True)
class SilverResource:
    """Recurso RENAMU seleccionado para transformación Silver."""

    resource_key: str
    bronze_path: Path
    silver_path: Path
    year: int


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_renamu_config() -> dict[str, Any]:
    """Carga la configuración de la fuente RENAMU."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise SilverTransformError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise SilverTransformError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def build_renamu_resource(
    source_config: dict[str, Any],
    *,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
) -> SilverResource:
    """Construye la definición del recurso Bronze RENAMU a transformar."""

    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    silver_subdir = source_config.get("silver_subdir", SOURCE_NAME)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)
    resolved_silver_dir = silver_dir or get_source_silver_path(silver_subdir)

    return SilverResource(
        resource_key=RESOURCE_KEY,
        bronze_path=resolved_bronze_dir / f"resource_key={RESOURCE_KEY}",
        silver_path=resolved_silver_dir / f"resource_key={RESOURCE_KEY}",
        year=SOURCE_YEAR,
    )


def validate_bronze_input(resource: SilverResource) -> SilverResource:
    """Valida que exista la ruta Bronze RENAMU seleccionada."""

    if not resource.bronze_path.exists():
        raise SilverTransformError(
            f"No existe el recurso RENAMU en Bronze: {resource.bronze_path}"
        )

    return resource


def require_bronze_columns(columns: list[str]) -> None:
    """Valida que Bronze RENAMU tenga las columnas mínimas requeridas."""

    missing_columns = sorted(set(REQUIRED_BRONZE_COLUMNS) - set(columns))

    if missing_columns:
        raise SilverTransformError(
            "El recurso RENAMU no tiene columnas requeridas para Silver: "
            f"{missing_columns}."
        )


def trim_string_columns(dataframe: Any) -> Any:
    """Aplica trim a todas las columnas string sin cambiar nombres originales."""

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


def normalize_territory_text(column_name: str) -> Any:
    """Normaliza texto territorial sin reemplazar el valor original."""

    from pyspark.sql import functions as spark_functions

    collapsed = spark_functions.regexp_replace(
        spark_functions.trim(spark_functions.col(column_name)),
        r"\s+",
        " ",
    )
    return spark_functions.upper(collapsed)


def try_cast_integer(column_name: str) -> Any:
    """Castea una columna a entero tolerando valores mal formados."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.expr(f"try_cast(`{column_name}` as int)")


def try_cast_decimal(column_name: str) -> Any:
    """Castea una columna financiera a decimal tolerando errores de parseo."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.expr(
        f"try_cast(regexp_replace(trim(`{column_name}`), ',', '') as decimal(20,4))"
    )


def is_nonblank(column_name: str) -> Any:
    """Retorna expresión booleana para valor no nulo ni vacío."""

    from pyspark.sql import functions as spark_functions

    return (
        spark_functions.col(column_name).isNotNull()
        & (spark_functions.trim(spark_functions.col(column_name)) != "")
    )


def detect_question_columns(columns: list[str]) -> dict[str, int]:
    """Cuenta columnas de cuestionario por prefijo técnico."""

    return {
        "p_columns": sum(1 for column in columns if re.match(r"^p\d+", column)),
        "vfi_columns": sum(1 for column in columns if column.startswith("vfi")),
        "c_columns": sum(1 for column in columns if re.match(r"^c\d+", column)),
    }


def detect_financial_columns(columns: list[str]) -> list[str]:
    """Detecta columnas financieras RENAMU C96/C97 para auxiliares decimales."""

    return [
        column
        for column in columns
        if column.startswith("c96") or column.startswith("c97")
    ]


def add_typed_columns(dataframe: Any) -> Any:
    """Agrega columnas auxiliares tipadas sin reemplazar originales."""

    from pyspark.sql import functions as spark_functions

    transformed = (
        dataframe.withColumn("anio", try_cast_integer("ano"))
        .withColumn("tipomuni_int", try_cast_integer("tipomuni"))
        .withColumn(
            "bronze_processed_at_timestamp",
            spark_functions.to_timestamp("bronze_processed_at_utc"),
        )
        .withColumn(
            "departamento_normalizado",
            normalize_territory_text("departamento"),
        )
        .withColumn("provincia_normalizada", normalize_territory_text("provincia"))
        .withColumn("distrito_normalizado", normalize_territory_text("distrito"))
    )

    for column in detect_financial_columns(dataframe.columns):
        transformed = transformed.withColumn(f"{column}_decimal", try_cast_decimal(column))

    return transformed


def add_validity_flags(dataframe: Any) -> Any:
    """Agrega flags técnicos de validez para RENAMU Silver."""

    from pyspark.sql import functions as spark_functions

    return (
        dataframe.withColumn(
            "is_valid_anio",
            spark_functions.col("anio") == spark_functions.lit(SOURCE_YEAR),
        )
        .withColumn("is_valid_ubigeo", spark_functions.col("ubigeo").rlike(r"^[0-9]{6}$"))
        .withColumn("is_valid_ccdd", spark_functions.col("ccdd").rlike(r"^[0-9]{2}$"))
        .withColumn("is_valid_ccpp", spark_functions.col("ccpp").rlike(r"^[0-9]{2}$"))
        .withColumn("is_valid_ccdi", spark_functions.col("ccdi").rlike(r"^[0-9]{2}$"))
        .withColumn(
            "has_complete_territory",
            is_nonblank("departamento")
            & is_nonblank("provincia")
            & is_nonblank("distrito"),
        )
        .withColumn(
            "has_municipal_identifier",
            is_nonblank("idmunici") & is_nonblank("ubigeo"),
        )
        .withColumn(
            "is_valid_tipomuni",
            spark_functions.col("tipomuni").isin("1", "2", "3"),
        )
    )


def add_silver_metadata(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Agrega metadata técnica Silver."""

    from pyspark.sql import functions as spark_functions

    return (
        dataframe.withColumn("silver_source_name", spark_functions.lit(SOURCE_NAME))
        .withColumn("silver_resource_key", spark_functions.lit(resource.resource_key))
        .withColumn("silver_source_year", spark_functions.lit(resource.year))
        .withColumn("silver_processed_at_utc", spark_functions.lit(processed_at))
    )


def transform_resource_dataframe(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Aplica limpieza, tipado auxiliar, flags y metadata Silver."""

    require_bronze_columns(dataframe.columns)

    transformed = trim_string_columns(dataframe)
    transformed = add_typed_columns(transformed)
    transformed = add_validity_flags(transformed)
    transformed = add_silver_metadata(
        dataframe=transformed,
        resource=resource,
        processed_at=processed_at,
    )

    return transformed


def build_dry_run_summary(
    *,
    spark: Any,
    resource: SilverResource,
    limit: int | None,
) -> dict[str, Any]:
    """Construye resumen de dry-run leyendo schema y conteo sin escribir datos."""

    summary: dict[str, Any] = {
        "resource_key": resource.resource_key,
        "bronze_path": str(resource.bronze_path),
        "silver_path": str(resource.silver_path),
        "bronze_exists": resource.bronze_path.exists(),
        "silver_exists": resource.silver_path.exists(),
    }

    if resource.bronze_path.exists():
        try:
            dataframe = spark.read.parquet(str(resource.bronze_path))
            if limit is not None:
                dataframe = dataframe.limit(limit)
            require_bronze_columns(dataframe.columns)
            financial_columns = detect_financial_columns(dataframe.columns)
            summary.update(
                {
                    "row_count": dataframe.count(),
                    "column_count": len(dataframe.columns),
                    "territorial_columns": [
                        column
                        for column in [
                            "ano",
                            "idmunici",
                            "ccdd",
                            "ccpp",
                            "ccdi",
                            "ubigeo",
                            "departamento",
                            "provincia",
                            "distrito",
                            "tipomuni",
                        ]
                        if column in dataframe.columns
                    ],
                    "question_column_counts": detect_question_columns(dataframe.columns),
                    "financial_column_count": len(financial_columns),
                    "financial_columns_sample": financial_columns[:20],
                    "readable": True,
                }
            )
        except Exception as exc:  # pragma: no cover - depende del entorno Spark local.
            summary.update(
                {
                    "row_count": "no evaluado",
                    "column_count": "no evaluado",
                    "territorial_columns": [],
                    "question_column_counts": {},
                    "financial_column_count": "no evaluado",
                    "financial_columns_sample": [],
                    "readable": False,
                    "read_error": str(exc).splitlines()[0],
                }
            )

    return summary


def write_resource_silver(
    *,
    spark: Any,
    resource: SilverResource,
    processed_at: str,
    overwrite: bool,
    limit: int | None,
) -> None:
    """Transforma y escribe RENAMU en Parquet Silver."""

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


def transform_renamu(
    *,
    resource: SilverResource,
    dry_run: bool,
    overwrite: bool,
    limit: int | None,
) -> dict[str, Any]:
    """Transforma RENAMU hacia Silver o retorna resumen de dry-run."""

    validate_bronze_input(resource)

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    spark = build_spark_session(app_name="SilverRENAMU")

    try:
        if dry_run:
            return build_dry_run_summary(spark=spark, resource=resource, limit=limit)

        processed_at = utc_now_iso()
        logger.info(
            "Transformando recurso Silver RENAMU %s desde %s",
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
        return {
            "resource_key": resource.resource_key,
            "bronze_path": str(resource.bronze_path),
            "silver_path": str(resource.silver_path),
            "silver_written": True,
        }
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa los argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description="Limpia y estandariza RENAMU 2022 desde Bronze hacia Silver."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida recurso, schema y conteo sin escribir Parquet Silver.",
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
        help="Limita filas para pruebas locales. Por defecto procesa todo.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        raise SilverTransformError("--limit debe ser un entero positivo.")

    source_config = load_renamu_config()
    resource = build_renamu_resource(source_config)

    summary = transform_renamu(
        resource=resource,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        limit=args.limit,
    )

    print("=" * 80)
    print("Silver RENAMU")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recurso seleccionado: {summary['resource_key']}")
    print(f"Bronze existe: {summary.get('bronze_exists', 'n/a')}")
    print(f"Silver existe: {summary.get('silver_exists', 'n/a')}")
    print(f"Filas: {summary.get('row_count', 'n/a')}")
    print(f"Columnas Bronze: {summary.get('column_count', 'n/a')}")
    print(f"Bronze: {summary['bronze_path']}")
    print(f"Silver: {summary['silver_path']}")
    print(
        "Columnas territoriales detectadas: "
        + ", ".join(summary.get("territorial_columns", []))
    )
    question_counts = summary.get("question_column_counts", {})
    print(
        "Columnas de cuestionario: "
        f"p*={question_counts.get('p_columns', 'n/a')}, "
        f"vfi*={question_counts.get('vfi_columns', 'n/a')}, "
        f"c*={question_counts.get('c_columns', 'n/a')}"
    )
    print(f"Columnas C96/C97 detectadas: {summary.get('financial_column_count', 'n/a')}")
    sample = summary.get("financial_columns_sample", [])
    print(f"Muestra C96/C97: {', '.join(sample) if sample else 'ninguna'}")

    if summary.get("readable") is False:
        print(f"Lectura Spark: no evaluada ({summary.get('read_error')})")

    if args.dry_run:
        print("Dry-run finalizado. No se escribió Parquet ni se tocó data/silver.")
    else:
        print("Transformación Silver RENAMU finalizada.")


if __name__ == "__main__":
    main()
