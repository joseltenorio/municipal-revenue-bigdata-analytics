"""Limpieza y estandarización Silver para SISMEPRE.

Este módulo lee datasets Bronze Parquet de `sismepre` y escribe un dataset
Silver por recurso bajo ``data/silver/sismepre``. La transformación aplica
limpieza técnica ligera, tipado progresivo y flags de validez por fila.

No integra tablas, no elimina filas y no decide todavía hechos, dimensiones ni
modelo analítico final.
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


SOURCE_NAME = "sismepre"

COMMON_BRONZE_COLUMNS = [
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_role",
    "bronze_source_priority",
    "bronze_processed_at_utc",
]

INTEGER_SOURCE_COLUMNS = {
    "ano_aplicacion": "ano_aplicacion_int",
    "periodo": "periodo_int",
    "ano_estadistica": "ano_estadistica_int",
    "mes_estadistica": "mes_estadistica_int",
}

RESPUESTA_TYPED_COLUMNS = {
    "respuesta_decimal": "respuesta_decimal_value",
    "respuesta_entero": "respuesta_entero_value",
    "respuesta_fecha": "respuesta_fecha_value",
}

DATE_SOURCE_COLUMNS = {
    "fecha_cierre",
    "fecha_pres_oficio",
    "fecha_ini_cierre",
    "fecha_ing",
    "usuario_creacion_fecha",
    "usuario_fecha_envio",
    "fecha_resol_alcal_adjunto",
    "respuesta_fecha",
}

RELATIONSHIP_KEYS_BY_RESOURCE = {
    "formulario": ["ano_aplicacion", "periodo", "formulario_id"],
    "preguntas": ["ano_aplicacion", "periodo", "formulario_id", "pregunta_id"],
    "respuestas": [
        "ano_aplicacion",
        "periodo",
        "formulario_id",
        "pregunta_id",
        "sec_ejec",
    ],
    "estadistica": ["ano_aplicacion", "periodo", "formulario_id"],
    "esat_estadistica_atm": ["ano_aplicacion", "periodo", "sec_ejec"],
    "entidad_estado": ["ano_aplicacion", "periodo", "sec_ejec"],
    "ano_aplicacion": ["ano_aplicacion", "periodo"],
}


class SilverTransformError(Exception):
    """Error controlado durante la transformación Silver de SISMEPRE."""


@dataclass(frozen=True)
class SilverResource:
    """Recurso predial seleccionado para transformación Silver."""

    resource_key: str
    bronze_path: Path
    silver_path: Path
    role: str
    priority: str | None


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_sismepre_config() -> dict[str, Any]:
    """Carga la configuración de la fuente sismepre."""

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
    """Indica si un recurso configurado corresponde a tabla predial transformable."""

    return (
        resource.get("format") == "csv"
        and resource.get("role") == "source_table"
        and bool(resource.get("use_for_ingestion", False))
    )


def select_silver_resources(
    source_config: dict[str, Any],
    *,
    resource_keys: list[str] | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
) -> list[SilverResource]:
    """Selecciona recursos prediales Bronze que se transformarán hacia Silver."""

    configured_resources = source_config.get("candidate_resources", {})

    if not isinstance(configured_resources, dict) or not configured_resources:
        raise SilverTransformError("No existen recursos prediales configurados.")

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

        selected_resources.append(
            SilverResource(
                resource_key=resource_key,
                bronze_path=resolved_bronze_dir / f"resource_key={resource_key}",
                silver_path=resolved_silver_dir / f"resource_key={resource_key}",
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
                if isinstance(resource, dict) and is_transformable_resource(resource)
            )
            raise SilverTransformError(
                f"Recursos prediales no válidos para Silver: {missing_keys}. "
                f"Recursos disponibles: {available_keys}."
            )

    if not selected_resources:
        raise SilverTransformError("No se seleccionó ningún recurso predial para Silver.")

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
            "Faltan recursos prediales en Bronze para construir Silver: "
            + ", ".join(missing_paths)
        )

    return resources


def require_common_bronze_columns(columns: list[str], resource: SilverResource) -> None:
    """Valida metadata Bronze común requerida para trazabilidad Silver."""

    missing_columns = sorted(set(COMMON_BRONZE_COLUMNS) - set(columns))

    if missing_columns:
        raise SilverTransformError(
            f"El recurso '{resource.resource_key}' no tiene metadata Bronze "
            f"requerida para Silver: {missing_columns}."
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


def try_cast_integer(column_name: str) -> Any:
    """Castea una columna a entero tolerando valores mal formados."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.expr(f"try_cast(`{column_name}` as int)")


def try_cast_decimal(column_name: str) -> Any:
    """Castea una columna a decimal tolerando separadores de miles y errores."""

    from pyspark.sql import functions as spark_functions

    return spark_functions.expr(
        f"try_cast(regexp_replace(trim(`{column_name}`), ',', '') as decimal(20,4))"
    )


def parse_human_date(column_name: str) -> Any:
    """Parsea fechas de origen con formato humano esperado dd/MM/yyyy.

    Spark en modo ANSI falla con cadenas vacías si se usa `to_date` directo.
    `try_to_timestamp` permite preservar la fila y devolver nulo cuando el
    valor no es parseable.
    """

    from pyspark.sql import functions as spark_functions

    return spark_functions.expr(
        f"cast(try_to_timestamp(nullif(trim(`{column_name}`), ''), 'dd/MM/yyyy') as date)"
    )


def is_parseable_or_blank(original_column: str, parsed_column: str) -> Any:
    """Construye flag de validez para campos opcionales casteados."""

    from pyspark.sql import functions as spark_functions

    original_value = spark_functions.trim(spark_functions.col(original_column))

    return (
        spark_functions.col(original_column).isNull()
        | (original_value == "")
        | spark_functions.col(parsed_column).isNotNull()
    )


def is_nonblank(column_name: str) -> Any:
    """Retorna expresión booleana para valor no nulo ni vacío."""

    from pyspark.sql import functions as spark_functions

    return (
        spark_functions.col(column_name).isNotNull()
        & (spark_functions.trim(spark_functions.col(column_name)) != "")
    )


def detect_typed_columns(columns: list[str]) -> list[str]:
    """Detecta columnas Silver tipables a partir del schema Bronze."""

    typed_columns: list[str] = []

    for source_column, target_column in INTEGER_SOURCE_COLUMNS.items():
        if source_column in columns:
            typed_columns.append(target_column)

    for source_column, target_column in RESPUESTA_TYPED_COLUMNS.items():
        if source_column in columns:
            typed_columns.append(target_column)

    typed_columns.extend(f"{column}_decimal" for column in columns if column.startswith("mon_"))
    typed_columns.extend(f"{column}_decimal" for column in columns if column.startswith("num_"))
    typed_columns.extend(
        f"{column}_date"
        for column in columns
        if column in DATE_SOURCE_COLUMNS and column != "respuesta_fecha"
    )
    typed_columns.append("bronze_processed_at_timestamp")

    return typed_columns


def add_typed_columns(dataframe: Any) -> Any:
    """Agrega columnas tipadas progresivas según columnas disponibles."""

    from pyspark.sql import functions as spark_functions

    transformed = dataframe
    columns = set(dataframe.columns)

    for source_column, target_column in INTEGER_SOURCE_COLUMNS.items():
        if source_column in columns:
            transformed = transformed.withColumn(target_column, try_cast_integer(source_column))

    if "respuesta_decimal" in columns:
        transformed = transformed.withColumn(
            "respuesta_decimal_value",
            try_cast_decimal("respuesta_decimal"),
        )

    if "respuesta_entero" in columns:
        transformed = transformed.withColumn(
            "respuesta_entero_value",
            try_cast_integer("respuesta_entero"),
        )

    if "respuesta_fecha" in columns:
        transformed = transformed.withColumn(
            "respuesta_fecha_value",
            parse_human_date("respuesta_fecha"),
        )

    for column in dataframe.columns:
        if column.startswith("mon_") or column.startswith("num_"):
            transformed = transformed.withColumn(f"{column}_decimal", try_cast_decimal(column))

    for column in sorted(DATE_SOURCE_COLUMNS & columns):
        if column != "respuesta_fecha":
            transformed = transformed.withColumn(f"{column}_date", parse_human_date(column))

    transformed = transformed.withColumn(
        "bronze_processed_at_timestamp",
        spark_functions.to_timestamp("bronze_processed_at_utc"),
    )

    return transformed


def add_validity_flags(dataframe: Any, resource: SilverResource) -> Any:
    """Agrega flags de validez solo cuando las columnas aplican al recurso."""

    from functools import reduce
    from operator import and_

    from pyspark.sql import functions as spark_functions

    transformed = dataframe
    columns = set(dataframe.columns)

    if {"ano_aplicacion", "ano_aplicacion_int"} <= columns:
        transformed = transformed.withColumn(
            "is_valid_ano_aplicacion",
            is_nonblank("ano_aplicacion") & spark_functions.col("ano_aplicacion_int").isNotNull(),
        )

    if {"periodo", "periodo_int"} <= columns:
        transformed = transformed.withColumn(
            "is_valid_periodo",
            is_nonblank("periodo") & spark_functions.col("periodo_int").isNotNull(),
        )

    if {"mes_estadistica", "mes_estadistica_int"} <= columns:
        transformed = transformed.withColumn(
            "is_valid_mes_estadistica",
            spark_functions.col("mes_estadistica_int").between(1, 12),
        )

    if "ubigeo" in columns:
        transformed = transformed.withColumn(
            "is_valid_ubigeo",
            spark_functions.col("ubigeo").rlike(r"^[0-9]{6}$"),
        )

    if {"respuesta_decimal", "respuesta_decimal_value"} <= columns:
        transformed = transformed.withColumn(
            "is_valid_respuesta_decimal",
            is_parseable_or_blank("respuesta_decimal", "respuesta_decimal_value"),
        )

    if {"respuesta_entero", "respuesta_entero_value"} <= columns:
        transformed = transformed.withColumn(
            "is_valid_respuesta_entero",
            is_parseable_or_blank("respuesta_entero", "respuesta_entero_value"),
        )

    relationship_keys = [
        column
        for column in RELATIONSHIP_KEYS_BY_RESOURCE.get(resource.resource_key, [])
        if column in columns
    ]
    if relationship_keys:
        transformed = transformed.withColumn(
            "has_required_relationship_keys",
            reduce(and_, [is_nonblank(column) for column in relationship_keys]),
        )

    territory_columns = [
        column
        for column in [
            "departamento",
            "provincia",
            "distrito",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
        ]
        if column in columns
    ]
    if territory_columns:
        transformed = transformed.withColumn(
            "has_complete_territory",
            reduce(and_, [is_nonblank(column) for column in territory_columns]),
        )

    return transformed


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
        .withColumn("silver_source_role", spark_functions.lit(resource.role))
        .withColumn("silver_source_priority", spark_functions.lit(resource.priority))
        .withColumn("silver_processed_at_utc", spark_functions.lit(processed_at))
    )


def transform_resource_dataframe(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Aplica limpieza, tipado progresivo, flags y metadata Silver."""

    require_common_bronze_columns(dataframe.columns, resource)

    transformed = trim_string_columns(dataframe)
    transformed = add_typed_columns(transformed)
    transformed = add_validity_flags(transformed, resource)
    transformed = add_silver_metadata(
        dataframe=transformed,
        resource=resource,
        processed_at=processed_at,
    )

    return transformed


def build_dry_run_summary(
    *,
    spark: Any,
    resources: list[SilverResource],
    limit: int | None,
) -> list[dict[str, Any]]:
    """Construye resumen de dry-run leyendo schema y conteo sin escribir datos."""

    summary: list[dict[str, Any]] = []

    for resource in resources:
        item: dict[str, Any] = {
            "resource_key": resource.resource_key,
            "role": resource.role,
            "priority": resource.priority,
            "bronze_path": str(resource.bronze_path),
            "silver_path": str(resource.silver_path),
            "bronze_exists": resource.bronze_path.exists(),
            "silver_exists": resource.silver_path.exists(),
            "typed_columns": [],
        }

        if resource.bronze_path.exists():
            try:
                dataframe = spark.read.parquet(str(resource.bronze_path))
                if limit is not None:
                    dataframe = dataframe.limit(limit)
                require_common_bronze_columns(dataframe.columns, resource)
                item["row_count"] = dataframe.count()
                item["column_count"] = len(dataframe.columns)
                item["typed_columns"] = detect_typed_columns(dataframe.columns)
                item["readable"] = True
            except Exception as exc:  # pragma: no cover - depende del entorno Spark local.
                item["row_count"] = "no evaluado"
                item["column_count"] = "no evaluado"
                item["readable"] = False
                item["read_error"] = str(exc).splitlines()[0]

        summary.append(item)

    return summary


def write_resource_silver(
    *,
    spark: Any,
    resource: SilverResource,
    processed_at: str,
    overwrite: bool,
    limit: int | None,
) -> None:
    """Transforma y escribe un recurso predial en Parquet Silver."""

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


def transform_sismepre(
    *,
    resources: list[SilverResource],
    dry_run: bool,
    overwrite: bool,
    limit: int | None,
) -> list[dict[str, Any]]:
    """Transforma SISMEPRE hacia Silver o retorna resumen de dry-run."""

    validate_bronze_inputs(resources)

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    spark = build_spark_session(app_name="SilverPredialGoal")

    try:
        if dry_run:
            return build_dry_run_summary(spark=spark, resources=resources, limit=limit)

        processed_at = utc_now_iso()
        summary: list[dict[str, Any]] = []

        for resource in resources:
            logger.info(
                "Transformando recurso Silver predial %s desde %s",
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
        description="Limpia y estandariza SISMEPRE desde Bronze hacia Silver."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help="Clave de recurso predial a transformar. Puede repetirse.",
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

    source_config = load_sismepre_config()
    resources = select_silver_resources(
        source_config=source_config,
        resource_keys=args.resources,
    )

    summary = transform_sismepre(
        resources=resources,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        limit=args.limit,
    )

    print("=" * 80)
    print("Silver SISMEPRE")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recursos seleccionados: {len(summary)}")

    for item in summary:
        row_count = item.get("row_count", "n/a")
        column_count = item.get("column_count", "n/a")
        bronze_exists = item.get("bronze_exists", "n/a")
        silver_exists = item.get("silver_exists", "n/a")
        typed_columns = item.get("typed_columns", [])
        typed_preview = ", ".join(typed_columns[:12])
        if len(typed_columns) > 12:
            typed_preview += f", ... (+{len(typed_columns) - 12})"

        print(
            f"- {item['resource_key']} | filas={row_count} | "
            f"columnas={column_count} | bronze_existe={bronze_exists} | "
            f"silver_existe={silver_exists}"
        )
        print(f"  bronze: {item['bronze_path']}")
        print(f"  silver: {item['silver_path']}")
        print(f"  columnas tipables: {typed_preview or 'ninguna'}")
        if item.get("readable") is False:
            print(f"  lectura Spark: no evaluada ({item.get('read_error')})")

    if args.dry_run:
        print("Dry-run finalizado. No se escribió Parquet ni se tocó data/silver.")
    else:
        print("Transformación Silver predial finalizada.")


if __name__ == "__main__":
    main()
