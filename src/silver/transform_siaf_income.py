"""Transformacion Silver curada para ingresos SIAF.

Este modulo lee datasets Bronze Parquet de `siaf_income` y escribe un dataset
Silver por recurso bajo ``data/silver/siaf_income``.

La salida Silver:
- conserva la granularidad original por `resource_key`
- tipa montos y periodos
- estandariza codigos administrativos como texto
- genera flags tecnicos sin corregir los datos contables observados
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

FINAL_COLUMNS = [
    "anio",
    "mes",
    "fecha_mes",
    "source_resource_key",
    "source_granularity",
    "nivel_gobierno_codigo",
    "nivel_gobierno_nombre",
    "sector_codigo",
    "sector_nombre",
    "pliego_codigo",
    "pliego_nombre",
    "sec_ejec",
    "ejecutora_codigo",
    "ejecutora_nombre",
    "departamento_codigo",
    "departamento_nombre",
    "provincia_codigo",
    "provincia_nombre",
    "distrito_codigo",
    "distrito_nombre",
    "ubigeo6_ejecutora",
    "fuente_financiamiento_codigo",
    "fuente_financiamiento_nombre",
    "rubro_codigo",
    "rubro_nombre",
    "tipo_recurso_codigo",
    "tipo_recurso_nombre",
    "generica_codigo",
    "generica_nombre",
    "subgenerica_codigo",
    "subgenerica_nombre",
    "subgenerica_det_codigo",
    "subgenerica_det_nombre",
    "especifica_codigo",
    "especifica_nombre",
    "especifica_det_codigo",
    "especifica_det_nombre",
    "monto_pia",
    "monto_pim",
    "monto_recaudado",
    "is_municipal_government",
    "is_valid_anio",
    "is_valid_mes",
    "is_valid_sec_ejec",
    "is_valid_ubigeo6_ejecutora",
    "is_valid_monto_pia",
    "is_valid_monto_pim",
    "is_valid_monto_recaudado",
    "flag_pim_menor_pia",
    "flag_recaudado_mayor_pim",
    "has_complete_territory",
    "silver_source_name",
    "silver_resource_key",
    "silver_processed_at_utc",
]

REQUIRED_BRONZE_COLUMNS = [
    "ano_doc",
    "mes_doc",
    "nivel_gobierno",
    "nivel_gobierno_nombre",
    "sector",
    "sector_nombre",
    "pliego",
    "pliego_nombre",
    "sec_ejec",
    "ejecutora",
    "ejecutora_nombre",
    "departamento_ejecutora",
    "departamento_ejecutora_nombre",
    "provincia_ejecutora",
    "provincia_ejecutora_nombre",
    "distrito_ejecutora",
    "distrito_ejecutora_nombre",
    "fuente_financiamiento",
    "fuente_financiamiento_nombre",
    "rubro",
    "rubro_nombre",
    "tipo_recurso",
    "tipo_recurso_nombre",
    "generica",
    "generica_nombre",
    "subgenerica",
    "subgenerica_nombre",
    "subgenerica_det",
    "subgenerica_det_nombre",
    "especifica",
    "especifica_nombre",
    "especifica_det",
    "especifica_det_nombre",
    "monto_pia",
    "monto_pim",
    "monto_recaudado",
]

BRONZE_METADATA_COLUMNS = [
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_year",
    "bronze_source_granularity",
    "bronze_processed_at_utc",
]

BRONZE_COLUMN_MAPPING = {
    "nivel_gobierno": "nivel_gobierno_codigo",
    "nivel_gobierno_nombre": "nivel_gobierno_nombre",
    "sector": "sector_codigo",
    "sector_nombre": "sector_nombre",
    "pliego": "pliego_codigo",
    "pliego_nombre": "pliego_nombre",
    "sec_ejec": "sec_ejec",
    "ejecutora": "ejecutora_codigo",
    "ejecutora_nombre": "ejecutora_nombre",
    "departamento_ejecutora": "departamento_codigo",
    "departamento_ejecutora_nombre": "departamento_nombre",
    "provincia_ejecutora": "provincia_codigo",
    "provincia_ejecutora_nombre": "provincia_nombre",
    "distrito_ejecutora": "distrito_codigo",
    "distrito_ejecutora_nombre": "distrito_nombre",
    "fuente_financiamiento": "fuente_financiamiento_codigo",
    "fuente_financiamiento_nombre": "fuente_financiamiento_nombre",
    "rubro": "rubro_codigo",
    "rubro_nombre": "rubro_nombre",
    "tipo_recurso": "tipo_recurso_codigo",
    "tipo_recurso_nombre": "tipo_recurso_nombre",
    "generica": "generica_codigo",
    "generica_nombre": "generica_nombre",
    "subgenerica": "subgenerica_codigo",
    "subgenerica_nombre": "subgenerica_nombre",
    "subgenerica_det": "subgenerica_det_codigo",
    "subgenerica_det_nombre": "subgenerica_det_nombre",
    "especifica": "especifica_codigo",
    "especifica_nombre": "especifica_nombre",
    "especifica_det": "especifica_det_codigo",
    "especifica_det_nombre": "especifica_det_nombre",
}

FIXED_WIDTH_CODES = {
    "departamento_codigo": 2,
    "provincia_codigo": 2,
    "distrito_codigo": 2,
}


class SilverTransformError(Exception):
    """Error controlado durante la transformacion Silver de SIAF."""


@dataclass(frozen=True)
class SilverResource:
    """Recurso SIAF seleccionado para transformacion Silver."""

    resource_key: str
    bronze_path: Path
    silver_path: Path
    year: int | None
    granularity: str


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_siaf_income_config() -> dict[str, Any]:
    """Carga la configuracion de la fuente SIAF."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise SilverTransformError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise SilverTransformError(f"La fuente '{SOURCE_NAME}' no esta habilitada.")

    return source_config


def is_transformable_resource(resource: dict[str, Any]) -> bool:
    """Indica si un recurso configurado corresponde a una tabla SIAF transformable."""

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
    """Selecciona recursos Bronze que se transformaran hacia Silver."""

    configured_resources = source_config.get("candidate_resources", {})

    if not isinstance(configured_resources, dict) or not configured_resources:
        raise SilverTransformError("No existen recursos SIAF configurados.")

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
        selected_resources.append(
            SilverResource(
                resource_key=resource_key,
                bronze_path=resolved_bronze_dir / f"resource_key={resource_key}",
                silver_path=resolved_silver_dir / f"resource_key={resource_key}",
                year=resource_year if isinstance(resource_year, int) else None,
                granularity=str(resource.get("granularity") or "unknown"),
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
                f"Recursos SIAF no validos para Silver: {missing_keys}. "
                f"Recursos disponibles: {available_keys}."
            )

    if not selected_resources:
        raise SilverTransformError("No se selecciono ningun recurso SIAF para Silver.")

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
            "Faltan recursos SIAF en Bronze para construir Silver: "
            + ", ".join(missing_paths)
        )

    return resources


def require_bronze_columns(columns: list[str], resource: SilverResource) -> None:
    """Valida que el recurso Bronze tenga las columnas requeridas para Silver."""

    missing_columns = sorted(
        set(REQUIRED_BRONZE_COLUMNS + BRONZE_METADATA_COLUMNS) - set(columns)
    )

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
            "final_columns": FINAL_COLUMNS,
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

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    selected_columns = []

    for field in dataframe.schema.fields:
        column = F.col(field.name)
        if isinstance(field.dataType, StringType):
            selected_columns.append(F.trim(column).alias(field.name))
        else:
            selected_columns.append(column)

    return dataframe.select(*selected_columns)


def try_cast_integer(column_name: str) -> Any:
    """Castea una columna a entero tolerando texto vacio o mal formado."""

    from pyspark.sql import functions as F

    return F.expr(f"try_cast(nullif(trim(`{column_name}`), '') as int)")


def cast_decimal_column(column_name: str) -> Any:
    """Castea una columna monetaria a decimal preservando precision."""

    from pyspark.sql import functions as F

    return F.expr(
        f"try_cast(regexp_replace(nullif(trim(`{column_name}`), ''), ',', '') as decimal(20,4))"
    )


def normalize_string_code(column_name: str, *, width: int | None = None) -> Any:
    """Normaliza codigos como string preservando ceros a la izquierda cuando aplica."""

    from pyspark.sql import functions as F

    cleaned = F.when(
        F.trim(F.col(column_name).cast("string")) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name).cast("string")))
    if width is None:
        return cleaned
    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.rlike(r"^[0-9]+$"), F.lpad(cleaned, width, "0"))
        .otherwise(cleaned)
    )


def normalize_string_label(column_name: str) -> Any:
    """Normaliza un campo descriptivo como texto sin forzar cambios semanticos."""

    from pyspark.sql import functions as F

    return F.when(
        F.trim(F.col(column_name).cast("string")) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name).cast("string")))


def is_parseable_or_blank(original_column: str, parsed_column: str) -> Any:
    """Construye flag de validez para campos opcionales casteados."""

    from pyspark.sql import functions as F

    original_value = F.trim(F.col(original_column).cast("string"))
    return F.col(original_column).isNull() | (original_value == "") | F.col(
        parsed_column
    ).isNotNull()


def nonblank_condition(column_name: str) -> Any:
    """Retorna expresion booleana para texto no nulo ni vacio."""

    from pyspark.sql import functions as F

    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def select_curated_columns(dataframe: Any) -> Any:
    """Proyecta y renombra las columnas de negocio finales de SIAF."""

    transformed = dataframe

    for bronze_column, silver_column in BRONZE_COLUMN_MAPPING.items():
        width = FIXED_WIDTH_CODES.get(silver_column)
        if silver_column.endswith("_nombre"):
            transformed = transformed.withColumn(
                silver_column, normalize_string_label(bronze_column)
            )
        else:
            transformed = transformed.withColumn(
                silver_column,
                normalize_string_code(bronze_column, width=width),
            )

    return transformed


def add_period_columns(dataframe: Any, resource: SilverResource) -> Any:
    """Agrega columnas de periodo y metadatos de origen por recurso."""

    from pyspark.sql import functions as F

    transformed = (
        dataframe.withColumn("anio", try_cast_integer("ano_doc"))
        .withColumn("mes", try_cast_integer("mes_doc"))
        .withColumn("source_resource_key", F.lit(resource.resource_key))
        .withColumn("source_granularity", F.lit(resource.granularity))
    )

    return transformed.withColumn(
        "fecha_mes",
        F.when(
            F.col("anio").isNotNull() & F.col("mes").between(1, 12),
            F.to_date(F.format_string("%04d-%02d-01", F.col("anio"), F.col("mes"))),
        ),
    )


def add_amount_columns(dataframe: Any) -> Any:
    """Parsea montos numericos bajo los nombres finales del contrato Silver."""

    return (
        dataframe.withColumn("_monto_pia_raw", normalize_string_label("monto_pia"))
        .withColumn("_monto_pim_raw", normalize_string_label("monto_pim"))
        .withColumn(
            "_monto_recaudado_raw",
            normalize_string_label("monto_recaudado"),
        )
        .withColumn("monto_pia", cast_decimal_column("monto_pia"))
        .withColumn("monto_pim", cast_decimal_column("monto_pim"))
        .withColumn("monto_recaudado", cast_decimal_column("monto_recaudado"))
    )


def add_derived_columns(dataframe: Any, resource: SilverResource) -> Any:
    """Agrega ubigeo, flags de negocio tecnico y metadata Silver."""

    from pyspark.sql import functions as F

    transformed = (
        dataframe.withColumn(
            "ubigeo6_ejecutora",
            F.when(
                nonblank_condition("departamento_codigo")
                & nonblank_condition("provincia_codigo")
                & nonblank_condition("distrito_codigo"),
                F.concat(
                    F.col("departamento_codigo"),
                    F.col("provincia_codigo"),
                    F.col("distrito_codigo"),
                ),
            ),
        )
        .withColumn(
            "is_municipal_government",
            F.col("nivel_gobierno_codigo") == F.lit("M"),
        )
        .withColumn(
            "is_valid_anio",
            F.col("anio").between(2010, 2030)
            & (
                F.lit(resource.year).isNull()
                | (F.col("anio") == F.lit(resource.year))
            ),
        )
        .withColumn(
            "is_valid_mes",
            F.col("mes").between(1, 12),
        )
        .withColumn("is_valid_sec_ejec", nonblank_condition("sec_ejec"))
        .withColumn(
            "is_valid_ubigeo6_ejecutora",
            F.coalesce(
                F.col("ubigeo6_ejecutora").rlike(r"^[0-9]{6}$"),
                F.lit(False),
            ),
        )
        .withColumn(
            "is_valid_monto_pia",
            is_parseable_or_blank("_monto_pia_raw", "monto_pia"),
        )
        .withColumn(
            "is_valid_monto_pim",
            is_parseable_or_blank("_monto_pim_raw", "monto_pim"),
        )
        .withColumn(
            "is_valid_monto_recaudado",
            is_parseable_or_blank("_monto_recaudado_raw", "monto_recaudado"),
        )
        .withColumn(
            "flag_pim_menor_pia",
            F.when(
                F.col("monto_pim").isNotNull() & F.col("monto_pia").isNotNull(),
                F.col("monto_pim") < F.col("monto_pia"),
            ),
        )
        .withColumn(
            "flag_recaudado_mayor_pim",
            F.when(
                F.col("monto_recaudado").isNotNull() & F.col("monto_pim").isNotNull(),
                F.col("monto_recaudado") > F.col("monto_pim"),
            ),
        )
        .withColumn(
            "has_complete_territory",
            nonblank_condition("departamento_codigo")
            & nonblank_condition("departamento_nombre")
            & nonblank_condition("provincia_codigo")
            & nonblank_condition("provincia_nombre")
            & nonblank_condition("distrito_codigo")
            & nonblank_condition("distrito_nombre"),
        )
    )

    return transformed


def add_silver_metadata(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Agrega metadata tecnica Silver normalizada al nuevo contrato."""

    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("silver_source_name", F.lit(SOURCE_NAME))
        .withColumn("silver_resource_key", F.lit(resource.resource_key))
        .withColumn("silver_processed_at_utc", F.lit(processed_at))
    )


def transform_resource_dataframe(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Aplica la transformacion Silver curada para un recurso SIAF."""

    require_bronze_columns(dataframe.columns, resource)

    transformed = trim_string_columns(dataframe)
    transformed = select_curated_columns(transformed)
    transformed = add_period_columns(transformed, resource)
    transformed = add_amount_columns(transformed)
    transformed = add_derived_columns(transformed, resource)
    transformed = add_silver_metadata(
        dataframe=transformed,
        resource=resource,
        processed_at=processed_at,
    )

    return transformed.select(*FINAL_COLUMNS)


def write_resource_silver(
    *,
    spark: Any,
    resource: SilverResource,
    processed_at: str,
    overwrite: bool,
    limit: int | None,
) -> None:
    """Transforma y escribe un recurso SIAF en Parquet Silver."""

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
    """Transforma SIAF hacia Silver o retorna un resumen de dry-run."""

    validate_bronze_inputs(resources)

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    spark = build_spark_session(app_name="SilverSIAFIncome")

    try:
        if dry_run:
            return build_dry_run_summary(spark=spark, resources=resources, limit=limit)

        processed_at = utc_now_iso()
        summary: list[dict[str, Any]] = []

        for resource in resources:
            logger.info(
                "Transformando recurso Silver SIAF %s desde %s",
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
    """Procesa los argumentos de linea de comandos."""

    parser = argparse.ArgumentParser(
        description="Transforma SIAF ingresos desde Bronze hacia Silver curado."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help="Clave de recurso SIAF a transformar. Puede repetirse.",
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
        help="Limita filas por recurso para pruebas locales.",
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
    print(f"Columnas finales previstas: {', '.join(FINAL_COLUMNS)}")

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
        print("Dry-run finalizado. No se escribio Parquet ni se toco data/silver.")
    else:
        print("Transformacion Silver SIAF finalizada.")


if __name__ == "__main__":
    main()
