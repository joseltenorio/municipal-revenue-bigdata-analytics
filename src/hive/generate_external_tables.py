"""Genera SQL Hive para tablas externas sobre Parquet del lakehouse.

El generador inspecciona Parquet existentes con Spark y escribe DDL para Hive.
No ejecuta Beeline, no modifica Parquet y no crea tablas Gold mientras no
existan marts Gold.
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from src.common.paths import BRONZE_DIR, GOLD_DIR, PROJECT_ROOT, SILVER_DIR, SQL_DIR


HIVE_PROJECT_ROOT = "/app"
HIVE_SQL_DIR = SQL_DIR / "hive"
BRONZE_SQL_PATH = HIVE_SQL_DIR / "create_bronze_external_tables.sql"
SILVER_SQL_PATH = HIVE_SQL_DIR / "create_silver_external_tables.sql"
GOLD_SQL_PATH = HIVE_SQL_DIR / "create_gold_external_tables.sql"


class HiveDdlError(Exception):
    """Error controlado durante generación de DDL Hive."""


@dataclass(frozen=True)
class ExternalTableSpec:
    """Definición de tabla externa a registrar en Hive."""

    database: str
    table_name: str
    dataset_path: Path
    hive_location: str


def normalize_hive_identifier(value: str) -> str:
    """Normaliza nombres de tabla a identificadores seguros para Hive."""

    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower())
    normalized = normalized.strip("_")
    if not normalized:
        raise HiveDdlError("El identificador Hive no puede quedar vacío.")
    if normalized[0].isdigit():
        normalized = f"t_{normalized}"
    return normalized


def quote_identifier(identifier: str) -> str:
    """Escapa identificadores Hive con backticks."""

    return f"`{identifier.replace('`', '``')}`"


def quote_table(database: str, table_name: str) -> str:
    """Escapa nombre completo database.table."""

    return f"{quote_identifier(database)}.{quote_identifier(table_name)}"


def spark_type_to_hive_type(spark_type: str) -> str:
    """Traduce un tipo Spark simple a tipo Hive."""

    normalized = spark_type.strip().lower()

    if normalized in {"string", "varchar", "char"}:
        return "STRING"
    if normalized in {"int", "integer"}:
        return "INT"
    if normalized in {"bigint", "long"}:
        return "BIGINT"
    if normalized == "double":
        return "DOUBLE"
    if normalized == "float":
        return "FLOAT"
    if normalized == "boolean":
        return "BOOLEAN"
    if normalized == "date":
        return "DATE"
    if normalized.startswith("timestamp"):
        return "TIMESTAMP"

    decimal_match = re.fullmatch(r"decimal\((\d+),(\d+)\)", normalized)
    if decimal_match:
        precision, scale = decimal_match.groups()
        return f"DECIMAL({precision},{scale})"

    return "STRING"


def validate_hive_location(location: str) -> None:
    """Valida que LOCATION sea una ruta absoluta del contenedor."""

    if re.match(r"^[a-zA-Z]:[\\/]", location):
        raise HiveDdlError(f"No se permiten rutas Windows en Hive LOCATION: {location}")
    if not location.startswith("/app/data/"):
        raise HiveDdlError(
            "Hive LOCATION debe apuntar al montaje del proyecto en /app/data: "
            f"{location}"
        )


def project_path_to_hive_location(path: Path) -> str:
    """Convierte una ruta local del repo a ruta visible desde HiveServer2."""

    resolved_path = path.resolve()
    try:
        relative_path = resolved_path.relative_to(PROJECT_ROOT.resolve())
    except ValueError as exc:
        raise HiveDdlError(f"La ruta no pertenece al proyecto: {path}") from exc

    hive_location = f"{HIVE_PROJECT_ROOT}/{relative_path.as_posix()}"
    validate_hive_location(hive_location)
    return hive_location


def parquet_files_exist(path: Path) -> bool:
    """Indica si una carpeta contiene archivos Parquet."""

    return path.exists() and any(file_path.is_file() for file_path in path.rglob("*.parquet"))


def list_resource_paths(source_path: Path) -> list[Path]:
    """Lista carpetas resource_key con Parquet."""

    return sorted(
        path
        for path in source_path.glob("resource_key=*")
        if path.is_dir() and parquet_files_exist(path)
    )


def discover_bronze_tables() -> list[ExternalTableSpec]:
    """Descubre recursos Bronze existentes."""

    specs: list[ExternalTableSpec] = []
    for source_path in sorted(path for path in BRONZE_DIR.iterdir() if path.is_dir()):
        source_name = normalize_hive_identifier(source_path.name)
        if parquet_files_exist(source_path):
            specs.append(
                ExternalTableSpec(
                    database="bronze",
                    table_name=source_name,
                    dataset_path=source_path,
                    hive_location=project_path_to_hive_location(source_path),
                )
            )
            continue
        for resource_path in list_resource_paths(source_path):
            resource_key = normalize_hive_identifier(
                resource_path.name.replace("resource_key=", "")
            )
            table_name = normalize_hive_identifier(f"{source_name}__{resource_key}")
            specs.append(
                ExternalTableSpec(
                    database="bronze",
                    table_name=table_name,
                    dataset_path=resource_path,
                    hive_location=project_path_to_hive_location(resource_path),
                )
            )
    return specs


def discover_silver_source_tables() -> list[ExternalTableSpec]:
    """Descubre recursos Silver por fuente, excluyendo integrados."""

    specs: list[ExternalTableSpec] = []
    for source_path in sorted(path for path in SILVER_DIR.iterdir() if path.is_dir()):
        if source_path.name == "integrated":
            continue
        source_name = normalize_hive_identifier(source_path.name)
        for resource_path in list_resource_paths(source_path):
            resource_key = normalize_hive_identifier(
                resource_path.name.replace("resource_key=", "")
            )
            table_name = normalize_hive_identifier(f"{source_name}__{resource_key}")
            specs.append(
                ExternalTableSpec(
                    database="silver",
                    table_name=table_name,
                    dataset_path=resource_path,
                    hive_location=project_path_to_hive_location(resource_path),
                )
            )
    return specs


def discover_silver_integrated_tables() -> list[ExternalTableSpec]:
    """Descubre datasets integrados Silver."""

    integrated_path = SILVER_DIR / "integrated"
    if not integrated_path.exists():
        return []

    specs: list[ExternalTableSpec] = []
    for dataset_path in sorted(path for path in integrated_path.iterdir() if path.is_dir()):
        if not parquet_files_exist(dataset_path):
            continue
        specs.append(
            ExternalTableSpec(
                database="silver",
                table_name=normalize_hive_identifier(dataset_path.name),
                dataset_path=dataset_path,
                hive_location=project_path_to_hive_location(dataset_path),
            )
        )
    return specs


def discover_gold_tables() -> list[ExternalTableSpec]:
    """Descubre recursos Gold directamente bajo la carpeta Gold."""

    if not GOLD_DIR.exists():
        return []

    specs: list[ExternalTableSpec] = []
    for dataset_path in sorted(path for path in GOLD_DIR.iterdir() if path.is_dir()):
        if not parquet_files_exist(dataset_path):
            continue
        specs.append(
            ExternalTableSpec(
                database="gold",
                table_name=normalize_hive_identifier(dataset_path.name),
                dataset_path=dataset_path,
                hive_location=project_path_to_hive_location(dataset_path),
            )
        )
    return specs


def validate_discovered_tables(
    bronze_specs: list[ExternalTableSpec],
    silver_specs: list[ExternalTableSpec],
    gold_specs: list[ExternalTableSpec] | None = None,
) -> None:
    """Valida que existan tablas esperadas mínimas."""

    if not bronze_specs:
        raise HiveDdlError("No se encontraron Parquet Bronze para registrar.")
    if not silver_specs:
        raise HiveDdlError("No se encontraron Parquet Silver para registrar.")
    if gold_specs is not None and not gold_specs:
        raise HiveDdlError("No se encontraron Parquet Gold para registrar.")


def schema_to_hive_columns(schema: Any) -> list[tuple[str, str]]:
    """Convierte schema Spark a columnas Hive."""

    return [
        (field.name, spark_type_to_hive_type(field.dataType.simpleString()))
        for field in schema.fields
    ]


def render_create_external_table(
    spec: ExternalTableSpec,
    columns: Iterable[tuple[str, str]],
) -> str:
    """Renderiza sentencia CREATE EXTERNAL TABLE."""

    validate_hive_location(spec.hive_location)
    column_lines = [
        f"  {quote_identifier(column_name)} {hive_type}"
        for column_name, hive_type in columns
    ]
    if not column_lines:
        raise HiveDdlError(f"La tabla {spec.table_name} no tiene columnas.")

    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {quote_table(spec.database, spec.table_name)} (\n"
        + ",\n".join(column_lines)
        + "\n)\n"
        + "STORED AS PARQUET\n"
        + f"LOCATION '{spec.hive_location}';\n"
    )


def render_sql_file(
    *,
    spark: Any,
    specs: list[ExternalTableSpec],
    title: str,
) -> str:
    """Genera contenido SQL para una lista de tablas externas."""

    statements = [
        f"-- {title}",
        "-- Generated from existing Parquet datasets.",
        "-- Do not edit data files from Hive; these are external lakehouse tables.",
        "",
    ]

    for spec in specs:
        dataframe = spark.read.parquet(str(spec.dataset_path))
        statements.append(
            render_create_external_table(
                spec,
                schema_to_hive_columns(dataframe.schema),
            )
        )

    return "\n".join(statements).rstrip() + "\n"


def write_sql_file(path: Path, content: str, overwrite_sql: bool) -> None:
    """Escribe un archivo SQL respetando --overwrite-sql."""

    if path.exists() and not overwrite_sql:
        raise HiveDdlError(
            f"Ya existe {path}. Usa --overwrite-sql para regenerarlo."
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def print_plan(
    bronze_specs: list[ExternalTableSpec],
    silver_specs: list[ExternalTableSpec],
    gold_specs: list[ExternalTableSpec],
) -> None:
    """Imprime plan de generación sin escribir SQL."""

    print("=" * 80)
    print("Plan de tablas externas Hive")
    print(f"Tablas Bronze a generar: {len(bronze_specs)}")
    for spec in bronze_specs:
        print(f"- {spec.database}.{spec.table_name} -> {spec.hive_location}")
    print(f"Tablas Silver a generar: {len(silver_specs)}")
    for spec in silver_specs:
        print(f"- {spec.database}.{spec.table_name} -> {spec.hive_location}")
    print(f"Tablas Gold a generar: {len(gold_specs)}")
    for spec in gold_specs:
        print(f"- {spec.database}.{spec.table_name} -> {spec.hive_location}")


def generate_external_table_sql(
    *,
    dry_run: bool,
    overwrite_sql: bool,
    validate_inputs: bool,
) -> dict[str, Any]:
    """Genera o planifica SQL Hive de tablas externas."""

    bronze_specs = discover_bronze_tables()
    silver_specs = [
        *discover_silver_source_tables(),
        *discover_silver_integrated_tables(),
    ]
    gold_specs = discover_gold_tables()

    if validate_inputs:
        validate_discovered_tables(bronze_specs, silver_specs, gold_specs)

    if dry_run:
        print_plan(bronze_specs, silver_specs, gold_specs)
        return {
            "bronze_tables": len(bronze_specs),
            "silver_tables": len(silver_specs),
            "gold_tables": len(gold_specs),
            "written_files": [],
        }

    from src.common.spark_session import build_spark_session

    spark = build_spark_session(
        app_name="GenerateHiveExternalTables",
        master="local[2]",
        extra_configs={
            "spark.sql.shuffle.partitions": "4",
            "spark.hadoop.fs.file.impl": "org.apache.hadoop.fs.RawLocalFileSystem",
        },
    )
    try:
        bronze_sql = render_sql_file(
            spark=spark,
            specs=bronze_specs,
            title="Bronze external tables",
        )
        silver_sql = render_sql_file(
            spark=spark,
            specs=silver_specs,
            title="Silver external tables",
        )
        gold_sql = render_sql_file(
            spark=spark,
            specs=gold_specs,
            title="Gold external tables",
        )
    finally:
        spark.stop()

    write_sql_file(BRONZE_SQL_PATH, bronze_sql, overwrite_sql=overwrite_sql)
    write_sql_file(SILVER_SQL_PATH, silver_sql, overwrite_sql=overwrite_sql)
    write_sql_file(GOLD_SQL_PATH, gold_sql, overwrite_sql=overwrite_sql)

    print("=" * 80)
    print("SQL Hive generado")
    print(f"- {BRONZE_SQL_PATH}")
    print(f"- {SILVER_SQL_PATH}")
    print(f"- {GOLD_SQL_PATH}")
    print(f"Tablas Bronze: {len(bronze_specs)}")
    print(f"Tablas Silver: {len(silver_specs)}")
    print(f"Tablas Gold: {len(gold_specs)}")

    return {
        "bronze_tables": len(bronze_specs),
        "silver_tables": len(silver_specs),
        "gold_tables": len(gold_specs),
        "written_files": [str(BRONZE_SQL_PATH), str(SILVER_SQL_PATH), str(GOLD_SQL_PATH)],
    }


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Genera DDL Hive para tablas externas sobre Parquet del lakehouse."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra tablas a generar sin escribir SQL.",
    )
    parser.add_argument(
        "--overwrite-sql",
        action="store_true",
        help="Sobrescribe SQL generado si ya existe.",
    )
    parser.add_argument(
        "--validate-inputs",
        action="store_true",
        help="Falla si no existen datasets mínimos Bronze/Silver.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    generate_external_table_sql(
        dry_run=args.dry_run,
        overwrite_sql=args.overwrite_sql,
        validate_inputs=args.validate_inputs,
    )


if __name__ == "__main__":
    main()
