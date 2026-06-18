"""Construye Bronze Parquet para la fuente manual de categorias municipales.

Esta fuente no se descarga desde internet: el CSV es un insumo academico local
versionado de forma controlada en ``data/landing/category``. Bronze conserva la
foto original en Parquet, normaliza nombres de columnas solo a nivel tecnico y
agrega metadata comun de trazabilidad. No resuelve cruces, duplicados de negocio
ni dimensiones analiticas; eso corresponde a Silver/Gold.
"""

from __future__ import annotations

import argparse
import re
import shutil
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.common.config import get_config_value, load_sources_config
from src.common.paths import get_source_bronze_path, get_source_landing_path


SOURCE_NAME = "municipal_categories"
DEFAULT_RESOURCE_KEY = "categorias_municipalidades"
DEFAULT_FILE_NAME = "CategoriasMunicipalidades.csv"
DEFAULT_DELIMITER = ";"


class BronzeCategoryBuildError(Exception):
    """Error controlado durante la construccion Bronze de categorias."""


@dataclass(frozen=True)
class BronzeCategoryResource:
    """Recurso manual de categorias seleccionado para Bronze."""

    resource_key: str
    source_path: Path
    output_path: Path
    file_name: str
    delimiter: str
    access_method: str


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def normalize_column_name(column_name: str) -> str:
    """Normaliza nombres de columnas a snake_case tecnico."""

    normalized = unicodedata.normalize("NFKD", str(column_name))
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "unnamed_column"


def normalize_columns(column_names: list[str]) -> list[str]:
    """Normaliza columnas y evita nombres duplicados."""

    seen: dict[str, int] = {}
    result: list[str] = []
    for column_name in column_names:
        base = normalize_column_name(column_name)
        count = seen.get(base, 0)
        result.append(base if count == 0 else f"{base}_{count + 1}")
        seen[base] = count + 1
    return result


def load_municipal_categories_config() -> dict[str, Any]:
    """Carga la configuracion de la fuente manual de categorias."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise BronzeCategoryBuildError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )
    if not source_config.get("enabled", False):
        raise BronzeCategoryBuildError(f"La fuente '{SOURCE_NAME}' no esta habilitada.")
    return source_config


def select_bronze_resource(
    source_config: dict[str, Any],
    *,
    landing_dir: Path | None = None,
    bronze_dir: Path | None = None,
) -> BronzeCategoryResource:
    """Selecciona el recurso manual de categorias para convertir a Bronze."""

    configured_resources = source_config.get("candidate_resources", {})
    resource = configured_resources.get(DEFAULT_RESOURCE_KEY)
    if not isinstance(resource, dict):
        raise BronzeCategoryBuildError(
            f"No existe el recurso '{DEFAULT_RESOURCE_KEY}' para categorias municipales."
        )

    file_name = str(resource.get("file_name") or DEFAULT_FILE_NAME)
    delimiter = str(resource.get("delimiter") or DEFAULT_DELIMITER)
    landing_subdir = str(source_config.get("landing_subdir") or "category")
    bronze_subdir = str(source_config.get("bronze_subdir") or SOURCE_NAME)
    access_method = str(source_config.get("access_method") or "manual_csv")

    resolved_landing_dir = landing_dir or get_source_landing_path(landing_subdir)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)

    return BronzeCategoryResource(
        resource_key=DEFAULT_RESOURCE_KEY,
        source_path=resolved_landing_dir / file_name,
        output_path=resolved_bronze_dir / f"resource_key={DEFAULT_RESOURCE_KEY}",
        file_name=file_name,
        delimiter=delimiter,
        access_method=access_method,
    )


def validate_landing_input(resource: BronzeCategoryResource) -> None:
    """Valida que exista el CSV manual en Landing."""

    if not resource.source_path.exists():
        raise BronzeCategoryBuildError(f"No existe archivo Landing: {resource.source_path}")
    if resource.source_path.suffix.lower() != ".csv":
        raise BronzeCategoryBuildError(f"El recurso no es CSV: {resource.source_path}")


def read_category_csv(resource: BronzeCategoryResource) -> pd.DataFrame:
    """Lee el CSV de categorias preservando valores como texto."""

    validate_landing_input(resource)
    try:
        return pd.read_csv(
            resource.source_path,
            sep=resource.delimiter,
            dtype="string",
            keep_default_na=False,
            encoding="utf-8-sig",
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            resource.source_path,
            sep=resource.delimiter,
            dtype="string",
            keep_default_na=False,
            encoding="latin-1",
        )


def build_bronze_dataframe(
    raw_df: pd.DataFrame,
    resource: BronzeCategoryResource,
) -> pd.DataFrame:
    """Agrega metadata Bronze comun sin aplicar reglas de negocio."""

    result = raw_df.copy()
    result.columns = normalize_columns([str(column) for column in result.columns])
    processed_at = utc_now_iso()
    result["bronze_source_name"] = SOURCE_NAME
    result["bronze_resource_key"] = resource.resource_key
    result["bronze_source_file_name"] = resource.file_name
    result["bronze_source_file_path"] = resource.source_path.as_posix()
    result["bronze_source_access_method"] = resource.access_method
    result["bronze_source_granularity"] = "manual_snapshot"
    result["bronze_processed_at_utc"] = processed_at
    return result


def write_parquet_dataset(
    df: pd.DataFrame,
    output_path: Path,
    *,
    overwrite: bool = False,
) -> None:
    """Escribe el dataset Bronze como Parquet local."""

    if output_path.exists():
        if not overwrite:
            raise BronzeCategoryBuildError(
                f"Ya existe salida Bronze: {output_path}. Usa --overwrite para reemplazar."
            )
        shutil.rmtree(output_path)

    output_path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path / "part-00000.parquet", index=False)


def build_dry_run_summary(resource: BronzeCategoryResource, raw_df: pd.DataFrame) -> dict[str, Any]:
    """Construye resumen de validacion sin escribir Parquet."""

    return {
        "source_name": SOURCE_NAME,
        "resource_key": resource.resource_key,
        "file_name": resource.file_name,
        "source_path": str(resource.source_path),
        "output_path": str(resource.output_path),
        "delimiter": resource.delimiter,
        "rows_detected": len(raw_df),
        "columns_detected": list(raw_df.columns),
        "source_exists": resource.source_path.exists(),
    }


def build_bronze_municipal_categories(
    *,
    resource: BronzeCategoryResource | None = None,
    dry_run: bool = False,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Ejecuta la conversion Bronze de categorias municipales."""

    if resource is None:
        source_config = load_municipal_categories_config()
        resource = select_bronze_resource(source_config)

    raw_df = read_category_csv(resource)
    summary = build_dry_run_summary(resource, raw_df)

    if dry_run:
        return summary

    bronze_df = build_bronze_dataframe(raw_df, resource)
    write_parquet_dataset(bronze_df, resource.output_path, overwrite=overwrite)
    summary["written"] = True
    summary["parquet_file"] = str(resource.output_path / "part-00000.parquet")
    return summary


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Construye Bronze para la fuente manual de categorias municipales."
    )
    parser.add_argument("--dry-run", action="store_true", help="Valida sin escribir Parquet.")
    parser.add_argument("--overwrite", action="store_true", help="Sobrescribe salida Bronze existente.")
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    summary = build_bronze_municipal_categories(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
    )

    print("=" * 80)
    print("Bronze categorias municipales")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Fuente: {summary['source_name']}")
    print(f"Recurso: {summary['resource_key']}")
    print(f"Archivo Landing: {summary['source_path']}")
    print(f"Salida Bronze: {summary['output_path']}")
    print(f"Filas detectadas: {summary['rows_detected']}")
    print(f"Columnas detectadas: {', '.join(summary['columns_detected'])}")

    if args.dry_run:
        print("Dry-run finalizado. No se escribio Parquet ni se toco data/bronze.")
    else:
        print("Conversion Bronze de categorias municipales finalizada.")


if __name__ == "__main__":
    main()

