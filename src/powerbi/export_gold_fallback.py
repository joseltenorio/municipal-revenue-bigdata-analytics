"""Script de exportación de contingencia para Power BI.

Permite leer las tablas Gold en formato Parquet desde el lakehouse local
y exportarlas a formato CSV UTF-8 sin índice bajo 'powerbi/exports/'.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
import pandas as pd

from src.common.logger import get_logger
from src.common.paths import GOLD_DIR, POWERBI_DIR

logger = get_logger("export_gold_fallback")

RECOMMENDED_TABLES = {
    "mart_municipal_revenue_overview": "municipal_revenue/mart_municipal_revenue_overview",
    "mart_predial_compliance_overview": "predial_compliance/mart_predial_compliance_overview",
    "mart_predial_ranking": "predial_compliance/mart_predial_ranking",
    "mart_municipal_capacity": "territorial_context/mart_municipal_capacity",
    "mart_territorial_context": "territorial_context/mart_territorial_context",
    "dim_geography": "territorial_context/dim_geography",
    "dim_time": "municipal_revenue/dim_time",
    "dim_municipality": "municipal_revenue/dim_municipality",
    "dim_predial_period": "predial_compliance/dim_predial_period",
}


def export_tables(dry_run: bool = False, overwrite: bool = False) -> None:
    """Exporta las tablas Gold recomendadas a CSV.

    Parámetros
    ----------
    dry_run: bool
        Si es True, solo imprime el origen y destino por tabla sin escribir.
    overwrite: bool
        Si es True, sobrescribe los archivos CSV existentes.
    """
    exports_dir = POWERBI_DIR / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Iniciando exportación de fallback de la capa Gold a CSV.")
    logger.info(f"Directorio de destino: {exports_dir}")
    logger.info(f"Modo dry-run: {dry_run}")
    logger.info(f"Permitir sobreescribir: {overwrite}")

    missing_tables = []
    table_paths = {}

    for table_name, rel_path in RECOMMENDED_TABLES.items():
        src_path = GOLD_DIR / rel_path
        table_paths[table_name] = src_path
        if not src_path.exists() or not any(src_path.rglob("*.parquet")):
            missing_tables.append(table_name)

    if missing_tables:
        msg = f"Faltan las siguientes tablas Gold o no contienen archivos Parquet: {', '.join(missing_tables)}"
        logger.error(msg)
        raise FileNotFoundError(msg)

    if not dry_run and not overwrite:
        existing_files = []
        for table_name in RECOMMENDED_TABLES:
            dest_file = exports_dir / f"{table_name}.csv"
            if dest_file.exists():
                existing_files.append(dest_file.name)
        if existing_files:
            msg = (
                f"Los siguientes archivos ya existen en el destino y no se permite sobreescribir "
                f"(usa --overwrite para habilitar): {', '.join(existing_files)}"
            )
            logger.error(msg)
            raise FileExistsError(msg)

    for table_name, src_path in table_paths.items():
        dest_file = exports_dir / f"{table_name}.csv"
        
        # Formato de impresión solicitado de origen/destino
        print(f"Tabla: {table_name}")
        print(f"  Origen : {src_path.resolve().as_posix()}")
        print(f"  Destino: {dest_file.resolve().as_posix()}")

        if dry_run:
            logger.info(f"[DRY-RUN] Planificado exportar {table_name} a {dest_file.name}")
            continue

        logger.info(f"Exportando {table_name}...")
        try:
            df = pd.read_parquet(src_path, engine="pyarrow")
            df.to_csv(dest_file, index=False, encoding="utf-8")
            logger.info(f"Exportación exitosa: {dest_file.name} ({len(df)} filas)")
        except Exception as e:
            logger.error(f"Error exportando tabla {table_name}: {e}")
            raise

    logger.info("Proceso completado correctamente.")


def main() -> None:
    """Punto de entrada CLI para exportación."""
    parser = argparse.ArgumentParser(
        description="Exporta tablas Gold recomendadas de Parquet a CSV para fallback de Power BI."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Imprime el plan de exportación sin escribir ningún archivo.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe los archivos CSV de destino si ya existen.",
    )
    args = parser.parse_args()

    try:
        export_tables(dry_run=args.dry_run, overwrite=args.overwrite)
    except Exception as e:
        print(f"Error en la ejecución: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
