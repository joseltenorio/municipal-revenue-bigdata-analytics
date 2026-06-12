"""Pruebas unitarias para el script de exportación de contingencia de Power BI."""

from __future__ import annotations

import pytest
from pathlib import Path
import pandas as pd

from src.powerbi.export_gold_fallback import RECOMMENDED_TABLES, export_tables


def test_recommended_tables_dictionary() -> None:
    """Valida que el diccionario de tablas recomendadas contenga los elementos correctos."""
    assert len(RECOMMENDED_TABLES) == 9
    assert "mart_municipal_revenue_overview" in RECOMMENDED_TABLES
    assert "mart_predial_compliance_overview" in RECOMMENDED_TABLES
    assert "mart_predial_ranking" in RECOMMENDED_TABLES
    assert "mart_municipal_capacity" in RECOMMENDED_TABLES
    assert "mart_territorial_context" in RECOMMENDED_TABLES
    assert "dim_geography" in RECOMMENDED_TABLES
    assert "dim_time" in RECOMMENDED_TABLES
    assert "dim_municipality" in RECOMMENDED_TABLES
    assert "dim_predial_period" in RECOMMENDED_TABLES


def test_export_tables_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Valida que dry-run no genere archivos físicos en el destino."""
    gold_mock = tmp_path / "gold"
    powerbi_mock = tmp_path / "powerbi"

    # Crear directorios y parquets falsos
    for table_name, rel_path in RECOMMENDED_TABLES.items():
        table_dir = gold_mock / rel_path
        table_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"dummy": [1, 2, 3]})
        df.to_parquet(table_dir / "data.parquet")

    # Patch de variables de entorno/rutas en el módulo
    monkeypatch.setattr("src.powerbi.export_gold_fallback.GOLD_DIR", gold_mock)
    monkeypatch.setattr("src.powerbi.export_gold_fallback.POWERBI_DIR", powerbi_mock)

    # Ejecutar dry-run
    export_tables(dry_run=True, overwrite=False)

    # Validar que no se crearon CSVs
    dest_dir = powerbi_mock / "exports"
    if dest_dir.exists():
        csv_files = list(dest_dir.glob("*.csv"))
        assert len(csv_files) == 0


def test_export_tables_missing_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Valida que falle con FileNotFoundError si falta alguna tabla de origen."""
    gold_mock = tmp_path / "gold"
    powerbi_mock = tmp_path / "powerbi"

    # No creamos las carpetas Parquet en gold
    monkeypatch.setattr("src.powerbi.export_gold_fallback.GOLD_DIR", gold_mock)
    monkeypatch.setattr("src.powerbi.export_gold_fallback.POWERBI_DIR", powerbi_mock)

    with pytest.raises(FileNotFoundError):
        export_tables(dry_run=True, overwrite=False)


def test_export_tables_no_overwrite_by_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Valida que por defecto (overwrite=False) falle si algún archivo de destino existe."""
    gold_mock = tmp_path / "gold"
    powerbi_mock = tmp_path / "powerbi"

    # Crear parquet origen falsos
    for table_name, rel_path in RECOMMENDED_TABLES.items():
        table_dir = gold_mock / rel_path
        table_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"dummy": [1]})
        df.to_parquet(table_dir / "data.parquet")

    # Crear CSV previo en el destino
    dest_dir = powerbi_mock / "exports"
    dest_dir.mkdir(parents=True, exist_ok=True)
    existing_csv = dest_dir / "mart_municipal_capacity.csv"
    existing_csv.write_text("contenido previo")

    monkeypatch.setattr("src.powerbi.export_gold_fallback.GOLD_DIR", gold_mock)
    monkeypatch.setattr("src.powerbi.export_gold_fallback.POWERBI_DIR", powerbi_mock)

    # Debe lanzar FileExistsError
    with pytest.raises(FileExistsError):
        export_tables(dry_run=False, overwrite=False)


def test_export_tables_success_with_overwrite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Valida exportación exitosa y el soporte de sobreescritura."""
    gold_mock = tmp_path / "gold"
    powerbi_mock = tmp_path / "powerbi"

    # Crear parquet origen falsos con datos conocidos
    for table_name, rel_path in RECOMMENDED_TABLES.items():
        table_dir = gold_mock / rel_path
        table_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame({"col1": [10, 20], "col2": ["x", "y"]})
        df.to_parquet(table_dir / "data.parquet")

    # Crear CSV previo en el destino que debería ser sobreescrito
    dest_dir = powerbi_mock / "exports"
    dest_dir.mkdir(parents=True, exist_ok=True)
    existing_csv = dest_dir / "mart_municipal_capacity.csv"
    existing_csv.write_text("previo")

    monkeypatch.setattr("src.powerbi.export_gold_fallback.GOLD_DIR", gold_mock)
    monkeypatch.setattr("src.powerbi.export_gold_fallback.POWERBI_DIR", powerbi_mock)

    # Ejecutar export real con overwrite=True
    export_tables(dry_run=False, overwrite=True)

    # Validar que todos los archivos se crearon
    for table_name in RECOMMENDED_TABLES:
        dest_file = dest_dir / f"{table_name}.csv"
        assert dest_file.exists()

    # Validar que el archivo sobreescrito contenga los datos del DataFrame
    df_result = pd.read_csv(existing_csv)
    assert len(df_result) == 2
    assert list(df_result.columns) == ["col1", "col2"]
    assert df_result["col1"].iloc[0] == 10
