"""Pruebas de rutas locales del proyecto."""

from pathlib import Path

import pytest

from src.common.paths import (
    BRONZE_DIR,
    DATA_DIR,
    GOLD_DIR,
    LANDING_DIR,
    PROJECT_ROOT,
    QUALITY_DIR,
    SILVER_DIR,
    get_layer_path,
    get_source_bronze_path,
    get_source_landing_path,
)


def test_project_root_exists() -> None:
    """Valida que PROJECT_ROOT apunte a la raíz del repositorio."""

    assert PROJECT_ROOT.exists()
    assert (PROJECT_ROOT / "README.md").exists()


def test_data_layer_paths_are_inside_data_dir() -> None:
    """Valida que las capas principales estén dentro de data/."""

    assert LANDING_DIR.parent == DATA_DIR
    assert BRONZE_DIR.parent == DATA_DIR
    assert SILVER_DIR.parent == DATA_DIR
    assert GOLD_DIR.parent == DATA_DIR
    assert QUALITY_DIR.parent == DATA_DIR


def test_get_layer_path_returns_expected_paths() -> None:
    """Valida resolución de rutas por nombre de capa."""
    
    assert get_layer_path("landing") == LANDING_DIR
    assert get_layer_path("bronze") == BRONZE_DIR
    assert get_layer_path("silver") == SILVER_DIR
    assert get_layer_path("gold") == GOLD_DIR
    assert get_layer_path("quality") == QUALITY_DIR


def test_get_layer_path_is_case_insensitive() -> None:
    """Valida que el nombre de capa no dependa de mayúsculas."""

    assert get_layer_path(" Bronze ") == BRONZE_DIR


def test_get_layer_path_rejects_invalid_layer() -> None:
    """Valida error para capas no soportadas."""

    with pytest.raises(ValueError):
        get_layer_path("raw")


def test_source_layer_paths() -> None:
    """Valida rutas específicas por fuente."""

    assert get_source_landing_path("siaf_income") == LANDING_DIR / "siaf_income"
    assert get_source_bronze_path("siaf_income") == BRONZE_DIR / "siaf_income"


def test_paths_are_path_objects() -> None:
    """Valida que las rutas usen pathlib.Path."""

    assert isinstance(PROJECT_ROOT, Path)
    assert isinstance(LANDING_DIR, Path)