"""Definición centralizada de rutas locales del proyecto.

Este módulo concentra las rutas internas del repositorio usando pathlib. Las
rutas de capas de datos no se cargan desde .env porque forman parte de la
estructura local del proyecto y pueden derivarse desde PROJECT_ROOT.
"""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"

LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUALITY_DIR = DATA_DIR / "quality"

LOGS_DIR = PROJECT_ROOT / "logs"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
SQL_DIR = PROJECT_ROOT / "sql"
DOCS_DIR = PROJECT_ROOT / "docs"
POWERBI_DIR = PROJECT_ROOT / "powerbi"
REPORTS_DIR = PROJECT_ROOT / "reports"
EVIDENCE_DIR = PROJECT_ROOT / "evidence"
TESTS_DIR = PROJECT_ROOT / "tests"


PROJECT_DIRECTORIES = [
    CONFIG_DIR,
    DATA_DIR,
    LANDING_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    QUALITY_DIR,
    LOGS_DIR,
    NOTEBOOKS_DIR,
    SQL_DIR,
    DOCS_DIR,
    POWERBI_DIR,
    REPORTS_DIR,
    EVIDENCE_DIR,
    TESTS_DIR,
]


DATA_LAYER_PATHS = {
    "landing": LANDING_DIR,
    "bronze": BRONZE_DIR,
    "silver": SILVER_DIR,
    "gold": GOLD_DIR,
    "quality": QUALITY_DIR,
}


def create_project_directories() -> None:
    """Crea las carpetas estándar del proyecto si no existen."""

    for directory in PROJECT_DIRECTORIES:
        directory.mkdir(parents=True, exist_ok=True)


def get_layer_path(layer_name: str) -> Path:
    """Devuelve la ruta local asociada a una capa Medallion.

    Parámetros
    ----------
    layer_name:
        Nombre lógico de la capa. Valores soportados: landing, bronze,
        silver, gold y quality.

    Retorna
    -------
    pathlib.Path
        Ruta local de la capa solicitada.

    Lanza
    -----
    ValueError
        Si la capa solicitada no está soportada.
    """

    normalized_layer_name = layer_name.strip().lower()

    if normalized_layer_name not in DATA_LAYER_PATHS:
        supported_layers = ", ".join(sorted(DATA_LAYER_PATHS))
        raise ValueError(
            f"Capa no soportada: '{layer_name}'. "
            f"Capas soportadas: {supported_layers}."
        )

    return DATA_LAYER_PATHS[normalized_layer_name]


def get_source_landing_path(source_name: str) -> Path:
    """Devuelve la ruta Landing para una fuente específica."""

    normalized_source_name = source_name.strip().lower()
    return LANDING_DIR / normalized_source_name


def get_source_bronze_path(source_name: str) -> Path:
    """Devuelve la ruta Bronze para una fuente específica."""

    normalized_source_name = source_name.strip().lower()
    return BRONZE_DIR / normalized_source_name


def get_source_silver_path(source_name: str) -> Path:
    """Devuelve la ruta Silver para una fuente específica."""

    normalized_source_name = source_name.strip().lower()
    return SILVER_DIR / normalized_source_name


def get_source_gold_path(subject_area: str) -> Path:
    """Devuelve la ruta Gold para un área analítica específica."""

    normalized_subject_area = subject_area.strip().lower()
    return GOLD_DIR / normalized_subject_area