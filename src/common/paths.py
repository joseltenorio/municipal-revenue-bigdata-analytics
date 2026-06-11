"""Definiciones de rutas de proyecto.

Este módulo centraliza las rutas de proyecto locales mediante pathlib. 
Las rutas internas de Lakehouse no se cargan desde archivos .env porque 
se derivan de la raíz del repositorio y deben ser portátiles entre máquinas locales.
"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
QUALITY_DIR = DATA_DIR / "quality"

LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"
EVIDENCE_DIR = PROJECT_ROOT / "evidence"
CONFIG_DIR = PROJECT_ROOT / "config"
DOCS_DIR = PROJECT_ROOT / "docs"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
POWERBI_DIR = PROJECT_ROOT / "powerbi"
SQL_DIR = PROJECT_ROOT / "sql"
TESTS_DIR = PROJECT_ROOT / "tests"


PROJECT_DIRECTORIES = [
    DATA_DIR,
    LANDING_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    QUALITY_DIR,
    LOGS_DIR,
    REPORTS_DIR,
    EVIDENCE_DIR,
    CONFIG_DIR,
    DOCS_DIR,
    NOTEBOOKS_DIR,
    POWERBI_DIR,
    SQL_DIR,
    TESTS_DIR,
]


def create_project_directories() -> None:
    """Crea los directorios locales estándar utilizados en el proyecto."""
    for directory in PROJECT_DIRECTORIES:
        directory.mkdir(parents=True, exist_ok=True)


def get_layer_path(layer_name: str) -> Path:
    """Devuelve la ruta local asociada a una capa de datos de Medallion."""

    layers = {
        "landing": LANDING_DIR,
        "bronze": BRONZE_DIR,
        "silver": SILVER_DIR,
        "gold": GOLD_DIR,
        "quality": QUALITY_DIR,
    }

    normalized_layer_name = layer_name.strip().lower()

    if normalized_layer_name not in layers:
        supported_layers = ", ".join(sorted(layers))
        raise ValueError(
            f"Unsupported layer '{layer_name}'. "
            f"Supported layers are: {supported_layers}."
        )

    return layers[normalized_layer_name]