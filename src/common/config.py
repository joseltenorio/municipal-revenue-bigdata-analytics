"""Lectura de archivos de configuración YAML del proyecto."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.common.paths import CONFIG_DIR


class ConfigError(Exception):
    """Error relacionado con lectura o validación de configuración."""


def load_yaml_config(config_path: str | Path) -> dict[str, Any]:
    """Carga un archivo YAML y devuelve su contenido como diccionario.

    Parámetros
    ----------
    config_path:
        Ruta absoluta o relativa del archivo YAML.

    Retorna
    -------
    dict[str, Any]
        Contenido del archivo YAML.

    Lanza
    -----
    ConfigError
        Si el archivo no existe, no es YAML válido o no contiene un diccionario.
    """

    path = Path(config_path)

    if not path.is_absolute():
        path = CONFIG_DIR / path

    if not path.exists():
        raise ConfigError(f"No existe el archivo de configuración: {path}")

    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"El archivo YAML no es válido: {path}") from exc

    if content is None:
        return {}

    if not isinstance(content, dict):
        raise ConfigError(
            f"El archivo de configuración debe contener un diccionario: {path}"
        )

    return content


def get_config_value(
    config: dict[str, Any],
    dotted_key: str,
    default: Any | None = None,
) -> Any:
    """Obtiene un valor anidado desde un diccionario usando notación con puntos.

    Ejemplo:
    get_config_value(config, "spark.app_name")
    """

    current_value: Any = config

    for key in dotted_key.split("."):
        if not isinstance(current_value, dict) or key not in current_value:
            return default
        current_value = current_value[key]

    return current_value


def load_sources_config() -> dict[str, Any]:
    """Carga la configuración de fuentes."""

    return load_yaml_config("sources.yaml")


def load_spark_config() -> dict[str, Any]:
    """Carga la configuración de Spark."""

    return load_yaml_config("spark.yaml")


def load_hive_config() -> dict[str, Any]:
    """Carga la configuración de Hive."""

    return load_yaml_config("hive.yaml")


def load_retry_policy_config() -> dict[str, Any]:
    """Carga la configuración de reintentos."""

    return load_yaml_config("retry_policy.yaml")


def load_audit_config() -> dict[str, Any]:
    """Carga la configuración de auditoría."""

    return load_yaml_config("audit.yaml")


def load_quality_rules_config() -> dict[str, Any]:
    """Carga la configuración de reglas de calidad."""
    
    return load_yaml_config("quality_rules.yaml")