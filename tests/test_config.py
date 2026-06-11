"""Pruebas de lectura de configuración YAML."""

from pathlib import Path

import pytest

from src.common.config import (
    ConfigError,
    get_config_value,
    load_hive_config,
    load_sources_config,
    load_spark_config,
    load_yaml_config,
)


def test_load_sources_config() -> None:
    """Valida que sources.yaml pueda cargarse."""

    config = load_sources_config()

    assert "project" in config
    assert "sources" in config
    assert "mef_income" in config["sources"]


def test_load_spark_config() -> None:
    """Valida que spark.yaml pueda cargarse."""

    config = load_spark_config()

    assert "spark" in config
    assert get_config_value(config, "spark.app_name") is not None


def test_load_hive_config() -> None:
    """Valida que hive.yaml pueda cargarse."""

    config = load_hive_config()

    assert "hive" in config
    assert get_config_value(config, "hive.port") == 10000


def test_get_config_value_returns_nested_value() -> None:
    """Valida acceso a valores anidados usando notación con puntos."""

    config = {"a": {"b": {"c": 123}}}

    assert get_config_value(config, "a.b.c") == 123


def test_get_config_value_returns_default_when_missing() -> None:
    """Valida valor por defecto cuando la llave no existe."""

    config = {"a": {"b": 1}}

    assert get_config_value(config, "a.x", default="missing") == "missing"


def test_load_yaml_config_rejects_missing_file() -> None:
    """Valida error para archivos inexistentes."""

    with pytest.raises(ConfigError):
        load_yaml_config("archivo_inexistente.yaml")


def test_load_yaml_config_accepts_absolute_path(tmp_path: Path) -> None:
    """Valida carga desde una ruta absoluta."""

    config_file = tmp_path / "sample.yaml"
    config_file.write_text("sample:\n  value: 1\n", encoding="utf-8")

    config = load_yaml_config(config_file)

    assert config["sample"]["value"] == 1