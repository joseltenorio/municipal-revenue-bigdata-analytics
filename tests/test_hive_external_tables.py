"""Pruebas unitarias para generación de tablas externas Hive."""

from pathlib import Path

import pytest

from src.hive.generate_external_tables import (
    ExternalTableSpec,
    HiveDdlError,
    normalize_hive_identifier,
    project_path_to_hive_location,
    quote_identifier,
    render_create_external_table,
    spark_type_to_hive_type,
    validate_hive_location,
)


def test_normalize_hive_identifier() -> None:
    """Los nombres de tabla se normalizan a identificadores seguros."""

    assert normalize_hive_identifier("MEF Income annual-2012") == "mef_income_annual_2012"
    assert normalize_hive_identifier("mef_income__annual_2012") == "mef_income__annual_2012"
    assert normalize_hive_identifier("123 tabla") == "t_123_tabla"

    with pytest.raises(HiveDdlError):
        normalize_hive_identifier("   !!!   ")


def test_quote_identifier_uses_backticks() -> None:
    """Los identificadores se escapan con backticks."""

    assert quote_identifier("columna") == "`columna`"
    assert quote_identifier("we`ird") == "`we``ird`"


@pytest.mark.parametrize(
    ("spark_type", "hive_type"),
    [
        ("string", "STRING"),
        ("int", "INT"),
        ("integer", "INT"),
        ("bigint", "BIGINT"),
        ("long", "BIGINT"),
        ("double", "DOUBLE"),
        ("float", "FLOAT"),
        ("decimal(20,4)", "DECIMAL(20,4)"),
        ("boolean", "BOOLEAN"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMP"),
        ("timestamp_ntz", "TIMESTAMP"),
        ("array<string>", "STRING"),
    ],
)
def test_spark_type_to_hive_type(spark_type: str, hive_type: str) -> None:
    """Los tipos Spark simples se traducen a Hive."""

    assert spark_type_to_hive_type(spark_type) == hive_type


def test_project_path_to_hive_location_uses_app_data() -> None:
    """Las rutas del proyecto se convierten a LOCATION del contenedor."""

    location = project_path_to_hive_location(
        Path("data/silver/integrated/integration_coverage")
    )

    assert location == "/app/data/silver/integrated/integration_coverage"


def test_validate_hive_location_rejects_windows_paths() -> None:
    """Hive LOCATION no debe usar rutas Windows."""

    with pytest.raises(HiveDdlError):
        validate_hive_location("C:\\data\\bronze")

    with pytest.raises(HiveDdlError):
        validate_hive_location("/opt/hive/data/warehouse/table")


def test_render_create_external_table() -> None:
    """La sentencia CREATE EXTERNAL TABLE incluye formato y LOCATION."""

    spec = ExternalTableSpec(
        database="silver",
        table_name="integration_coverage",
        dataset_path=Path("data/silver/integrated/integration_coverage"),
        hive_location="/app/data/silver/integrated/integration_coverage",
    )
    sql = render_create_external_table(
        spec,
        [
            ("metric_name", "STRING"),
            ("coverage_percentage", "DOUBLE"),
        ],
    )

    assert "CREATE EXTERNAL TABLE IF NOT EXISTS `silver`.`integration_coverage`" in sql
    assert "`metric_name` STRING" in sql
    assert "STORED AS PARQUET" in sql
    assert "LOCATION '/app/data/silver/integrated/integration_coverage'" in sql
