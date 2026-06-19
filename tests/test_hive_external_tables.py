"""Pruebas unitarias para generación de tablas externas Hive."""

from pathlib import Path

import pytest

from src.hive.generate_external_tables import (
    ExternalTableSpec,
    HiveDdlError,
    discover_bronze_tables,
    normalize_hive_identifier,
    project_path_to_hive_location,
    quote_identifier,
    render_create_external_table,
    spark_type_to_hive_type,
    validate_hive_location,
)


def test_normalize_hive_identifier() -> None:
    """Los nombres de tabla se normalizan a identificadores seguros."""

    assert normalize_hive_identifier("SIAF Income annual-2012") == "siaf_income_annual_2012"
    assert normalize_hive_identifier("siaf_income__annual_2012") == "siaf_income__annual_2012"
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


def test_discover_gold_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """La función discover_gold_tables descubre directorios Gold con Parquet."""

    # Crear estructura simulada: dataset/file.parquet
    gold_mock = tmp_path / "data" / "gold"
    dataset_mock = gold_mock / "fact_siaf_income"
    dataset_mock.mkdir(parents=True)

    # Crear un archivo parquet vacío para que sea detectado por parquet_files_exist
    (dataset_mock / "part-0.parquet").write_text("dummy content")

    # Mockear GOLD_DIR y PROJECT_ROOT en generate_external_tables
    import src.hive.generate_external_tables as gen

    monkeypatch.setattr(gen, "GOLD_DIR", gold_mock)
    monkeypatch.setattr(gen, "PROJECT_ROOT", tmp_path)

    specs = gen.discover_gold_tables()

    assert len(specs) == 1
    spec = specs[0]
    assert spec.database == "gold"
    assert spec.table_name == "fact_siaf_income"
    assert spec.hive_location == "/app/data/gold/fact_siaf_income"


def test_render_create_external_table_gold() -> None:
    """La sentencia CREATE EXTERNAL TABLE para Gold incluye base de datos gold y location."""

    spec = ExternalTableSpec(
        database="gold",
        table_name="fact_siaf_income",
        dataset_path=Path("data/gold/fact_siaf_income"),
        hive_location="/app/data/gold/fact_siaf_income",
    )
    sql = render_create_external_table(
        spec,
        [
            ("sec_ejec", "STRING"),
            ("monto_recaudado", "DECIMAL(30,4)"),
        ],
    )

    assert "CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`fact_siaf_income`" in sql
    assert "`sec_ejec` STRING" in sql
    assert "`monto_recaudado` DECIMAL(30,4)" in sql
    assert "STORED AS PARQUET" in sql
    assert "LOCATION '/app/data/gold/fact_siaf_income'" in sql


def test_discover_bronze_tables_supports_direct_datasets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """La deteccion Bronze reconoce datasets directos sin resource_key."""

    bronze_mock = tmp_path / "data" / "bronze"
    dataset_mock = bronze_mock / "municipal_classification"
    dataset_mock.mkdir(parents=True)
    (dataset_mock / "data.parquet").write_text("dummy content")

    import src.hive.generate_external_tables as gen

    monkeypatch.setattr(gen, "BRONZE_DIR", bronze_mock)
    monkeypatch.setattr(gen, "PROJECT_ROOT", tmp_path)

    specs = discover_bronze_tables()

    assert len(specs) == 1
    assert specs[0].database == "bronze"
    assert specs[0].table_name == "municipal_classification"
    assert specs[0].hive_location == "/app/data/bronze/municipal_classification"

