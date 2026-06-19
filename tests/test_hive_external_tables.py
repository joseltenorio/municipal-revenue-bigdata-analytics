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


def test_create_silver_external_tables_sql_contracts() -> None:
    """Valida los contratos de create_silver_external_tables.sql."""
    sql_path = Path("sql/hive/create_silver_external_tables.sql")
    assert sql_path.exists(), "El archivo create_silver_external_tables.sql no existe."
    
    content = sql_path.read_text(encoding="utf-8")
    
    # 1. Registra silver.map_sec_ejec_ubigeo
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS `silver`.`map_sec_ejec_ubigeo`" in content
    assert "LOCATION '/app/data/silver/integrated/map_sec_ejec_ubigeo'" in content
    
    # 2. Registra silver.integration_coverage
    assert "CREATE EXTERNAL TABLE IF NOT EXISTS `silver`.`integration_coverage`" in content
    assert "LOCATION '/app/data/silver/integrated/integration_coverage'" in content
    
    # 3. NO contiene base_renamu_2022, renamu_full, la ruta legacy, ni columnas crudas
    assert "base_renamu_2022" not in content
    assert "renamu_full" not in content
    assert "/app/data/silver/renamu/resource_key=base_renamu_2022" not in content
    
    # Columnas crudas representativas
    for col in ["p35_", "p36_", "c96_", "c97_"]:
        assert col not in content, f"Columna cruda representativa {col} no debe estar en Silver DDL."


def test_create_gold_external_tables_sql_contracts() -> None:
    """Valida que create_gold_external_tables.sql registre las tablas Gold finales y no legacy."""
    sql_path = Path("sql/hive/create_gold_external_tables.sql")
    assert sql_path.exists(), "El archivo create_gold_external_tables.sql no existe."
    
    content = sql_path.read_text(encoding="utf-8")
    
    # Gold dimensions
    expected_gold_tables = [
        "dim_municipality",
        "dim_geography",
        "dim_renamu_context",
        "dim_time",
        "dim_sismepre_period",
        "fact_siaf_income",
        "fact_predial_statistics",
        "mart_municipal_revenue_overview",
        "mart_predial_statistics_overview",
        "mart_municipal_context",
        "mart_territorial_summary",
        "audit_quality_results",
        "audit_dataset_summary",
        "audit_integration_coverage",
    ]
    
    for table in expected_gold_tables:
        assert f"CREATE EXTERNAL TABLE IF NOT EXISTS `gold`.`{table}`" in content
        assert f"LOCATION '/app/data/gold/{table}'" in content

    # No se registran tablas legacy en ningun SQL
    silver_sql_path = Path("sql/hive/create_silver_external_tables.sql")
    silver_content = silver_sql_path.read_text(encoding="utf-8")
    
    legacy_tables = [
        "municipal_categories",
        "municipal_entity_bridge",
        "mef_municipal_amounts",
        "fact_municipal_income_execution",
    ]
    
    for legacy in legacy_tables:
        assert legacy not in content, f"Tabla legacy {legacy} no debe estar en Gold DDL."
        assert legacy not in silver_content, f"Tabla legacy {legacy} no debe estar en Silver DDL."

    # Validar renamu_municipal_context legacy si apunta a RENAMU completo
    assert "/app/data/silver/renamu/resource_key=base_renamu_2022" not in silver_content
    # Tampoco debe estar registrado renamu_municipal_context como tabla apuntando a la versión cruda completa
    assert "renamu_municipal_context" not in content
    # Si renamu_municipal_context está en silver_content, no debe ser legacy (completo)
    if "renamu_municipal_context" in silver_content:
        # Si se registra, debe ser curado y compacto, no RENAMU completo con columnas crudas
        for col in ["p35_", "p36_", "c96_", "c97_"]:
            assert col not in silver_content


