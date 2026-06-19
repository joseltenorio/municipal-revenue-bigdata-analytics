"""Pruebas unitarias para la transformacion Silver curada de SIAF."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.silver.transform_siaf_income import (
    FINAL_COLUMNS,
    SilverResource,
    SilverTransformError,
    select_silver_resources,
    transform_resource_dataframe,
    transform_siaf_income,
)


@pytest.fixture()
def spark() -> SparkSession:
    """Crea una sesion Spark local para pruebas aisladas."""

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-siaf-income")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def sample_source_config() -> dict[str, object]:
    """Retorna una configuracion minima de recursos SIAF para pruebas."""

    return {
        "bronze_subdir": "siaf_income",
        "silver_subdir": "siaf_income",
        "candidate_resources": {
            "annual_2024": {
                "file_name": "2024-Ingreso.csv",
                "format": "csv",
                "role": "annual_source_file",
                "year": 2024,
                "granularity": "annual",
            }
        },
    }


def sample_bronze_rows() -> list[dict[str, str]]:
    """Retorna filas Bronze con valores validos e invalidos controlados."""

    return [
        {
            "ano_doc": "2024",
            "mes_doc": "4",
            "nivel_gobierno": "M",
            "nivel_gobierno_nombre": "GOBIERNOS LOCALES",
            "sector": "01",
            "sector_nombre": "SECTOR 01",
            "pliego": "000123",
            "pliego_nombre": "PLIEGO DEMO",
            "sec_ejec": "301863",
            "ejecutora": "030220",
            "ejecutora_nombre": "MUNICIPALIDAD DISTRITAL DE SAMUGARI",
            "departamento_ejecutora": "3",
            "departamento_ejecutora_nombre": "APURIMAC",
            "provincia_ejecutora": "2",
            "provincia_ejecutora_nombre": "ANDAHUAYLAS",
            "distrito_ejecutora": "20",
            "distrito_ejecutora_nombre": "JOSE MARIA ARGUEDAS",
            "fuente_financiamiento": "2",
            "fuente_financiamiento_nombre": "RECURSOS DIRECTAMENTE RECAUDADOS",
            "rubro": "09",
            "rubro_nombre": "RUBRO DEMO",
            "tipo_recurso": "7",
            "tipo_recurso_nombre": "TIPO DEMO",
            "generica": "5",
            "generica_nombre": "GENERICA DEMO",
            "subgenerica": "1",
            "subgenerica_nombre": "SUBGENERICA DEMO",
            "subgenerica_det": "1",
            "subgenerica_det_nombre": "SUBGENERICA DET DEMO",
            "especifica": "1",
            "especifica_nombre": "ESPECIFICA DEMO",
            "especifica_det": "1",
            "especifica_det_nombre": "ESPECIFICA DET DEMO",
            "monto_pia": "100.50",
            "monto_pim": "90.25",
            "monto_recaudado": "120.75",
            "bronze_source_name": "mef_income",
            "bronze_resource_key": "annual_2024",
            "bronze_source_file_name": "2024-Ingreso.csv",
            "bronze_source_file_path": "/app/data/landing/mef_income/2024-Ingreso.csv",
            "bronze_source_year": "2024",
            "bronze_source_granularity": "annual",
            "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
        {
            "ano_doc": "2024",
            "mes_doc": "5",
            "nivel_gobierno": "R",
            "nivel_gobierno_nombre": "GOBIERNOS REGIONALES",
            "sector": "01",
            "sector_nombre": "SECTOR REGIONAL",
            "pliego": "000111",
            "pliego_nombre": "PLIEGO REGIONAL",
            "sec_ejec": "111111",
            "ejecutora": "030111",
            "ejecutora_nombre": "REGION AMAZONAS-SEDE CENTRAL",
            "departamento_ejecutora": "1",
            "departamento_ejecutora_nombre": "AMAZONAS",
            "provincia_ejecutora": "1",
            "provincia_ejecutora_nombre": "CHACHAPOYAS",
            "distrito_ejecutora": "1",
            "distrito_ejecutora_nombre": "CHACHAPOYAS",
            "fuente_financiamiento": "2",
            "fuente_financiamiento_nombre": "RECURSOS DIRECTAMENTE RECAUDADOS",
            "rubro": "09",
            "rubro_nombre": "RUBRO DEMO",
            "tipo_recurso": "7",
            "tipo_recurso_nombre": "TIPO DEMO",
            "generica": "5",
            "generica_nombre": "GENERICA DEMO",
            "subgenerica": "1",
            "subgenerica_nombre": "SUBGENERICA DEMO",
            "subgenerica_det": "1",
            "subgenerica_det_nombre": "SUBGENERICA DET DEMO",
            "especifica": "1",
            "especifica_nombre": "ESPECIFICA DEMO",
            "especifica_det": "1",
            "especifica_det_nombre": "ESPECIFICA DET DEMO",
            "monto_pia": "1000.00",
            "monto_pim": "1000.00",
            "monto_recaudado": "1000.00",
            "bronze_source_name": "mef_income",
            "bronze_resource_key": "annual_2024",
            "bronze_source_file_name": "2024-Ingreso.csv",
            "bronze_source_file_path": "/app/data/landing/mef_income/2024-Ingreso.csv",
            "bronze_source_year": "2024",
            "bronze_source_granularity": "annual",
            "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
        {
            "ano_doc": "2024",
            "mes_doc": "6",
            "nivel_gobierno": "M",
            "nivel_gobierno_nombre": "GOBIERNOS LOCALES",
            "sector": "01",
            "sector_nombre": "SECTOR LOCAL",
            "pliego": "000222",
            "pliego_nombre": "PLIEGO MANCOMUNIDAD",
            "sec_ejec": "222222",
            "ejecutora": "030222",
            "ejecutora_nombre": "MANCOMUNIDAD MUNICIPAL DE LA AMAZONIA DE PUNO",
            "departamento_ejecutora": "21",
            "departamento_ejecutora_nombre": "PUNO",
            "provincia_ejecutora": "1",
            "provincia_ejecutora_nombre": "PUNO",
            "distrito_ejecutora": "1",
            "distrito_ejecutora_nombre": "PUNO",
            "fuente_financiamiento": "2",
            "fuente_financiamiento_nombre": "RECURSOS DIRECTAMENTE RECAUDADOS",
            "rubro": "09",
            "rubro_nombre": "RUBRO DEMO",
            "tipo_recurso": "7",
            "tipo_recurso_nombre": "TIPO DEMO",
            "generica": "5",
            "generica_nombre": "GENERICA DEMO",
            "subgenerica": "1",
            "subgenerica_nombre": "SUBGENERICA DEMO",
            "subgenerica_det": "1",
            "subgenerica_det_nombre": "SUBGENERICA DET DEMO",
            "especifica": "1",
            "especifica_nombre": "ESPECIFICA DEMO",
            "especifica_det": "1",
            "especifica_det_nombre": "ESPECIFICA DET DEMO",
            "monto_pia": "1000.00",
            "monto_pim": "1000.00",
            "monto_recaudado": "1000.00",
            "bronze_source_name": "mef_income",
            "bronze_resource_key": "annual_2024",
            "bronze_source_file_name": "2024-Ingreso.csv",
            "bronze_source_file_path": "/app/data/landing/mef_income/2024-Ingreso.csv",
            "bronze_source_year": "2024",
            "bronze_source_granularity": "annual",
            "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
        {
            "ano_doc": "2024",
            "mes_doc": "7",
            "nivel_gobierno": "N",
            "nivel_gobierno_nombre": "GOBIERNO NACIONAL",
            "sector": "10",
            "sector_nombre": "SECTOR EDUCACION",
            "pliego": "000333",
            "pliego_nombre": "PLIEGO MINEDU",
            "sec_ejec": "333333",
            "ejecutora": "030333",
            "ejecutora_nombre": "MINISTERIO DE EDUCACION",
            "departamento_ejecutora": "15",
            "departamento_ejecutora_nombre": "LIMA",
            "provincia_ejecutora": "1",
            "provincia_ejecutora_nombre": "LIMA",
            "distrito_ejecutora": "1",
            "distrito_ejecutora_nombre": "LIMA",
            "fuente_financiamiento": "1",
            "fuente_financiamiento_nombre": "RECURSOS ORDINARIOS",
            "rubro": "00",
            "rubro_nombre": "RECURSOS ORDINARIOS",
            "tipo_recurso": "0",
            "tipo_recurso_nombre": "TIPO DEMO",
            "generica": "5",
            "generica_nombre": "GENERICA DEMO",
            "subgenerica": "1",
            "subgenerica_nombre": "SUBGENERICA DEMO",
            "subgenerica_det": "1",
            "subgenerica_det_nombre": "SUBGENERICA DET DEMO",
            "especifica": "1",
            "especifica_nombre": "ESPECIFICA DEMO",
            "especifica_det": "1",
            "especifica_det_nombre": "ESPECIFICA DET DEMO",
            "monto_pia": "5000.00",
            "monto_pim": "5000.00",
            "monto_recaudado": "5000.00",
            "bronze_source_name": "mef_income",
            "bronze_resource_key": "annual_2024",
            "bronze_source_file_name": "2024-Ingreso.csv",
            "bronze_source_file_path": "/app/data/landing/mef_income/2024-Ingreso.csv",
            "bronze_source_year": "2024",
            "bronze_source_granularity": "annual",
            "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
        {
            "ano_doc": "2024",
            "mes_doc": "8",
            "nivel_gobierno": "M",
            "nivel_gobierno_nombre": "GOBIERNOS LOCALES",
            "sector": "01",
            "sector_nombre": "SECTOR LOCAL",
            "pliego": "000444",
            "pliego_nombre": "PLIEGO ASOCIACION",
            "sec_ejec": "444444",
            "ejecutora": "030444",
            "ejecutora_nombre": "ASOCIACION DE MUNICIPALIDADES DE LA PROVINCIA X",
            "departamento_ejecutora": "15",
            "departamento_ejecutora_nombre": "LIMA",
            "provincia_ejecutora": "1",
            "provincia_ejecutora_nombre": "LIMA",
            "distrito_ejecutora": "1",
            "distrito_ejecutora_nombre": "LIMA",
            "fuente_financiamiento": "2",
            "fuente_financiamiento_nombre": "RECURSOS DIRECTAMENTE RECAUDADOS",
            "rubro": "09",
            "rubro_nombre": "RUBRO DEMO",
            "tipo_recurso": "7",
            "tipo_recurso_nombre": "TIPO DEMO",
            "generica": "5",
            "generica_nombre": "GENERICA DEMO",
            "subgenerica": "1",
            "subgenerica_nombre": "SUBGENERICA DEMO",
            "subgenerica_det": "1",
            "subgenerica_det_nombre": "SUBGENERICA DET DEMO",
            "especifica": "1",
            "especifica_nombre": "ESPECIFICA DEMO",
            "especifica_det": "1",
            "especifica_det_nombre": "ESPECIFICA DET DEMO",
            "monto_pia": "1000.00",
            "monto_pim": "1000.00",
            "monto_recaudado": "1000.00",
            "bronze_source_name": "mef_income",
            "bronze_resource_key": "annual_2024",
            "bronze_source_file_name": "2024-Ingreso.csv",
            "bronze_source_file_path": "/app/data/landing/mef_income/2024-Ingreso.csv",
            "bronze_source_year": "2024",
            "bronze_source_granularity": "annual",
            "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
    ]


def write_bronze_dataset(spark: SparkSession, output_path: Path) -> None:
    """Escribe un dataset Bronze de ejemplo en Parquet."""

    spark.createDataFrame(sample_bronze_rows()).write.mode("overwrite").parquet(
        str(output_path)
    )


def test_transform_resource_dataframe_builds_curated_contract(
    spark: SparkSession,
) -> None:
    """La transformacion genera exactamente el contrato Silver esperado."""

    resource = SilverResource(
        resource_key="annual_2024",
        bronze_path=Path("unused"),
        silver_path=Path("unused"),
        year=2024,
        granularity="annual",
    )

    dataframe = spark.createDataFrame(sample_bronze_rows())
    result = transform_resource_dataframe(
        dataframe=dataframe,
        resource=resource,
        processed_at="2026-06-19T00:10:00+00:00",
    )

    rows = {row["mes"]: row.asDict(recursive=True) for row in result.collect()}

    assert result.columns == FINAL_COLUMNS
    assert "bronze_source_name" not in result.columns
    assert "monto_pia_decimal" not in result.columns
    assert "monto_pim_decimal" not in result.columns
    assert "monto_recaudado_decimal" not in result.columns

    # De 5 filas originales, solo la municipalidad distrital de Samugari (mes 4) sobrevive.
    # Las demas (mes 5=regional, 6=mancomunidad, 7=nacional, 8=asociacion) son filtradas.
    assert len(rows) == 1
    assert 4 in rows
    assert 5 not in rows
    assert 6 not in rows
    assert 7 not in rows
    assert 8 not in rows

    row_valid = rows[4]
    assert row_valid["nivel_gobierno_codigo"] == "M"
    assert row_valid["ejecutora_nombre"] == "MUNICIPALIDAD DISTRITAL DE SAMUGARI"
    assert row_valid["sec_ejec"] == "301863"
    assert row_valid["departamento_codigo"] == "03"
    assert row_valid["provincia_codigo"] == "02"
    assert row_valid["distrito_codigo"] == "20"
    assert row_valid["ubigeo6_ejecutora"] == "030220"
    assert row_valid["fecha_mes"] == date(2024, 4, 1)
    assert row_valid["monto_pia"] == Decimal("100.5000")
    assert row_valid["monto_pim"] == Decimal("90.2500")
    assert row_valid["monto_recaudado"] == Decimal("120.7500")
    assert row_valid["is_municipal_government"] is True
    assert row_valid["flag_pim_menor_pia"] is True
    assert row_valid["flag_recaudado_mayor_pim"] is True
    assert row_valid["has_complete_territory"] is True
    assert row_valid["is_valid_sec_ejec"] is True
    assert row_valid["is_valid_ubigeo6_ejecutora"] is True
    assert row_valid["silver_source_name"] == "siaf_income"
    assert row_valid["silver_resource_key"] == "annual_2024"
    assert row_valid["source_resource_key"] == "annual_2024"
    assert row_valid["source_granularity"] == "annual"


def test_dry_run_does_not_write_silver_outputs(tmp_path: Path, spark: SparkSession) -> None:
    """El dry-run valida entradas sin escribir salidas Silver."""

    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    resource_bronze_path = bronze_dir / "resource_key=annual_2024"
    write_bronze_dataset(spark, resource_bronze_path)

    resources = select_silver_resources(
        sample_source_config(),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
    )

    summary = transform_siaf_income(
        resources=resources,
        dry_run=True,
        overwrite=False,
        limit=None,
    )

    assert len(summary) == 1
    assert summary[0]["resource_key"] == "annual_2024"
    assert summary[0]["row_count"] == 5
    assert not (silver_dir / "resource_key=annual_2024").exists()


def test_overwrite_control_replaces_previous_output(tmp_path: Path, spark: SparkSession) -> None:
    """`overwrite` permite regenerar la salida y evita escrituras ambiguas."""

    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    resource_bronze_path = bronze_dir / "resource_key=annual_2024"
    resource_silver_path = silver_dir / "resource_key=annual_2024"
    write_bronze_dataset(spark, resource_bronze_path)

    resources = select_silver_resources(
        sample_source_config(),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
    )

    transform_siaf_income(
        resources=resources,
        dry_run=False,
        overwrite=False,
        limit=None,
    )
    assert resource_silver_path.exists()

    with pytest.raises(Exception):
        transform_siaf_income(
            resources=resources,
            dry_run=False,
            overwrite=False,
            limit=None,
        )

    rewrite_spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-siaf-income-overwrite")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    rewrite_spark.sparkContext.setLogLevel("ERROR")
    rewrite_spark.createDataFrame(sample_bronze_rows()[:1]).write.mode("overwrite").parquet(
        str(resource_bronze_path)
    )
    rewrite_spark.stop()

    transform_siaf_income(
        resources=resources,
        dry_run=False,
        overwrite=True,
        limit=None,
    )

    read_spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-siaf-income-readback")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    read_spark.sparkContext.setLogLevel("ERROR")
    regenerated = read_spark.read.parquet(str(resource_silver_path))
    assert regenerated.count() == 1
    assert regenerated.select("silver_source_name").first()[0] == "siaf_income"
    read_spark.stop()


def test_select_silver_resources_rejects_unknown_resource(tmp_path: Path) -> None:
    """La seleccion rechaza recursos fuera del contrato configurado."""

    with pytest.raises(SilverTransformError):
        select_silver_resources(
            sample_source_config(),
            resource_keys=["annual_2030"],
            bronze_dir=tmp_path / "bronze",
            silver_dir=tmp_path / "silver",
        )
