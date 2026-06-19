"""Pruebas focales para facts Gold de ingresos y predial."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.gold.build_revenue_predial_facts import (
    GOLD_FACT_DATASETS,
    GoldFactError,
    build_fact_predial_statistics,
    build_fact_siaf_income,
    build_siaf_resolution_map,
    output_dataset_path,
    validate_selected_datasets,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Crea una sesion Spark local para contratos Gold."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-revenue-predial-facts")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture()
def map_sec_ejec_ubigeo(spark: SparkSession):
    schema = StructType(
        [
            StructField("sec_ejec", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("municipality_key", StringType(), True),
            StructField("has_siaf_match", BooleanType(), True),
            StructField("has_sismepre_match", BooleanType(), True),
            StructField("has_renamu_match", BooleanType(), True),
            StructField("has_classification_match", BooleanType(), True),
            StructField("match_status", StringType(), True),
            StructField("confidence_level", StringType(), True),
            StructField("issue_reason", StringType(), True),
        ]
    )
    rows = [
        ("301260", "150111", "150111", True, True, True, True, "matched", "high", "ok"),
        (
            "301261",
            "150112",
            "150112",
            True,
            True,
            False,
            True,
            "missing_renamu",
            "medium",
            "ubigeo_not_found_in_renamu",
        ),
        (
            "301270",
            "150113",
            "150113",
            True,
            True,
            True,
            True,
            "ambiguous_sec_ejec_ubigeo",
            "low",
            "duplicated_sec_ejec_ubigeo",
        ),
        (
            "301270",
            "150114",
            "150114",
            True,
            True,
            True,
            True,
            "ambiguous_sec_ejec",
            "low",
            "sec_ejec_maps_to_multiple_ubigeo6",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def siaf_frames(spark: SparkSession):
    schema = StructType(
        [
            StructField("anio", IntegerType(), True),
            StructField("mes", IntegerType(), True),
            StructField("sec_ejec", StringType(), True),
            StructField("source_resource_key", StringType(), True),
            StructField("source_granularity", StringType(), True),
            StructField("monto_pia", DecimalType(18, 4), True),
            StructField("monto_pim", DecimalType(18, 4), True),
            StructField("monto_recaudado", DecimalType(18, 4), True),
            StructField("departamento_nombre", StringType(), True),
        ]
    )
    rows = [
        (2024, 4, "301260", "monthly_2024", "monthly", Decimal("100.0000"), Decimal("110.0000"), Decimal("95.0000"), "LIMA"),
        (2024, None, "301261", "annual_2024", "annual", Decimal("200.0000"), Decimal("220.0000"), Decimal("210.0000"), "LIMA"),
        (2025, 6, "301270", "monthly_2025", "monthly", Decimal("300.0000"), Decimal("330.0000"), Decimal("310.0000"), "CUSCO"),
        (2025, 13, "999999", "daily_2025", "daily", Decimal("400.0000"), Decimal("440.0000"), Decimal("410.0000"), "AREQUIPA"),
    ]
    return [spark.createDataFrame(rows, schema)]


@pytest.fixture()
def sismepre_esat(spark: SparkSession):
    schema = StructType(
        [
            StructField("sec_ejec", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("anio_aplicacion", IntegerType(), True),
            StructField("periodo", IntegerType(), True),
            StructField("anio_estadistica", IntegerType(), True),
            StructField("mes_estadistica", IntegerType(), True),
            StructField("formulario_id", IntegerType(), True),
            StructField("monto_emision_predial_total", DecimalType(18, 4), True),
            StructField("monto_recaudacion_predial_total", DecimalType(18, 4), True),
            StructField("monto_saldo_predial_total", DecimalType(18, 4), True),
            StructField("numero_predios_total", IntegerType(), True),
            StructField("numero_contribuyentes_predio", IntegerType(), True),
            StructField("municipalidad_nombre", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
        ]
    )
    rows = [
        (
            "301260",
            "150111",
            2024,
            2,
            2023,
            12,
            6,
            Decimal("100.0000"),
            Decimal("60.0000"),
            Decimal("40.0000"),
            50,
            20,
            "MUNICIPALIDAD DISTRITAL DE EL AGUSTINO",
            "LIMA",
        ),
        (
            "301261",
            "150112",
            2024,
            3,
            2024,
            5,
            7,
            Decimal("0.0000"),
            Decimal("0.0000"),
            Decimal("0.0000"),
            0,
            0,
            "MUNICIPALIDAD DISTRITAL DE INDEPENDENCIA",
            "LIMA",
        ),
    ]
    return spark.createDataFrame(rows, schema)


def test_registry_incluye_solo_facts_objetivo():
    assert GOLD_FACT_DATASETS == ["fact_siaf_income", "fact_predial_statistics"]


def test_validate_selected_datasets_rechaza_no_soportados():
    assert validate_selected_datasets(None) == GOLD_FACT_DATASETS
    with pytest.raises(GoldFactError):
        validate_selected_datasets(["fact_siaf_income", "mart_predial_statistics_overview"])


def test_output_dataset_path_escribe_en_raiz_gold():
    assert output_dataset_path(Path("data/gold"), "fact_siaf_income") == Path(
        "data/gold/fact_siaf_income"
    )


def test_build_siaf_resolution_map_resuelve_y_bloquea_ambiguedades(map_sec_ejec_ubigeo):
    resolution = build_siaf_resolution_map(map_sec_ejec_ubigeo)
    rows = {row["sec_ejec"]: row.asDict() for row in resolution.collect()}

    assert rows["301260"]["municipality_key"] == "150111"
    assert rows["301260"]["has_municipality_match"] is True
    assert rows["301260"]["match_status"] == "matched"

    assert rows["301261"]["municipality_key"] == "150112"
    assert rows["301261"]["has_municipality_match"] is True
    assert rows["301261"]["match_status"] == "missing_renamu"

    assert rows["301270"]["municipality_key"] is None
    assert rows["301270"]["has_municipality_match"] is False
    assert rows["301270"]["match_status"] == "ambiguous_sec_ejec"


def test_fact_siaf_income_tiene_contrato_y_resuelve_municipality_key(
    siaf_frames,
    map_sec_ejec_ubigeo,
):
    fact = build_fact_siaf_income(
        siaf_frames,
        map_sec_ejec_ubigeo,
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert fact.columns == [
        "municipality_key",
        "sec_ejec",
        "date_key",
        "source_resource_key",
        "source_granularity",
        "monto_pia",
        "monto_pim",
        "monto_recaudado",
        "has_municipality_match",
        "match_status",
        "gold_processed_at_utc",
    ]

    forbidden_columns = {
        "municipalidad_siaf_nombre",
        "municipalidad_sismepre_nombre",
        "nombre_siaf",
        "nombre_sismepre",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
    }
    assert forbidden_columns.isdisjoint(fact.columns)

    rows = {row["sec_ejec"]: row.asDict() for row in fact.collect()}
    assert rows["301260"]["municipality_key"] == "150111"
    assert rows["301260"]["has_municipality_match"] is True
    assert rows["301260"]["match_status"] == "matched"
    assert rows["301260"]["source_resource_key"] == "monthly_2024"
    assert rows["301260"]["source_granularity"] == "monthly"
    assert rows["301260"]["date_key"] == 20240401
    assert rows["301260"]["monto_pia"] == Decimal("100.0000")
    assert rows["301260"]["monto_pim"] == Decimal("110.0000")
    assert rows["301260"]["monto_recaudado"] == Decimal("95.0000")

    assert rows["301261"]["municipality_key"] == "150112"
    assert rows["301261"]["has_municipality_match"] is True
    assert rows["301261"]["match_status"] == "missing_renamu"
    assert rows["301261"]["date_key"] == 20240101

    assert rows["301270"]["municipality_key"] is None
    assert rows["301270"]["has_municipality_match"] is False
    assert rows["301270"]["match_status"] == "ambiguous_sec_ejec"

    assert rows["999999"]["municipality_key"] is None
    assert rows["999999"]["has_municipality_match"] is False
    assert rows["999999"]["match_status"] == "missing_map"
    assert rows["999999"]["date_key"] is None


def test_fact_predial_statistics_tiene_contrato_y_calcula_ratio(sismepre_esat):
    fact = build_fact_predial_statistics(
        sismepre_esat,
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert fact.columns == [
        "municipality_key",
        "sismepre_period_key",
        "sec_ejec",
        "ubigeo6",
        "formulario_id",
        "monto_emision_predial_total",
        "monto_recaudacion_predial_total",
        "monto_saldo_predial_total",
        "ratio_recaudacion_emision",
        "numero_predios_total",
        "numero_contribuyentes_predio",
        "gold_processed_at_utc",
    ]

    forbidden_columns = {
        "municipalidad_siaf_nombre",
        "municipalidad_sismepre_nombre",
        "nombre_siaf",
        "nombre_sismepre",
        "municipalidad_nombre",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "tipo_clasificacion_municipal",
    }
    assert forbidden_columns.isdisjoint(fact.columns)

    rows = {row["ubigeo6"]: row.asDict() for row in fact.collect()}
    assert rows["150111"]["municipality_key"] == "150111"
    assert rows["150111"]["sismepre_period_key"] == "2024_02_2023_12"
    assert rows["150111"]["ratio_recaudacion_emision"] == Decimal("0.60000000")
    assert rows["150111"]["monto_emision_predial_total"] == Decimal("100.0000")
    assert rows["150111"]["monto_recaudacion_predial_total"] == Decimal("60.0000")
    assert rows["150111"]["monto_saldo_predial_total"] == Decimal("40.0000")

    assert rows["150112"]["municipality_key"] == "150112"
    assert rows["150112"]["sismepre_period_key"] == "2024_03_2024_05"
    assert rows["150112"]["ratio_recaudacion_emision"] is None


def test_builder_no_usa_fuentes_legacy_ni_construye_marts():
    source_text = Path("src/gold/build_revenue_predial_facts.py").read_text(
        encoding="utf-8"
    )

    blocked_terms = [
        "municipal_categories",
        "municipal_entity_bridge",
        "renamu_full",
        "mef_municipal_amounts",
        "resource_key=respuestas",
        "resource_key=preguntas",
        "resource_key=formulario",
        "resource_key=ano_aplicacion",
        "resource_key=entidad_estado",
        "mart_",
    ]
    for term in blocked_terms:
        assert term not in source_text
