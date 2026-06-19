"""Pruebas focales para marts analiticos Gold orientados a Power BI."""

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
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.gold.build_powerbi_analytic_marts import (
    GOLD_MART_DATASETS,
    GoldMartError,
    build_mart_municipal_context,
    build_mart_municipal_revenue_overview,
    build_mart_predial_statistics_overview,
    build_mart_territorial_summary,
    output_dataset_path,
    validate_selected_datasets,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Crea una sesion Spark local para contratos Gold de marts."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-powerbi-analytic-marts")
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
def dim_municipality(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("geography_key", StringType(), True),
            StructField("idmunici", StringType(), True),
            StructField("municipalidad_nombre", StringType(), True),
            StructField("tipomuni_codigo", StringType(), True),
            StructField("tipomuni_nombre", StringType(), True),
            StructField("tipo_clasificacion_municipal", StringType(), True),
            StructField("ambito_municipal", StringType(), True),
            StructField("descripcion_tipo", StringType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            "150111",
            "150111",
            "MUNICIPALIDAD DISTRITAL DE EL AGUSTINO",
            "2",
            "Distrital",
            "G",
            "distrital",
            "Municipalidad tipo G",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "150112",
            "150112",
            "150112",
            "150112",
            "MUNICIPALIDAD DISTRITAL DE INDEPENDENCIA",
            "2",
            "Distrital",
            "F",
            "distrital",
            "Municipalidad tipo F",
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def dim_geography(spark: SparkSession):
    schema = StructType(
        [
            StructField("geography_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("ccdd", StringType(), True),
            StructField("ccpp", StringType(), True),
            StructField("ccdi", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
            StructField("provincia_nombre", StringType(), True),
            StructField("distrito_nombre", StringType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            "15",
            "01",
            "11",
            "LIMA",
            "LIMA",
            "EL AGUSTINO",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "150112",
            "150112",
            "15",
            "01",
            "12",
            "LIMA",
            "LIMA",
            "INDEPENDENCIA",
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def dim_time(spark: SparkSession):
    schema = StructType(
        [
            StructField("date_key", IntegerType(), True),
            StructField("fecha_mes", DateType(), True),
            StructField("anio", IntegerType(), True),
            StructField("mes", IntegerType(), True),
            StructField("anio_mes", StringType(), True),
            StructField("trimestre", IntegerType(), True),
            StructField("semestre", IntegerType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (20240401, __import__("datetime").date(2024, 4, 1), 2024, 4, "2024-04", 2, 1, "2026-06-19T00:00:00+00:00"),
        (20240101, __import__("datetime").date(2024, 1, 1), 2024, 1, "2024-01", 1, 1, "2026-06-19T00:00:00+00:00"),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def dim_sismepre_period(spark: SparkSession):
    schema = StructType(
        [
            StructField("sismepre_period_key", StringType(), True),
            StructField("anio_aplicacion", IntegerType(), True),
            StructField("periodo", IntegerType(), True),
            StructField("anio_estadistica", IntegerType(), True),
            StructField("mes_estadistica", IntegerType(), True),
            StructField("periodo_estadistica_tipo", StringType(), True),
            StructField("is_annual_stat_period", BooleanType(), True),
            StructField("periodo_label", StringType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (
            "2024_02_2023_12",
            2024,
            2,
            2023,
            12,
            "ANUAL",
            True,
            "Aplicacion 2024 - Periodo 2 - Estadistica 2023 12 (ANUAL)",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "2024_03_2024_05",
            2024,
            3,
            2024,
            5,
            "MENSUAL",
            False,
            "Aplicacion 2024 - Periodo 3 - Estadistica 2024 05 (MENSUAL)",
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def dim_renamu_context(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("total_computadoras_operativas", IntegerType(), True),
            StructField("cuenta_servicio_internet", BooleanType(), True),
            StructField("computadoras_con_acceso_internet", IntegerType(), True),
            StructField("tipo_conexion_internet_codigo", StringType(), True),
            StructField("tipo_conexion_internet_nombre", StringType(), True),
            StructField("usa_siaf", BooleanType(), True),
            StructField("usa_sistema_recaudacion_tributaria_municipal", BooleanType(), True),
            StructField("usa_sistema_rentas_administracion_tributaria", BooleanType(), True),
            StructField("usa_sistema_catastro", BooleanType(), True),
            StructField("portal_transparencia_actualizado", BooleanType(), True),
            StructField("total_personal_dic_2021", IntegerType(), True),
            StructField("total_personal_mar_2022", IntegerType(), True),
            StructField("tiene_area_ejecucion_coactiva", BooleanType(), True),
            StructField("requiere_asistencia_administracion_tributaria", BooleanType(), True),
            StructField("requiere_asistencia_catastro", BooleanType(), True),
            StructField("requiere_capacitacion_administracion_tributaria", BooleanType(), True),
            StructField("requiere_capacitacion_catastro", BooleanType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            25,
            True,
            20,
            "FO",
            "FIBRA",
            True,
            True,
            False,
            True,
            True,
            100,
            110,
            True,
            False,
            True,
            False,
            True,
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "150112",
            "150112",
            8,
            False,
            0,
            None,
            None,
            False,
            False,
            False,
            False,
            False,
            30,
            35,
            False,
            True,
            False,
            True,
            False,
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def fact_siaf_income(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("sec_ejec", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("source_resource_key", StringType(), True),
            StructField("source_granularity", StringType(), True),
            StructField("monto_pia", DecimalType(18, 4), True),
            StructField("monto_pim", DecimalType(18, 4), True),
            StructField("monto_recaudado", DecimalType(18, 4), True),
            StructField("has_municipality_match", BooleanType(), True),
            StructField("match_status", StringType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "301260",
            20240401,
            "monthly_2024",
            "monthly",
            Decimal("100.0000"),
            Decimal("110.0000"),
            Decimal("95.0000"),
            True,
            "matched",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "150112",
            "301261",
            20240101,
            "annual_2024",
            "annual",
            Decimal("200.0000"),
            Decimal("220.0000"),
            Decimal("210.0000"),
            True,
            "missing_renamu",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            None,
            "301270",
            20250601,
            "monthly_2025",
            "monthly",
            Decimal("300.0000"),
            Decimal("330.0000"),
            Decimal("310.0000"),
            False,
            "ambiguous_sec_ejec",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            None,
            "999999",
            20251301,
            "daily_2025",
            "daily",
            Decimal("400.0000"),
            Decimal("440.0000"),
            Decimal("410.0000"),
            False,
            "missing_map",
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def fact_predial_statistics(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("sismepre_period_key", StringType(), True),
            StructField("sec_ejec", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("formulario_id", IntegerType(), True),
            StructField("monto_emision_predial_total", DecimalType(18, 4), True),
            StructField("monto_recaudacion_predial_total", DecimalType(18, 4), True),
            StructField("monto_saldo_predial_total", DecimalType(18, 4), True),
            StructField("ratio_recaudacion_emision", DecimalType(18, 8), True),
            StructField("numero_predios_total", IntegerType(), True),
            StructField("numero_contribuyentes_predio", IntegerType(), True),
            StructField("gold_processed_at_utc", StringType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "2024_02_2023_12",
            "301260",
            "150111",
            6,
            Decimal("100.0000"),
            Decimal("60.0000"),
            Decimal("40.0000"),
            Decimal("0.60000000"),
            50,
            20,
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "150112",
            "2024_03_2024_05",
            "301261",
            "150112",
            7,
            Decimal("0.0000"),
            Decimal("0.0000"),
            Decimal("0.0000"),
            None,
            0,
            0,
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    return spark.createDataFrame(rows, schema)


def test_registry_incluye_solo_marts_objetivo():
    assert GOLD_MART_DATASETS == [
        "mart_municipal_revenue_overview",
        "mart_predial_statistics_overview",
        "mart_municipal_context",
        "mart_territorial_summary",
    ]


def test_validate_selected_datasets_rechaza_no_soportados():
    assert validate_selected_datasets(None) == GOLD_MART_DATASETS
    with pytest.raises(GoldMartError):
        validate_selected_datasets(["mart_municipal_context", "fact_siaf_income"])


def test_output_dataset_path_escribe_en_raiz_gold():
    assert output_dataset_path(Path("data/gold"), "mart_municipal_context") == Path(
        "data/gold/mart_municipal_context"
    )


def test_mart_municipal_revenue_overview_tiene_columnas_y_contenido_gold(
    fact_siaf_income,
    dim_municipality,
    dim_geography,
    dim_time,
):
    mart = build_mart_municipal_revenue_overview(
        fact_siaf_income,
        dim_municipality,
        dim_geography,
        dim_time,
    )

    assert mart.columns == [
        "municipality_key",
        "ubigeo6",
        "municipalidad_nombre",
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "tipomuni_codigo",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        "date_key",
        "fecha_mes",
        "anio",
        "mes",
        "anio_mes",
        "trimestre",
        "semestre",
        "sec_ejec",
        "source_resource_key",
        "source_granularity",
        "monto_pia",
        "monto_pim",
        "monto_recaudado",
        "has_municipality_match",
        "match_status",
        "gold_processed_at_utc",
    ]

    forbidden = {
        "municipalidad_siaf_nombre",
        "municipalidad_sismepre_nombre",
        "nombre_siaf",
        "nombre_sismepre",
        "usa_siaf",
    }
    assert forbidden.isdisjoint(mart.columns)

    row = mart.where(mart.municipality_key == "150111").first().asDict()
    assert row["municipalidad_nombre"] == "MUNICIPALIDAD DISTRITAL DE EL AGUSTINO"
    assert row["departamento_nombre"] == "LIMA"
    assert row["tipo_clasificacion_municipal"] == "G"
    assert row["anio_mes"] == "2024-04"
    assert row["monto_recaudado"] == Decimal("95.0000")

    # Validaciones de filtrado estricto
    from pyspark.sql import functions as F
    assert mart.where(mart.municipality_key.isNull()).count() == 0
    assert mart.where(F.col("has_municipality_match") == F.lit(False)).count() == 0
    assert mart.where(F.col("match_status").isin("missing_map", "ambiguous_sec_ejec", "unmatched", "invalid_ubigeo")).count() == 0


def test_mart_predial_statistics_overview_tiene_columnas_y_metricas_gold(
    fact_predial_statistics,
    dim_municipality,
    dim_geography,
    dim_sismepre_period,
):
    mart = build_mart_predial_statistics_overview(
        fact_predial_statistics,
        dim_municipality,
        dim_geography,
        dim_sismepre_period,
    )

    assert mart.columns == [
        "municipality_key",
        "ubigeo6",
        "municipalidad_nombre",
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "tipomuni_codigo",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        "sismepre_period_key",
        "anio_aplicacion",
        "periodo",
        "anio_estadistica",
        "mes_estadistica",
        "periodo_estadistica_tipo",
        "is_annual_stat_period",
        "periodo_label",
        "sec_ejec",
        "formulario_id",
        "monto_emision_predial_total",
        "monto_recaudacion_predial_total",
        "monto_saldo_predial_total",
        "ratio_recaudacion_emision",
        "numero_predios_total",
        "numero_contribuyentes_predio",
        "gold_processed_at_utc",
    ]

    forbidden = {
        "municipalidad_siaf_nombre",
        "municipalidad_sismepre_nombre",
        "nombre_siaf",
        "nombre_sismepre",
    }
    assert forbidden.isdisjoint(mart.columns)

    row = mart.where(mart.municipality_key == "150111").first().asDict()
    assert row["municipalidad_nombre"] == "MUNICIPALIDAD DISTRITAL DE EL AGUSTINO"
    assert row["departamento_nombre"] == "LIMA"
    assert row["periodo_estadistica_tipo"] == "ANUAL"
    assert row["monto_recaudacion_predial_total"] == Decimal("60.0000")


def test_mart_municipal_context_tiene_una_fila_por_municipalidad(
    dim_municipality,
    dim_geography,
    dim_renamu_context,
):
    mart = build_mart_municipal_context(
        dim_municipality,
        dim_geography,
        dim_renamu_context,
    )

    assert mart.count() == mart.select("municipality_key").distinct().count()
    assert "total_computadoras_operativas" in mart.columns
    assert "portal_transparencia_actualizado" in mart.columns
    assert "monto_pia" not in mart.columns
    assert "monto_emision_predial_total" not in mart.columns

    row = mart.where(mart.municipality_key == "150111").first().asDict()
    assert row["municipalidad_nombre"] == "MUNICIPALIDAD DISTRITAL DE EL AGUSTINO"
    assert row["cuenta_servicio_internet"] is True
    assert row["total_personal_dic_2021"] == 100


def test_mart_territorial_summary_no_duplica_municipalidades_y_agrega_contexto(
    dim_municipality,
    dim_geography,
    dim_renamu_context,
):
    context_mart = build_mart_municipal_context(
        dim_municipality,
        dim_geography,
        dim_renamu_context,
    )
    summary = build_mart_territorial_summary(context_mart)

    assert summary.select("geography_key").distinct().count() == summary.count()
    row = summary.where(summary.geography_key == "150111").first().asDict()
    assert row["total_municipalidades"] == 1
    assert row["municipalidades_con_siaf"] == 1
    assert row["municipalidades_con_sistema_recaudacion"] == 1
    assert row["municipalidades_con_catastro"] == 1
    assert row["municipalidades_con_internet"] == 1
    assert row["total_computadoras_operativas"] == 25
    assert row["total_personal_dic_2021"] == 100
    assert row["total_personal_mar_2022"] == 110


def test_builder_no_usa_fuentes_legacy_ni_tablas_tecnicas_visibles():
    source_text = Path("src/gold/build_powerbi_analytic_marts.py").read_text(
        encoding="utf-8"
    )

    blocked_terms = [
        "municipal_categories",
        "municipal_entity_bridge",
        "mef_municipal_amounts",
        "renamu_full",
        "renamu_municipal_context",
        "map_sec_ejec_ubigeo",
        "integration_coverage",
        "resource_key=respuestas",
        "resource_key=preguntas",
        "resource_key=formulario",
        "resource_key=ano_aplicacion",
        "resource_key=entidad_estado",
        "audit_quality_results",
        "hive",
        "pbix",
        "export_gold_fallback",
    ]
    for term in blocked_terms:
        assert term not in source_text
