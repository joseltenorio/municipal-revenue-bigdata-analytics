"""Pruebas focales para la capa dashboard-ready de Power BI."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.powerbi.build_dashboard_export_marts import (
    BLOCKED_EXPORT_FILE_NAMES,
    DASHBOARD_DATASETS,
    OPTIONAL_DASHBOARD_DATASETS,
    build_municipal_context_dashboard,
    build_municipal_performance_dashboard,
    build_predial_dashboard,
    build_revenue_annual_dashboard,
    build_revenue_monthly_dashboard,
    build_revenue_source_annual_dashboard,
    build_revenue_source_monthly_dashboard,
    build_territorial_revenue_dashboard,
    export_dataset_path,
    output_dataset_path,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Crea una sesion Spark local para los contratos dashboard-ready."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-powerbi-dashboard-export-marts")
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
def revenue_mart(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("municipalidad_nombre", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
            StructField("provincia_nombre", StringType(), True),
            StructField("distrito_nombre", StringType(), True),
            StructField("tipo_clasificacion_municipal", StringType(), True),
            StructField("ambito_municipal", StringType(), True),
            StructField("tipomuni_nombre", StringType(), True),
            StructField("anio", IntegerType(), True),
            StructField("mes", IntegerType(), True),
            StructField("anio_mes", StringType(), True),
            StructField("trimestre", IntegerType(), True),
            StructField("semestre", IntegerType(), True),
            StructField("monto_pia", DecimalType(18, 4), True),
            StructField("monto_pim", DecimalType(18, 4), True),
            StructField("monto_recaudado", DecimalType(18, 4), True),
            StructField("has_municipality_match", BooleanType(), True),
            StructField("match_status", StringType(), True),
            StructField("sec_ejec", StringType(), True),
            StructField("source_resource_key", StringType(), True),
            StructField("source_granularity", StringType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            2024,
            4,
            "2024-04",
            2,
            1,
            Decimal("100.0000"),
            Decimal("120.0000"),
            Decimal("90.0000"),
            True,
            "matched",
            "301260",
            "monthly_2024",
            "monthly",
        ),
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            2024,
            4,
            "2024-04",
            2,
            1,
            Decimal("50.0000"),
            Decimal("80.0000"),
            Decimal("60.0000"),
            True,
            "matched",
            "301260",
            "monthly_2024",
            "monthly",
        ),
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            2024,
            4,
            "2024-04",
            2,
            1,
            Decimal("999.0000"),
            Decimal("999.0000"),
            Decimal("999.0000"),
            False,
            "missing_map",
            "999999",
            "daily_2024",
            "daily",
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def revenue_source_detail(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("municipalidad_nombre", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
            StructField("provincia_nombre", StringType(), True),
            StructField("distrito_nombre", StringType(), True),
            StructField("tipo_clasificacion_municipal", StringType(), True),
            StructField("ambito_municipal", StringType(), True),
            StructField("tipomuni_nombre", StringType(), True),
            StructField("anio", IntegerType(), True),
            StructField("mes", IntegerType(), True),
            StructField("anio_mes", StringType(), True),
            StructField("trimestre", IntegerType(), True),
            StructField("semestre", IntegerType(), True),
            StructField("fuente_financiamiento_codigo", StringType(), True),
            StructField("fuente_financiamiento_nombre", StringType(), True),
            StructField("rubro_codigo", StringType(), True),
            StructField("rubro_nombre", StringType(), True),
            StructField("tipo_recurso_codigo", StringType(), True),
            StructField("tipo_recurso_nombre", StringType(), True),
            StructField("generica_codigo", StringType(), True),
            StructField("generica_nombre", StringType(), True),
            StructField("subgenerica_codigo", StringType(), True),
            StructField("subgenerica_nombre", StringType(), True),
            StructField("especifica_codigo", StringType(), True),
            StructField("especifica_nombre", StringType(), True),
            StructField("monto_pia", DoubleType(), True),
            StructField("monto_pim", DoubleType(), True),
            StructField("monto_recaudado", DoubleType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            2024,
            4,
            "2024-04",
            2,
            1,
            "2",
            "RECURSOS DIRECTAMENTE RECAUDADOS",
            "09",
            "RUBRO DEMO",
            "7",
            "TIPO DEMO",
            "1",
            "IMPUESTOS",
            "1.1",
            "PREDIAL",
            "1.1.1",
            "PREDIAL",
            100.0,
            120.0,
            90.0,
        ),
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            2024,
            4,
            "2024-04",
            2,
            1,
            "2",
            "RECURSOS DIRECTAMENTE RECAUDADOS",
            "09",
            "RUBRO DEMO",
            "7",
            "TIPO DEMO",
            "1",
            "IMPUESTOS",
            "1.1",
            "PREDIAL",
            "1.1.1",
            "PREDIAL",
            50.0,
            80.0,
            60.0,
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def predial_mart(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("municipalidad_nombre", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
            StructField("provincia_nombre", StringType(), True),
            StructField("distrito_nombre", StringType(), True),
            StructField("tipo_clasificacion_municipal", StringType(), True),
            StructField("ambito_municipal", StringType(), True),
            StructField("tipomuni_nombre", StringType(), True),
            StructField("anio_aplicacion", IntegerType(), True),
            StructField("periodo", IntegerType(), True),
            StructField("periodo_label", StringType(), True),
            StructField("periodo_estadistica_tipo", StringType(), True),
            StructField("monto_emision_predial_total", DecimalType(18, 4), True),
            StructField("monto_recaudacion_predial_total", DecimalType(18, 4), True),
            StructField("monto_saldo_predial_total", DecimalType(18, 4), True),
            StructField("numero_predios_total", IntegerType(), True),
            StructField("numero_contribuyentes_predio", IntegerType(), True),
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            2024,
            2,
            "Periodo 2",
            "ANUAL",
            Decimal("100.0000"),
            Decimal("60.0000"),
            Decimal("40.0000"),
            50,
            20,
        ),
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            2024,
            2,
            "Periodo 2",
            "ANUAL",
            Decimal("50.0000"),
            Decimal("30.0000"),
            Decimal("20.0000"),
            10,
            5,
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def context_mart(spark: SparkSession):
    schema = StructType(
        [
            StructField("municipality_key", StringType(), True),
            StructField("ubigeo6", StringType(), True),
            StructField("municipalidad_nombre", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
            StructField("provincia_nombre", StringType(), True),
            StructField("distrito_nombre", StringType(), True),
            StructField("tipo_clasificacion_municipal", StringType(), True),
            StructField("ambito_municipal", StringType(), True),
            StructField("tipomuni_nombre", StringType(), True),
            StructField("total_computadoras_operativas", IntegerType(), True),
            StructField("cuenta_servicio_internet", BooleanType(), True),
            StructField("computadoras_con_acceso_internet", IntegerType(), True),
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
        ]
    )
    rows = [
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            25,
            True,
            20,
            True,
            True,
            True,
            True,
            100,
            110,
            True,
            False,
            True,
            False,
            True,
        ),
        (
            "150111",
            "150111",
            "MUNI A",
            "LIMA",
            "LIMA",
            "A",
            "G",
            "distrital",
            "Distrital",
            None,
            True,
            None,
            None,
            True,
            None,
            True,
            None,
            None,
            True,
            None,
            None,
            None,
            None,
        ),
    ]
    return spark.createDataFrame(rows, schema)


def test_registry_incluye_datasets_dashboard_ready_requeridos() -> None:
    assert DASHBOARD_DATASETS == [
        "revenue_monthly_dashboard",
        "revenue_source_monthly_dashboard",
        "revenue_source_annual_dashboard",
        "revenue_annual_dashboard",
        "territorial_revenue_dashboard",
        "predial_dashboard",
        "municipal_context_dashboard",
        "municipal_performance_dashboard",
        "audit_dataset_summary_dashboard",
        "audit_integration_coverage_dashboard",
        "audit_quality_results_dashboard",
    ]
    assert OPTIONAL_DASHBOARD_DATASETS == ["revenue_concept_monthly_dashboard"]


def test_revenue_monthly_dashboard_agrega_por_municipalidad_anio_mes(revenue_mart) -> None:
    dashboard = build_revenue_monthly_dashboard(revenue_mart)

    assert dashboard.count() == 1
    row = dashboard.first().asDict()
    assert row["municipality_key"] == "150111"
    assert row["anio"] == 2024
    assert row["mes"] == 4
    assert row["monto_pia"] == 150.0
    assert row["monto_pim"] == 200.0
    assert row["monto_recaudado"] == 150.0
    assert row["brecha_recaudacion"] == 50.0
    assert row["eficiencia_recaudacion"] == 0.75
    assert row["tiene_recaudacion"] is True
    assert "sec_ejec" not in dashboard.columns
    assert "source_resource_key" not in dashboard.columns
    assert "source_granularity" not in dashboard.columns
    assert dashboard.where(F.col("municipality_key").isNull()).count() == 0
    assert dashboard.where(F.col("tiene_recaudacion") == F.lit(False)).count() == 0


def test_revenue_source_monthly_dashboard_conserva_fuente_rubro_tipo(revenue_source_detail) -> None:
    dashboard = build_revenue_source_monthly_dashboard(revenue_source_detail)

    assert dashboard.count() == 1
    row = dashboard.first().asDict()
    assert row["fuente_financiamiento_codigo"] == "2"
    assert row["rubro_codigo"] == "09"
    assert row["tipo_recurso_codigo"] == "7"
    assert row["monto_pia"] == 150.0
    assert row["monto_recaudado"] == 150.0
    assert "generica_codigo" not in dashboard.columns
    assert "subgenerica_codigo" not in dashboard.columns
    assert "especifica_codigo" not in dashboard.columns


def test_revenue_source_annual_dashboard_agrega_por_municipalidad_anio_fuente(revenue_source_detail) -> None:
    monthly = build_revenue_source_monthly_dashboard(revenue_source_detail)
    annual = build_revenue_source_annual_dashboard(monthly)

    assert annual.count() == 1
    row = annual.first().asDict()
    assert row["anio"] == 2024
    assert row["fuente_financiamiento_codigo"] == "2"
    assert row["monto_pim"] == 200.0
    assert row["eficiencia_recaudacion"] == 0.75


def test_revenue_annual_dashboard_agrega_por_municipalidad_anio(revenue_mart) -> None:
    monthly = build_revenue_monthly_dashboard(revenue_mart)
    annual = build_revenue_annual_dashboard(monthly)

    assert annual.count() == 1
    assert annual.columns.count("anio") == 1
    assert "mes" not in annual.columns
    assert annual.first().asDict()["monto_recaudado"] == 150.0


def test_territorial_revenue_dashboard_no_duplica_municipalidades(revenue_mart) -> None:
    monthly = build_revenue_monthly_dashboard(revenue_mart)
    annual = build_revenue_annual_dashboard(monthly)
    territorial = build_territorial_revenue_dashboard(annual)

    assert territorial.count() == 1
    row = territorial.first().asDict()
    assert row["total_municipalidades"] == 1
    assert row["participacion_recaudacion_anual"] == 1.0


def test_predial_dashboard_recalcula_ratio_despues_de_agregar(predial_mart) -> None:
    dashboard = build_predial_dashboard(predial_mart)

    assert dashboard.count() == 1
    row = dashboard.first().asDict()
    assert row["monto_emision_predial_total"] == 150.0
    assert row["monto_recaudacion_predial_total"] == 90.0
    assert row["ratio_recaudacion_emision"] == 0.6


def test_municipal_context_dashboard_tiene_una_fila_por_municipalidad(context_mart) -> None:
    dashboard = build_municipal_context_dashboard(context_mart)

    assert dashboard.count() == 1
    row = dashboard.first().asDict()
    assert row["municipality_key"] == "150111"
    assert row["total_computadoras_operativas"] == 25
    assert row["cuenta_servicio_internet"] is True


def test_municipal_performance_dashboard_combina_sin_duplicar_por_siaf(
    revenue_mart,
    predial_mart,
    context_mart,
) -> None:
    revenue_monthly = build_revenue_monthly_dashboard(revenue_mart)
    revenue_annual = build_revenue_annual_dashboard(revenue_monthly)
    predial_dashboard = build_predial_dashboard(predial_mart)
    context_dashboard = build_municipal_context_dashboard(context_mart)

    performance = build_municipal_performance_dashboard(
        revenue_annual,
        predial_dashboard,
        context_dashboard,
    )

    assert performance.count() == 1
    row = performance.first().asDict()
    assert row["municipality_key"] == "150111"
    assert row["anio"] == 2024
    assert row["monto_recaudado"] == 150.0
    assert row["monto_emision_predial_total"] == 150.0
    assert row["indice_capacidad_digital"] == 1.0
    assert row["indice_capacidad_tributaria"] == 1.0
    assert row["segmento_desempeno"] == "desempeno_medio"


def test_no_exporta_facts_crudas_ni_depende_de_hive_odbc() -> None:
    assert "fact_siaf_income.csv" in BLOCKED_EXPORT_FILE_NAMES
    assert "mart_municipal_revenue_overview.csv" in BLOCKED_EXPORT_FILE_NAMES

    source_text = Path("src/powerbi/build_dashboard_export_marts.py").read_text(
        encoding="utf-8"
    ).lower()
    assert "odbc" not in source_text
    assert "hiveserver2" not in source_text
    assert "municipal_categories" not in source_text
    assert "municipal_entity_bridge" not in source_text
    assert "mef_municipal_amounts" not in source_text
    assert "renamu_full" not in source_text
    assert "renamu_municipal_context" not in source_text


def test_rutas_de_salida_se_restringen_a_gold_powerbi_y_exports_dashboard() -> None:
    base_path = Path("C:/dashboard-test")
    assert output_dataset_path(base_path / "data" / "gold" / "powerbi", "revenue_monthly_dashboard") == (
        base_path / "data" / "gold" / "powerbi" / "revenue_monthly_dashboard"
    )
    assert export_dataset_path(
        base_path / "powerbi" / "exports" / "dashboard",
        "revenue_monthly_dashboard",
        "csv",
    ) == (
        base_path / "powerbi" / "exports" / "dashboard" / "revenue_monthly_dashboard.csv"
    )
