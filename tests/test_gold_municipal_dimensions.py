"""Pruebas focales para dimensiones Gold municipales."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.gold.build_municipal_dimensions import (
    GOLD_DIMENSION_DATASETS,
    GoldDimensionError,
    build_dim_geography,
    build_dim_municipality,
    build_dim_renamu_context,
    build_dim_sismepre_period,
    build_dim_time_from_siaf_frames,
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
        .appName("test-gold-municipal-dimensions")
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
def renamu_context(spark: SparkSession):
    schema = StructType(
        [
            StructField("ubigeo6", StringType(), True),
            StructField("idmunici", StringType(), True),
            StructField("ccdd", StringType(), True),
            StructField("ccpp", StringType(), True),
            StructField("ccdi", StringType(), True),
            StructField("departamento_nombre", StringType(), True),
            StructField("provincia_nombre", StringType(), True),
            StructField("distrito_nombre", StringType(), True),
            StructField("tipomuni_codigo", StringType(), True),
            StructField("tipomuni_nombre", StringType(), True),
            StructField("total_computadoras_operativas", IntegerType(), True),
            StructField("cuenta_servicio_internet", BooleanType(), True),
            StructField("computadoras_con_acceso_internet", IntegerType(), True),
            StructField("usa_siaf", BooleanType(), True),
            StructField("portal_transparencia_actualizado", BooleanType(), True),
            StructField("total_personal_dic_2021", IntegerType(), True),
            StructField("tiene_area_ejecucion_coactiva", BooleanType(), True),
            StructField("requiere_asistencia_administracion_tributaria", BooleanType(), True),
            StructField("requiere_capacitacion_catastro", BooleanType(), True),
        ]
    )
    rows = [
        (
            "010101",
            "010101",
            "01",
            "01",
            "01",
            "AMAZONAS",
            "CHACHAPOYAS",
            "CHACHAPOYAS",
            "1",
            "Provincial",
            25,
            True,
            20,
            True,
            True,
            100,
            True,
            False,
            True,
        ),
        (
            "010102",
            "010102",
            "01",
            "01",
            "02",
            "AMAZONAS",
            "CHACHAPOYAS",
            "ASUNCION",
            "2",
            "Distrital",
            8,
            True,
            5,
            False,
            False,
            30,
            False,
            True,
            False,
        ),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture()
def classification_2019(spark: SparkSession):
    schema = StructType(
        [
            StructField("ubigeo6", StringType(), True),
            StructField("tipo_clasificacion_municipal", StringType(), True),
            StructField("ambito_municipal", StringType(), True),
            StructField("descripcion_tipo", StringType(), True),
        ]
    )
    return spark.createDataFrame(
        [
            ("010101", "A", "provincial", "Municipalidad tipo A"),
            ("010102", "G", "distrital", "Municipalidad tipo G"),
        ],
        schema,
    )


@pytest.fixture()
def siaf_income(spark: SparkSession):
    schema = StructType(
        [
            StructField("anio", IntegerType(), True),
            StructField("mes", IntegerType(), True),
        ]
    )
    return spark.createDataFrame([(2024, 1), (2024, 1), (2024, 2), (2024, 13)], schema)


@pytest.fixture()
def sismepre_esat(spark: SparkSession):
    schema = StructType(
        [
            StructField("anio_aplicacion", IntegerType(), True),
            StructField("periodo", IntegerType(), True),
            StructField("anio_estadistica", IntegerType(), True),
            StructField("mes_estadistica", IntegerType(), True),
            StructField("periodo_estadistica_tipo", StringType(), True),
            StructField("is_annual_stat_period", BooleanType(), True),
            StructField("monto_recaudacion_predial_total", IntegerType(), True),
        ]
    )
    return spark.createDataFrame([(2024, 1, 2023, 12, "anual", True, 500)], schema)


def test_registry_incluye_solo_dimensiones_municipales_objetivo():
    assert GOLD_DIMENSION_DATASETS == [
        "dim_municipality",
        "dim_geography",
        "dim_renamu_context",
        "dim_time",
        "dim_sismepre_period",
    ]
    assert not any(dataset.startswith("fact_") for dataset in GOLD_DIMENSION_DATASETS)
    assert not any(dataset.startswith("mart_") for dataset in GOLD_DIMENSION_DATASETS)


def test_validate_selected_datasets_rechaza_no_soportados():
    assert validate_selected_datasets(None) == GOLD_DIMENSION_DATASETS
    with pytest.raises(GoldDimensionError):
        validate_selected_datasets(["dim_time", "fact_siaf_income"])


def test_output_dataset_path_escribe_en_raiz_gold():
    assert output_dataset_path(Path("data/gold"), "dim_municipality") == Path(
        "data/gold/dim_municipality"
    )


def test_dim_municipality_tiene_contrato_y_nombre_estandar(
    renamu_context,
    classification_2019,
):
    dim = build_dim_municipality(
        renamu_context,
        classification_2019,
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert dim.columns == [
        "municipality_key",
        "ubigeo6",
        "geography_key",
        "idmunici",
        "municipalidad_nombre",
        "tipomuni_codigo",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        "gold_processed_at_utc",
    ]
    assert "municipalidad_nombre" in dim.columns

    forbidden_columns = {
        "municipalidad_siaf_nombre",
        "municipalidad_sismepre_nombre",
        "nombre_siaf",
        "nombre_sismepre",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
    }
    assert forbidden_columns.isdisjoint(dim.columns)

    rows = {row["ubigeo6"]: row.asDict() for row in dim.collect()}
    assert rows["010101"]["municipality_key"] == "010101"
    assert rows["010101"]["geography_key"] == "010101"
    assert rows["010102"]["municipality_key"] == "010102"
    assert rows["010102"]["geography_key"] == "010102"
    assert rows["010101"]["municipalidad_nombre"] == (
        "MUNICIPALIDAD PROVINCIAL DE CHACHAPOYAS"
    )
    assert rows["010102"]["municipalidad_nombre"] == (
        "MUNICIPALIDAD DISTRITAL DE ASUNCION"
    )


def test_dim_geography_tiene_una_fila_por_ubigeo(renamu_context):
    dim = build_dim_geography(
        renamu_context,
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert dim.columns == [
        "geography_key",
        "ubigeo6",
        "ccdd",
        "ccpp",
        "ccdi",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "gold_processed_at_utc",
    ]
    assert dim.count() == dim.select("ubigeo6").distinct().count()
    assert dim.where(dim.geography_key != dim.ubigeo6).count() == 0


def test_dim_renamu_context_queda_separada_sin_clasificacion_ni_metricas(
    renamu_context,
):
    dim = build_dim_renamu_context(
        renamu_context,
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert "municipality_key" in dim.columns
    assert "ubigeo6" in dim.columns
    assert "total_computadoras_operativas" in dim.columns
    assert "portal_transparencia_actualizado" in dim.columns

    forbidden_columns = {
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        "monto_recaudado",
        "monto_recaudacion_predial_total",
        "ccdd",
        "ccpp",
        "ccdi",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
    }
    assert forbidden_columns.isdisjoint(dim.columns)
    assert dim.where(dim.municipality_key != dim.ubigeo6).count() == 0


def test_dim_time_tiene_grano_mensual(siaf_income):
    dim = build_dim_time_from_siaf_frames(
        [siaf_income],
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert dim.columns == [
        "date_key",
        "fecha_mes",
        "anio",
        "mes",
        "anio_mes",
        "trimestre",
        "semestre",
        "gold_processed_at_utc",
    ]
    assert isinstance(dim.schema["fecha_mes"].dataType, DateType)
    assert dim.count() == 2

    rows = {row["anio_mes"]: row.asDict() for row in dim.collect()}
    assert rows["2024-01"]["date_key"] == 20240101
    assert rows["2024-01"]["trimestre"] == 1
    assert rows["2024-01"]["semestre"] == 1
    assert rows["2024-02"]["date_key"] == 20240201


def test_dim_sismepre_period_usa_campos_del_recurso_principal(sismepre_esat):
    dim = build_dim_sismepre_period(
        sismepre_esat,
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )

    assert dim.columns == [
        "sismepre_period_key",
        "anio_aplicacion",
        "periodo",
        "anio_estadistica",
        "mes_estadistica",
        "periodo_estadistica_tipo",
        "is_annual_stat_period",
        "periodo_label",
        "gold_processed_at_utc",
    ]
    assert dim.count() == 1
    row = dim.first().asDict()
    assert row["sismepre_period_key"] == "2024_01_2023_12"
    assert row["periodo_label"] == "Aplicacion 2024 - Periodo 1 - Estadistica 2023 12 (anual)"
    assert row["is_annual_stat_period"] is True
    assert "monto_recaudacion_predial_total" not in dim.columns


def test_builder_no_usa_fuentes_legacy_ni_construye_hechos_o_marts():
    source_text = Path("src/gold/build_municipal_dimensions.py").read_text(encoding="utf-8")

    blocked_terms = [
        "municipal_categories",
        "municipal_entity_bridge",
        "renamu_full",
        "base_renamu_2022",
        "fact_",
        "mart_",
        "respuestas",
        "preguntas",
        "formulario",
        "ano_aplicacion",
        "entidad_estado",
    ]
    for term in blocked_terms:
        assert term not in source_text
