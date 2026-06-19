"""Pruebas para el mapa tecnico Silver `sec_ejec -> ubigeo6`."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.silver.integrate_municipal_sources import (
    INTEGRATED_DATASETS,
    IntegrationPaths,
    SilverIntegrationError,
    build_map_sec_ejec_ubigeo_from_frames,
    calculate_coverage_percentage,
    existing_columns,
    missing_required_columns,
    normalize_metric_row,
    output_dataset_path,
    run_integration,
    selected_dataset_names,
)


@pytest.fixture()
def spark() -> SparkSession:
    """Crea una sesion Spark local para pruebas."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-sec-ejec-ubigeo")
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


def sample_frames(spark: SparkSession) -> dict[str, object]:
    """Construye DataFrames en memoria para el mapa tecnico."""

    schema_sec_ejec = StructType([StructField("sec_ejec", StringType(), True)])
    schema_ubigeo = StructType([StructField("ubigeo6", StringType(), True)])
    schema_mapping = StructType(
        [
            StructField("sec_ejec", StringType(), True),
            StructField("ubigeo6", StringType(), True),
        ]
    )

    siaf_sec_ejec = spark.createDataFrame(
        [
            {"sec_ejec": "000123"},
            {"sec_ejec": "000125"},
            {"sec_ejec": "000126"},
            {"sec_ejec": "000127"},
        ],
        schema=schema_sec_ejec,
    )
    sismepre = spark.createDataFrame(
        [
            {"sec_ejec": "000123", "ubigeo6": "010101"},
            {"sec_ejec": "000124", "ubigeo6": "010102"},
            {"sec_ejec": "000125", "ubigeo6": "010103"},
            {"sec_ejec": "000126", "ubigeo6": "010104"},
            {"sec_ejec": "000127", "ubigeo6": "010105"},
            {"sec_ejec": "000127", "ubigeo6": "010106"},
            {"sec_ejec": "000127", "ubigeo6": "010105"},
        ],
        schema=schema_mapping,
    )
    renamu_ubigeos = spark.createDataFrame(
        [
            {"ubigeo6": "010101"},
            {"ubigeo6": "010102"},
            {"ubigeo6": "010104"},
            {"ubigeo6": "010105"},
            {"ubigeo6": "010106"},
        ],
        schema=schema_ubigeo,
    )
    classification_ubigeos = spark.createDataFrame(
        [
            {"ubigeo6": "010101"},
            {"ubigeo6": "010102"},
            {"ubigeo6": "010103"},
            {"ubigeo6": "010105"},
            {"ubigeo6": "010106"},
        ],
        schema=schema_ubigeo,
    )

    return {
        "siaf_sec_ejec": siaf_sec_ejec,
        "sismepre": sismepre,
        "renamu_ubigeos": renamu_ubigeos,
        "classification_ubigeos": classification_ubigeos,
    }


def test_output_dataset_path_accepts_only_map_dataset() -> None:
    """La ruta integrada solo acepta el mapa tecnico objetivo."""

    root = Path("data/silver/integrated")

    assert output_dataset_path(root, "map_sec_ejec_ubigeo") == root / "map_sec_ejec_ubigeo"
    assert INTEGRATED_DATASETS == ["map_sec_ejec_ubigeo"]

    with pytest.raises(SilverIntegrationError):
        output_dataset_path(root, "integration_coverage")


def test_selected_dataset_names_validates_cli_values() -> None:
    """La seleccion opcional de datasets rechaza nombres no soportados."""

    assert selected_dataset_names(None) == ["map_sec_ejec_ubigeo"]
    assert selected_dataset_names(["map_sec_ejec_ubigeo"]) == ["map_sec_ejec_ubigeo"]

    with pytest.raises(SilverIntegrationError):
        selected_dataset_names(["municipal_entity_bridge"])


def test_existing_columns_and_missing_required_columns() -> None:
    """La seleccion de columnas conserva orden y detecta faltantes."""

    columns = ["sec_ejec", "ubigeo6", "municipality_key"]
    desired = ["municipality_key", "match_status", "sec_ejec"]

    assert existing_columns(columns, desired) == ["municipality_key", "sec_ejec"]
    assert missing_required_columns(columns, ["sec_ejec", "ubigeo6"]) == []
    assert missing_required_columns(columns, ["sec_ejec", "issue_reason"]) == ["issue_reason"]


def test_calculate_coverage_percentage() -> None:
    """El porcentaje de cobertura evita division entre cero."""

    assert calculate_coverage_percentage(8, 10) == 80.0
    assert calculate_coverage_percentage(1, 3) == 33.3333
    assert calculate_coverage_percentage(1, 0) == 0.0


def test_normalize_metric_row_is_serializable() -> None:
    """Una metrica de cobertura conserva estructura esperada."""

    row = normalize_metric_row(
        metric_name="matched",
        numerator=4,
        denominator=5,
        description="Cobertura de prueba.",
    )

    assert row == {
        "metric_name": "matched",
        "numerator": 4,
        "denominator": 5,
        "coverage_percentage": 80.0,
        "description": "Cobertura de prueba.",
    }


def test_build_map_sec_ejec_ubigeo_creates_expected_contract(
    spark: SparkSession,
) -> None:
    """El mapa tecnico final expone el contrato esperado y casos de cobertura."""

    frames = sample_frames(spark)
    dataframe = build_map_sec_ejec_ubigeo_from_frames(
        sismepre=frames["sismepre"],
        siaf_sec_ejec=frames["siaf_sec_ejec"],
        renamu_ubigeos=frames["renamu_ubigeos"],
        classification_ubigeos=frames["classification_ubigeos"],
        processed_at="2026-01-01T00:00:00+00:00",
    )
    rows = {(row["sec_ejec"], row["ubigeo6"]): row.asDict() for row in dataframe.collect()}

    assert dataframe.columns == [
        "sec_ejec",
        "ubigeo6",
        "municipality_key",
        "has_siaf_match",
        "has_sismepre_match",
        "has_renamu_match",
        "has_classification_match",
        "match_status",
        "confidence_level",
        "issue_reason",
        "silver_source_name",
        "silver_resource_key",
        "silver_processed_at_utc",
    ]

    assert "municipalidad_siaf_nombre" not in dataframe.columns
    assert "municipalidad_sismepre_nombre" not in dataframe.columns
    assert "municipalidad_renamu_nombre" not in dataframe.columns
    assert "nombre_siaf" not in dataframe.columns
    assert "nombre_sismepre" not in dataframe.columns

    matched = rows[("000123", "010101")]
    assert matched["municipality_key"] == matched["ubigeo6"]
    assert matched["ubigeo6"] == "010101"
    assert matched["has_siaf_match"] is True
    assert matched["has_sismepre_match"] is True
    assert matched["has_renamu_match"] is True
    assert matched["has_classification_match"] is True
    assert matched["match_status"] == "matched"
    assert matched["confidence_level"] == "high"
    assert matched["issue_reason"] == "ok"
    assert matched["silver_source_name"] == "integrated"
    assert matched["silver_resource_key"] == "map_sec_ejec_ubigeo"
    assert matched["silver_processed_at_utc"] == "2026-01-01T00:00:00+00:00"

    assert rows[("000124", "010102")]["match_status"] == "missing_siaf"
    assert rows[("000124", "010102")]["issue_reason"] == "sec_ejec_not_found_in_siaf"
    assert rows[("000124", "010102")]["confidence_level"] == "medium"
    assert rows[("000124", "010102")]["has_siaf_match"] is False

    assert rows[("000125", "010103")]["match_status"] == "missing_renamu"
    assert rows[("000125", "010103")]["issue_reason"] == "ubigeo_not_found_in_renamu"
    assert rows[("000125", "010103")]["has_renamu_match"] is False

    assert rows[("000126", "010104")]["match_status"] == "missing_classification"
    assert rows[("000126", "010104")]["issue_reason"] == "ubigeo_not_found_in_classification"
    assert rows[("000126", "010104")]["has_classification_match"] is False

    assert rows[("000127", "010105")]["match_status"] == "ambiguous_sec_ejec_ubigeo"
    assert rows[("000127", "010105")]["issue_reason"] == "duplicated_sec_ejec_ubigeo"
    assert rows[("000127", "010105")]["confidence_level"] == "low"
    assert rows[("000127", "010106")]["match_status"] == "ambiguous_sec_ejec"
    assert rows[("000127", "010106")]["issue_reason"] == "sec_ejec_maps_to_multiple_ubigeo6"

    assert {row["ubigeo6"] for row in dataframe.select("ubigeo6").distinct().collect()} <= {
        "010101",
        "010102",
        "010103",
        "010104",
        "010105",
        "010106",
    }


def test_source_module_does_not_reference_legacy_inputs() -> None:
    """El modulo de integracion no debe depender de nombres legacy."""

    source_text = Path("src/silver/integrate_municipal_sources.py").read_text(encoding="utf-8")

    assert "municipal_categories" not in source_text
    assert "categorias_municipalidades" not in source_text
    assert "CategoriasMunicipalidades.csv" not in source_text
    assert "renamu_full" not in source_text
    assert "base_renamu_2022" not in source_text
    assert "municipal_entity_bridge" not in source_text


def test_run_integration_dry_run_uses_resolved_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """El dry-run usa las rutas resueltas y no escribe salida pesada."""

    dummy_paths = IntegrationPaths(
        siaf_root=Path("C:/tmp/siaf"),
        sismepre_path=Path("C:/tmp/sismepre"),
        renamu_path=Path("C:/tmp/renamu"),
        classification_path=Path("C:/tmp/classification"),
        output_root=Path("C:/tmp/integrated"),
    )

    monkeypatch.setattr(
        "src.silver.integrate_municipal_sources.resolve_paths",
        lambda output_subdir: dummy_paths,
    )
    monkeypatch.setattr(
        "src.silver.integrate_municipal_sources.validate_input_paths",
        lambda paths: None,
    )
    monkeypatch.setattr(
        "src.silver.integrate_municipal_sources.build_dry_run_schema_summary",
        lambda spark_obj, paths, limit: {"ok": True, "paths": str(paths.output_root)},
    )

    result = run_integration(
        dry_run=True,
        overwrite=False,
        selected_sources=None,
        limit=None,
        output_subdir="integrated",
    )

    assert result["datasets"] == ["map_sec_ejec_ubigeo"]
    assert result["output_root"] == str(dummy_paths.output_root)
