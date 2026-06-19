"""Pruebas focales para marts Gold de auditoria y monitoreo."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from src.gold.build_audit_quality_marts import (
    DATASET_SUMMARY_COLUMNS,
    GOLD_AUDIT_DATASETS,
    GoldAuditError,
    build_audit_dataset_summary,
    build_audit_integration_coverage,
    build_audit_quality_results,
    output_dataset_path,
    validate_selected_datasets,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Crea una sesion Spark local para contratos Gold de auditoria."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-audit-quality-marts")
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
def bronze_quality_records():
    return [
        {
            "run_id": "quality_bronze_1",
            "source_name": "siaf_income",
            "resource_key": "annual_2024",
            "dataset_path": "data/bronze/siaf_income/resource_key=annual_2024",
            "rule_id": "row_count_positive",
            "rule_type": "completeness",
            "severity": "FAIL",
            "status": "PASS",
            "evaluated": True,
            "observed_value": "25",
            "expected_value": ">0",
            "message": "El dataset tiene filas.",
            "processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
        {
            "run_id": "quality_bronze_1",
            "source_name": "siaf_income",
            "resource_key": "annual_2024",
            "dataset_path": "data/bronze/siaf_income/resource_key=annual_2024",
            "rule_id": "duplicate_rows_detected",
            "rule_type": "conformity",
            "severity": "WARNING",
            "status": "WARNING",
            "evaluated": True,
            "observed_value": "2",
            "expected_value": "0",
            "message": "Se detectaron duplicados.",
            "processed_at_utc": "2026-06-19T00:00:00+00:00",
        },
    ]


@pytest.fixture()
def silver_quality_records():
    return [
        {
            "run_id": "quality_silver_1",
            "layer": "silver",
            "source_name": "integrated",
            "resource_key": "integration_coverage",
            "rule_name": "match_rate_within_bounds",
            "status": "PASS",
            "severity": "FAIL",
            "evaluated": True,
            "message": "Match rate valido.",
            "details": {
                "rule_category": "validity",
                "check_name": "match_rate",
                "metric_name": "match_rate",
                "metric_value": 0.95,
                "expected_value": "0..1",
                "actual_value": 0.95,
                "dataset_path": "data/silver/integrated/integration_coverage",
            },
            "checked_at_utc": "2026-06-19T01:00:00+00:00",
        },
        {
            "run_id": "quality_silver_1",
            "layer": "silver",
            "source_name": "integrated",
            "resource_key": "integration_coverage",
            "rule_name": "issue_rate_within_bounds",
            "status": "ERROR",
            "severity": "FAIL",
            "evaluated": True,
            "message": "No se pudo evaluar issue rate.",
            "details": {
                "rule_category": "validity",
                "check_name": "issue_rate",
                "metric_name": "issue_rate",
                "metric_value": None,
                "expected_value": "0..1",
                "actual_value": None,
                "dataset_path": "data/silver/integrated/integration_coverage",
            },
            "checked_at_utc": "2026-06-19T01:00:00+00:00",
        },
    ]


@pytest.fixture()
def integration_coverage_dataframe(spark: SparkSession):
    rows = [
        (
            "map_sec_ejec_ubigeo",
            "integrated",
            "match_rate",
            0.95,
            100.0,
            95.0,
            5.0,
            0.95,
            5.0,
            0.05,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "issue_rate",
            0.05,
            100.0,
            95.0,
            5.0,
            0.95,
            5.0,
            0.05,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
    ]
    schema = (
        "coverage_scope string, source_name string, metric_name string, metric_value double, "
        "total_records double, matched_records double, unmatched_records double, match_rate double, "
        "issue_count double, issue_rate double, silver_source_name string, silver_resource_key string, "
        "silver_processed_at_utc string"
    )
    return spark.createDataFrame(rows, schema=schema)


def test_registry_incluye_datasets_audit_objetivo():
    assert GOLD_AUDIT_DATASETS == [
        "audit_quality_results",
        "audit_dataset_summary",
        "audit_integration_coverage",
    ]


def test_validate_selected_datasets_rechaza_no_soportados():
    assert validate_selected_datasets(None) == GOLD_AUDIT_DATASETS
    with pytest.raises(GoldAuditError):
        validate_selected_datasets(["audit_quality_results", "mart_municipal_context"])


def test_output_dataset_path_escribe_en_raiz_gold():
    assert output_dataset_path(Path("data/gold"), "audit_quality_results") == Path(
        "data/gold/audit_quality_results"
    )


def test_audit_quality_results_tiene_contrato_y_trazabilidad(
    spark: SparkSession,
    bronze_quality_records,
    silver_quality_records,
):
    audit = build_audit_quality_results(
        spark,
        bronze_results=bronze_quality_records,
        silver_results=silver_quality_records,
        processed_at_utc="2026-06-19T02:00:00+00:00",
    )

    assert audit.columns == [
        "quality_result_key",
        "layer_name",
        "dataset_name",
        "resource_key",
        "check_name",
        "rule_name",
        "rule_category",
        "severity",
        "status",
        "message",
        "metric_name",
        "metric_value",
        "expected_value",
        "actual_value",
        "checked_at_utc",
        "source_file_path",
        "gold_processed_at_utc",
    ]

    rows = {(row["layer_name"], row["rule_name"]): row.asDict() for row in audit.collect()}
    bronze_row = rows[("bronze", "row_count_positive")]
    silver_row = rows[("silver", "match_rate_within_bounds")]

    assert bronze_row["dataset_name"] == "siaf_income"
    assert bronze_row["resource_key"] == "annual_2024"
    assert bronze_row["status"] == "PASS"
    assert bronze_row["severity"] == "FAIL"
    assert bronze_row["message"] == "El dataset tiene filas."
    assert bronze_row["metric_name"] == "row_count"
    assert bronze_row["metric_value"] == 25.0

    assert silver_row["dataset_name"] == "integrated"
    assert silver_row["resource_key"] == "integration_coverage"
    assert silver_row["check_name"] == "match_rate"
    assert silver_row["rule_category"] == "validity"
    assert silver_row["metric_name"] == "match_rate"
    assert silver_row["metric_value"] == 0.95


def test_audit_dataset_summary_agrega_estados_y_calcula_quality_score(
    spark: SparkSession,
    bronze_quality_records,
    silver_quality_records,
):
    audit = build_audit_quality_results(
        spark,
        bronze_results=bronze_quality_records,
        silver_results=silver_quality_records,
        processed_at_utc="2026-06-19T02:00:00+00:00",
    )
    summary = build_audit_dataset_summary(
        audit,
        processed_at_utc="2026-06-19T02:10:00+00:00",
    )

    assert summary.columns == DATASET_SUMMARY_COLUMNS

    rows = {
        (row["layer_name"], row["dataset_name"], row["resource_key"]): row.asDict()
        for row in summary.collect()
    }

    bronze_row = rows[("bronze", "siaf_income", "annual_2024")]
    silver_row = rows[("silver", "integrated", "integration_coverage")]

    assert bronze_row["total_checks"] == 2
    assert bronze_row["pass_count"] == 1
    assert bronze_row["warning_count"] == 1
    assert bronze_row["fail_count"] == 0
    assert bronze_row["error_count"] == 0
    assert bronze_row["quality_score"] == 0.5
    assert bronze_row["row_count"] == 25.0
    assert bronze_row["duplicate_rows"] == 2.0

    assert silver_row["total_checks"] == 2
    assert silver_row["pass_count"] == 1
    assert silver_row["warning_count"] == 0
    assert silver_row["fail_count"] == 0
    assert silver_row["error_count"] == 1
    assert silver_row["quality_score"] == 0.5
    assert silver_row["validity_score"] == 0.5


def test_quality_score_evita_division_por_cero(spark: SparkSession):
    audit = build_audit_quality_results(
        spark,
        silver_results=[
            {
                "run_id": "quality_silver_2",
                "layer": "silver",
                "source_name": "sample",
                "resource_key": "resource",
                "rule_name": "sample_rule",
                "status": "ERROR",
                "severity": "FAIL",
                "evaluated": True,
                "message": "Fallo de lectura.",
                "details": {"check_name": "sample_check"},
                "checked_at_utc": "2026-06-19T00:00:00+00:00",
            }
        ],
        processed_at_utc="2026-06-19T00:00:00+00:00",
    )
    summary = build_audit_dataset_summary(audit)
    row = summary.first().asDict()

    assert row["total_checks"] == 1
    assert row["quality_score"] == 0.0


def test_audit_integration_coverage_conserva_metricas_y_tasas(
    integration_coverage_dataframe,
):
    audit = build_audit_integration_coverage(
        integration_coverage_dataframe,
        processed_at_utc="2026-06-19T03:00:00+00:00",
    )

    assert audit.columns == [
        "coverage_scope",
        "source_name",
        "metric_name",
        "metric_value",
        "total_records",
        "matched_records",
        "unmatched_records",
        "match_rate",
        "issue_count",
        "issue_rate",
        "gold_processed_at_utc",
    ]
    assert isinstance(audit.schema["match_rate"].dataType, DoubleType)

    rows = {row["metric_name"]: row.asDict() for row in audit.collect()}
    assert 0.0 <= rows["match_rate"]["match_rate"] <= 1.0
    assert 0.0 <= rows["issue_rate"]["issue_rate"] <= 1.0


def test_builder_no_usa_tablas_legacy_ni_contamina_modelo_principal():
    source_text = Path("src/gold/build_audit_quality_marts.py").read_text(
        encoding="utf-8"
    )

    blocked_terms = [
        "municipal_categories",
        "municipal_entity_bridge",
        "mef_municipal_amounts",
        "renamu_full",
        "renamu_municipal_context",
        "dim_municipality",
        "fact_siaf_income",
        "fact_predial_statistics",
        "mart_municipal_revenue_overview",
        "export_gold_fallback",
        "runner_global",
        "pbix",
    ]
    for term in blocked_terms:
        assert term not in source_text
