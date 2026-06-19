"""Pruebas unitarias para reglas de calidad Silver."""

from __future__ import annotations

import json
import os
import tempfile
import sys
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import load_quality_rules_config
from src.quality.generate_silver_quality_report import (
    SilverQualityReportError,
    read_silver_quality_results,
    render_html_report,
)
from src.quality.run_silver_quality_checks import (
    VALID_STATUSES,
    SilverDataset,
    build_expected_datasets,
    build_silver_quality_result,
    check_dataset_contract_rules,
    evaluate_required_columns,
    invalid_boolean_flag_values,
    load_silver_quality_config,
    run_silver_quality_checks,
    status_from_warning_count,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Crea una sesion Spark local para pruebas de contrato Silver."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-quality-rules")
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


def make_map_dataframe(
    spark: SparkSession,
    *,
    sec_ejec: str = "301260",
    ubigeo6: str = "150111",
    municipality_key: str | None = None,
    match_status: str = "matched",
    confidence_level: str = "high",
    issue_reason: str = "ok",
    has_siaf_match: bool = True,
    has_sismepre_match: bool = True,
    has_renamu_match: bool = True,
    has_classification_match: bool = True,
    include_forbidden: bool = False,
):
    """Construye un mapa tecnico minimo para pruebas de contrato."""

    schema_fields = [
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
        StructField("silver_source_name", StringType(), True),
        StructField("silver_resource_key", StringType(), True),
        StructField("silver_processed_at_utc", StringType(), True),
    ]

    row = {
        "sec_ejec": sec_ejec,
        "ubigeo6": ubigeo6,
        "municipality_key": municipality_key or ubigeo6,
        "has_siaf_match": has_siaf_match,
        "has_sismepre_match": has_sismepre_match,
        "has_renamu_match": has_renamu_match,
        "has_classification_match": has_classification_match,
        "match_status": match_status,
        "confidence_level": confidence_level,
        "issue_reason": issue_reason,
        "silver_source_name": "integrated",
        "silver_resource_key": "map_sec_ejec_ubigeo",
        "silver_processed_at_utc": "2026-06-19T00:00:00+00:00",
    }

    if include_forbidden:
        schema_fields.extend(
            [
                StructField("municipalidad_siaf_nombre", StringType(), True),
                StructField("nombre_sismepre", StringType(), True),
            ]
        )
        row["municipalidad_siaf_nombre"] = "MUNICIPALIDAD DEMO"
        row["nombre_sismepre"] = "MUNICIPALIDAD DEMO"

    schema = StructType(schema_fields)
    return spark.createDataFrame([row], schema=schema)


def make_coverage_dataframe(
    spark: SparkSession,
    *,
    match_rate: float = 0.75,
    issue_rate: float = 0.25,
):
    """Construye una cobertura tecnica minima para pruebas de contrato."""

    rows = [
        (
            "map_sec_ejec_ubigeo",
            "integrated",
            "total_map_records",
            6.0,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_sec_ejec_ubigeo",
            "integrated",
            "matched_records",
            5.0,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_sec_ejec_ubigeo",
            "integrated",
            "unmatched_records",
            1.0,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_sec_ejec_ubigeo",
            "integrated",
            "match_rate",
            match_rate,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "missing_siaf",
            1.0,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "missing_renamu",
            1.0,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "missing_classification",
            1.0,
            6,
            5,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "invalid_ubigeo",
            0.0,
            6,
            6,
            0,
            match_rate,
            0,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "high_confidence_records",
            1.0,
            6,
            1,
            5,
            match_rate,
            0,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "medium_confidence_records",
            3.0,
            6,
            3,
            3,
            match_rate,
            0,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "map_quality",
            "integrated",
            "low_confidence_records",
            2.0,
            6,
            2,
            4,
            match_rate,
            0,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "siaf_to_sismepre",
            "siaf_income",
            "missing_sismepre",
            0.0,
            5,
            5,
            0,
            match_rate,
            0,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "renamu_to_classification",
            "renamu",
            "missing_classification",
            1.0,
            5,
            4,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
        (
            "classification_to_renamu",
            "municipal_classification",
            "missing_renamu",
            1.0,
            5,
            4,
            1,
            match_rate,
            1,
            issue_rate,
            "integrated",
            "integration_coverage",
            "2026-06-19T00:00:00+00:00",
        ),
    ]

    schema = StructType(
        [
            StructField("coverage_scope", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("metric_name", StringType(), True),
            StructField("metric_value", DoubleType(), True),
            StructField("total_records", IntegerType(), True),
            StructField("matched_records", IntegerType(), True),
            StructField("unmatched_records", IntegerType(), True),
            StructField("match_rate", DoubleType(), True),
            StructField("issue_count", IntegerType(), True),
            StructField("issue_rate", DoubleType(), True),
            StructField("silver_source_name", StringType(), True),
            StructField("silver_resource_key", StringType(), True),
            StructField("silver_processed_at_utc", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def test_silver_quality_config_has_valid_statuses() -> None:
    """La configuracion Silver usa PASS, WARNING y FAIL."""

    config = load_quality_rules_config()
    statuses = set(config["quality"]["valid_statuses"])

    assert statuses == {"PASS", "WARNING", "FAIL"}
    assert "SKIPPED" not in statuses


def test_silver_quality_config_defines_sources_and_outputs() -> None:
    """La seccion Silver define fuentes y salidas propias."""

    quality_config = load_silver_quality_config()
    silver_config = quality_config["silver"]

    assert silver_config["output"]["results_jsonl"] == (
        "data/quality/silver_quality_results.jsonl"
    )
    assert silver_config["output"]["report_html"] == "reports/silver_quality_report.html"
    assert set(silver_config["sources"]) == {
        "siaf_income",
        "sismepre",
        "renamu",
        "municipal_classification",
        "integrated",
    }


def test_build_expected_datasets_reads_curated_silver_resources() -> None:
    """La configuracion Silver declara los recursos curados vigentes."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(quality_config)
    source_to_resources: dict[str, set[str]] = {}
    for dataset in datasets:
        source_to_resources.setdefault(dataset.source_name, set()).add(dataset.resource_key)

    assert "siaf_income" in source_to_resources
    assert source_to_resources["sismepre"] == {"esat_estadistica_atm"}
    assert source_to_resources["renamu"] == {"municipal_context"}
    assert source_to_resources["municipal_classification"] == {"classification_2019"}
    assert source_to_resources["integrated"] == {
        "map_sec_ejec_ubigeo",
        "integration_coverage",
    }
    assert "municipal_categories" not in source_to_resources
    all_resources = {resource for resources in source_to_resources.values() for resource in resources}
    assert "base_renamu_2022" not in all_resources
    assert "renamu_full" not in all_resources


def test_build_expected_datasets_filters_source_and_resource() -> None:
    """Los filtros de fuente y recurso reducen el plan de evaluacion."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(
        quality_config,
        selected_sources=["renamu"],
        selected_resources=["municipal_context"],
    )

    assert len(datasets) == 1
    assert datasets[0].source_name == "renamu"
    assert datasets[0].resource_key == "municipal_context"


def test_map_sec_ejec_ubigeo_contract_rules_validate_expected_contract(
    spark: SparkSession,
) -> None:
    """El mapa tecnico Silver cumple el contrato curado esperado."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(quality_config, selected_sources=["integrated"])
    dataset = next(item for item in datasets if item.resource_key == "map_sec_ejec_ubigeo")
    dataframe = make_map_dataframe(spark)

    results = check_dataset_contract_rules(
        run_id="run",
        dataset=dataset,
        dataframe=dataframe,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    results_by_rule = {result.rule_name: result for result in results}

    assert results_by_rule["contract_required_columns_present"].status == "PASS"
    assert results_by_rule["contract_forbidden_columns_absent"].status == "PASS"
    assert results_by_rule["expected_column_types"].status == "PASS"
    assert results_by_rule["sec_ejec_format"].status == "PASS"
    assert results_by_rule["ubigeo6_format"].status == "PASS"
    assert results_by_rule["municipality_key_equals_ubigeo6"].status == "PASS"
    assert results_by_rule["match_status_allowed_values"].status == "PASS"
    assert results_by_rule["confidence_level_allowed_values"].status == "PASS"


def test_map_sec_ejec_ubigeo_contract_rules_detect_invalid_values(
    spark: SparkSession,
) -> None:
    """El mapa tecnico detecta ubigeo invalido, estados invalidos y llaves rotas."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(quality_config, selected_sources=["integrated"])
    dataset = next(item for item in datasets if item.resource_key == "map_sec_ejec_ubigeo")
    dataframe = make_map_dataframe(
        spark,
        sec_ejec="30A26",
        ubigeo6="ABCDE",
        municipality_key="999999",
        match_status="desconocido",
        confidence_level="ultra",
        include_forbidden=True,
    )

    results = check_dataset_contract_rules(
        run_id="run",
        dataset=dataset,
        dataframe=dataframe,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    results_by_rule = {result.rule_name: result for result in results}

    assert results_by_rule["sec_ejec_format"].status == "FAIL"
    assert results_by_rule["ubigeo6_format"].status == "FAIL"
    assert results_by_rule["municipality_key_equals_ubigeo6"].status == "FAIL"
    assert results_by_rule["match_status_allowed_values"].status == "FAIL"
    assert results_by_rule["confidence_level_allowed_values"].status == "FAIL"
    assert results_by_rule["contract_forbidden_columns_absent"].status == "FAIL"


def test_integration_coverage_contract_rules_validate_expected_contract(
    spark: SparkSession,
) -> None:
    """La cobertura integrada expone el contrato tecnico esperado."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(quality_config, selected_sources=["integrated"])
    dataset = next(
        item for item in datasets if item.resource_key == "integration_coverage"
    )
    dataframe = make_coverage_dataframe(spark)

    results = check_dataset_contract_rules(
        run_id="run",
        dataset=dataset,
        dataframe=dataframe,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    results_by_rule = {result.rule_name: result for result in results}

    assert results_by_rule["contract_required_columns_present"].status == "PASS"
    assert results_by_rule["expected_column_types"].status == "PASS"
    assert results_by_rule["required_metric_names_present"].status == "PASS"
    assert results_by_rule["match_rate_within_bounds"].status == "PASS"
    assert results_by_rule["issue_rate_within_bounds"].status == "PASS"
    assert results_by_rule["total_records_nonnegative"].status == "PASS"


def test_integration_coverage_contract_rules_detect_out_of_range_rates(
    spark: SparkSession,
) -> None:
    """La cobertura integrada detecta tasas fuera de rango."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(quality_config, selected_sources=["integrated"])
    dataset = next(
        item for item in datasets if item.resource_key == "integration_coverage"
    )
    dataframe = make_coverage_dataframe(spark, match_rate=1.2, issue_rate=-0.1)

    results = check_dataset_contract_rules(
        run_id="run",
        dataset=dataset,
        dataframe=dataframe,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    results_by_rule = {result.rule_name: result for result in results}

    assert results_by_rule["match_rate_within_bounds"].status == "FAIL"
    assert results_by_rule["issue_rate_within_bounds"].status == "FAIL"


def test_build_silver_quality_result_is_json_serializable() -> None:
    """Un resultado Silver conserva la estructura JSONL esperada."""

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp_dir:
        dataset = SilverDataset(
            source_name="sample",
            resource_key="one",
            dataset_path=Path(temp_dir) / "resource_key=one",
            source_config={},
            resource_config={},
        )
        result = build_silver_quality_result(
            run_id="run",
            dataset=dataset,
            rule_name="dataset_readable",
            status="PASS",
            severity="FAIL",
            evaluated=True,
            message="ok",
            details={"rows": 1},
            checked_at_utc="2026-01-01T00:00:00+00:00",
        )

    payload = asdict(result)
    json.dumps(payload, ensure_ascii=False)

    assert payload["layer"] == "silver"
    assert payload["status"] in VALID_STATUSES
    assert payload["rule_name"] == "dataset_readable"


def test_required_columns_present_and_missing() -> None:
    """La validacion de columnas requeridas identifica faltantes."""

    passed, missing = evaluate_required_columns(
        existing_columns=["anio", "mes", "monto"],
        required_columns=["anio", "mes"],
    )
    assert passed is True
    assert missing == []

    passed, missing = evaluate_required_columns(
        existing_columns=["anio"],
        required_columns=["anio", "mes"],
    )
    assert passed is False
    assert missing == ["mes"]


def test_status_from_warning_count() -> None:
    """Los problemas no bloqueantes se clasifican como WARNING."""

    assert status_from_warning_count(0) == "PASS"
    assert status_from_warning_count(3) == "WARNING"


def test_invalid_boolean_flag_values() -> None:
    """Los flags aceptan booleanos y textos true/false."""

    invalid_values = invalid_boolean_flag_values(
        [True, False, "true", "FALSE", "si", "", None, 1]
    )

    assert invalid_values == ["si", "", None, 1]


def test_silver_report_rejects_missing_file() -> None:
    """El reporte Silver falla claramente si no existe el JSONL."""

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp_dir:
        with pytest.raises(SilverQualityReportError):
            read_silver_quality_results(Path(temp_dir) / "missing.jsonl")


def test_render_html_report_from_simulated_silver_results() -> None:
    """El reporte Silver renderiza resultados simulados."""

    html = render_html_report(
        [
            {
                "source_name": "siaf_income",
                "resource_key": "annual_2024",
                "rule_name": "row_count_positive",
                "status": "PASS",
                "evaluated": True,
                "message": "ok",
            },
            {
                "source_name": "renamu",
                "resource_key": "municipal_context",
                "rule_name": "renamu_tipomuni_invalid_values",
                "status": "WARNING",
                "evaluated": True,
                "message": "alerta",
            },
        ]
    )

    assert "Reporte de calidad Silver" in html
    assert "renamu_tipomuni_invalid_values" in html
    assert "WARNING" in html


def test_dry_run_does_not_create_silver_quality_outputs() -> None:
    """Dry-run no escribe JSONL aunque reciba ruta de salida."""

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as temp_dir:
        output_path = Path(temp_dir) / "quality" / "silver_quality_results.jsonl"

        results = run_silver_quality_checks(dry_run=True, output_path=output_path)

        assert results == []
        assert not output_path.exists()


def test_check_siaf_municipal_only_pass_and_fail(spark: SparkSession) -> None:
    """La regla siaf_municipal_only aprueba datos puramente municipales y reprueba no municipales."""
    from src.quality.run_silver_quality_checks import check_siaf_municipal_only

    dataset = SilverDataset(
        source_name="siaf_income",
        resource_key="annual_2024",
        dataset_path=Path("unused"),
        source_config={},
        resource_config={},
    )

    schema = StructType(
        [
            StructField("nivel_gobierno_codigo", StringType(), True),
            StructField("nivel_gobierno_nombre", StringType(), True),
            StructField("ejecutora_nombre", StringType(), True),
        ]
    )

    # 1. Caso PASS: solo registros municipales válidos
    df_pass = spark.createDataFrame(
        [
            ("M", "GOBIERNOS LOCALES", "MUNICIPALIDAD DISTRITAL DE SAMUGARI"),
            ("M", "GOBIERNOS LOCALES", "MUNICIPALIDAD PROVINCIAL DE HUANCAYO"),
        ],
        schema=schema,
    )

    result_pass = check_siaf_municipal_only(
        run_id="run_test",
        dataset=dataset,
        dataframe=df_pass,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    assert result_pass.status == "PASS"
    assert result_pass.rule_name == "siaf_municipal_only"
    assert result_pass.details["non_municipal_records"] == 0

    # 2. Caso FAIL: contiene un gobierno regional
    df_fail_regional = spark.createDataFrame(
        [
            ("M", "GOBIERNOS LOCALES", "MUNICIPALIDAD DISTRITAL DE SAMUGARI"),
            ("R", "GOBIERNOS REGIONALES", "REGION AMAZONAS-SEDE CENTRAL"),
        ],
        schema=schema,
    )
    result_fail_reg = check_siaf_municipal_only(
        run_id="run_test",
        dataset=dataset,
        dataframe=df_fail_regional,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    assert result_fail_reg.status == "FAIL"
    assert result_fail_reg.details["non_municipal_records"] == 1

    # 3. Caso FAIL: contiene una mancomunidad
    df_fail_manco = spark.createDataFrame(
        [
            ("M", "GOBIERNOS LOCALES", "MANCOMUNIDAD MUNICIPAL DE LA AMAZONIA"),
        ],
        schema=schema,
    )
    result_fail_manco = check_siaf_municipal_only(
        run_id="run_test",
        dataset=dataset,
        dataframe=df_fail_manco,
        checked_at_utc="2026-06-19T00:00:00+00:00",
    )
    assert result_fail_manco.status == "FAIL"
    assert result_fail_manco.details["non_municipal_records"] == 1

