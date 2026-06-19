"""Pruebas unitarias para reglas de calidad Silver."""

import json
from dataclasses import asdict
from pathlib import Path

import pytest

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
    evaluate_required_columns,
    invalid_boolean_flag_values,
    load_silver_quality_config,
    run_silver_quality_checks,
    status_from_warning_count,
)


def test_silver_quality_config_has_valid_statuses() -> None:
    """La configuración Silver usa PASS, WARNING y FAIL."""

    config = load_quality_rules_config()
    statuses = set(config["quality"]["valid_statuses"])

    assert statuses == {"PASS", "WARNING", "FAIL"}
    assert "SKIPPED" not in statuses


def test_silver_quality_config_defines_sources_and_outputs() -> None:
    """La sección Silver define fuentes y salidas propias."""

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
        "municipal_categories",
    }


def test_build_expected_datasets_reads_all_silver_resources() -> None:
    """La configuración Silver declara los 26 recursos esperados."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(quality_config)

    assert len(datasets) == 26
    assert any(
        dataset.source_name == "siaf_income" and dataset.resource_key == "annual_2024"
        for dataset in datasets
    )
    assert any(
        dataset.source_name == "sismepre" and dataset.resource_key == "respuestas"
        for dataset in datasets
    )
    assert any(
        dataset.source_name == "renamu"
        and dataset.resource_key == "base_renamu_2022"
        for dataset in datasets
    )


def test_build_expected_datasets_filters_source_and_resource() -> None:
    """Los filtros de fuente y recurso reducen el plan de evaluación."""

    quality_config = load_silver_quality_config()
    datasets = build_expected_datasets(
        quality_config,
        selected_sources=["renamu"],
        selected_resources=["base_renamu_2022"],
    )

    assert len(datasets) == 1
    assert datasets[0].source_name == "renamu"
    assert datasets[0].resource_key == "base_renamu_2022"


def test_build_silver_quality_result_is_json_serializable(tmp_path: Path) -> None:
    """Un resultado Silver conserva la estructura JSONL esperada."""

    dataset = SilverDataset(
        source_name="sample",
        resource_key="one",
        dataset_path=tmp_path / "resource_key=one",
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
    """La validación de columnas requeridas identifica faltantes."""

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
        [True, False, "true", "FALSE", "sí", "", None, 1]
    )

    assert invalid_values == ["sí", "", None, 1]


def test_silver_report_rejects_missing_file(tmp_path: Path) -> None:
    """El reporte Silver falla claramente si no existe el JSONL."""

    with pytest.raises(SilverQualityReportError):
        read_silver_quality_results(tmp_path / "missing.jsonl")


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
                "resource_key": "base_renamu_2022",
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


def test_dry_run_does_not_create_silver_quality_outputs(tmp_path: Path) -> None:
    """Dry-run no escribe JSONL aunque reciba ruta de salida."""

    output_path = tmp_path / "quality" / "silver_quality_results.jsonl"

    results = run_silver_quality_checks(dry_run=True, output_path=output_path)

    assert results == []
    assert not output_path.exists()


def test_silver_quality_rules_config_defines_four_source_families() -> None:
    """La configuración declara las cuatro familias Silver vigentes."""

    config = load_quality_rules_config()
    sources = config["quality"]["silver"]["sources"]

    assert set(sources) == {
        "siaf_income",
        "sismepre",
        "renamu",
        "municipal_categories",
    }


def test_silver_quality_rules_config_declares_municipal_categories_resource() -> None:
    """Silver quality reconoce la fuente manual de categorias municipales."""

    config = load_quality_rules_config()
    sources = config["quality"]["silver"]["sources"]

    expected_resources = sources["municipal_categories"]["expected_resources"]
    category_resource = expected_resources["categorias_municipalidades"]

    assert set(expected_resources) == {"categorias_municipalidades"}
    assert isinstance(category_resource, dict)

    assert category_resource["typed_columns"] == [
        "municipalidad_original",
        "municipalidad_normalizada",
        "categoria_municipal",
    ]
    assert category_resource["expected_flags"] == [
        "is_valid_categoria_municipal",
        "has_municipalidad_normalizada",
    ]
    assert category_resource["critical_null_columns"] == [
        "municipalidad_original",
        "municipalidad_normalizada",
        "categoria_municipal",
    ]
    assert category_resource["candidate_key"] == ["municipalidad_normalizada"]
    assert category_resource["valid_categoria_values"] == [
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
    ]

