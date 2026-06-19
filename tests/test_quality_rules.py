"""Pruebas para reglas y reportes de calidad Bronze."""

from pathlib import Path

import pytest

from src.common.config import load_quality_rules_config
from src.quality.generate_quality_report import (
    QualityReportError,
    read_quality_results,
    render_html_report,
)
from src.quality.run_quality_checks import (
    VALID_STATUSES,
    BronzeDataset,
    build_expected_datasets,
    build_quality_result,
    evaluate_required_columns,
    load_quality_config,
    not_evaluated_warning,
    run_quality_checks,
    validate_percentage_value,
    validate_ubigeo_value,
    validate_year_value,
)


def test_quality_rules_config_has_valid_statuses() -> None:
    """La configuración usa solo PASS, WARNING y FAIL."""

    config = load_quality_rules_config()
    statuses = set(config["quality"]["valid_statuses"])

    assert statuses == {"PASS", "WARNING", "FAIL"}
    assert "SKIPPED" not in statuses


def test_quality_rules_config_defines_bronze_sources() -> None:
    """La configuración declara las cuatro familias Bronze vigentes."""

    quality_config = load_quality_config()
    sources = quality_config["bronze"]["sources"]

    assert set(sources) == {
        "siaf_income",
        "sismepre",
        "renamu",
        "municipal_categories",
    }

    assert "annual_2012" in sources["siaf_income"]["expected_resources"]
    assert "respuestas" in sources["sismepre"]["expected_resources"]
    assert sources["renamu"]["expected_resources"] == ["base_renamu_2022"]
    assert sources["municipal_categories"]["expected_resources"] == [
        "categorias_municipalidades"
    ]

def test_build_expected_datasets_uses_resource_key_paths() -> None:
    """Los recursos esperados apuntan a carpetas resource_key."""

    quality_config = {
        "bronze": {
            "sources": {
                "sample": {
                    "expected_resources": ["one", "two"],
                }
            }
        }
    }

    datasets = build_expected_datasets(quality_config)

    assert [dataset.resource_key for dataset in datasets] == ["one", "two"]
    assert str(datasets[0].dataset_path).endswith("data\\bronze\\sample\\resource_key=one") or str(
        datasets[0].dataset_path
    ).endswith("data/bronze/sample/resource_key=one")


def test_build_quality_result_serializes_supported_statuses(tmp_path: Path) -> None:
    """Un resultado de calidad conserva la estructura esperada."""

    dataset = BronzeDataset(
        source_name="sample",
        resource_key="one",
        dataset_path=tmp_path / "resource_key=one",
    )
    result = build_quality_result(
        run_id="run",
        dataset=dataset,
        rule_id="dataset_path_exists",
        rule_type="technical",
        severity="FAIL",
        status="PASS",
        evaluated=True,
        observed_value=True,
        expected_value=True,
        message="ok",
        processed_at_utc="2026-01-01T00:00:00+00:00",
    )

    assert result.status == "PASS"
    assert result.severity == "FAIL"
    assert result.evaluated is True
    assert result.source_name == "sample"


def test_required_columns_present_and_missing() -> None:
    """La validación de columnas requeridas identifica faltantes."""

    passed, missing = evaluate_required_columns(
        existing_columns=["a", "b", "c"],
        required_columns=["a", "c"],
    )
    assert passed is True
    assert missing == []

    passed, missing = evaluate_required_columns(
        existing_columns=["a"],
        required_columns=["a", "b"],
    )
    assert passed is False
    assert missing == ["b"]


def test_not_evaluated_rule_uses_warning_without_skipped(tmp_path: Path) -> None:
    """Una regla no evaluable usa WARNING y evaluated=false."""

    dataset = BronzeDataset(
        source_name="sample",
        resource_key="one",
        dataset_path=tmp_path / "resource_key=one",
    )
    result = not_evaluated_warning(
        run_id="run",
        dataset=dataset,
        rule_id="invalid_ubigeo",
        rule_type="validity",
        expected_value="columna ubigeo",
        message="Regla no evaluada porque la columna no existe todavía en Bronze.",
        processed_at_utc="2026-01-01T00:00:00+00:00",
    )

    assert result.status == "WARNING"
    assert result.evaluated is False
    assert result.status in VALID_STATUSES
    assert result.status != "SKIPPED"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("010101", True),
        ("150101", True),
        ("", False),
        ("10101", False),
        ("ABCDEF", False),
        (None, False),
    ],
)
def test_validate_ubigeo_value(value: object, expected: bool) -> None:
    """El formato de ubigeo exige seis dígitos."""

    assert validate_ubigeo_value(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2022", True),
        (2026, True),
        ("2035", False),
        ("texto", False),
        ("", False),
        (None, False),
    ],
)
def test_validate_year_value(value: object, expected: bool) -> None:
    """El año válido queda dentro del rango configurado."""

    assert validate_year_value(value, min_year=2010, max_year=2030) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", True),
        ("50.5", True),
        ("99,9", True),
        ("100%", True),
        ("120", False),
        ("texto", False),
        ("", False),
        (None, False),
    ],
)
def test_validate_percentage_value(value: object, expected: bool) -> None:
    """Los porcentajes válidos quedan entre 0 y 100."""

    assert validate_percentage_value(value) is expected


def test_render_html_report_from_simulated_results() -> None:
    """El reporte HTML puede renderizar resultados simulados."""

    html = render_html_report(
        [
            {
                "source_name": "siaf_income",
                "resource_key": "annual_2024",
                "rule_id": "row_count_positive",
                "status": "PASS",
                "evaluated": True,
                "message": "El dataset tiene filas.",
            },
            {
                "source_name": "renamu",
                "resource_key": "base_renamu_2022",
                "rule_id": "invalid_percentage",
                "status": "WARNING",
                "evaluated": False,
                "message": "Regla no evaluada.",
            },
        ]
    )

    assert "Reporte de calidad Bronze" in html
    assert "siaf_income" in html
    assert "WARNING" in html


def test_read_quality_results_rejects_missing_file(tmp_path: Path) -> None:
    """La generación de reporte falla claramente si no existe el JSONL."""

    with pytest.raises(QualityReportError):
        read_quality_results(tmp_path / "missing.jsonl")


def test_dry_run_does_not_create_quality_outputs(tmp_path: Path) -> None:
    """Dry-run no escribe resultados aunque reciba una ruta de salida."""

    output_path = tmp_path / "quality" / "bronze_quality_results.jsonl"

    results = run_quality_checks(dry_run=True, output_path=output_path)

    assert results == []
    assert not output_path.exists()
