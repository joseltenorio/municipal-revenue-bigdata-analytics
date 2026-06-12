"""Pruebas unitarias para integración Silver municipal."""

from pathlib import Path

import pytest

from src.silver.integrate_municipal_sources import (
    INTEGRATED_DATASETS,
    SilverIntegrationError,
    calculate_coverage_percentage,
    decimal_columns_by_prefix,
    existing_columns,
    missing_required_columns,
    normalize_metric_row,
    output_dataset_path,
    selected_dataset_names,
)


def test_existing_columns_preserves_requested_order() -> None:
    """La selección conserva el orden de columnas deseadas."""

    columns = ["sec_ejec", "ubigeo", "anio"]
    desired = ["anio", "mes", "sec_ejec"]

    assert existing_columns(columns, desired) == ["anio", "sec_ejec"]


def test_missing_required_columns() -> None:
    """La validación de llaves requeridas identifica faltantes."""

    columns = ["sec_ejec", "ubigeo"]
    required = ["sec_ejec", "ubigeo", "anio"]

    assert missing_required_columns(columns, required) == ["anio"]
    assert missing_required_columns(columns, ["sec_ejec"]) == []


def test_calculate_coverage_percentage() -> None:
    """El porcentaje de cobertura evita división entre cero."""

    assert calculate_coverage_percentage(8, 10) == 80.0
    assert calculate_coverage_percentage(1, 3) == 33.3333
    assert calculate_coverage_percentage(1, 0) == 0.0


def test_normalize_metric_row_is_serializable() -> None:
    """Una métrica de cobertura conserva estructura esperada."""

    row = normalize_metric_row(
        metric_name="mef_sec_ejec_with_bridge",
        numerator=80,
        denominator=100,
        description="Cobertura MEF.",
    )

    assert row == {
        "metric_name": "mef_sec_ejec_with_bridge",
        "numerator": 80,
        "denominator": 100,
        "coverage_percentage": 80.0,
        "description": "Cobertura MEF.",
    }


def test_output_dataset_path_accepts_known_dataset() -> None:
    """La ruta de salida solo acepta datasets integrados conocidos."""

    root = Path("data/silver/integrated")

    assert output_dataset_path(root, "integration_coverage") == (
        root / "integration_coverage"
    )

    with pytest.raises(SilverIntegrationError):
        output_dataset_path(root, "raw_join")


def test_selected_dataset_names_validates_cli_values() -> None:
    """La selección opcional de datasets rechaza nombres no soportados."""

    assert selected_dataset_names(None) == INTEGRATED_DATASETS
    assert selected_dataset_names(["municipal_entity_bridge"]) == [
        "municipal_entity_bridge"
    ]

    with pytest.raises(SilverIntegrationError):
        selected_dataset_names(["gold_table"])


def test_decimal_columns_by_prefix() -> None:
    """Las columnas decimales se seleccionan por prefijo técnico."""

    columns = [
        "mon_recaudado",
        "mon_recaudado_decimal",
        "num_predios_decimal",
        "respuesta_decimal_value",
        "c96_decimal",
    ]

    assert decimal_columns_by_prefix(columns, ["mon_", "num_"]) == [
        "mon_recaudado_decimal",
        "num_predios_decimal",
    ]
