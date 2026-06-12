from pathlib import Path

import pytest

from src.gold.build_municipal_revenue_marts import (
    GOLD_DATASETS,
    GoldMartError,
    build_period_key,
    classify_integration_quality,
    existing_columns,
    missing_columns,
    output_dataset_path,
    safe_ratio,
    validate_selected_datasets,
)


def test_safe_ratio_calcula_division_valida():
    assert safe_ratio(50, 100) == 0.5


def test_safe_ratio_devuelve_none_con_denominador_cero_o_nulo():
    assert safe_ratio(50, 0) is None
    assert safe_ratio(50, None) is None
    assert safe_ratio(None, 100) is None


def test_build_period_key_usa_mes_cero_para_registro_anual():
    assert build_period_key(2024, None) == "202400"
    assert build_period_key(2024, 0) == "202400"


def test_build_period_key_formatea_anio_mes():
    assert build_period_key(2024, 7) == "202407"


def test_build_period_key_devuelve_none_sin_anio():
    assert build_period_key(None, 7) is None


@pytest.mark.parametrize(
    ("has_bridge", "has_ubigeo", "has_renamu", "expected"),
    [
        (True, True, True, "matched_renamu"),
        (True, True, False, "valid_ubigeo_without_renamu"),
        (True, False, False, "bridge_without_valid_ubigeo"),
        (False, False, False, "without_bridge"),
    ],
)
def test_classify_integration_quality(has_bridge, has_ubigeo, has_renamu, expected):
    assert (
        classify_integration_quality(
            has_municipal_bridge=has_bridge,
            has_valid_ubigeo=has_ubigeo,
            has_renamu_match=has_renamu,
        )
        == expected
    )


def test_existing_columns_preserva_orden_deseado():
    assert existing_columns(["b", "a", "c"], ["a", "b", "x"]) == ["a", "b"]


def test_missing_columns_detecta_faltantes():
    assert missing_columns(["anio", "mes"], ["anio", "sec_ejec"]) == ["sec_ejec"]


def test_output_dataset_path_construye_ruta_soportada():
    root = Path("data/gold/municipal_revenue")
    assert (
        output_dataset_path(root, "dim_time")
        == Path("data/gold/municipal_revenue/dim_time")
    )


def test_output_dataset_path_rechaza_dataset_no_soportado():
    with pytest.raises(GoldMartError):
        output_dataset_path(Path("data/gold/municipal_revenue"), "predial_no_todavia")


def test_validate_selected_datasets_devuelve_todos_por_defecto():
    assert validate_selected_datasets(None) == GOLD_DATASETS


def test_validate_selected_datasets_rechaza_no_soportados():
    with pytest.raises(GoldMartError):
        validate_selected_datasets(["dim_time", "mart_predial_compliance"])
