from pathlib import Path

import pytest

from src.gold.build_predial_compliance_marts import (
    GOLD_DATASETS,
    GoldPredialError,
    build_period_key,
    detect_columns_by_patterns,
    detect_predial_columns,
    existing_columns,
    metric_availability,
    missing_columns,
    output_dataset_path,
    safe_ratio,
    validate_selected_datasets,
)


def test_detect_columns_by_patterns_encuentra_patrones():
    columns = ["mon_recaudactual_total", "mon_saldo_total", "otro"]
    assert detect_columns_by_patterns(columns, ["recaud"]) == ["mon_recaudactual_total"]


def test_detect_predial_columns_clasifica_metricas_reales():
    columns = [
        "mon_emisionpredial_afecto_decimal_total",
        "num_emisionpredial_afecto_decimal_total",
        "mon_recaudactual_ordin_decimal_total",
        "mon_saldopredial_ord_decimal_total",
        "num_contripredio_decimal_total",
        "num_prediototal_decimal_total",
        "flag_emision_inicial",
    ]
    detected = detect_predial_columns(columns)
    assert detected["issue"] == ["mon_emisionpredial_afecto_decimal_total"]
    assert detected["collection"] == ["mon_recaudactual_ordin_decimal_total"]
    assert detected["balance"] == ["mon_saldopredial_ord_decimal_total"]
    assert detected["taxpayer"] == ["num_contripredio_decimal_total"]
    assert detected["property"] == ["num_prediototal_decimal_total"]
    assert detected["flag"] == ["flag_emision_inicial"]


def test_metric_availability_requiere_columnas_base():
    detected = {
        "collection": ["mon_recaud_total"],
        "issue": ["mon_emision_total"],
        "balance": [],
        "taxpayer": ["num_contrib_total"],
        "property": [],
    }
    availability = metric_availability(detected)
    assert availability["predial_collection_total"] is True
    assert availability["predial_issue_total"] is True
    assert availability["predial_effectiveness_ratio"] is True
    assert availability["predial_balance_total"] is False
    assert availability["property_count_total"] is False


def test_safe_ratio_evita_division_invalida():
    assert safe_ratio(25, 100) == 0.25
    assert safe_ratio(25, 0) is None
    assert safe_ratio(None, 100) is None


def test_build_period_key_con_periodo_estadistico():
    assert build_period_key("2024", "1", "2024", "7") == "2024_1_202407"


def test_build_period_key_sin_requeridos_devuelve_none():
    assert build_period_key(None, "1", "2024", "7") is None
    assert build_period_key("2024", None, "2024", "7") is None


def test_existing_columns_preserva_orden_solicitado():
    assert existing_columns(["b", "a", "c"], ["a", "b", "x"]) == ["a", "b"]


def test_missing_columns_detecta_faltantes():
    assert missing_columns(["sec_ejec", "ubigeo"], ["sec_ejec", "periodo"]) == ["periodo"]


def test_output_dataset_path_construye_ruta_valida():
    root = Path("data/gold/predial_compliance")
    assert (
        output_dataset_path(root, "mart_predial_ranking")
        == Path("data/gold/predial_compliance/mart_predial_ranking")
    )


def test_output_dataset_path_rechaza_dataset_no_soportado():
    with pytest.raises(GoldPredialError):
        output_dataset_path(Path("data/gold/predial_compliance"), "mart_mef")


def test_validate_selected_datasets_devuelve_todos_por_defecto():
    assert validate_selected_datasets(None) == GOLD_DATASETS


def test_validate_selected_datasets_rechaza_no_soportados():
    with pytest.raises(GoldPredialError):
        validate_selected_datasets(["fact_predial_compliance", "mart_territorial"])
