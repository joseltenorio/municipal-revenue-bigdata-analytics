from pathlib import Path

import pytest

from src.gold.build_territorial_context_marts import (
    GOLD_DATASETS,
    GoldTerritorialError,
    build_geography_key,
    capacity_metric_availability,
    detect_columns_by_patterns,
    detect_renamu_capacity_columns,
    existing_columns,
    interpret_tipomuni,
    missing_columns,
    output_dataset_path,
    safe_percentage,
    validate_selected_datasets,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, "Provincial"),
        ("2", "Distrital"),
        ("3", "Centro Poblado"),
        (None, "No informado"),
        ("", "No informado"),
        ("9", "No informado"),
    ],
)
def test_interpret_tipomuni(value, expected):
    assert interpret_tipomuni(value) == expected


def test_detect_columns_by_patterns_encuentra_patrones():
    columns = ["c96_decimal", "p31_tributaria", "software_municipal", "otro"]
    assert detect_columns_by_patterns(columns, ["tribut", "software"]) == [
        "p31_tributaria",
        "software_municipal",
    ]


def test_detect_renamu_capacity_columns_clasifica_c96_c97_y_capacidad():
    columns = [
        "c96_decimal",
        "c96",
        "c97_1_decimal",
        "c97_1",
        "p19d_t",
        "p13a_1",
        "p14",
        "p16_1",
        "p17_5",
        "p22_at5",
        "p22_c5",
    ]
    detected = detect_renamu_capacity_columns(columns)
    assert detected["renamu_income"] == ["c96_decimal"]
    assert detected["renamu_expense"] == ["c97_1_decimal"]
    assert detected["workers"] == ["p19d_t"]
    assert detected["computers"] == ["p13a_1"]
    assert detected["internet"] == ["p14"]
    assert detected["state_systems"] == ["p16_1"]
    assert detected["municipal_systems"] == ["p17_5"]
    assert detected["technical_assistance"] == ["p22_at5"]
    assert detected["training"] == ["p22_c5"]


def test_capacity_metric_availability_refleja_columnas_disponibles():
    availability = capacity_metric_availability(
        {
            "renamu_income": ["c96_decimal"],
            "renamu_expense": [],
            "workers": [],
            "computers": ["p13a_1"],
            "internet": [],
            "state_systems": [],
            "municipal_systems": [],
            "technical_assistance": [],
            "training": [],
        }
    )
    assert availability["renamu_income_total"] is True
    assert availability["renamu_expense_total"] is False
    assert availability["total_computadoras_operativas"] is True
    assert availability["total_personal_dic_2021"] is False


def test_capacity_metric_availability_detecta_metricas_especificas():
    availability = capacity_metric_availability(
        {
            "renamu_income": [],
            "renamu_expense": [],
            "workers": ["p19d_t", "p19m_t"],
            "computers": ["p13a_1"],
            "internet": ["p14", "p14a_1", "p14a_2"],
            "state_systems": ["p16_1", "p16_8"],
            "municipal_systems": ["p17_1", "p17_5"],
            "technical_assistance": ["p22_at1", "p22_at5"],
            "training": ["p22_c1", "p22_c5"],
        }
    )
    assert availability["total_personal_dic_2021"] is True
    assert availability["total_personal_mar_2022"] is True
    assert availability["ratio_computadoras_con_internet"] is True
    assert availability["computadoras_por_trabajador"] is True
    assert availability["tiene_siaf"] is True
    assert availability["tiene_srtm"] is True
    assert availability["tiene_sistema_rentas"] is True
    assert availability["tiene_catastro"] is True
    assert availability["requiere_asistencia_administracion_tributaria"] is True
    assert availability["requiere_asistencia_catastro"] is True
    assert availability["requiere_capacitacion_administracion_tributaria"] is True
    assert availability["requiere_capacitacion_catastro"] is True


def test_build_geography_key_usa_ubigeo_limpio():
    assert build_geography_key(" 010101 ") == "010101"
    assert build_geography_key("") is None
    assert build_geography_key(None) is None


def test_safe_percentage_evita_division_invalida():
    assert safe_percentage(25, 100) == 25.0
    assert safe_percentage(25, 0) is None
    assert safe_percentage(None, 100) is None


def test_existing_columns_preserva_orden():
    assert existing_columns(["b", "a", "c"], ["a", "b", "x"]) == ["a", "b"]


def test_missing_columns_detecta_faltantes():
    assert missing_columns(["ubigeo", "ccdd"], ["ubigeo", "ccpp"]) == ["ccpp"]


def test_output_dataset_path_construye_ruta_valida():
    root = Path("data/gold/territorial_context")
    assert (
        output_dataset_path(root, "dim_geography")
        == Path("data/gold/territorial_context/dim_geography")
    )


def test_output_dataset_path_rechaza_dataset_no_soportado():
    with pytest.raises(GoldTerritorialError):
        output_dataset_path(Path("data/gold/territorial_context"), "mart_predial")


def test_validate_selected_datasets_devuelve_todos_por_defecto():
    assert validate_selected_datasets(None) == GOLD_DATASETS


def test_validate_selected_datasets_rechaza_no_soportados():
    with pytest.raises(GoldTerritorialError):
        validate_selected_datasets(["dim_geography", "mart_no_existe"])
