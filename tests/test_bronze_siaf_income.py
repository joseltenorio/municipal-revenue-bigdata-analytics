"""Pruebas para la lógica del builder Bronze de SIAF ingresos."""

from pathlib import Path

import pytest

from src.bronze.build_bronze_siaf_income import (
    BronzeBuildError,
    DICTIONARY_FILE_NAME,
    build_bronze_siaf_income,
    build_dry_run_summary,
    is_fact_resource,
    normalize_column_name,
    normalize_column_names,
    select_bronze_resources,
)


def test_normalize_column_name_returns_snake_case_ascii() -> None:
    """Los nombres de columnas se normalizan sin limpieza de negocio."""

    assert normalize_column_name("Año Ejecución (%)") == "ano_ejecucion"
    assert normalize_column_name("  Monto S/.  ") == "monto_s"
    assert normalize_column_name("123 Código") == "col_123_codigo"


def test_normalize_column_names_deduplicates_collisions() -> None:
    """Los nombres duplicados normalizados reciben sufijos determinísticos."""

    assert normalize_column_names(["Año", "Ano", "AÑO"]) == [
        "ano",
        "ano_2",
        "ano_3",
    ]


def test_dictionary_resource_is_excluded_from_bronze_facts() -> None:
    """El diccionario MEF es documentación, no una tabla de hechos Bronze."""

    resource = {
        "file_name": DICTIONARY_FILE_NAME,
        "format": "csv",
        "role": "dictionary",
    }

    assert not is_fact_resource(resource)


def test_select_bronze_resources_preserves_resource_granularity(tmp_path: Path) -> None:
    """Los recursos seleccionados conservan granularidad y ruta de salida."""

    source_config = {
        "landing_subdir": "siaf_income",
        "bronze_subdir": "siaf_income",
        "candidate_resources": {
            "annual_2024": {
                "file_name": "2024-Ingreso.csv",
                "format": "csv",
                "role": "annual_source_file",
                "year": 2024,
                "granularity": "annual",
            },
            "dictionary": {
                "file_name": DICTIONARY_FILE_NAME,
                "format": "csv",
                "role": "dictionary",
            },
        },
    }

    resources = select_bronze_resources(
        source_config,
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
    )

    assert len(resources) == 1
    assert resources[0].resource_key == "annual_2024"
    assert resources[0].granularity == "annual"
    assert resources[0].source_path == tmp_path / "landing" / "2024-Ingreso.csv"
    assert resources[0].output_path == tmp_path / "bronze" / "resource_key=annual_2024"


def test_select_bronze_resources_rejects_dictionary_by_key(tmp_path: Path) -> None:
    """Los recursos documentales no pueden seleccionarse como hechos Bronze."""

    source_config = {
        "candidate_resources": {
            "dictionary": {
                "file_name": DICTIONARY_FILE_NAME,
                "format": "csv",
                "role": "dictionary",
            },
        },
    }

    with pytest.raises(BronzeBuildError):
        select_bronze_resources(
            source_config,
            resource_keys=["dictionary"],
            landing_dir=tmp_path / "landing",
            bronze_dir=tmp_path / "bronze",
        )


def test_dry_run_does_not_create_bronze_directory(tmp_path: Path) -> None:
    """Dry-run valida entradas de Landing sin escribir salidas Bronze."""

    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"
    landing_dir.mkdir()
    source_file = landing_dir / "2024-Ingreso.csv"
    source_file.write_text("Año,Monto\n2024,10\n", encoding="utf-8")

    source_config = {
        "candidate_resources": {
            "annual_2024": {
                "file_name": "2024-Ingreso.csv",
                "format": "csv",
                "role": "annual_source_file",
                "year": 2024,
                "granularity": "annual",
            },
        },
    }
    resources = select_bronze_resources(
        source_config,
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
    )

    summary = build_bronze_siaf_income(
        resources=resources,
        dry_run=True,
        overwrite=False,
    )

    assert summary == build_dry_run_summary(resources)
    assert not bronze_dir.exists()
