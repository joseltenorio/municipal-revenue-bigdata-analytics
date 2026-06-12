"""Pruebas para la lógica del builder Bronze de RENAMU."""

from pathlib import Path

import pytest

from src.bronze.build_bronze_renamu import (
    CSV_ENCODING,
    CSV_SEPARATOR,
    RESOURCE_KEY,
    SOURCE_FILE_NAME,
    SOURCE_YEAR,
    BronzeBuildError,
    build_bronze_renamu,
    build_dry_run_summary,
    build_renamu_resource,
    normalize_column_name,
    normalize_column_names,
    validate_landing_input,
)


def test_normalize_column_name_returns_snake_case_ascii() -> None:
    """Los nombres de columnas se normalizan como identificadores técnicos."""

    assert normalize_column_name("Año") == "ano"
    assert normalize_column_name("Código de Municipalidad") == "codigo_de_municipalidad"
    assert normalize_column_name("123 Ubigeo") == "col_123_ubigeo"


def test_normalize_column_names_deduplicates_collisions() -> None:
    """Los nombres duplicados normalizados reciben sufijos determinísticos."""

    assert normalize_column_names(["Año", "Ano", "AÑO"]) == [
        "ano",
        "ano_2",
        "ano_3",
    ]


def test_build_renamu_resource_points_to_main_extracted_csv(tmp_path: Path) -> None:
    """El recurso Bronze RENAMU apunta solo al CSV tabular principal extraído."""

    source_config = {
        "landing_subdir": "renamu",
        "bronze_subdir": "renamu",
    }

    resource = build_renamu_resource(
        source_config,
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
    )

    assert resource.resource_key == RESOURCE_KEY
    assert resource.file_name == SOURCE_FILE_NAME
    assert resource.year == SOURCE_YEAR
    assert resource.source_path == (
        tmp_path
        / "landing"
        / "extracted"
        / "783-Modulo1726"
        / "Base_RENAMU_2022_f.csv"
    )
    assert resource.output_path == tmp_path / "bronze" / f"resource_key={RESOURCE_KEY}"


def test_validate_landing_input_rejects_missing_csv(tmp_path: Path) -> None:
    """La validación falla si el CSV principal no existe en Landing."""

    resource = build_renamu_resource(
        {"landing_subdir": "renamu", "bronze_subdir": "renamu"},
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
    )

    with pytest.raises(BronzeBuildError):
        validate_landing_input(resource)


def test_dry_run_does_not_create_bronze_directory(tmp_path: Path) -> None:
    """Dry-run valida el CSV principal sin escribir salidas Bronze."""

    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"
    source_dir = landing_dir / "extracted" / "783-Modulo1726"
    source_dir.mkdir(parents=True)
    source_file = source_dir / SOURCE_FILE_NAME
    source_file.write_text("Año;Ubigeo;Distrito\n2022;010101;CHACHAPOYAS\n", encoding="utf-8")

    resource = build_renamu_resource(
        {"landing_subdir": "renamu", "bronze_subdir": "renamu"},
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
    )

    summary = build_bronze_renamu(
        resource=resource,
        dry_run=True,
        overwrite=False,
    )

    assert summary == build_dry_run_summary(resource)
    assert summary["csv_separator"] == CSV_SEPARATOR
    assert summary["csv_encoding"] == CSV_ENCODING
    assert not bronze_dir.exists()
