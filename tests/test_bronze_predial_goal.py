"""Pruebas para la lógica del builder Bronze de meta predial."""

from pathlib import Path

import pytest

from src.bronze.build_bronze_predial_goal import (
    BronzeBuildError,
    build_bronze_predial_goal,
    build_dry_run_summary,
    is_bronze_source_table,
    normalize_column_name,
    normalize_column_names,
    select_bronze_resources,
)


def test_normalize_column_name_returns_snake_case_ascii() -> None:
    """Los nombres de columnas se normalizan como identificadores técnicos."""

    assert normalize_column_name("Año Aplicación") == "ano_aplicacion"
    assert normalize_column_name("Monto Recaudación (S/)") == "monto_recaudacion_s"
    assert normalize_column_name("123 Código") == "col_123_codigo"


def test_normalize_column_names_deduplicates_collisions() -> None:
    """Los nombres duplicados normalizados reciben sufijos determinísticos."""

    assert normalize_column_names(["Año", "Ano", "AÑO"]) == [
        "ano",
        "ano_2",
        "ano_3",
    ]


def test_only_source_tables_enabled_for_ingestion_are_bronze_inputs() -> None:
    """Solo recursos de datos habilitados deben convertirse a Bronze."""

    assert is_bronze_source_table(
        {
            "file_name": "rentas_respuestas.csv",
            "format": "csv",
            "role": "source_table",
            "use_for_ingestion": True,
        }
    )
    assert not is_bronze_source_table(
        {
            "file_name": "rentas_respuestas_diccionario.csv",
            "format": "csv",
            "role": "dictionary",
            "use_for_ingestion": False,
        }
    )
    assert not is_bronze_source_table(
        {
            "file_name": "rentas_auxiliar.csv",
            "format": "csv",
            "role": "source_table",
            "use_for_ingestion": False,
        }
    )


def test_select_bronze_resources_excludes_dictionaries(tmp_path: Path) -> None:
    """Los diccionarios configurados no se seleccionan como tablas Bronze."""

    source_config = {
        "landing_subdir": "predial_goal",
        "bronze_subdir": "predial_goal",
        "candidate_resources": {
            "respuestas": {
                "file_name": "rentas_respuestas.csv",
                "format": "csv",
                "role": "source_table",
                "priority": "high",
                "use_for_ingestion": True,
            },
            "respuestas_dictionary": {
                "file_name": "rentas_respuestas_diccionario.csv",
                "format": "csv",
                "role": "dictionary",
                "priority": "medium",
                "use_for_ingestion": False,
                "use_for_documentation": True,
            },
        },
    }

    resources = select_bronze_resources(
        source_config,
        landing_dir=tmp_path / "landing",
        bronze_dir=tmp_path / "bronze",
    )

    assert len(resources) == 1
    assert resources[0].resource_key == "respuestas"
    assert resources[0].role == "source_table"
    assert resources[0].source_path == tmp_path / "landing" / "rentas_respuestas.csv"
    assert resources[0].output_path == tmp_path / "bronze" / "resource_key=respuestas"


def test_select_bronze_resources_rejects_dictionary_by_key(tmp_path: Path) -> None:
    """Un diccionario no puede seleccionarse explícitamente para Bronze."""

    source_config = {
        "candidate_resources": {
            "respuestas_dictionary": {
                "file_name": "rentas_respuestas_diccionario.csv",
                "format": "csv",
                "role": "dictionary",
                "use_for_ingestion": False,
            },
        },
    }

    with pytest.raises(BronzeBuildError):
        select_bronze_resources(
            source_config,
            resource_keys=["respuestas_dictionary"],
            landing_dir=tmp_path / "landing",
            bronze_dir=tmp_path / "bronze",
        )


def test_dry_run_does_not_create_bronze_directory(tmp_path: Path) -> None:
    """Dry-run valida entradas de Landing sin escribir salidas Bronze."""

    landing_dir = tmp_path / "landing"
    bronze_dir = tmp_path / "bronze"
    landing_dir.mkdir()
    source_file = landing_dir / "rentas_respuestas.csv"
    source_file.write_text("Año,Respuesta\n2024,Sí\n", encoding="utf-8")

    source_config = {
        "candidate_resources": {
            "respuestas": {
                "file_name": "rentas_respuestas.csv",
                "format": "csv",
                "role": "source_table",
                "priority": "high",
                "use_for_ingestion": True,
            },
        },
    }
    resources = select_bronze_resources(
        source_config,
        landing_dir=landing_dir,
        bronze_dir=bronze_dir,
    )

    summary = build_bronze_predial_goal(
        resources=resources,
        dry_run=True,
        overwrite=False,
    )

    assert summary == build_dry_run_summary(resources)
    assert not bronze_dir.exists()
