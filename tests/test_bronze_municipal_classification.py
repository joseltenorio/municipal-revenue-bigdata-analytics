"""Pruebas Bronze para la fuente oficial municipal_classification."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.bronze.build_bronze_municipal_classification import (
    TOTAL_EXPECTED_ROWS,
    BronzeMunicipalClassificationBuildError,
    MunicipalClassificationPaths,
    MunicipalClassificationResource,
    build_bronze_municipal_classification,
    build_source_paths,
    load_municipal_classification_config,
    select_resources,
    validate_duplicates,
    validate_tipo_distribution,
    validate_ubigeo_format,
)


def make_test_config() -> dict[str, object]:
    """Construye configuracion minima para pruebas aisladas."""

    return {
        "landing_subdir": "municipal_classification",
        "bronze_subdir": "municipal_classification",
        "page_url": "https://www.gob.pe/institucion/mef/informes-publicaciones/4299668-clasificacion-municipal-2019",
        "candidate_resources": {
            "tipo_a": {
                "file_name": "tipo_a.pdf",
                "url": "https://example.test/tipo_a.pdf",
                "format": "pdf",
                "tipo_clasificacion": "A",
                "ambito_municipal": "provincial",
                "descripcion_tipo": "A",
                "expected_rows": 74,
                "use_for_ingestion": True,
            },
            "tipo_b": {
                "file_name": "tipo_b.pdf",
                "url": "https://example.test/tipo_b.pdf",
                "format": "pdf",
                "tipo_clasificacion": "B",
                "ambito_municipal": "provincial",
                "descripcion_tipo": "B",
                "expected_rows": 122,
                "use_for_ingestion": True,
            },
            "tipo_c": {
                "file_name": "tipo_c.pdf",
                "url": "https://example.test/tipo_c.pdf",
                "format": "pdf",
                "tipo_clasificacion": "C",
                "ambito_municipal": "distrital",
                "descripcion_tipo": "C",
                "expected_rows": 42,
                "use_for_ingestion": True,
            },
            "tipo_d": {
                "file_name": "tipo_d.pdf",
                "url": "https://example.test/tipo_d.pdf",
                "format": "pdf",
                "tipo_clasificacion": "D",
                "ambito_municipal": "distrital",
                "descripcion_tipo": "D",
                "expected_rows": 129,
                "use_for_ingestion": True,
            },
            "tipo_e": {
                "file_name": "tipo_e.pdf",
                "url": "https://example.test/tipo_e.pdf",
                "format": "pdf",
                "tipo_clasificacion": "E",
                "ambito_municipal": "distrital",
                "descripcion_tipo": "E",
                "expected_rows": 378,
                "use_for_ingestion": True,
            },
            "tipo_f": {
                "file_name": "tipo_f.pdf",
                "url": "https://example.test/tipo_f.pdf",
                "format": "pdf",
                "tipo_clasificacion": "F",
                "ambito_municipal": "distrital",
                "descripcion_tipo": "F",
                "expected_rows": 509,
                "use_for_ingestion": True,
            },
            "tipo_g": {
                "file_name": "tipo_g.pdf",
                "url": "https://example.test/tipo_g.pdf",
                "format": "pdf",
                "tipo_clasificacion": "G",
                "ambito_municipal": "distrital",
                "descripcion_tipo": "G",
                "expected_rows": 620,
                "use_for_ingestion": True,
            },
        },
    }


def build_paths(tmp_path: Path) -> MunicipalClassificationPaths:
    """Construye rutas temporales equivalentes a Landing y Bronze."""

    return build_source_paths(
        make_test_config(),
        landing_dir=tmp_path / "landing" / "municipal_classification",
        bronze_dir=tmp_path / "bronze" / "municipal_classification",
    )


def make_resource_rows(resource: MunicipalClassificationResource, start_index: int) -> pd.DataFrame:
    """Genera un dataframe valido con el conteo esperado por recurso."""

    rows = []
    for offset in range(resource.expected_rows):
        ubigeo_number = start_index + offset
        rows.append(
            {
                "anio": 2019,
                "tipo_clasificacion": resource.tipo_clasificacion,
                "ambito_municipal": resource.ambito_municipal,
                "descripcion_tipo": resource.descripcion_tipo,
                "nro": offset + 1,
                "ubigeo": f"{ubigeo_number:06d}",
                "departamento_nombre": "LIMA",
                "provincia_nombre": "LIMA",
                "distrito_nombre": f"DISTRITO {resource.tipo_clasificacion} {offset + 1}",
            }
        )
    return pd.DataFrame(rows)


def test_sources_config_declares_municipal_classification() -> None:
    """La fuente oficial existe y define siete recursos PDF A-G."""

    source_config = load_municipal_classification_config()
    resources = source_config["candidate_resources"]

    assert source_config["enabled"] is True
    assert set(resources) == {
        "tipo_a",
        "tipo_b",
        "tipo_c",
        "tipo_d",
        "tipo_e",
        "tipo_f",
        "tipo_g",
    }
    assert sum(int(resource["expected_rows"]) for resource in resources.values()) == TOTAL_EXPECTED_ROWS


def test_build_source_paths_uses_direct_dataset_layout(tmp_path: Path) -> None:
    """Las rutas siguen la estructura raw/extracted_csv/Bronze consolidado."""

    paths = build_paths(tmp_path)

    assert paths.raw_dir == tmp_path / "landing" / "municipal_classification" / "raw"
    assert paths.extracted_csv_dir == tmp_path / "landing" / "municipal_classification" / "extracted_csv"
    assert paths.manifest_path == tmp_path / "landing" / "municipal_classification" / "manifest.json"
    assert paths.bronze_output_dir == tmp_path / "bronze" / "municipal_classification"


def test_validate_ubigeo_format_rejects_invalid_values() -> None:
    """El builder rechaza ubigeos distintos de seis digitos."""

    dataframe = pd.DataFrame(
        {
            "anio": [2019, 2019],
            "ubigeo": ["010101", "10101"],
        }
    )

    with pytest.raises(BronzeMunicipalClassificationBuildError):
        validate_ubigeo_format(dataframe)


def test_validate_duplicates_rejects_duplicate_anio_ubigeo() -> None:
    """El builder rechaza duplicados por anio + ubigeo."""

    dataframe = pd.DataFrame(
        {
            "anio": [2019, 2019],
            "ubigeo": ["010101", "010101"],
        }
    )

    with pytest.raises(BronzeMunicipalClassificationBuildError):
        validate_duplicates(dataframe)


def test_validate_tipo_distribution_rejects_wrong_counts(tmp_path: Path) -> None:
    """La distribucion por tipo debe coincidir exactamente con la configuracion."""

    paths = build_paths(tmp_path)
    resources = select_resources(make_test_config(), paths)
    dataframe = make_resource_rows(resources[0], 10101)

    with pytest.raises(BronzeMunicipalClassificationBuildError):
        validate_tipo_distribution(dataframe, resources)


def test_dry_run_does_not_write_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Dry-run solo valida recursos y no genera PDFs, CSV ni Parquet."""

    paths = build_paths(tmp_path)
    resources = select_resources(make_test_config(), paths)

    monkeypatch.setattr(
        "src.bronze.build_bronze_municipal_classification.probe_resource",
        lambda url, timeout_seconds: {
            "http_status_code": 200,
            "content_type": "application/pdf",
            "content_length_bytes": 1234,
        },
    )

    summary = build_bronze_municipal_classification(
        source_config=make_test_config(),
        paths=paths,
        resources=resources,
        dry_run=True,
        overwrite=False,
    )

    assert summary["downloads_performed"] is False
    assert summary["parquet_written"] is False
    assert not paths.raw_dir.exists()
    assert not paths.extracted_csv_dir.exists()
    assert not paths.bronze_output_dir.exists()


def test_build_bronze_municipal_classification_writes_manifest_and_parquet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """La ejecucion real consolida un unico Bronze y genera manifest."""

    paths = build_paths(tmp_path)
    resources = select_resources(make_test_config(), paths)

    def fake_download_pdf(*, resource: MunicipalClassificationResource, timeout_seconds: int, overwrite: bool) -> None:
        resource.pdf_path.parent.mkdir(parents=True, exist_ok=True)
        resource.pdf_path.write_bytes(b"%PDF-simulado")

    def fake_extract(resource: MunicipalClassificationResource) -> pd.DataFrame:
        index_by_resource = {
            "tipo_a": 10101,
            "tipo_b": 20101,
            "tipo_c": 30101,
            "tipo_d": 40101,
            "tipo_e": 50101,
            "tipo_f": 60101,
            "tipo_g": 70101,
        }
        return make_resource_rows(resource, index_by_resource[resource.resource_key])

    monkeypatch.setattr(
        "src.bronze.build_bronze_municipal_classification.download_pdf",
        fake_download_pdf,
    )
    monkeypatch.setattr(
        "src.bronze.build_bronze_municipal_classification.extract_resource_dataframe",
        fake_extract,
    )

    summary = build_bronze_municipal_classification(
        source_config=make_test_config(),
        paths=paths,
        resources=resources,
        dry_run=False,
        overwrite=True,
    )

    assert summary["actual_total_rows"] == TOTAL_EXPECTED_ROWS
    assert paths.manifest_path.exists()
    assert (paths.bronze_output_dir / "data.parquet").exists()
    assert (paths.extracted_csv_dir / "tipo_a.csv").exists()
    assert (paths.raw_dir / "tipo_a.pdf").exists()

    manifest = json.loads(paths.manifest_path.read_text(encoding="utf-8"))
    assert manifest["actual_total_rows"] == TOTAL_EXPECTED_ROWS

    bronze = pd.read_parquet(paths.bronze_output_dir / "data.parquet")
    assert len(bronze) == TOTAL_EXPECTED_ROWS
    assert set(bronze["tipo_clasificacion"].unique()) == {"A", "B", "C", "D", "E", "F", "G"}
    assert bronze["anio"].nunique() == 1
    assert bronze["anio"].iloc[0] == 2019
    assert bronze["ubigeo"].astype("string").str.fullmatch(r"\d{6}").all()
    assert bronze["bronze_source_name"].iloc[0] == "municipal_classification"
