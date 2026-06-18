"""Pruebas Bronze para la fuente manual de categorias municipales."""

from pathlib import Path

import pandas as pd
import pytest

from src.bronze.build_bronze_municipal_categories import (
    BronzeCategoryBuildError,
    BronzeCategoryResource,
    build_bronze_dataframe,
    build_bronze_municipal_categories,
    normalize_column_name,
    read_category_csv,
    validate_landing_input,
)


def make_resource(tmp_path: Path, *, delimiter: str = ";") -> BronzeCategoryResource:
    """Crea un recurso temporal de categorias para pruebas."""

    return BronzeCategoryResource(
        resource_key="categorias_municipalidades",
        source_path=tmp_path / "landing" / "category" / "CategoriasMunicipalidades.csv",
        output_path=tmp_path / "bronze" / "municipal_categories" / "resource_key=categorias_municipalidades",
        file_name="CategoriasMunicipalidades.csv",
        delimiter=delimiter,
        access_method="manual_csv",
    )


def test_normalize_column_name_remueve_acentos_y_simbolos() -> None:
    """Los nombres de columnas quedan en formato tecnico."""

    assert normalize_column_name("Categoría Municipal") == "categoria_municipal"
    assert normalize_column_name("Municipalidad") == "municipalidad"


def test_validate_landing_input_rejects_missing_file(tmp_path: Path) -> None:
    """El builder falla si el CSV manual no existe en Landing."""

    resource = make_resource(tmp_path)
    with pytest.raises(BronzeCategoryBuildError):
        validate_landing_input(resource)


def test_read_category_csv_uses_semicolon_and_text_values(tmp_path: Path) -> None:
    """La lectura respeta separador punto y coma y conserva valores como texto."""

    resource = make_resource(tmp_path)
    resource.source_path.parent.mkdir(parents=True)
    resource.source_path.write_text(
        "Municipalidad;Categoria\nM. D. DE CHETO;F\nM. P. DE TEST;A\n",
        encoding="utf-8",
    )

    df = read_category_csv(resource)

    assert list(df.columns) == ["Municipalidad", "Categoria"]
    assert df.shape == (2, 2)
    assert str(df.dtypes["Categoria"]) == "string"


def test_build_bronze_dataframe_adds_common_metadata(tmp_path: Path) -> None:
    """Bronze agrega metadata comun y no calcula reglas de negocio."""

    resource = make_resource(tmp_path)
    raw_df = pd.DataFrame({"Municipalidad": ["M. D. DE CHETO"], "Categoria": ["F"]})

    bronze_df = build_bronze_dataframe(raw_df, resource)

    assert {"municipalidad", "categoria"}.issubset(bronze_df.columns)
    assert bronze_df.loc[0, "bronze_source_name"] == "municipal_categories"
    assert bronze_df.loc[0, "bronze_resource_key"] == "categorias_municipalidades"
    assert bronze_df.loc[0, "bronze_source_file_name"] == "CategoriasMunicipalidades.csv"
    assert bronze_df.loc[0, "bronze_source_access_method"] == "manual_csv"
    assert "bronze_processed_at_utc" in bronze_df.columns


def test_dry_run_does_not_create_bronze_directory(tmp_path: Path) -> None:
    """Dry-run valida el CSV sin escribir salidas Bronze."""

    resource = make_resource(tmp_path)
    resource.source_path.parent.mkdir(parents=True)
    resource.source_path.write_text("Municipalidad;Categoria\nM. D. DE CHETO;F\n", encoding="utf-8")

    summary = build_bronze_municipal_categories(resource=resource, dry_run=True)

    assert summary["rows_detected"] == 1
    assert summary["source_exists"] is True
    assert not resource.output_path.exists()


def test_build_writes_parquet_dataset(tmp_path: Path) -> None:
    """La ejecucion real escribe Parquet bajo data/bronze equivalente."""

    resource = make_resource(tmp_path)
    resource.source_path.parent.mkdir(parents=True)
    resource.source_path.write_text("Municipalidad;Categoria\nM. D. DE CHETO;F\n", encoding="utf-8")

    summary = build_bronze_municipal_categories(resource=resource, overwrite=False)

    parquet_file = resource.output_path / "part-00000.parquet"
    assert summary["written"] is True
    assert parquet_file.exists()

    result = pd.read_parquet(resource.output_path)
    assert result.loc[0, "municipalidad"] == "M. D. DE CHETO"
    assert result.loc[0, "categoria"] == "F"
    assert result.loc[0, "bronze_source_name"] == "municipal_categories"

