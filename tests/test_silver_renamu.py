"""Pruebas unitarias para la transformacion Silver curada de RENAMU."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.silver.transform_renamu import (
    BRONZE_RESOURCE_KEY,
    FINAL_COLUMNS,
    SILVER_RESOURCE_KEY,
    SilverResource,
    SilverTransformError,
    build_renamu_resource,
    transform_renamu,
    transform_resource_dataframe,
)


@pytest.fixture()
def spark() -> SparkSession:
    """Crea una sesion Spark local para pruebas."""

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-renamu")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def sample_source_config() -> dict[str, object]:
    """Retorna una configuracion minima para RENAMU."""

    return {
        "bronze_subdir": "renamu",
        "silver_subdir": "renamu",
    }


def common_bronze_metadata() -> dict[str, str]:
    """Retorna metadata Bronze comun para RENAMU."""

    return {
        "bronze_source_name": "renamu",
        "bronze_resource_key": BRONZE_RESOURCE_KEY,
        "bronze_source_file_name": "base_renamu_2022.parquet",
        "bronze_source_file_path": "/app/data/landing/renamu/base_renamu_2022.parquet",
        "bronze_source_year": "2022",
        "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
    }


def renamu_rows() -> list[dict[str, object]]:
    """Filas Bronze de ejemplo con las variables usadas por municipal_context."""

    return [
        {
            **common_bronze_metadata(),
            "ano": "2022",
            "idmunici": "010101",
            "ccdd": "01",
            "ccpp": "01",
            "ccdi": "01",
            "ubigeo": "010101",
            "departamento": "AMAZONAS",
            "provincia": "CHACHAPOYAS",
            "distrito": "CHACHAPOYAS",
            "tipomuni": "1",
            "p13a_1": "1",
            "p13a_2": "2",
            "p13a_3": "3",
            "p13a_4": "4",
            "p13a_5": "5",
            "p13a_6": "6",
            "p13a_7": "7",
            "p13a_8": "8",
            "p13a_9": "9",
            "p14": "1",
            "p14a_1": "12",
            "p14a_2": "5",
            "p16_4": "4",
            "p16_5": "5",
            "p17_7": "7",
            "p17_8": "8",
            "p17_14": "14",
            "p18": "1",
            "p18_portal": "https://muni1.gob.pe/transparencia",
            "p19d_t": "100",
            "p19m_t": "110",
            "p19a": "1",
            "p19a_1_t": "8",
            "p19a_2_t": "9",
            "p20": "1",
            "p20_1_t": "4",
            "p20_2_t": "5",
            "p22_at2": "2",
            "p22_at3": "3",
            "p22_c2": "2",
            "p22_c3": "3",
            "p31_1": "1",
            "p31_2": "2",
            "p31_3": "3",
            "p31_5": "5",
            "p32": "1",
            "p32_1_t": "10",
            "p32_2_t": "11",
            "p33a": "1",
            "p04_1": "NO_DEBE_SALIR",
            "p11a_1": "NO_DEBE_SALIR",
            "p96": "NO_DEBE_SALIR",
            "c96": "NO_DEBE_SALIR",
            "c97": "NO_DEBE_SALIR",
        },
        {
            **common_bronze_metadata(),
            "ano": "2022",
            "idmunici": "010102",
            "ccdd": "01",
            "ccpp": "01",
            "ccdi": "02",
            "ubigeo": "010102",
            "departamento": "AMAZONAS",
            "provincia": "CHACHAPOYAS",
            "distrito": "ASUNCION",
            "tipomuni": "2",
            "p13a_1": " ",
            "p13a_2": None,
            "p13a_3": "X",
            "p13a_4": "1",
            "p13a_5": "1",
            "p13a_6": "1",
            "p13a_7": "1",
            "p13a_8": "1",
            "p13a_9": "1",
            "p14": "2",
            "p14a_1": "3",
            "p14a_2": "2",
            "p16_4": "0",
            "p16_5": "0",
            "p17_7": "0",
            "p17_8": "0",
            "p17_14": "0",
            "p18": "4",
            "p18_portal": "https://muni2.gob.pe/transparencia",
            "p19d_t": "200",
            "p19m_t": "210",
            "p19a": "2",
            "p19a_1_t": "0",
            "p19a_2_t": "0",
            "p20": "2",
            "p20_1_t": "0",
            "p20_2_t": "0",
            "p22_at2": "0",
            "p22_at3": "0",
            "p22_c2": "0",
            "p22_c3": "0",
            "p31_1": "0",
            "p31_2": "0",
            "p31_3": "0",
            "p31_5": "0",
            "p32": "2",
            "p32_1_t": "0",
            "p32_2_t": "0",
            "p33a": "2",
            "p04_1": "NO_DEBE_SALIR",
            "p11a_1": "NO_DEBE_SALIR",
            "p96": "NO_DEBE_SALIR",
            "c96": "NO_DEBE_SALIR",
            "c97": "NO_DEBE_SALIR",
        },
        {
            **common_bronze_metadata(),
            "ano": "2022",
            "idmunici": "999999",
            "ccdd": "99",
            "ccpp": "98",
            "ccdi": "97",
            "ubigeo": "999998",
            "departamento": "TEST",
            "provincia": "TEST",
            "distrito": "TEST",
            "tipomuni": "3",
            "p13a_1": "0",
            "p13a_2": "0",
            "p13a_3": "0",
            "p13a_4": "0",
            "p13a_5": "0",
            "p13a_6": "0",
            "p13a_7": "0",
            "p13a_8": "0",
            "p13a_9": "0",
            "p14": "",
            "p14a_1": "",
            "p14a_2": " ",
            "p16_4": "4",
            "p16_5": "0",
            "p17_7": "0",
            "p17_8": "8",
            "p17_14": "0",
            "p18": "2",
            "p18_portal": "",
            "p19d_t": "300",
            "p19m_t": "305",
            "p19a": "1",
            "p19a_1_t": "1",
            "p19a_2_t": "2",
            "p20": "1",
            "p20_1_t": "6",
            "p20_2_t": "7",
            "p22_at2": "0",
            "p22_at3": "3",
            "p22_c2": "2",
            "p22_c3": "0",
            "p31_1": "0",
            "p31_2": "2",
            "p31_3": "0",
            "p31_5": "5",
            "p32": "1",
            "p32_1_t": "2",
            "p32_2_t": "3",
            "p33a": "3",
            "p04_1": "NO_DEBE_SALIR",
            "p11a_1": "NO_DEBE_SALIR",
            "p96": "NO_DEBE_SALIR",
            "c96": "NO_DEBE_SALIR",
            "c97": "NO_DEBE_SALIR",
        },
    ]


def write_bronze_dataset(spark: SparkSession, root: Path) -> None:
    """Escribe un Bronze RENAMU de ejemplo."""

    output_path = root / f"resource_key={BRONZE_RESOURCE_KEY}"
    spark.createDataFrame(renamu_rows()).write.mode("overwrite").parquet(str(output_path))


def test_transform_resource_dataframe_builds_curated_contract(
    spark: SparkSession,
) -> None:
    """La salida municipal_context solo conserva columnas curadas."""

    resource = SilverResource(
        bronze_resource_key=BRONZE_RESOURCE_KEY,
        silver_resource_key=SILVER_RESOURCE_KEY,
        bronze_path=Path("unused"),
        silver_path=Path("unused"),
        silver_root=Path("unused"),
    )

    dataframe = spark.createDataFrame(renamu_rows())
    result = transform_resource_dataframe(
        dataframe=dataframe,
        resource=resource,
        processed_at="2026-06-19T01:00:00+00:00",
    )

    assert result.columns == FINAL_COLUMNS
    assert "p04_1" not in result.columns
    assert "p11a_1" not in result.columns
    assert "p96" not in result.columns
    assert "c96" not in result.columns
    assert "c97" not in result.columns

    rows = {row["idmunici"]: row.asDict(recursive=True) for row in result.collect()}

    row_1 = rows["010101"]
    assert row_1["ubigeo6"] == "010101"
    assert row_1["ccdd"] == "01"
    assert row_1["ccpp"] == "01"
    assert row_1["ccdi"] == "01"
    assert row_1["tipomuni_nombre"] == "Provincial"
    assert row_1["total_computadoras_operativas"] == 45
    assert row_1["cuenta_servicio_internet"] is True
    assert row_1["computadoras_con_acceso_internet"] == 12
    assert row_1["tipo_conexion_internet_nombre"] == "Cable de fibra óptica"
    assert row_1["usa_siaf"] is True
    assert row_1["usa_sistema_recaudacion_tributaria_municipal"] is True
    assert row_1["usa_sistema_rentas_administracion_tributaria"] is True
    assert row_1["usa_sistema_catastro"] is True
    assert row_1["no_tiene_sistemas_gestion"] is True
    assert row_1["portal_transparencia_estado_nombre"] == "Sí y está actualizado"
    assert row_1["tiene_portal_transparencia"] is True
    assert row_1["portal_transparencia_actualizado"] is True
    assert row_1["total_personal_dic_2021"] == 100
    assert row_1["total_personal_mar_2022"] == 110
    assert row_1["tiene_personal_locacion_servicios"] is True
    assert row_1["personal_locacion_total_dic_2021"] == 8
    assert row_1["personal_locacion_total_mar_2022"] == 9
    assert row_1["tiene_personal_discapacidad"] is True
    assert row_1["personal_discapacidad_total_dic_2021"] == 4
    assert row_1["personal_discapacidad_total_mar_2022"] == 5
    assert row_1["acepta_pago_efectivo_ventanilla"] is True
    assert row_1["acepta_pago_tarjeta_ventanilla"] is True
    assert row_1["acepta_pago_web_en_linea"] is True
    assert row_1["acepta_otro_medio_pago"] is True
    assert row_1["tiene_personal_exclusivo_administracion_tributaria"] is True
    assert row_1["personal_admin_tributaria_dic_2021"] == 10
    assert row_1["personal_admin_tributaria_mar_2022"] == 11
    assert row_1["tiene_area_ejecucion_coactiva"] is True
    assert row_1["requiere_asistencia_administracion_tributaria"] is True
    assert row_1["requiere_asistencia_catastro"] is True
    assert row_1["requiere_capacitacion_administracion_tributaria"] is True
    assert row_1["requiere_capacitacion_catastro"] is True
    assert row_1["silver_source_name"] == "renamu"
    assert row_1["silver_resource_key"] == "municipal_context"

    row_2 = rows["010102"]
    assert row_2["tipomuni_nombre"] == "Distrital"
    assert row_2["total_computadoras_operativas"] == 6
    assert row_2["cuenta_servicio_internet"] is False
    assert row_2["tipo_conexion_internet_nombre"] == "Banda ancha móvil"
    assert row_2["usa_siaf"] is False
    assert row_2["usa_sistema_recaudacion_tributaria_municipal"] is False
    assert row_2["usa_sistema_rentas_administracion_tributaria"] is False
    assert row_2["usa_sistema_catastro"] is False
    assert row_2["no_tiene_sistemas_gestion"] is False
    assert row_2["portal_transparencia_estado_nombre"] == "Sí y está desactualizado"
    assert row_2["tiene_portal_transparencia"] is True
    assert row_2["portal_transparencia_actualizado"] is False
    assert row_2["tiene_personal_discapacidad"] is False
    assert row_2["tiene_personal_exclusivo_administracion_tributaria"] is False
    assert row_2["tiene_area_ejecucion_coactiva"] is False

    row_3 = rows["999999"]
    assert row_3["tipomuni_nombre"] == "Centro Poblado"
    assert row_3["cuenta_servicio_internet"] is None
    assert row_3["tipo_conexion_internet_nombre"] is None
    assert row_3["portal_transparencia_estado_nombre"] == "No tiene, en proceso de implementación"
    assert row_3["tiene_portal_transparencia"] is False
    assert row_3["portal_transparencia_actualizado"] is False
    assert row_3["tiene_area_ejecucion_coactiva"] is None
    assert row_3["requiere_asistencia_administracion_tributaria"] is False
    assert row_3["requiere_asistencia_catastro"] is True
    assert row_3["requiere_capacitacion_administracion_tributaria"] is True
    assert row_3["requiere_capacitacion_catastro"] is False


def test_dry_run_and_overwrite_materialize_only_municipal_context(
    tmp_path: Path,
) -> None:
    """Dry-run no escribe y overwrite deja solo municipal_context."""

    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    writer = (
        SparkSession.builder.master("local[1]")
        .appName("test-renamu-writer")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    writer.sparkContext.setLogLevel("ERROR")
    write_bronze_dataset(writer, bronze_dir)
    writer.stop()

    resource = build_renamu_resource(
        sample_source_config(),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
    )

    dry_run_summary = transform_renamu(
        resource=resource,
        dry_run=True,
        overwrite=False,
        limit=None,
    )
    assert dry_run_summary["silver_resource_key"] == SILVER_RESOURCE_KEY
    assert not (silver_dir / "resource_key=municipal_context").exists()

    # Simula salidas Silver heredadas que deben desaparecer con overwrite.
    (silver_dir / "resource_key=base_renamu_2022").mkdir(parents=True, exist_ok=True)
    (silver_dir / "resource_key=full_clean").mkdir(parents=True, exist_ok=True)

    overwrite_summary = transform_renamu(
        resource=resource,
        dry_run=False,
        overwrite=True,
        limit=None,
    )

    assert overwrite_summary["silver_resource_key"] == SILVER_RESOURCE_KEY
    assert (silver_dir / "resource_key=municipal_context").exists()
    assert not (silver_dir / "resource_key=base_renamu_2022").exists()
    assert not (silver_dir / "resource_key=full_clean").exists()

    with pytest.raises(Exception):
        transform_renamu(
            resource=resource,
            dry_run=False,
            overwrite=False,
            limit=None,
        )

    reader = (
        SparkSession.builder.master("local[1]")
        .appName("test-renamu-reader")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    reader.sparkContext.setLogLevel("ERROR")
    result = reader.read.parquet(str(silver_dir / "resource_key=municipal_context"))
    assert result.count() == 3
    assert result.select("silver_source_name").distinct().first()[0] == "renamu"
    assert result.where(result.silver_resource_key == "base_renamu_2022").count() == 0
    assert result.where(result.silver_source_name == "mef_income").count() == 0
    assert result.where(result.silver_source_name == "predial_goal").count() == 0
    reader.stop()


def test_build_renamu_resource_uses_municipal_context_output(tmp_path: Path) -> None:
    """La definicion del recurso debe escribir municipal_context como salida final."""

    resource = build_renamu_resource(
        sample_source_config(),
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
    )

    assert resource.bronze_resource_key == "base_renamu_2022"
    assert resource.silver_resource_key == "municipal_context"
    assert resource.silver_path.name == "resource_key=municipal_context"


def test_transform_renamu_rejects_missing_bronze_resource(tmp_path: Path) -> None:
    """Falla de forma controlada cuando no existe el Bronze esperado."""

    resource = build_renamu_resource(
        sample_source_config(),
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
    )

    with pytest.raises(SilverTransformError):
        transform_renamu(
            resource=resource,
            dry_run=True,
            overwrite=False,
            limit=None,
        )
