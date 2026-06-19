"""Pruebas unitarias para la transformacion Silver curada de SISMEPRE."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.silver.transform_sismepre import (
    COMMON_SILVER_METADATA_COLUMNS,
    FINAL_COLUMNS_BY_RESOURCE,
    SilverResource,
    SilverTransformError,
    select_silver_resources,
    transform_resource_dataframe,
    transform_sismepre,
)


@pytest.fixture()
def spark() -> SparkSession:
    """Crea una sesion Spark local para pruebas."""

    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-sismepre")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


def sample_source_config() -> dict[str, object]:
    """Retorna una configuracion minima con los 7 recursos SISMEPRE."""

    resources = {
        "ano_aplicacion": {"format": "csv", "role": "source_table", "priority": "medium", "use_for_ingestion": True},
        "entidad_estado": {"format": "csv", "role": "source_table", "priority": "high", "use_for_ingestion": True},
        "esat_estadistica_atm": {"format": "csv", "role": "source_table", "priority": "medium", "use_for_ingestion": True},
        "estadistica": {"format": "csv", "role": "source_table", "priority": "high", "use_for_ingestion": True},
        "formulario": {"format": "csv", "role": "source_table", "priority": "medium", "use_for_ingestion": True},
        "preguntas": {"format": "csv", "role": "source_table", "priority": "medium", "use_for_ingestion": True},
        "respuestas": {"format": "csv", "role": "source_table", "priority": "high", "use_for_ingestion": True},
    }
    return {
        "bronze_subdir": "sismepre",
        "silver_subdir": "sismepre",
        "candidate_resources": resources,
    }


def common_bronze_metadata(resource_key: str) -> dict[str, str]:
    """Retorna metadata Bronze comun para recursos SISMEPRE."""

    return {
        "bronze_source_name": "sismepre",
        "bronze_resource_key": resource_key,
        "bronze_source_file_name": f"{resource_key}.csv",
        "bronze_source_file_path": f"/app/data/landing/sismepre/{resource_key}.csv",
        "bronze_source_role": "source_table",
        "bronze_source_priority": "medium",
        "bronze_processed_at_utc": "2026-06-19T00:00:00+00:00",
    }


def esat_rows() -> list[dict[str, object]]:
    """Filas Bronze de ejemplo para esat_estadistica_atm."""

    return [
        {
            **common_bronze_metadata("esat_estadistica_atm"),
            "sec_ejec": "301260",
            "ubigeo": "150111",
            "departamento": "15",
            "departamento_nombre": "LIMA",
            "provincia": "01",
            "provincia_nombre": "LIMA",
            "distrito": "11",
            "distrito_nombre": "EL AGUSTINO",
            "municipalidad_nombre": "MUNICIPALIDAD DISTRITAL DE EL AGUSTINO",
            "ano_aplicacion": "2017",
            "periodo": "2",
            "ano_estadistica": "2011",
            "mes_estadistica": "13",
            "mon_emisionpredial_afecto": "100.25",
            "mon_emisionpredial_exon": "50.75",
            "mon_emisionpredial_inso": "10",
            "mon_baseimponible_afecto": "500",
            "mon_baseimponible_exon": "200",
            "mon_autoavaluo_inafecto": "25",
            "num_emisionpredial_afecto": "10",
            "num_emisionpredial_exon": "5",
            "num_emisionpredial_casa": "3",
            "num_emisionpredial_otros": "2",
            "mon_recaudactual_ordin": "70",
            "mon_recaudactual_coac": "5",
            "mon_recaudanter_ordi": "10",
            "mon_recaudanter_coac": "15",
            "mon_saldopredial_ord": "12",
            "mon_saldopredial_coac": "8",
            "num_inafectos": "1",
            "num_contripredio": "7",
            "num_prediousoch": "4",
            "num_prediootrouso": "5",
            "num_prediototal": "9",
            "mon_inicialadultomayor": "9",
            "mon_predialadultomayor": "6",
            "num_contribadultomayor": "2",
            "mon_recuadadultomayor": "4",
            "tipo_meta": "A",
            "flag_emiliquida": "S",
            "flag_emision_inicial": "N",
            "formulario_id": "6",
        },
        {
            **common_bronze_metadata("esat_estadistica_atm"),
            "sec_ejec": "301261",
            "ubigeo": "150112",
            "departamento": "15",
            "departamento_nombre": "LIMA",
            "provincia": "01",
            "provincia_nombre": "LIMA",
            "distrito": "12",
            "distrito_nombre": "INDEPENDENCIA",
            "municipalidad_nombre": "MUNICIPALIDAD DISTRITAL DE INDEPENDENCIA",
            "ano_aplicacion": "2018",
            "periodo": "2",
            "ano_estadistica": "2012",
            "mes_estadistica": "5",
            "mon_emisionpredial_afecto": "0",
            "mon_emisionpredial_exon": "0",
            "mon_emisionpredial_inso": "0",
            "mon_baseimponible_afecto": "0",
            "mon_baseimponible_exon": "0",
            "mon_autoavaluo_inafecto": "0",
            "num_emisionpredial_afecto": "0",
            "num_emisionpredial_exon": "0",
            "num_emisionpredial_casa": "0",
            "num_emisionpredial_otros": "0",
            "mon_recaudactual_ordin": "0",
            "mon_recaudactual_coac": "0",
            "mon_recaudanter_ordi": "0",
            "mon_recaudanter_coac": "0",
            "mon_saldopredial_ord": "0",
            "mon_saldopredial_coac": "0",
            "num_inafectos": "0",
            "num_contripredio": "0",
            "num_prediousoch": "0",
            "num_prediootrouso": "0",
            "num_prediototal": "0",
            "mon_inicialadultomayor": "0",
            "mon_predialadultomayor": "0",
            "num_contribadultomayor": "0",
            "mon_recuadadultomayor": "0",
            "tipo_meta": "B",
            "flag_emiliquida": "N",
            "flag_emision_inicial": "S",
            "formulario_id": "7",
        },
    ]


def formulario_rows() -> list[dict[str, object]]:
    return [
        {
            **common_bronze_metadata("formulario"),
            "ano_aplicacion": "2021",
            "periodo": "1",
            "formulario_id": "6",
            "orden_formulario": "001",
            "titulo": "Titulo",
            "sub_titulo": "Subtitulo",
            "abreviatura": "3A",
            "clasificacion": "A|B",
            "tipo_formulario": "E",
            "estado_registro": "A",
        }
    ]


def preguntas_rows() -> list[dict[str, object]]:
    return [
        {
            **common_bronze_metadata("preguntas"),
            "ano_aplicacion": "2020",
            "periodo": "1",
            "formulario_id": "2",
            "pregunta_id": "65",
            "pregunta_padre_id": "0",
            "orden_pregunta": "034",
            "descripcion": "Descripcion",
            "objeto_activo": "1",
            "tipo_cuestionario_id": "11",
            "respuesta": "",
            "rango_ini": "0",
            "rango_fin": "999",
            "texto_apoyo": "Apoyo",
            "texto_lectura": "Lectura",
            "estado_registro": "A",
        }
    ]


def respuestas_rows() -> list[dict[str, object]]:
    return [
        {
            **common_bronze_metadata("respuestas"),
            "sec_ejec": "301253",
            "ano_aplicacion": "2024",
            "periodo": "2",
            "formulario_id": "2",
            "pregunta_id": "86",
            "respuesta_id": "72",
            "respuesta_texto": "0",
            "respuesta_decimal": "334892.07",
            "respuesta_entero": "0",
            "respuesta_fecha": "",
            "estado_registro": "A",
        },
        {
            **common_bronze_metadata("respuestas"),
            "sec_ejec": "301253",
            "ano_aplicacion": "2024",
            "periodo": "2",
            "formulario_id": "2",
            "pregunta_id": "90",
            "respuesta_id": "76",
            "respuesta_texto": "0",
            "respuesta_decimal": "0",
            "respuesta_entero": "410",
            "respuesta_fecha": "1/12/2024",
            "estado_registro": "I",
        },
    ]


def estadistica_rows() -> list[dict[str, object]]:
    return [
        {
            **common_bronze_metadata("estadistica"),
            "ano_aplicacion": "2021",
            "periodo": "1",
            "formulario_id": "6",
            "ano_estadistica": "2017",
            "mes_estadistica": "13",
            "estado_registro": "A",
            "ano_estadistica_desc": "Anual",
        }
    ]


def ano_aplicacion_rows() -> list[dict[str, object]]:
    return [
        {
            **common_bronze_metadata("ano_aplicacion"),
            "ano_aplicacion": "2019",
            "ano_aplicacion_inicio": "2011",
            "ano_aplicacion_fin": "2019",
            "fecha_cierre": "31/12/2019 00:00:00",
            "estado": "I",
            "periodo": "2",
            "fecha_pres_oficio": "31/12/2019 00:00:00",
            "fecha_ini_cierre": "1/12/2019 00:00:00",
            "fecha_ing": "12/11/2019 18:59:26",
        }
    ]


def entidad_estado_rows() -> list[dict[str, object]]:
    return [
        {
            **common_bronze_metadata("entidad_estado"),
            "sec_ejec": "301261",
            "ano_aplicacion": "2015",
            "usuario_creacion_fecha": "31/07/2015 11:30:58",
            "estado": "A",
            "usuario_envio_id": "0",
            "usuario_fecha_envio": "",
            "correo": "",
            "origen_informacion": "1",
            "clasificacion": "A",
            "periodo": "1",
            "tipo_meta": "0",
            "ind_resol_alcal_adjunto": "P",
            "fecha_resol_alcal_adjunto": "0",
        }
    ]


RESOURCE_ROWS = {
    "ano_aplicacion": ano_aplicacion_rows,
    "entidad_estado": entidad_estado_rows,
    "esat_estadistica_atm": esat_rows,
    "estadistica": estadistica_rows,
    "formulario": formulario_rows,
    "preguntas": preguntas_rows,
    "respuestas": respuestas_rows,
}


def write_bronze_datasets(spark: SparkSession, root: Path) -> None:
    """Escribe datasets Bronze de ejemplo para los 7 recursos."""

    for resource_key, rows_factory in RESOURCE_ROWS.items():
        output_path = root / f"resource_key={resource_key}"
        spark.createDataFrame(rows_factory()).write.mode("overwrite").parquet(
            str(output_path)
        )


def test_select_silver_resources_detects_seven_resources(tmp_path: Path) -> None:
    """La seleccion reconoce exactamente los 7 recursos SISMEPRE."""

    resources = select_silver_resources(
        sample_source_config(),
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
    )

    assert {resource.resource_key for resource in resources} == {
        "preguntas",
        "estadistica",
        "formulario",
        "esat_estadistica_atm",
        "respuestas",
        "ano_aplicacion",
        "entidad_estado",
    }


def test_transform_esat_estadistica_atm_builds_curated_contract(
    spark: SparkSession,
) -> None:
    """El recurso principal genera montos, flags y metricas derivadas."""

    resource = SilverResource(
        resource_key="esat_estadistica_atm",
        bronze_path=Path("unused"),
        silver_path=Path("unused"),
        role="source_table",
        priority="medium",
    )

    dataframe = spark.createDataFrame(esat_rows())
    result = transform_resource_dataframe(
        dataframe=dataframe,
        resource=resource,
        processed_at="2026-06-19T01:00:00+00:00",
    )
    rows = {row["formulario_id"]: row.asDict(recursive=True) for row in result.collect()}

    assert result.columns == [
        *FINAL_COLUMNS_BY_RESOURCE["esat_estadistica_atm"],
        *COMMON_SILVER_METADATA_COLUMNS,
    ]

    annual = rows[6]
    assert annual["sec_ejec"] == "301260"
    assert annual["ubigeo6"] == "150111"
    assert annual["periodo_estadistica_tipo"] == "ANUAL"
    assert annual["is_annual_stat_period"] is True
    assert annual["is_valid_mes_estadistica"] is True
    assert annual["monto_emision_predial_total"] == Decimal("151.0000")
    assert annual["monto_recaudacion_predial_total"] == Decimal("100.0000")
    assert annual["monto_saldo_predial_total"] == Decimal("20.0000")
    assert annual["ratio_recaudacion_emision"] == Decimal("0.66225166")
    assert annual["silver_source_name"] == "sismepre"

    monthly = rows[7]
    assert monthly["periodo_estadistica_tipo"] == "MENSUAL"
    assert monthly["is_annual_stat_period"] is False
    assert monthly["is_valid_mes_estadistica"] is True
    assert monthly["monto_emision_predial_total"] == Decimal("0.0000")
    assert monthly["ratio_recaudacion_emision"] is None


def test_transform_respuestas_keeps_long_format_and_metadata(
    spark: SparkSession,
) -> None:
    """Respuestas conserva formato largo y variantes de respuesta separadas."""

    resource = SilverResource(
        resource_key="respuestas",
        bronze_path=Path("unused"),
        silver_path=Path("unused"),
        role="source_table",
        priority="high",
    )

    dataframe = spark.createDataFrame(respuestas_rows())
    result = transform_resource_dataframe(
        dataframe=dataframe,
        resource=resource,
        processed_at="2026-06-19T01:00:00+00:00",
    )

    rows = result.collect()

    assert result.columns == [
        *FINAL_COLUMNS_BY_RESOURCE["respuestas"],
        *COMMON_SILVER_METADATA_COLUMNS,
    ]
    assert result.count() == 2
    assert "respuesta_texto" in result.columns
    assert "respuesta_decimal" in result.columns
    assert "respuesta_entero" in result.columns
    assert "respuesta_fecha" in result.columns
    assert "predial_goal" not in {row["silver_source_name"] for row in rows}
    assert rows[0]["silver_source_name"] == "sismepre"
    assert rows[0]["respuesta_decimal"] == Decimal("334892.0700")
    assert rows[1]["respuesta_entero"] == 410
    assert rows[1]["respuesta_fecha"] == date(2024, 12, 1)


def test_dry_run_and_overwrite_write_seven_separate_outputs(tmp_path: Path) -> None:
    """Dry-run no escribe y overwrite materializa los 7 recursos separados."""

    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    writer = (
        SparkSession.builder.master("local[1]")
        .appName("test-sismepre-writer")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    writer.sparkContext.setLogLevel("ERROR")
    write_bronze_datasets(writer, bronze_dir)
    writer.stop()

    resources = select_silver_resources(
        sample_source_config(),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
    )

    dry_run_summary = transform_sismepre(
        resources=resources,
        dry_run=True,
        overwrite=False,
        limit=None,
    )
    assert len(dry_run_summary) == 7
    assert not any((silver_dir / f"resource_key={key}").exists() for key in RESOURCE_ROWS)

    transform_sismepre(
        resources=resources,
        dry_run=False,
        overwrite=True,
        limit=None,
    )

    for resource_key in RESOURCE_ROWS:
        assert (silver_dir / f"resource_key={resource_key}").exists()

    with pytest.raises(Exception):
        transform_sismepre(
            resources=resources,
            dry_run=False,
            overwrite=False,
            limit=None,
        )

    reader = (
        SparkSession.builder.master("local[1]")
        .appName("test-sismepre-reader")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    reader.sparkContext.setLogLevel("ERROR")
    respuestas_df = reader.read.parquet(str(silver_dir / "resource_key=respuestas"))
    esat_df = reader.read.parquet(str(silver_dir / "resource_key=esat_estadistica_atm"))
    formulario_df = reader.read.parquet(str(silver_dir / "resource_key=formulario"))

    assert respuestas_df.count() == 2
    assert esat_df.where(esat_df.mes_estadistica == 13).select("periodo_estadistica_tipo").first()[0] == "ANUAL"
    assert "pregunta_86" not in respuestas_df.columns
    assert set(
        ["respuesta_texto", "respuesta_decimal", "respuesta_entero", "respuesta_fecha"]
    ).issubset(set(respuestas_df.columns))
    assert formulario_df.select("is_active_record").first()[0] is True
    assert respuestas_df.where(respuestas_df.silver_source_name == "predial_goal").count() == 0
    reader.stop()


def test_select_silver_resources_rejects_unknown_resource(tmp_path: Path) -> None:
    """La seleccion rechaza recursos fuera del contrato configurado."""

    with pytest.raises(SilverTransformError):
        select_silver_resources(
            sample_source_config(),
            resource_keys=["desconocido"],
            bronze_dir=tmp_path / "bronze",
            silver_dir=tmp_path / "silver",
        )
