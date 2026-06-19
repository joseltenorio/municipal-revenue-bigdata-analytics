"""Transformacion Silver curada para SISMEPRE.

Este modulo lee datasets Bronze Parquet de `sismepre` y escribe un dataset
Silver por `resource_key` bajo ``data/silver/sismepre``.

La salida Silver mantiene los siete recursos separados, tipa campos operativos,
normaliza identificadores territoriales y agrega metadata tecnica sin mezclar
formularios, preguntas, respuestas ni estadisticas.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_sources_config
from src.common.logger import get_logger
from src.common.paths import get_source_bronze_path, get_source_silver_path


SOURCE_NAME = "sismepre"
COMMON_SILVER_METADATA_COLUMNS = [
    "silver_source_name",
    "silver_resource_key",
    "silver_processed_at_utc",
]

COMMON_BRONZE_COLUMNS = [
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_role",
    "bronze_source_priority",
    "bronze_processed_at_utc",
]

FIXED_WIDTH_CODES = {
    "departamento_codigo": 2,
    "provincia_codigo": 2,
    "distrito_codigo": 2,
    "ubigeo6": 6,
}

FINAL_COLUMNS_BY_RESOURCE = {
    "esat_estadistica_atm": [
        "sec_ejec",
        "ubigeo6",
        "departamento_codigo",
        "departamento_nombre",
        "provincia_codigo",
        "provincia_nombre",
        "distrito_codigo",
        "distrito_nombre",
        "municipalidad_nombre",
        "anio_aplicacion",
        "periodo",
        "anio_estadistica",
        "mes_estadistica",
        "periodo_estadistica_tipo",
        "formulario_id",
        "monto_emision_predial_afecto",
        "monto_emision_predial_exonerado",
        "monto_emision_predial_insoluto",
        "monto_base_imponible_afecto",
        "monto_base_imponible_exonerado",
        "monto_autoavaluo_inafecto",
        "numero_emision_predial_afecto",
        "numero_emision_predial_exonerado",
        "numero_emision_predial_casa_habitacion",
        "numero_emision_predial_otros",
        "monto_recaudacion_actual_ordinaria",
        "monto_recaudacion_actual_coactiva",
        "monto_recaudacion_anterior_ordinaria",
        "monto_recaudacion_anterior_coactiva",
        "monto_saldo_predial_ordinario",
        "monto_saldo_predial_coactivo",
        "numero_inafectos",
        "numero_contribuyentes_predio",
        "numero_predios_uso_casa_habitacion",
        "numero_predios_otro_uso",
        "numero_predios_total",
        "monto_inicial_adulto_mayor",
        "monto_predial_adulto_mayor",
        "numero_contribuyentes_adulto_mayor",
        "monto_recaudacion_adulto_mayor",
        "tipo_meta",
        "flag_emision_liquidacion",
        "flag_emision_inicial",
        "monto_emision_predial_total",
        "monto_recaudacion_predial_total",
        "monto_saldo_predial_total",
        "ratio_recaudacion_emision",
        "is_valid_sec_ejec",
        "is_valid_ubigeo6",
        "is_valid_anio_aplicacion",
        "is_valid_periodo",
        "is_valid_anio_estadistica",
        "is_valid_mes_estadistica",
        "is_annual_stat_period",
    ],
    "formulario": [
        "anio_aplicacion",
        "periodo",
        "formulario_id",
        "orden_formulario",
        "titulo",
        "sub_titulo",
        "abreviatura",
        "clasificacion",
        "tipo_formulario",
        "estado_registro",
        "is_active_record",
    ],
    "preguntas": [
        "anio_aplicacion",
        "periodo",
        "formulario_id",
        "pregunta_id",
        "pregunta_padre_id",
        "orden_pregunta",
        "descripcion",
        "objeto_activo",
        "tipo_cuestionario_id",
        "respuesta",
        "rango_ini",
        "rango_fin",
        "texto_apoyo",
        "texto_lectura",
        "estado_registro",
        "is_active_record",
    ],
    "respuestas": [
        "sec_ejec",
        "anio_aplicacion",
        "periodo",
        "formulario_id",
        "pregunta_id",
        "respuesta_id",
        "respuesta_texto",
        "respuesta_decimal",
        "respuesta_entero",
        "respuesta_fecha",
        "estado_registro",
        "is_active_record",
    ],
    "estadistica": [
        "anio_aplicacion",
        "periodo",
        "formulario_id",
        "anio_estadistica",
        "mes_estadistica",
        "periodo_estadistica_tipo",
        "estado_registro",
        "anio_estadistica_desc",
        "is_active_record",
    ],
    "ano_aplicacion": [
        "anio_aplicacion",
        "anio_aplicacion_inicio",
        "anio_aplicacion_fin",
        "fecha_cierre",
        "estado",
        "periodo",
        "fecha_pres_oficio",
        "fecha_ini_cierre",
        "fecha_ing",
        "is_active_record",
    ],
    "entidad_estado": [
        "sec_ejec",
        "ubigeo6",
        "departamento_codigo",
        "departamento_nombre",
        "provincia_codigo",
        "provincia_nombre",
        "distrito_codigo",
        "distrito_nombre",
        "municipalidad_nombre",
        "anio_aplicacion",
        "periodo",
        "estado_registro",
        "is_active_record",
    ],
}


class SilverTransformError(Exception):
    """Error controlado durante la transformacion Silver de SISMEPRE."""


@dataclass(frozen=True)
class SilverResource:
    """Recurso SISMEPRE seleccionado para transformacion Silver."""

    resource_key: str
    bronze_path: Path
    silver_path: Path
    role: str
    priority: str | None


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_sismepre_config() -> dict[str, Any]:
    """Carga la configuracion de la fuente SISMEPRE."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise SilverTransformError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise SilverTransformError(f"La fuente '{SOURCE_NAME}' no esta habilitada.")

    return source_config


def is_transformable_resource(resource: dict[str, Any]) -> bool:
    """Indica si un recurso configurado corresponde a una tabla SISMEPRE util."""

    return (
        resource.get("format") == "csv"
        and resource.get("role") == "source_table"
        and bool(resource.get("use_for_ingestion", False))
    )


def select_silver_resources(
    source_config: dict[str, Any],
    *,
    resource_keys: list[str] | None = None,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
) -> list[SilverResource]:
    """Selecciona recursos Bronze de SISMEPRE a transformar."""

    configured_resources = source_config.get("candidate_resources", {})

    if not isinstance(configured_resources, dict) or not configured_resources:
        raise SilverTransformError("No existen recursos SISMEPRE configurados.")

    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    silver_subdir = source_config.get("silver_subdir", SOURCE_NAME)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)
    resolved_silver_dir = silver_dir or get_source_silver_path(silver_subdir)

    selected_resources: list[SilverResource] = []

    for resource_key, resource in configured_resources.items():
        if not isinstance(resource, dict) or not is_transformable_resource(resource):
            continue

        if resource_keys and resource_key not in resource_keys:
            continue

        selected_resources.append(
            SilverResource(
                resource_key=resource_key,
                bronze_path=resolved_bronze_dir / f"resource_key={resource_key}",
                silver_path=resolved_silver_dir / f"resource_key={resource_key}",
                role=str(resource.get("role")),
                priority=resource.get("priority"),
            )
        )

    if resource_keys:
        found_keys = {resource.resource_key for resource in selected_resources}
        missing_keys = sorted(set(resource_keys) - found_keys)
        if missing_keys:
            available_keys = sorted(
                key
                for key, resource in configured_resources.items()
                if isinstance(resource, dict) and is_transformable_resource(resource)
            )
            raise SilverTransformError(
                f"Recursos SISMEPRE no validos para Silver: {missing_keys}. "
                f"Recursos disponibles: {available_keys}."
            )

    if not selected_resources:
        raise SilverTransformError(
            "No se selecciono ningun recurso SISMEPRE para Silver."
        )

    return selected_resources


def validate_bronze_inputs(resources: list[SilverResource]) -> list[SilverResource]:
    """Valida que existan las rutas Bronze seleccionadas."""

    missing_paths = [
        str(resource.bronze_path)
        for resource in resources
        if not resource.bronze_path.exists()
    ]

    if missing_paths:
        raise SilverTransformError(
            "Faltan recursos SISMEPRE en Bronze para construir Silver: "
            + ", ".join(missing_paths)
        )

    return resources


def require_common_bronze_columns(columns: list[str], resource: SilverResource) -> None:
    """Valida la metadata Bronze minima requerida."""

    missing_columns = sorted(set(COMMON_BRONZE_COLUMNS) - set(columns))

    if missing_columns:
        raise SilverTransformError(
            f"El recurso '{resource.resource_key}' no tiene metadata Bronze "
            f"requerida para Silver: {missing_columns}."
        )


def trim_string_columns(dataframe: Any) -> Any:
    """Aplica trim a todas las columnas string sin cambiar los nombres."""

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    selected_columns = []

    for field in dataframe.schema.fields:
        column = F.col(field.name)
        if isinstance(field.dataType, StringType):
            selected_columns.append(F.trim(column).alias(field.name))
        else:
            selected_columns.append(column)

    return dataframe.select(*selected_columns)


def try_cast_integer(column_name: str) -> Any:
    """Castea una columna a entero tolerando vacios o valores invalidos."""

    from pyspark.sql import functions as F

    return F.expr(f"try_cast(nullif(trim(`{column_name}`), '') as int)")


def try_cast_decimal(column_name: str) -> Any:
    """Castea una columna numerica a decimal tolerando separadores de miles."""

    from pyspark.sql import functions as F

    return F.expr(
        f"try_cast(regexp_replace(nullif(trim(`{column_name}`), ''), ',', '') as decimal(20,4))"
    )


def normalize_string_label(column_name: str) -> Any:
    """Normaliza un valor descriptivo como texto, preservando nulos."""

    from pyspark.sql import functions as F

    return F.when(
        F.trim(F.col(column_name).cast("string")) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name).cast("string")))


def normalize_string_code(column_name: str, *, width: int | None = None) -> Any:
    """Normaliza codigos como string preservando ceros a la izquierda."""

    from pyspark.sql import functions as F

    cleaned = normalize_string_label(column_name)
    if width is None:
        return cleaned
    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.rlike(r"^[0-9]+$"), F.lpad(cleaned, width, "0"))
        .otherwise(cleaned)
    )


def parse_human_date(column_name: str) -> Any:
    """Parsea fechas de origen tolerando formatos observados en SISMEPRE."""

    from pyspark.sql import functions as F

    return F.expr(
        "coalesce("
        f"cast(try_to_timestamp(nullif(trim(`{column_name}`), ''), 'd/M/yyyy H:m:s') as date),"
        f"cast(try_to_timestamp(nullif(trim(`{column_name}`), ''), 'dd/MM/yyyy H:m:s') as date),"
        f"cast(try_to_timestamp(nullif(trim(`{column_name}`), ''), 'd/M/yyyy') as date),"
        f"cast(try_to_timestamp(nullif(trim(`{column_name}`), ''), 'dd/MM/yyyy') as date)"
        ")"
    )


def add_is_active_record(dataframe: Any) -> Any:
    """Deriva is_active_record cuando existe estado_registro.

    Se usa `null` cuando `estado_registro` viene vacio o nulo, para distinguir
    ausencia de dato frente a un estado explicito distinto de `A`.
    """

    from pyspark.sql import functions as F

    if "estado_registro" not in dataframe.columns:
        return dataframe

    return dataframe.withColumn(
        "is_active_record",
        F.when(F.col("estado_registro") == F.lit("A"), F.lit(True))
        .when(normalize_string_label("estado_registro").isNull(), F.lit(None))
        .otherwise(F.lit(False)),
    )


def add_silver_metadata(
    *, dataframe: Any, resource: SilverResource, processed_at: str
) -> Any:
    """Agrega metadata tecnica Silver."""

    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("silver_source_name", F.lit(SOURCE_NAME))
        .withColumn("silver_resource_key", F.lit(resource.resource_key))
        .withColumn("silver_processed_at_utc", F.lit(processed_at))
    )


def safe_sum(columns: list[str]) -> Any:
    """Suma columnas decimales usando cero solo como neutro aritmetico."""

    from pyspark.sql import functions as F

    expression = F.lit(0).cast("decimal(20,4)")
    for column in columns:
        expression = expression + F.coalesce(
            F.col(column), F.lit(0).cast("decimal(20,4)")
        )
    return expression


def add_period_flags(dataframe: Any) -> Any:
    """Agrega clasificacion y flags de periodo estadistico."""

    from pyspark.sql import functions as F

    if "mes_estadistica" not in dataframe.columns:
        return dataframe

    return (
        dataframe.withColumn(
            "periodo_estadistica_tipo",
            F.when(F.col("mes_estadistica") == 13, F.lit("ANUAL"))
            .when(F.col("mes_estadistica").between(1, 12), F.lit("MENSUAL"))
            .otherwise(F.lit(None)),
        )
        .withColumn("is_annual_stat_period", F.col("mes_estadistica") == 13)
        .withColumn(
            "is_valid_mes_estadistica",
            F.col("mes_estadistica").between(1, 13),
        )
    )


def transform_esat_estadistica_atm(dataframe: Any) -> Any:
    """Transforma el recurso principal de estadistica predial ATM."""

    from pyspark.sql import functions as F

    transformed = (
        dataframe.withColumn("sec_ejec", normalize_string_code("sec_ejec"))
        .withColumn("ubigeo6", normalize_string_code("ubigeo", width=6))
        .withColumn("departamento_codigo", normalize_string_code("departamento", width=2))
        .withColumn("departamento_nombre", normalize_string_label("departamento_nombre"))
        .withColumn("provincia_codigo", normalize_string_code("provincia", width=2))
        .withColumn("provincia_nombre", normalize_string_label("provincia_nombre"))
        .withColumn("distrito_codigo", normalize_string_code("distrito", width=2))
        .withColumn("distrito_nombre", normalize_string_label("distrito_nombre"))
        .withColumn("municipalidad_nombre", normalize_string_label("municipalidad_nombre"))
        .withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
        .withColumn("periodo", try_cast_integer("periodo"))
        .withColumn("anio_estadistica", try_cast_integer("ano_estadistica"))
        .withColumn("mes_estadistica", try_cast_integer("mes_estadistica"))
        .withColumn("formulario_id", try_cast_integer("formulario_id"))
        .withColumn("monto_emision_predial_afecto", try_cast_decimal("mon_emisionpredial_afecto"))
        .withColumn("monto_emision_predial_exonerado", try_cast_decimal("mon_emisionpredial_exon"))
        .withColumn("monto_emision_predial_insoluto", try_cast_decimal("mon_emisionpredial_inso"))
        .withColumn("monto_base_imponible_afecto", try_cast_decimal("mon_baseimponible_afecto"))
        .withColumn("monto_base_imponible_exonerado", try_cast_decimal("mon_baseimponible_exon"))
        .withColumn("monto_autoavaluo_inafecto", try_cast_decimal("mon_autoavaluo_inafecto"))
        .withColumn("numero_emision_predial_afecto", try_cast_integer("num_emisionpredial_afecto"))
        .withColumn("numero_emision_predial_exonerado", try_cast_integer("num_emisionpredial_exon"))
        .withColumn("numero_emision_predial_casa_habitacion", try_cast_integer("num_emisionpredial_casa"))
        .withColumn("numero_emision_predial_otros", try_cast_integer("num_emisionpredial_otros"))
        .withColumn("monto_recaudacion_actual_ordinaria", try_cast_decimal("mon_recaudactual_ordin"))
        .withColumn("monto_recaudacion_actual_coactiva", try_cast_decimal("mon_recaudactual_coac"))
        .withColumn("monto_recaudacion_anterior_ordinaria", try_cast_decimal("mon_recaudanter_ordi"))
        .withColumn("monto_recaudacion_anterior_coactiva", try_cast_decimal("mon_recaudanter_coac"))
        .withColumn("monto_saldo_predial_ordinario", try_cast_decimal("mon_saldopredial_ord"))
        .withColumn("monto_saldo_predial_coactivo", try_cast_decimal("mon_saldopredial_coac"))
        .withColumn("numero_inafectos", try_cast_integer("num_inafectos"))
        .withColumn("numero_contribuyentes_predio", try_cast_integer("num_contripredio"))
        .withColumn("numero_predios_uso_casa_habitacion", try_cast_integer("num_prediousoch"))
        .withColumn("numero_predios_otro_uso", try_cast_integer("num_prediootrouso"))
        .withColumn("numero_predios_total", try_cast_integer("num_prediototal"))
        .withColumn("monto_inicial_adulto_mayor", try_cast_decimal("mon_inicialadultomayor"))
        .withColumn("monto_predial_adulto_mayor", try_cast_decimal("mon_predialadultomayor"))
        .withColumn("numero_contribuyentes_adulto_mayor", try_cast_integer("num_contribadultomayor"))
        .withColumn("monto_recaudacion_adulto_mayor", try_cast_decimal("mon_recuadadultomayor"))
        .withColumn("tipo_meta", normalize_string_label("tipo_meta"))
        .withColumn("flag_emision_liquidacion", normalize_string_label("flag_emiliquida"))
        .withColumn("flag_emision_inicial", normalize_string_label("flag_emision_inicial"))
    )

    transformed = add_period_flags(transformed)
    transformed = (
        transformed.withColumn(
            "monto_emision_predial_total",
            safe_sum(
                [
                    "monto_emision_predial_afecto",
                    "monto_emision_predial_exonerado",
                ]
            ),
        )
        .withColumn(
            "monto_recaudacion_predial_total",
            safe_sum(
                [
                    "monto_recaudacion_actual_ordinaria",
                    "monto_recaudacion_actual_coactiva",
                    "monto_recaudacion_anterior_ordinaria",
                    "monto_recaudacion_anterior_coactiva",
                ]
            ),
        )
        .withColumn(
            "monto_saldo_predial_total",
            safe_sum(
                [
                    "monto_saldo_predial_ordinario",
                    "monto_saldo_predial_coactivo",
                ]
            ),
        )
        .withColumn(
            "is_valid_sec_ejec",
            F.coalesce(F.col("sec_ejec").isNotNull(), F.lit(False)),
        )
        .withColumn(
            "is_valid_ubigeo6",
            F.coalesce(F.col("ubigeo6").rlike(r"^[0-9]{6}$"), F.lit(False)),
        )
        .withColumn(
            "is_valid_anio_aplicacion",
            F.coalesce(F.col("anio_aplicacion").between(2010, 2030), F.lit(False)),
        )
        .withColumn(
            "is_valid_periodo",
            F.coalesce(F.col("periodo").isNotNull(), F.lit(False)),
        )
        .withColumn(
            "is_valid_anio_estadistica",
            F.coalesce(F.col("anio_estadistica").between(2010, 2030), F.lit(False)),
        )
        .withColumn(
            "ratio_recaudacion_emision",
            F.when(
                F.col("monto_emision_predial_total").isNull()
                | (F.col("monto_emision_predial_total") <= 0),
                F.lit(None).cast("decimal(20,8)"),
            ).otherwise(
                (F.col("monto_recaudacion_predial_total") / F.col("monto_emision_predial_total")).cast(
                    "decimal(20,8)"
                )
            ),
        )
    )

    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["esat_estadistica_atm"])


def transform_formulario(dataframe: Any) -> Any:
    """Transforma el catalogo de formularios."""

    transformed = (
        dataframe.withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
        .withColumn("periodo", try_cast_integer("periodo"))
        .withColumn("formulario_id", try_cast_integer("formulario_id"))
        .withColumn("orden_formulario", normalize_string_label("orden_formulario"))
        .withColumn("titulo", normalize_string_label("titulo"))
        .withColumn("sub_titulo", normalize_string_label("sub_titulo"))
        .withColumn("abreviatura", normalize_string_label("abreviatura"))
        .withColumn("clasificacion", normalize_string_label("clasificacion"))
        .withColumn("tipo_formulario", normalize_string_label("tipo_formulario"))
        .withColumn("estado_registro", normalize_string_label("estado_registro"))
    )
    transformed = add_is_active_record(transformed)
    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["formulario"])


def transform_preguntas(dataframe: Any) -> Any:
    """Transforma el catalogo de preguntas."""

    transformed = (
        dataframe.withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
        .withColumn("periodo", try_cast_integer("periodo"))
        .withColumn("formulario_id", try_cast_integer("formulario_id"))
        .withColumn("pregunta_id", try_cast_integer("pregunta_id"))
        .withColumn("pregunta_padre_id", try_cast_integer("pregunta_padre_id"))
        .withColumn("orden_pregunta", normalize_string_label("orden_pregunta"))
        .withColumn("descripcion", normalize_string_label("descripcion"))
        .withColumn("objeto_activo", normalize_string_label("objeto_activo"))
        .withColumn("tipo_cuestionario_id", try_cast_integer("tipo_cuestionario_id"))
        .withColumn("respuesta", normalize_string_label("respuesta"))
        .withColumn("rango_ini", try_cast_decimal("rango_ini"))
        .withColumn("rango_fin", try_cast_decimal("rango_fin"))
        .withColumn("texto_apoyo", normalize_string_label("texto_apoyo"))
        .withColumn("texto_lectura", normalize_string_label("texto_lectura"))
        .withColumn("estado_registro", normalize_string_label("estado_registro"))
    )
    transformed = add_is_active_record(transformed)
    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["preguntas"])


def transform_respuestas(dataframe: Any) -> Any:
    """Transforma respuestas manteniendo formato largo."""

    transformed = (
        dataframe.withColumn("sec_ejec", normalize_string_code("sec_ejec"))
        .withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
        .withColumn("periodo", try_cast_integer("periodo"))
        .withColumn("formulario_id", try_cast_integer("formulario_id"))
        .withColumn("pregunta_id", try_cast_integer("pregunta_id"))
        .withColumn("respuesta_id", try_cast_integer("respuesta_id"))
        .withColumn("respuesta_texto", normalize_string_label("respuesta_texto"))
        .withColumn("respuesta_decimal", try_cast_decimal("respuesta_decimal"))
        .withColumn("respuesta_entero", try_cast_integer("respuesta_entero"))
        .withColumn("respuesta_fecha", parse_human_date("respuesta_fecha"))
        .withColumn("estado_registro", normalize_string_label("estado_registro"))
    )
    transformed = add_is_active_record(transformed)
    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["respuestas"])


def transform_estadistica(dataframe: Any) -> Any:
    """Transforma la tabla de control de periodos estadisticos."""

    transformed = (
        dataframe.withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
        .withColumn("periodo", try_cast_integer("periodo"))
        .withColumn("formulario_id", try_cast_integer("formulario_id"))
        .withColumn("anio_estadistica", try_cast_integer("ano_estadistica"))
        .withColumn("mes_estadistica", try_cast_integer("mes_estadistica"))
        .withColumn("estado_registro", normalize_string_label("estado_registro"))
        .withColumn("anio_estadistica_desc", normalize_string_label("ano_estadistica_desc"))
    )
    transformed = add_period_flags(transformed)
    transformed = add_is_active_record(transformed)
    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["estadistica"])


def transform_ano_aplicacion(dataframe: Any) -> Any:
    """Transforma la tabla de control operativo de anios de aplicacion."""

    transformed = (
        dataframe.withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
        .withColumn("anio_aplicacion_inicio", try_cast_integer("ano_aplicacion_inicio"))
        .withColumn("anio_aplicacion_fin", try_cast_integer("ano_aplicacion_fin"))
        .withColumn("fecha_cierre", parse_human_date("fecha_cierre"))
        .withColumn("estado", normalize_string_label("estado"))
        .withColumn("periodo", try_cast_integer("periodo"))
        .withColumn("fecha_pres_oficio", parse_human_date("fecha_pres_oficio"))
        .withColumn("fecha_ini_cierre", parse_human_date("fecha_ini_cierre"))
        .withColumn("fecha_ing", parse_human_date("fecha_ing"))
    )
    transformed = transformed.withColumn(
        "is_active_record",
        transformed["estado"] == "A",
    )
    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["ano_aplicacion"])


def ensure_optional_column(dataframe: Any, source_column: str, target_column: str) -> Any:
    """Crea una columna nula cuando el recurso Bronze no expone el dato esperado."""

    from pyspark.sql import functions as F

    if source_column in dataframe.columns:
        return dataframe.withColumn(target_column, normalize_string_label(source_column))
    return dataframe.withColumn(target_column, F.lit(None).cast("string"))


def transform_entidad_estado(dataframe: Any) -> Any:
    """Transforma la tabla de estado/cobertura de entidades."""

    from pyspark.sql import functions as F

    transformed = dataframe.withColumn("sec_ejec", normalize_string_code("sec_ejec"))
    transformed = ensure_optional_column(transformed, "estado", "estado_registro")
    transformed = transformed.withColumn("anio_aplicacion", try_cast_integer("ano_aplicacion"))
    transformed = transformed.withColumn("periodo", try_cast_integer("periodo"))
    transformed = transformed.withColumn("ubigeo6", F.lit(None).cast("string"))
    transformed = transformed.withColumn("departamento_codigo", F.lit(None).cast("string"))
    transformed = transformed.withColumn("departamento_nombre", F.lit(None).cast("string"))
    transformed = transformed.withColumn("provincia_codigo", F.lit(None).cast("string"))
    transformed = transformed.withColumn("provincia_nombre", F.lit(None).cast("string"))
    transformed = transformed.withColumn("distrito_codigo", F.lit(None).cast("string"))
    transformed = transformed.withColumn("distrito_nombre", F.lit(None).cast("string"))
    transformed = transformed.withColumn("municipalidad_nombre", F.lit(None).cast("string"))
    transformed = add_is_active_record(transformed)
    return transformed.select(*FINAL_COLUMNS_BY_RESOURCE["entidad_estado"])


TRANSFORMERS = {
    "esat_estadistica_atm": transform_esat_estadistica_atm,
    "formulario": transform_formulario,
    "preguntas": transform_preguntas,
    "respuestas": transform_respuestas,
    "estadistica": transform_estadistica,
    "ano_aplicacion": transform_ano_aplicacion,
    "entidad_estado": transform_entidad_estado,
}


def transform_resource_dataframe(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Aplica limpieza, curacion y metadata Silver a un recurso SISMEPRE."""

    require_common_bronze_columns(dataframe.columns, resource)

    transformed = trim_string_columns(dataframe)
    transformer = TRANSFORMERS.get(resource.resource_key)
    if transformer is None:
        raise SilverTransformError(
            f"No existe transformador Silver para el recurso '{resource.resource_key}'."
        )

    transformed = transformer(transformed)
    transformed = add_silver_metadata(
        dataframe=transformed,
        resource=resource,
        processed_at=processed_at,
    )

    final_columns = [
        *FINAL_COLUMNS_BY_RESOURCE[resource.resource_key],
        *COMMON_SILVER_METADATA_COLUMNS,
    ]
    return transformed.select(*final_columns)


def build_dry_run_summary(
    *,
    spark: Any,
    resources: list[SilverResource],
    limit: int | None,
) -> list[dict[str, Any]]:
    """Construye un resumen de dry-run leyendo schema y conteo sin escribir datos."""

    summary: list[dict[str, Any]] = []

    for resource in resources:
        item: dict[str, Any] = {
            "resource_key": resource.resource_key,
            "role": resource.role,
            "priority": resource.priority,
            "bronze_path": str(resource.bronze_path),
            "silver_path": str(resource.silver_path),
            "bronze_exists": resource.bronze_path.exists(),
            "silver_exists": resource.silver_path.exists(),
            "final_columns": FINAL_COLUMNS_BY_RESOURCE.get(resource.resource_key, []),
        }

        if resource.bronze_path.exists():
            try:
                dataframe = spark.read.parquet(str(resource.bronze_path))
                if limit is not None:
                    dataframe = dataframe.limit(limit)
                require_common_bronze_columns(dataframe.columns, resource)
                item["row_count"] = dataframe.count()
                item["column_count"] = len(dataframe.columns)
                item["readable"] = True
            except Exception as exc:  # pragma: no cover
                item["row_count"] = "no evaluado"
                item["column_count"] = "no evaluado"
                item["readable"] = False
                item["read_error"] = str(exc).splitlines()[0]

        summary.append(item)

    return summary


def write_resource_silver(
    *,
    spark: Any,
    resource: SilverResource,
    processed_at: str,
    overwrite: bool,
    limit: int | None,
) -> None:
    """Transforma y escribe un recurso SISMEPRE en Parquet Silver."""

    dataframe = spark.read.parquet(str(resource.bronze_path))
    if limit is not None:
        dataframe = dataframe.limit(limit)

    transformed = transform_resource_dataframe(
        dataframe=dataframe,
        resource=resource,
        processed_at=processed_at,
    )

    write_mode = "overwrite" if overwrite else "errorifexists"
    (
        transformed.write.mode(write_mode)
        .option("compression", "snappy")
        .parquet(str(resource.silver_path))
    )


def transform_sismepre(
    *,
    resources: list[SilverResource],
    dry_run: bool,
    overwrite: bool,
    limit: int | None,
) -> list[dict[str, Any]]:
    """Transforma SISMEPRE hacia Silver o retorna un resumen de dry-run."""

    validate_bronze_inputs(resources)

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    spark = build_spark_session(app_name="SilverSismepre")

    try:
        if dry_run:
            return build_dry_run_summary(spark=spark, resources=resources, limit=limit)

        processed_at = utc_now_iso()
        summary: list[dict[str, Any]] = []

        for resource in resources:
            logger.info(
                "Transformando recurso Silver SISMEPRE %s desde %s",
                resource.resource_key,
                resource.bronze_path,
            )
            write_resource_silver(
                spark=spark,
                resource=resource,
                processed_at=processed_at,
                overwrite=overwrite,
                limit=limit,
            )
            summary.append(
                {
                    "resource_key": resource.resource_key,
                    "bronze_path": str(resource.bronze_path),
                    "silver_path": str(resource.silver_path),
                    "silver_written": True,
                }
            )

        return summary
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa argumentos de linea de comandos."""

    parser = argparse.ArgumentParser(
        description="Transforma SISMEPRE desde Bronze hacia Silver curado."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help="Clave de recurso SISMEPRE a transformar. Puede repetirse.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida recursos, schema y conteos sin escribir Parquet Silver.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe el Parquet Silver de salida si ya existe.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita filas por recurso para pruebas locales.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        raise SilverTransformError("--limit debe ser un entero positivo.")

    source_config = load_sismepre_config()
    resources = select_silver_resources(
        source_config=source_config,
        resource_keys=args.resources,
    )

    summary = transform_sismepre(
        resources=resources,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        limit=args.limit,
    )

    print("=" * 80)
    print("Silver SISMEPRE")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Recursos seleccionados: {len(summary)}")

    for item in summary:
        row_count = item.get("row_count", "n/a")
        column_count = item.get("column_count", "n/a")
        bronze_exists = item.get("bronze_exists", "n/a")
        silver_exists = item.get("silver_exists", "n/a")
        print(
            f"- {item['resource_key']} | filas={row_count} | "
            f"columnas={column_count} | bronze_existe={bronze_exists} | "
            f"silver_existe={silver_exists}"
        )
        print(f"  bronze: {item['bronze_path']}")
        print(f"  silver: {item['silver_path']}")
        if item.get("readable") is False:
            print(f"  lectura Spark: no evaluada ({item.get('read_error')})")

    if args.dry_run:
        print("Dry-run finalizado. No se escribio Parquet ni se toco data/silver.")
    else:
        print("Transformacion Silver SISMEPRE finalizada.")


if __name__ == "__main__":
    main()
