"""Transformacion Silver curada para RENAMU municipal_context.

Este modulo lee el recurso Bronze completo `base_renamu_2022` y construye una
sola salida Silver compacta: `resource_key=municipal_context`.

RENAMU completo permanece en Bronze. Silver solo proyecta columnas curadas y
explicables para integracion posterior; no genera `full_clean` ni replica las
1,300+ columnas del cuestionario.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_sources_config
from src.common.logger import get_logger
from src.common.paths import get_source_bronze_path, get_source_silver_path


SOURCE_NAME = "renamu"
BRONZE_RESOURCE_KEY = "base_renamu_2022"
SILVER_RESOURCE_KEY = "municipal_context"
LEGACY_SILVER_RESOURCE_KEYS = ["base_renamu_2022", "full_clean"]
SOURCE_YEAR = 2022

COMMON_BRONZE_COLUMNS = [
    "bronze_source_name",
    "bronze_resource_key",
    "bronze_source_file_name",
    "bronze_source_file_path",
    "bronze_source_year",
    "bronze_processed_at_utc",
]

REQUIRED_BRONZE_COLUMNS = [
    "ano",
    "idmunici",
    "ccdd",
    "ccpp",
    "ccdi",
    "ubigeo",
    "departamento",
    "provincia",
    "distrito",
    "tipomuni",
    "p13a_1",
    "p13a_2",
    "p13a_3",
    "p13a_4",
    "p13a_5",
    "p13a_6",
    "p13a_7",
    "p13a_8",
    "p13a_9",
    "p14",
    "p14a_1",
    "p14a_2",
    "p16_4",
    "p16_5",
    "p17_7",
    "p17_8",
    "p17_14",
    "p18",
    "p18_portal",
    "p19d_t",
    "p19m_t",
    "p19a",
    "p19a_1_t",
    "p19a_2_t",
    "p20",
    "p20_1_t",
    "p20_2_t",
    "p22_at2",
    "p22_at3",
    "p22_c2",
    "p22_c3",
    "p31_1",
    "p31_2",
    "p31_3",
    "p31_5",
    "p32",
    "p32_1_t",
    "p32_2_t",
    "p33a",
    *COMMON_BRONZE_COLUMNS,
]

FINAL_COLUMNS = [
    "anio",
    "idmunici",
    "ccdd",
    "ccpp",
    "ccdi",
    "ubigeo6",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre",
    "tipomuni_codigo",
    "tipomuni_nombre",
    "total_computadoras_operativas",
    "cuenta_servicio_internet",
    "computadoras_con_acceso_internet",
    "tipo_conexion_internet_codigo",
    "tipo_conexion_internet_nombre",
    "usa_siaf",
    "usa_sistema_recaudacion_tributaria_municipal",
    "usa_sistema_rentas_administracion_tributaria",
    "usa_sistema_catastro",
    "no_tiene_sistemas_gestion",
    "portal_transparencia_estado_codigo",
    "portal_transparencia_estado_nombre",
    "tiene_portal_transparencia",
    "portal_transparencia_actualizado",
    "portal_transparencia_url",
    "total_personal_dic_2021",
    "total_personal_mar_2022",
    "tiene_personal_locacion_servicios",
    "personal_locacion_total_dic_2021",
    "personal_locacion_total_mar_2022",
    "tiene_personal_discapacidad",
    "personal_discapacidad_total_dic_2021",
    "personal_discapacidad_total_mar_2022",
    "acepta_pago_efectivo_ventanilla",
    "acepta_pago_tarjeta_ventanilla",
    "acepta_pago_web_en_linea",
    "acepta_otro_medio_pago",
    "tiene_personal_exclusivo_administracion_tributaria",
    "personal_admin_tributaria_dic_2021",
    "personal_admin_tributaria_mar_2022",
    "tiene_area_ejecucion_coactiva",
    "requiere_asistencia_administracion_tributaria",
    "requiere_asistencia_catastro",
    "requiere_capacitacion_administracion_tributaria",
    "requiere_capacitacion_catastro",
    "silver_source_name",
    "silver_resource_key",
    "silver_processed_at_utc",
]

TIPOMUNI_LABELS = {
    "1": "Provincial",
    "2": "Distrital",
    "3": "Centro Poblado",
}

INTERNET_CONNECTION_LABELS = {
    "1": "Banda ancha inalámbrica (Wi-fi)",
    "2": "Banda ancha móvil",
    "3": "Línea digital (ADSL/DSL)",
    "4": "Satelital",
    "5": "Cable de fibra óptica",
}

PORTAL_STATUS_LABELS = {
    "1": "Sí y está actualizado",
    "2": "No tiene, en proceso de implementación",
    "3": "No tiene, desconoce cómo implementarlo",
    "4": "Sí y está desactualizado",
}


class SilverTransformError(Exception):
    """Error controlado durante la transformacion Silver de RENAMU."""


@dataclass(frozen=True)
class SilverResource:
    """Recurso Bronze y salida Silver objetivo para RENAMU."""

    bronze_resource_key: str
    silver_resource_key: str
    bronze_path: Path
    silver_path: Path
    silver_root: Path


def utc_now_iso() -> str:
    """Retorna la fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_renamu_config() -> dict[str, Any]:
    """Carga la configuracion de la fuente RENAMU."""

    config = load_sources_config()
    source_config = get_config_value(config, f"sources.{SOURCE_NAME}")

    if not isinstance(source_config, dict):
        raise SilverTransformError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    if not source_config.get("enabled", False):
        raise SilverTransformError(f"La fuente '{SOURCE_NAME}' no esta habilitada.")

    return source_config


def build_renamu_resource(
    source_config: dict[str, Any],
    *,
    bronze_dir: Path | None = None,
    silver_dir: Path | None = None,
) -> SilverResource:
    """Construye la definicion del recurso Bronze y la salida Silver curada."""

    bronze_subdir = source_config.get("bronze_subdir", SOURCE_NAME)
    silver_subdir = source_config.get("silver_subdir", SOURCE_NAME)
    resolved_bronze_dir = bronze_dir or get_source_bronze_path(bronze_subdir)
    resolved_silver_dir = silver_dir or get_source_silver_path(silver_subdir)

    return SilverResource(
        bronze_resource_key=BRONZE_RESOURCE_KEY,
        silver_resource_key=SILVER_RESOURCE_KEY,
        bronze_path=resolved_bronze_dir / f"resource_key={BRONZE_RESOURCE_KEY}",
        silver_path=resolved_silver_dir / f"resource_key={SILVER_RESOURCE_KEY}",
        silver_root=resolved_silver_dir,
    )


def validate_bronze_input(resource: SilverResource) -> SilverResource:
    """Valida que exista la ruta Bronze RENAMU."""

    if not resource.bronze_path.exists():
        raise SilverTransformError(
            f"No existe el recurso RENAMU en Bronze: {resource.bronze_path}"
        )

    return resource


def require_bronze_columns(columns: list[str]) -> None:
    """Valida que Bronze tenga el subconjunto minimo requerido."""

    missing_columns = sorted(set(REQUIRED_BRONZE_COLUMNS) - set(columns))

    if missing_columns:
        raise SilverTransformError(
            "El recurso RENAMU no tiene columnas requeridas para municipal_context: "
            f"{missing_columns}."
        )


def trim_string_columns(dataframe: Any) -> Any:
    """Aplica trim a todas las columnas string sin cambiar nombres."""

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


def normalize_string_label(column_name: str) -> Any:
    """Normaliza texto preservando nulos cuando el valor llega vacio."""

    from pyspark.sql import functions as F

    return F.when(
        F.trim(F.col(column_name).cast("string")) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name).cast("string")))


def normalize_string_code(column_name: str, *, width: int | None = None) -> Any:
    """Normaliza codigos como string, preservando ceros a la izquierda."""

    from pyspark.sql import functions as F

    cleaned = normalize_string_label(column_name)
    if width is None:
        return cleaned
    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.rlike(r"^[0-9]+$"), F.lpad(cleaned, width, "0"))
        .otherwise(cleaned)
    )


def try_cast_integer(column_name: str) -> Any:
    """Castea una columna a entero tolerando vacios o valores invalidos."""

    from pyspark.sql import functions as F

    return F.expr(f"try_cast(nullif(trim(`{column_name}`), '') as int)")


def integer_with_zero_default(column_name: str) -> Any:
    """Convierte a entero usando cero cuando el campo viene vacio o invalido.

    Se usa solo en la suma de computadoras operativas, donde un valor vacio o
    raro se interpreta como ausencia de equipos en esa subcategoria.
    """

    from pyspark.sql import functions as F

    return F.coalesce(try_cast_integer(column_name), F.lit(0))


def map_static_label(column_name: str, mapping: dict[str, str]) -> Any:
    """Mapea un codigo string a su etiqueta explicable."""

    from pyspark.sql import functions as F

    expression = None
    for key, value in mapping.items():
        clause = F.when(F.col(column_name) == F.lit(key), F.lit(value))
        expression = clause if expression is None else expression.when(
            F.col(column_name) == F.lit(key), F.lit(value)
        )

    if expression is None:
        return F.lit(None)

    return expression.otherwise(F.lit(None))


def map_binary_choice(column_name: str, *, true_value: str, false_value: str) -> Any:
    """Mapea preguntas binarias con codigos explicitos.

    Devuelve `null` para valores distintos, porque en RENAMU existen respuestas
    fuera del binario esperado y no deben forzarse a falso.
    """

    from pyspark.sql import functions as F

    return (
        F.when(F.col(column_name) == F.lit(true_value), F.lit(True))
        .when(F.col(column_name) == F.lit(false_value), F.lit(False))
        .otherwise(F.lit(None))
    )


def map_multiselect_code(column_name: str, *, true_code: str) -> Any:
    """Mapea multiseleccion codificada.

    Estas preguntas guardan el codigo de la alternativa elegida y `0` cuando la
    opcion no aplica. No se usa una regla generica `1 = sí`.
    """

    from pyspark.sql import functions as F

    return (
        F.when(F.col(column_name) == F.lit(true_code), F.lit(True))
        .when(F.col(column_name) == F.lit("0"), F.lit(False))
        .otherwise(F.lit(None))
    )


def cleanup_legacy_outputs(resource: SilverResource) -> list[str]:
    """Elimina salidas Silver heredadas que ya no pertenecen al contrato final."""

    removed_paths: list[str] = []

    for legacy_key in LEGACY_SILVER_RESOURCE_KEYS:
        legacy_path = resource.silver_root / f"resource_key={legacy_key}"
        if legacy_path.exists():
            shutil.rmtree(legacy_path)
            removed_paths.append(str(legacy_path))

    return removed_paths


def transform_resource_dataframe(
    *,
    dataframe: Any,
    resource: SilverResource,
    processed_at: str,
) -> Any:
    """Construye la salida curada municipal_context."""

    from pyspark.sql import functions as F

    require_bronze_columns(dataframe.columns)

    transformed = trim_string_columns(dataframe)

    transformed = (
        transformed.withColumn("anio", try_cast_integer("ano"))
        .withColumn("idmunici", normalize_string_code("idmunici"))
        .withColumn("ccdd", normalize_string_code("ccdd", width=2))
        .withColumn("ccpp", normalize_string_code("ccpp", width=2))
        .withColumn("ccdi", normalize_string_code("ccdi", width=2))
        .withColumn("ubigeo6", normalize_string_code("ubigeo", width=6))
        .withColumn("departamento_nombre", normalize_string_label("departamento"))
        .withColumn("provincia_nombre", normalize_string_label("provincia"))
        .withColumn("distrito_nombre", normalize_string_label("distrito"))
        .withColumn("tipomuni_codigo", normalize_string_code("tipomuni"))
        .withColumn(
            "tipomuni_nombre",
            map_static_label("tipomuni_codigo", TIPOMUNI_LABELS),
        )
        .withColumn(
            "total_computadoras_operativas",
            (
                integer_with_zero_default("p13a_1")
                + integer_with_zero_default("p13a_2")
                + integer_with_zero_default("p13a_3")
                + integer_with_zero_default("p13a_4")
                + integer_with_zero_default("p13a_5")
                + integer_with_zero_default("p13a_6")
                + integer_with_zero_default("p13a_7")
                + integer_with_zero_default("p13a_8")
                + integer_with_zero_default("p13a_9")
            ).cast("int"),
        )
        .withColumn(
            "cuenta_servicio_internet",
            map_binary_choice("p14", true_value="1", false_value="2"),
        )
        .withColumn(
            "computadoras_con_acceso_internet",
            try_cast_integer("p14a_1"),
        )
        .withColumn(
            "tipo_conexion_internet_codigo",
            normalize_string_code("p14a_2"),
        )
        .withColumn(
            "tipo_conexion_internet_nombre",
            map_static_label(
                "tipo_conexion_internet_codigo",
                INTERNET_CONNECTION_LABELS,
            ),
        )
        .withColumn("usa_siaf", map_multiselect_code("p16_4", true_code="4"))
        .withColumn(
            "usa_sistema_recaudacion_tributaria_municipal",
            map_multiselect_code("p16_5", true_code="5"),
        )
        .withColumn(
            "usa_sistema_rentas_administracion_tributaria",
            map_multiselect_code("p17_7", true_code="7"),
        )
        .withColumn(
            "usa_sistema_catastro",
            map_multiselect_code("p17_8", true_code="8"),
        )
        .withColumn(
            "no_tiene_sistemas_gestion",
            map_multiselect_code("p17_14", true_code="14"),
        )
        .withColumn(
            "portal_transparencia_estado_codigo",
            normalize_string_code("p18"),
        )
        .withColumn(
            "portal_transparencia_estado_nombre",
            map_static_label(
                "portal_transparencia_estado_codigo",
                PORTAL_STATUS_LABELS,
            ),
        )
        .withColumn(
            "tiene_portal_transparencia",
            F.when(F.col("portal_transparencia_estado_codigo").isin("1", "4"), F.lit(True))
            .when(F.col("portal_transparencia_estado_codigo").isin("2", "3"), F.lit(False))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "portal_transparencia_actualizado",
            F.when(F.col("portal_transparencia_estado_codigo") == "1", F.lit(True))
            .when(F.col("portal_transparencia_estado_codigo").isin("2", "3", "4"), F.lit(False))
            .otherwise(F.lit(None)),
        )
        .withColumn("portal_transparencia_url", normalize_string_label("p18_portal"))
        .withColumn("total_personal_dic_2021", try_cast_integer("p19d_t"))
        .withColumn("total_personal_mar_2022", try_cast_integer("p19m_t"))
        .withColumn(
            "tiene_personal_locacion_servicios",
            map_binary_choice("p19a", true_value="1", false_value="2"),
        )
        .withColumn("personal_locacion_total_dic_2021", try_cast_integer("p19a_1_t"))
        .withColumn("personal_locacion_total_mar_2022", try_cast_integer("p19a_2_t"))
        .withColumn(
            "tiene_personal_discapacidad",
            map_binary_choice("p20", true_value="1", false_value="2"),
        )
        .withColumn("personal_discapacidad_total_dic_2021", try_cast_integer("p20_1_t"))
        .withColumn("personal_discapacidad_total_mar_2022", try_cast_integer("p20_2_t"))
        # En P31 el propio codigo de la alternativa indica seleccion.
        .withColumn(
            "acepta_pago_efectivo_ventanilla",
            map_multiselect_code("p31_1", true_code="1"),
        )
        .withColumn(
            "acepta_pago_tarjeta_ventanilla",
            map_multiselect_code("p31_2", true_code="2"),
        )
        .withColumn(
            "acepta_pago_web_en_linea",
            map_multiselect_code("p31_3", true_code="3"),
        )
        .withColumn(
            "acepta_otro_medio_pago",
            map_multiselect_code("p31_5", true_code="5"),
        )
        .withColumn(
            "tiene_personal_exclusivo_administracion_tributaria",
            map_binary_choice("p32", true_value="1", false_value="2"),
        )
        .withColumn("personal_admin_tributaria_dic_2021", try_cast_integer("p32_1_t"))
        .withColumn("personal_admin_tributaria_mar_2022", try_cast_integer("p32_2_t"))
        .withColumn(
            "tiene_area_ejecucion_coactiva",
            map_binary_choice("p33a", true_value="1", false_value="2"),
        )
        .withColumn(
            "requiere_asistencia_administracion_tributaria",
            map_multiselect_code("p22_at2", true_code="2"),
        )
        .withColumn(
            "requiere_asistencia_catastro",
            map_multiselect_code("p22_at3", true_code="3"),
        )
        .withColumn(
            "requiere_capacitacion_administracion_tributaria",
            map_multiselect_code("p22_c2", true_code="2"),
        )
        .withColumn(
            "requiere_capacitacion_catastro",
            map_multiselect_code("p22_c3", true_code="3"),
        )
        .withColumn("silver_source_name", F.lit(SOURCE_NAME))
        .withColumn("silver_resource_key", F.lit(resource.silver_resource_key))
        .withColumn("silver_processed_at_utc", F.lit(processed_at))
    )

    return transformed.select(*FINAL_COLUMNS)


def build_dry_run_summary(
    *,
    spark: Any,
    resource: SilverResource,
    limit: int | None,
) -> dict[str, Any]:
    """Construye resumen de dry-run sin escribir datos."""

    summary: dict[str, Any] = {
        "bronze_resource_key": resource.bronze_resource_key,
        "silver_resource_key": resource.silver_resource_key,
        "bronze_path": str(resource.bronze_path),
        "silver_path": str(resource.silver_path),
        "bronze_exists": resource.bronze_path.exists(),
        "silver_exists": resource.silver_path.exists(),
        "legacy_silver_paths": [
            str(resource.silver_root / f"resource_key={legacy_key}")
            for legacy_key in LEGACY_SILVER_RESOURCE_KEYS
            if (resource.silver_root / f"resource_key={legacy_key}").exists()
        ],
    }

    if resource.bronze_path.exists():
        dataframe = spark.read.parquet(str(resource.bronze_path))
        if limit is not None:
            dataframe = dataframe.limit(limit)
        require_bronze_columns(dataframe.columns)
        summary.update(
            {
                "row_count": dataframe.count(),
                "column_count": len(dataframe.columns),
                "final_column_count": len(FINAL_COLUMNS),
            }
        )

    return summary


def write_resource_silver(
    *,
    spark: Any,
    resource: SilverResource,
    processed_at: str,
    overwrite: bool,
    limit: int | None,
) -> list[str]:
    """Transforma y escribe municipal_context."""

    if overwrite:
        removed_paths = cleanup_legacy_outputs(resource)
    else:
        removed_paths = []

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

    return removed_paths


def transform_renamu(
    *,
    resource: SilverResource,
    dry_run: bool,
    overwrite: bool,
    limit: int | None,
) -> dict[str, Any]:
    """Transforma RENAMU hacia municipal_context o retorna dry-run."""

    validate_bronze_input(resource)

    from src.common.spark_session import build_spark_session

    logger = get_logger(__name__)
    spark = build_spark_session(app_name="SilverRenamuMunicipalContext")

    try:
        if dry_run:
            return build_dry_run_summary(spark=spark, resource=resource, limit=limit)

        processed_at = utc_now_iso()
        logger.info(
            "Transformando recurso Silver RENAMU %s hacia %s",
            resource.bronze_resource_key,
            resource.silver_path,
        )
        removed_paths = write_resource_silver(
            spark=spark,
            resource=resource,
            processed_at=processed_at,
            overwrite=overwrite,
            limit=limit,
        )
        return {
            "bronze_resource_key": resource.bronze_resource_key,
            "silver_resource_key": resource.silver_resource_key,
            "bronze_path": str(resource.bronze_path),
            "silver_path": str(resource.silver_path),
            "silver_written": True,
            "removed_legacy_paths": removed_paths,
        }
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa argumentos de linea de comandos."""

    parser = argparse.ArgumentParser(
        description="Transforma RENAMU Bronze a Silver curado municipal_context."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida recurso, schema y conteo sin escribir Parquet Silver.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe la salida Silver y limpia rutas heredadas si existen.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limita filas para pruebas locales.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        raise SilverTransformError("--limit debe ser un entero positivo.")

    source_config = load_renamu_config()
    resource = build_renamu_resource(source_config)

    summary = transform_renamu(
        resource=resource,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        limit=args.limit,
    )

    print("=" * 80)
    print("Silver RENAMU municipal_context")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Bronze resource_key: {summary['bronze_resource_key']}")
    print(f"Silver resource_key: {summary['silver_resource_key']}")
    print(f"Bronze existe: {summary.get('bronze_exists', 'n/a')}")
    print(f"Silver existe: {summary.get('silver_exists', 'n/a')}")
    print(f"Filas Bronze evaluadas: {summary.get('row_count', 'n/a')}")
    print(f"Columnas Bronze: {summary.get('column_count', 'n/a')}")
    print(f"Columnas Silver finales: {summary.get('final_column_count', len(FINAL_COLUMNS))}")
    print(f"Bronze: {summary['bronze_path']}")
    print(f"Silver: {summary['silver_path']}")

    legacy_paths = summary.get("legacy_silver_paths") or summary.get("removed_legacy_paths", [])
    if legacy_paths:
        print("Rutas Silver heredadas detectadas/limpiadas:")
        for path in legacy_paths:
            print(f"  - {path}")

    if args.dry_run:
        print("Dry-run finalizado. No se escribio Parquet ni se toco data/silver.")
    else:
        print("Transformacion Silver RENAMU municipal_context finalizada.")


if __name__ == "__main__":
    main()
