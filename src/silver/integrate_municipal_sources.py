"""Integración Silver controlada de fuentes municipales.

Este módulo prepara datasets integrables a partir de Silver MEF, Predial y
RENAMU. No construye Gold, no calcula KPIs finales y no hace joins fila-a-fila
entre fuentes con granularidades incompatibles.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.common.logger import get_logger
from src.common.paths import SILVER_DIR, get_source_silver_path


MEF_SOURCE = "siaf_income"
PREDIAL_SOURCE = "sismepre"
RENAMU_SOURCE = "renamu"
RENAMU_RESOURCE_KEY = "base_renamu_2022"

INTEGRATED_DATASETS = [
    "municipal_entity_bridge",
    "mef_municipal_amounts",
    "predial_entity_period",
    "renamu_municipal_context",
    "integration_coverage",
]

MEF_GROUP_COLUMNS = [
    "source_dataset",
    "silver_source_granularity",
    "anio",
    "mes",
    "nivel_gobierno",
    "sector",
    "pliego",
    "sec_ejec",
    "ejecutora",
    "fuente_financiamiento",
    "rubro",
    "tipo_recurso",
    "generica",
    "subgenerica",
    "subgenerica_det",
    "especifica",
    "especifica_det",
]

PREDIAL_ENTITY_GRAIN = [
    "ano_aplicacion",
    "periodo",
    "sec_ejec",
    "ubigeo",
    "formulario_id",
    "ano_estadistica",
    "mes_estadistica",
]


class SilverIntegrationError(Exception):
    """Error controlado durante la integración Silver."""


@dataclass(frozen=True)
class IntegrationPaths:
    """Rutas de entrada y salida para integración Silver."""

    mef_path: Path
    predial_path: Path
    renamu_path: Path
    output_root: Path


def utc_now_iso() -> str:
    """Retorna fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def existing_columns(columns: Iterable[str], desired_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas deseadas que existen en el dataset."""

    available = set(columns)
    return [column for column in desired_columns if column in available]


def missing_required_columns(
    columns: Iterable[str],
    required_columns: Iterable[str],
) -> list[str]:
    """Devuelve columnas requeridas faltantes."""

    available = set(columns)
    return [column for column in required_columns if column not in available]


def calculate_coverage_percentage(numerator: int, denominator: int) -> float:
    """Calcula porcentaje de cobertura evitando división entre cero."""

    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 4)


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta de salida de un dataset integrado."""

    if dataset_name not in INTEGRATED_DATASETS:
        raise SilverIntegrationError(f"Dataset integrado no soportado: {dataset_name}")
    return output_root / dataset_name


def decimal_columns_by_prefix(columns: Iterable[str], prefixes: Iterable[str]) -> list[str]:
    """Selecciona columnas decimales por prefijo técnico."""

    prefix_tuple = tuple(prefixes)
    return [
        column
        for column in columns
        if column.startswith(prefix_tuple) and column.endswith("_decimal")
    ]


def normalize_metric_row(
    metric_name: str,
    numerator: int,
    denominator: int,
    description: str,
) -> dict[str, Any]:
    """Construye una fila de cobertura serializable."""

    return {
        "metric_name": metric_name,
        "numerator": int(numerator),
        "denominator": int(denominator),
        "coverage_percentage": calculate_coverage_percentage(numerator, denominator),
        "description": description,
    }


def resolve_paths(output_subdir: str) -> IntegrationPaths:
    """Resuelve rutas Silver de entrada y salida."""

    return IntegrationPaths(
        mef_path=get_source_silver_path(MEF_SOURCE),
        predial_path=get_source_silver_path(PREDIAL_SOURCE),
        renamu_path=get_source_silver_path(RENAMU_SOURCE)
        / f"resource_key={RENAMU_RESOURCE_KEY}",
        output_root=SILVER_DIR / output_subdir,
    )


def validate_input_paths(paths: IntegrationPaths) -> None:
    """Valida que existan las rutas Silver necesarias."""

    required_paths = [paths.mef_path, paths.predial_path, paths.renamu_path]
    missing_paths = [str(path) for path in required_paths if not path.exists()]
    if missing_paths:
        raise SilverIntegrationError(
            "Faltan rutas Silver requeridas para integración: "
            + ", ".join(missing_paths)
        )


def resource_path(source_path: Path, resource_key: str) -> Path:
    """Construye ruta de un recurso Silver por resource_key."""

    return source_path / f"resource_key={resource_key}"


def list_resource_paths(source_path: Path) -> list[Path]:
    """Lista carpetas resource_key existentes bajo una fuente Silver."""

    return sorted(
        path
        for path in source_path.glob("resource_key=*")
        if path.is_dir()
    )


def read_parquet(spark: Any, path: Path, limit: int | None = None) -> Any:
    """Lee Parquet con límite opcional para pruebas."""

    dataframe = spark.read.parquet(str(path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def nonblank(column_name: str) -> Any:
    """Expresión Spark para texto no vacío."""

    from pyspark.sql import functions as F

    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def column_or_null(columns: list[str], column_name: str) -> Any:
    """Retorna columna Spark si existe o literal nulo."""

    from pyspark.sql import functions as F

    if column_name in columns:
        return F.col(column_name)
    return F.lit(None)


def first_existing_column(columns: list[str], candidates: list[str]) -> Any:
    """Retorna la primera columna existente entre candidatas o nulo."""

    from pyspark.sql import functions as F

    existing = [F.col(column) for column in candidates if column in columns]
    if not existing:
        return F.lit(None)
    return F.coalesce(*existing)


def add_integration_metadata(dataframe: Any, dataset_name: str, grain: str) -> Any:
    """Agrega metadata técnica de integración Silver."""

    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("silver_integration_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("integration_grain", F.lit(grain))
        .withColumn("source_dataset", F.lit(dataset_name))
    )


def build_renamu_context(spark: Any, paths: IntegrationPaths, limit: int | None) -> Any:
    """Prepara contexto municipal RENAMU con columnas territoriales mínimas."""

    from pyspark.sql import functions as F

    renamu = read_parquet(spark, paths.renamu_path, limit)
    columns = renamu.columns
    base_columns = [
        "anio",
        "idmunici",
        "ubigeo",
        "ccdd",
        "ccpp",
        "ccdi",
        "departamento",
        "provincia",
        "distrito",
        "departamento_normalizado",
        "provincia_normalizada",
        "distrito_normalizado",
        "tipomuni",
        "tipomuni_int",
        "is_valid_anio",
        "is_valid_ubigeo",
        "is_valid_ccdd",
        "is_valid_ccpp",
        "is_valid_ccdi",
        "has_complete_territory",
        "has_municipal_identifier",
        "is_valid_tipomuni",
    ]
    financial_columns = decimal_columns_by_prefix(columns, ["c96", "c97"])
    selected = existing_columns(columns, [*base_columns, *financial_columns])

    return (
        renamu.select(*selected)
        .dropDuplicates(["ubigeo"])
        .withColumn("silver_integration_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("integration_grain", F.lit("ubigeo"))
        .withColumn("source_dataset", F.lit("renamu/base_renamu_2022"))
    )


def select_predial_mapping(dataframe: Any, mapping_source: str) -> Any:
    """Selecciona columnas de mapeo territorial desde un recurso predial."""

    from pyspark.sql import functions as F

    columns = dataframe.columns
    return dataframe.select(
        column_or_null(columns, "sec_ejec").cast("string").alias("sec_ejec"),
        column_or_null(columns, "ubigeo").cast("string").alias("ubigeo"),
        first_existing_column(columns, ["departamento", "departamento_nombre"])
        .cast("string")
        .alias("departamento"),
        first_existing_column(columns, ["provincia", "provincia_nombre"])
        .cast("string")
        .alias("provincia"),
        first_existing_column(columns, ["distrito", "distrito_nombre"])
        .cast("string")
        .alias("distrito"),
        column_or_null(columns, "departamento_nombre")
        .cast("string")
        .alias("departamento_nombre"),
        column_or_null(columns, "provincia_nombre")
        .cast("string")
        .alias("provincia_nombre"),
        column_or_null(columns, "distrito_nombre")
        .cast("string")
        .alias("distrito_nombre"),
        column_or_null(columns, "municipalidad_nombre")
        .cast("string")
        .alias("municipalidad_nombre"),
        F.lit(mapping_source).alias("mapping_source"),
    )


def build_municipal_entity_bridge(
    spark: Any,
    paths: IntegrationPaths,
    renamu_context: Any,
    limit: int | None,
) -> Any:
    """Construye puente sec_ejec -> ubigeo sin asumir equivalencia directa."""

    from pyspark.sql import functions as F

    mapping_frames = []
    for resource_key in ["entidad_estado", "esat_estadistica_atm"]:
        path = resource_path(paths.predial_path, resource_key)
        if path.exists():
            mapping_frames.append(
                select_predial_mapping(
                    read_parquet(spark, path, limit),
                    mapping_source=f"sismepre/{resource_key}",
                )
            )

    if not mapping_frames:
        raise SilverIntegrationError(
            "No se encontraron recursos prediales para construir el puente."
        )

    bridge = mapping_frames[0]
    for frame in mapping_frames[1:]:
        bridge = bridge.unionByName(frame, allowMissingColumns=True)

    bridge = (
        bridge.where(nonblank("sec_ejec") | nonblank("ubigeo"))
        .dropDuplicates(
            [
                "sec_ejec",
                "ubigeo",
                "departamento",
                "provincia",
                "distrito",
                "mapping_source",
            ]
        )
        .withColumn("is_valid_sec_ejec", nonblank("sec_ejec"))
        .withColumn("is_valid_ubigeo", F.col("ubigeo").rlike(r"^[0-9]{6}$"))
    )

    renamu_keys = renamu_context.select("ubigeo").dropDuplicates()
    return (
        bridge.join(
            renamu_keys.withColumn("has_renamu_match", F.lit(True)),
            on="ubigeo",
            how="left",
        )
        .withColumn(
            "has_renamu_match",
            F.coalesce(F.col("has_renamu_match"), F.lit(False)),
        )
        .withColumn("silver_integration_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("integration_grain", F.lit("sec_ejec_ubigeo_mapping"))
    )


def read_mef_resources(spark: Any, paths: IntegrationPaths, limit: int | None) -> Any:
    """Lee y une recursos MEF Silver con esquema compatible."""

    from pyspark.sql import functions as F

    frames = []
    for path in list_resource_paths(paths.mef_path):
        resource_key = path.name.replace("resource_key=", "")
        dataframe = read_parquet(spark, path, limit)
        frames.append(dataframe.withColumn("source_dataset", F.lit(resource_key)))

    if not frames:
        raise SilverIntegrationError("No se encontraron recursos MEF Silver.")

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def build_mef_municipal_amounts(
    spark: Any,
    paths: IntegrationPaths,
    limit: int | None,
) -> Any:
    """Agrega MEF por granularidad presupuestal controlada.

    La suma es deliberada: los duplicados por llave candidata no eran exactos y
    reflejan granularidad presupuestal más fina o cambios de atributos. No se
    deduplican filas a ciegas.
    """

    from pyspark.sql import functions as F

    mef = read_mef_resources(spark, paths, limit)
    required = ["anio", "mes", "sec_ejec", "monto_pia_decimal", "monto_pim_decimal", "monto_recaudado_decimal"]
    missing = missing_required_columns(mef.columns, required)
    if missing:
        raise SilverIntegrationError(f"MEF Silver no tiene columnas requeridas: {missing}")

    group_columns = existing_columns(mef.columns, MEF_GROUP_COLUMNS)
    return (
        mef.groupBy(*group_columns)
        .agg(
            F.sum("monto_pia_decimal").alias("monto_pia_total"),
            F.sum("monto_pim_decimal").alias("monto_pim_total"),
            F.sum("monto_recaudado_decimal").alias("monto_recaudado_total"),
            F.count(F.lit(1)).alias("source_record_count"),
        )
        .withColumn("silver_integration_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn(
            "integration_grain",
            F.lit("source_dataset_anio_mes_sec_ejec_budget_classifier"),
        )
    )


def build_active_responses_summary(
    spark: Any,
    paths: IntegrationPaths,
    limit: int | None,
) -> Any | None:
    """Resume respuestas prediales activas sin usarlas como tabla final cruda."""

    from pyspark.sql import functions as F

    path = resource_path(paths.predial_path, "respuestas")
    if not path.exists():
        return None

    respuestas = read_parquet(spark, path, limit)
    keys = ["ano_aplicacion", "periodo", "sec_ejec", "formulario_id"]
    if missing_required_columns(respuestas.columns, keys):
        return None

    if "estado_registro" in respuestas.columns:
        respuestas = respuestas.where(F.col("estado_registro") == F.lit("A"))

    return respuestas.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("active_response_count")
    )


def build_predial_entity_period(
    spark: Any,
    paths: IntegrationPaths,
    limit: int | None,
) -> Any:
    """Agrega Predial preservando granularidad de entidad, periodo y formulario."""

    from pyspark.sql import functions as F

    path = resource_path(paths.predial_path, "esat_estadistica_atm")
    if not path.exists():
        raise SilverIntegrationError("No existe esat_estadistica_atm en Silver Predial.")

    esat = read_parquet(spark, path, limit)
    grain = existing_columns(esat.columns, PREDIAL_ENTITY_GRAIN)
    missing = missing_required_columns(esat.columns, ["ano_aplicacion", "periodo", "sec_ejec"])
    if missing:
        raise SilverIntegrationError(
            f"Predial esat_estadistica_atm no tiene columnas requeridas: {missing}"
        )

    numeric_columns = decimal_columns_by_prefix(esat.columns, ["mon_", "num_"])
    aggregations = [F.sum(column).alias(f"{column}_total") for column in numeric_columns]
    aggregations.append(F.count(F.lit(1)).alias("source_record_count"))

    predial = esat.groupBy(*grain).agg(*aggregations)
    active_responses = build_active_responses_summary(spark, paths, limit)
    if active_responses is not None:
        join_keys = existing_columns(predial.columns, ["ano_aplicacion", "periodo", "sec_ejec", "formulario_id"])
        predial = predial.join(active_responses, on=join_keys, how="left").fillna(
            {"active_response_count": 0}
        )

    return (
        predial.withColumn("silver_integration_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn(
            "integration_grain",
            F.lit("ano_aplicacion_periodo_sec_ejec_formulario_ano_mes_estadistica"),
        )
        .withColumn("source_dataset", F.lit("sismepre/esat_estadistica_atm"))
    )


def metric_count(dataframe: Any) -> int:
    """Cuenta filas de un DataFrame Spark como entero."""

    return int(dataframe.count())


def build_integration_coverage(
    spark: Any,
    bridge: Any,
    mef_amounts: Any,
    renamu_context: Any,
) -> Any:
    """Construye métricas de cobertura de cruce."""

    from pyspark.sql import functions as F

    predial_entities = bridge.where(nonblank("sec_ejec")).select("sec_ejec").distinct()
    valid_ubigeo_entities = (
        bridge.where(nonblank("sec_ejec") & F.col("is_valid_ubigeo"))
        .select("sec_ejec")
        .distinct()
    )
    renamu_matched_entities = (
        bridge.where(nonblank("sec_ejec") & F.col("has_renamu_match"))
        .select("sec_ejec")
        .distinct()
    )
    mef_entities = mef_amounts.where(nonblank("sec_ejec")).select("sec_ejec").distinct()
    bridge_entities = bridge.where(nonblank("sec_ejec")).select("sec_ejec").distinct()
    mef_with_bridge = mef_entities.join(bridge_entities, on="sec_ejec", how="inner")
    mef_without_bridge = mef_entities.join(bridge_entities, on="sec_ejec", how="left_anti")
    renamu_ubigeos = renamu_context.where(nonblank("ubigeo")).select("ubigeo").distinct()
    predial_ubigeos = bridge.where(F.col("is_valid_ubigeo")).select("ubigeo").distinct()
    renamu_without_predial = renamu_ubigeos.join(
        predial_ubigeos,
        on="ubigeo",
        how="left_anti",
    )

    total_predial_entities = metric_count(predial_entities)
    total_mef_entities = metric_count(mef_entities)
    total_renamu_ubigeos = metric_count(renamu_ubigeos)

    rows = [
        normalize_metric_row(
            "total_predial_sec_ejec_entities",
            total_predial_entities,
            total_predial_entities,
            "Entidades prediales con sec_ejec en el puente.",
        ),
        normalize_metric_row(
            "predial_entities_with_valid_ubigeo",
            metric_count(valid_ubigeo_entities),
            total_predial_entities,
            "Entidades prediales con ubigeo válido.",
        ),
        normalize_metric_row(
            "predial_entities_with_renamu_match",
            metric_count(renamu_matched_entities),
            total_predial_entities,
            "Entidades prediales que cruzan con RENAMU por ubigeo.",
        ),
        normalize_metric_row(
            "mef_sec_ejec_with_bridge",
            metric_count(mef_with_bridge),
            total_mef_entities,
            "Sec_ejec MEF que cruza con el puente municipal.",
        ),
        normalize_metric_row(
            "mef_sec_ejec_without_bridge",
            metric_count(mef_without_bridge),
            total_mef_entities,
            "Sec_ejec MEF sin correspondencia en el puente municipal.",
        ),
        normalize_metric_row(
            "renamu_ubigeos_without_predial",
            metric_count(renamu_without_predial),
            total_renamu_ubigeos,
            "Ubigeos RENAMU sin presencia en el puente predial.",
        ),
    ]

    return spark.createDataFrame(rows).withColumn(
        "silver_integration_processed_at_utc",
        F.lit(utc_now_iso()),
    )


def write_dataset(dataframe: Any, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset integrado en Parquet."""

    mode = "overwrite" if overwrite else "errorifexists"
    dataframe.write.mode(mode).option("compression", "snappy").parquet(str(output_path))


def selected_dataset_names(selected_sources: list[str] | None) -> list[str]:
    """Resuelve datasets integrados seleccionados por CLI."""

    if not selected_sources:
        return INTEGRATED_DATASETS

    invalid = sorted(set(selected_sources) - set(INTEGRATED_DATASETS))
    if invalid:
        raise SilverIntegrationError(
            f"Datasets integrados no soportados: {invalid}. "
            f"Disponibles: {INTEGRATED_DATASETS}."
        )
    return selected_sources


def print_dry_run(paths: IntegrationPaths, datasets: list[str]) -> None:
    """Imprime plan de integración sin escribir Parquet."""

    print("=" * 80)
    print("Plan de integración Silver municipal")
    print(f"MEF Silver: {paths.mef_path} | existe={paths.mef_path.exists()}")
    print(f"Predial Silver: {paths.predial_path} | existe={paths.predial_path.exists()}")
    print(f"RENAMU Silver: {paths.renamu_path} | existe={paths.renamu_path.exists()}")
    print(f"Salida integrada: {paths.output_root}")
    print("Datasets a crear:")
    for dataset_name in datasets:
        print(f"- {dataset_name}: {output_dataset_path(paths.output_root, dataset_name)}")
    print("Dry-run finalizado. No se escribió data/silver/integrated.")


def build_dry_run_schema_summary(spark: Any, paths: IntegrationPaths) -> None:
    """Muestra columnas clave disponibles sin ejecutar integración real."""

    inputs = {
        "predial/entidad_estado": resource_path(paths.predial_path, "entidad_estado"),
        "predial/esat_estadistica_atm": resource_path(
            paths.predial_path,
            "esat_estadistica_atm",
        ),
        "predial/respuestas": resource_path(paths.predial_path, "respuestas"),
        "renamu/base_renamu_2022": paths.renamu_path,
    }
    for label, path in inputs.items():
        if not path.exists():
            print(f"- {label}: ruta no existe")
            continue
        columns = spark.read.parquet(str(path)).columns
        keys = [
            column
            for column in [
                "sec_ejec",
                "ubigeo",
                "ano_aplicacion",
                "periodo",
                "formulario_id",
                "ano_estadistica",
                "mes_estadistica",
                "estado_registro",
                "idmunici",
                "tipomuni",
            ]
            if column in columns
        ]
        print(f"- {label}: columnas clave disponibles={keys}")


def run_integration(
    *,
    dry_run: bool,
    overwrite: bool,
    selected_sources: list[str] | None = None,
    limit: int | None = None,
    output_subdir: str = "integrated",
) -> dict[str, Any]:
    """Ejecuta o planifica integración Silver municipal."""

    from src.common.spark_session import build_spark_session

    paths = resolve_paths(output_subdir)
    validate_input_paths(paths)
    datasets = selected_dataset_names(selected_sources)

    spark = build_spark_session(
        app_name="SilverMunicipalIntegration",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "4"},
    )

    try:
        if dry_run:
            print_dry_run(paths, datasets)
            build_dry_run_schema_summary(spark, paths)
            return {"datasets": datasets, "output_root": str(paths.output_root)}

        logger = get_logger(__name__)
        outputs: dict[str, Any] = {}
        renamu_context = build_renamu_context(spark, paths, limit)
        bridge = build_municipal_entity_bridge(spark, paths, renamu_context, limit)
        mef_amounts = build_mef_municipal_amounts(spark, paths, limit)
        predial_period = build_predial_entity_period(spark, paths, limit)
        coverage = build_integration_coverage(
            spark,
            bridge=bridge,
            mef_amounts=mef_amounts,
            renamu_context=renamu_context,
        )

        built = {
            "municipal_entity_bridge": bridge,
            "mef_municipal_amounts": mef_amounts,
            "predial_entity_period": predial_period,
            "renamu_municipal_context": renamu_context,
            "integration_coverage": coverage,
        }

        for dataset_name in datasets:
            output_path = output_dataset_path(paths.output_root, dataset_name)
            logger.info("Escribiendo dataset integrado %s en %s", dataset_name, output_path)
            write_dataset(built[dataset_name], output_path, overwrite=overwrite)
            outputs[dataset_name] = str(output_path)

        print("=" * 80)
        print("Integración Silver municipal finalizada")
        for dataset_name, output_path in outputs.items():
            print(f"- {dataset_name}: {output_path}")

        coverage_rows = [
            row.asDict()
            for row in coverage.select(
                "metric_name",
                "numerator",
                "denominator",
                "coverage_percentage",
            ).collect()
        ]
        print("Cobertura de integración:")
        for row in coverage_rows:
            print(
                f"- {row['metric_name']}: {row['numerator']}/"
                f"{row['denominator']} ({row['coverage_percentage']}%)"
            )

        return {"datasets": outputs, "coverage": coverage_rows}
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Integra datasets Silver municipales por llaves geográficas controladas."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida entradas y muestra plan sin escribir Parquet.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe salidas integradas existentes.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=None,
        help="Dataset integrado a crear. Puede repetirse.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite opcional de filas por recurso para pruebas locales.",
    )
    parser.add_argument(
        "--output-subdir",
        default="integrated",
        help="Subcarpeta bajo data/silver para salidas integradas.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    run_integration(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        selected_sources=args.source,
        limit=args.limit,
        output_subdir=args.output_subdir,
    )


if __name__ == "__main__":
    main()
