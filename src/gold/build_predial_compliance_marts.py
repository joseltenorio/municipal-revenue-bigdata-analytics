"""Construcción de marts Gold para cumplimiento predial.

Este módulo usa Silver integrado predial para construir marts analíticos de
emisión, recaudación, saldos y cobertura territorial. No cruza fila-a-fila con
MEF y no inventa KPIs cuando faltan columnas base.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.common.logger import get_logger
from src.common.paths import SILVER_DIR, get_source_gold_path


SUBJECT_AREA = "predial_compliance"
INTEGRATED_SUBDIR = "integrated"

INPUT_DATASETS = [
    "predial_entity_period",
    "municipal_entity_bridge",
    "renamu_municipal_context",
    "integration_coverage",
]

GOLD_DATASETS = [
    "fact_predial_compliance",
    "mart_predial_compliance_overview",
    "mart_predial_ranking",
    "fact_predial_integration_coverage",
    "dim_predial_period",
]

PREDIAL_GRAIN_COLUMNS = [
    "ano_aplicacion",
    "periodo",
    "sec_ejec",
    "ubigeo",
    "formulario_id",
    "ano_estadistica",
    "mes_estadistica",
]

PREDIAL_COVERAGE_METRICS = [
    "total_predial_sec_ejec_entities",
    "predial_entities_with_valid_ubigeo",
    "predial_entities_with_renamu_match",
]


class GoldPredialError(Exception):
    """Error controlado durante la construcción Gold predial."""


@dataclass(frozen=True)
class PredialGoldPaths:
    """Rutas de entrada Silver integrada y salida Gold predial."""

    input_root: Path
    output_root: Path


def utc_now_iso() -> str:
    """Retorna fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Calcula ratio evitando división entre cero o nulos."""

    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def build_period_key(
    ano_aplicacion: int | str | None,
    periodo: int | str | None,
    ano_estadistica: int | str | None = None,
    mes_estadistica: int | str | None = None,
) -> str | None:
    """Construye llave técnica de periodo predial."""

    if ano_aplicacion is None or periodo is None:
        return None
    year = "" if ano_estadistica is None else f"{int(ano_estadistica):04d}"
    month = "" if mes_estadistica is None else f"{int(mes_estadistica):02d}"
    return f"{ano_aplicacion}_{periodo}_{year}{month}"


def existing_columns(columns: Iterable[str], desired_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas deseadas que existen, preservando orden."""

    available = set(columns)
    return [column for column in desired_columns if column in available]


def missing_columns(columns: Iterable[str], required_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas requeridas faltantes."""

    available = set(columns)
    return [column for column in required_columns if column not in available]


def detect_columns_by_patterns(columns: Iterable[str], patterns: Iterable[str]) -> list[str]:
    """Detecta columnas que contienen cualquiera de los patrones indicados."""

    normalized_patterns = [pattern.lower() for pattern in patterns]
    return [
        column
        for column in columns
        if any(pattern in column.lower() for pattern in normalized_patterns)
    ]


def detect_predial_columns(columns: Iterable[str]) -> dict[str, list[str]]:
    """Clasifica columnas prediales disponibles por categoría analítica."""

    column_list = list(columns)
    monetary_columns = [
        column
        for column in column_list
        if column.startswith("mon_") and column.endswith("_total")
    ]
    numeric_columns = [
        column
        for column in column_list
        if column.startswith("num_") and column.endswith("_total")
    ]
    return {
        "monetary": monetary_columns,
        "numeric": numeric_columns,
        "issue": detect_columns_by_patterns(monetary_columns, ["emision"]),
        "collection": detect_columns_by_patterns(
            monetary_columns,
            ["recaud", "recuad"],
        ),
        "balance": detect_columns_by_patterns(monetary_columns, ["saldo"]),
        "taxpayer": detect_columns_by_patterns(numeric_columns, ["contrib", "contri"]),
        "property": [
            column
            for column in detect_columns_by_patterns(numeric_columns, ["predio"])
            if "contrib" not in column.lower() and "contri" not in column.lower()
        ],
        "meta": detect_columns_by_patterns(column_list, ["meta"]),
        "flag": detect_columns_by_patterns(column_list, ["flag"]),
        "liquidation": detect_columns_by_patterns(column_list, ["liquid"]),
        "coupon": detect_columns_by_patterns(column_list, ["cupon"]),
    }


def metric_availability(columns_by_category: dict[str, list[str]]) -> dict[str, bool]:
    """Indica qué métricas Gold pueden construirse con columnas reales."""

    return {
        "predial_collection_total": bool(columns_by_category["collection"]),
        "predial_issue_total": bool(columns_by_category["issue"]),
        "predial_balance_total": bool(columns_by_category["balance"]),
        "predial_effectiveness_ratio": bool(
            columns_by_category["collection"] and columns_by_category["issue"]
        ),
        "taxpayer_count_total": bool(columns_by_category["taxpayer"]),
        "property_count_total": bool(columns_by_category["property"]),
    }


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye ruta Gold para un dataset predial soportado."""

    if dataset_name not in GOLD_DATASETS:
        raise GoldPredialError(f"Dataset Gold predial no soportado: {dataset_name}")
    return output_root / dataset_name


def validate_selected_datasets(selected: list[str] | None) -> list[str]:
    """Valida selección de datasets Gold prediales."""

    if not selected:
        return GOLD_DATASETS
    invalid = sorted(set(selected) - set(GOLD_DATASETS))
    if invalid:
        raise GoldPredialError(
            f"Datasets Gold prediales no soportados: {invalid}. "
            f"Disponibles: {GOLD_DATASETS}."
        )
    return selected


def resolve_paths(output_subdir: str) -> PredialGoldPaths:
    """Resuelve rutas para Gold predial."""

    return PredialGoldPaths(
        input_root=SILVER_DIR / INTEGRATED_SUBDIR,
        output_root=get_source_gold_path(output_subdir),
    )


def validate_input_paths(paths: PredialGoldPaths) -> None:
    """Valida existencia de entradas Silver integradas."""

    missing = [
        str(paths.input_root / dataset)
        for dataset in INPUT_DATASETS
        if not (paths.input_root / dataset).exists()
    ]
    if missing:
        raise GoldPredialError(
            "Faltan datasets Silver integrados requeridos para Gold predial: "
            + ", ".join(missing)
        )


def read_input_dataset(
    spark: Any,
    paths: PredialGoldPaths,
    dataset_name: str,
    limit: int | None,
) -> Any:
    """Lee un dataset Silver integrado con límite opcional."""

    dataframe = spark.read.parquet(str(paths.input_root / dataset_name))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def nonblank(column_name: str) -> Any:
    """Expresión Spark para texto no vacío."""

    from pyspark.sql import functions as F

    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def spark_safe_ratio(numerator_column: str, denominator_column: str) -> Any:
    """Expresión Spark de división segura."""

    from pyspark.sql import functions as F

    denominator = F.col(denominator_column)
    return F.when(
        denominator.isNotNull() & (denominator != 0),
        F.col(numerator_column) / denominator,
    )


def sum_existing_columns(dataframe: Any, columns: list[str]) -> Any:
    """Suma columnas existentes con coalesce a cero para métricas disponibles."""

    from functools import reduce

    from pyspark.sql import functions as F

    if not columns:
        return F.lit(None).cast("decimal(30,4)")
    expressions = [F.coalesce(F.col(column), F.lit(0)) for column in columns]
    return reduce(lambda left, right: left + right, expressions)


def classify_quality_column() -> Any:
    """Clasifica cobertura territorial predial."""

    from pyspark.sql import functions as F

    return (
        F.when(F.col("has_renamu_match"), F.lit("matched_renamu"))
        .when(F.col("has_valid_ubigeo"), F.lit("valid_ubigeo_without_renamu"))
        .when(F.col("has_municipal_bridge"), F.lit("bridge_without_valid_ubigeo"))
        .otherwise(F.lit("without_bridge"))
    )


def build_bridge_coverage_by_sec_ejec(bridge: Any) -> Any:
    """Resume cobertura territorial por `sec_ejec` sin multiplicar hechos."""

    from pyspark.sql import functions as F

    required = ["sec_ejec", "ubigeo", "is_valid_ubigeo", "has_renamu_match"]
    missing = missing_columns(bridge.columns, required)
    if missing:
        raise GoldPredialError(f"El puente municipal no tiene columnas requeridas: {missing}")

    territory_columns = [
        column
        for column in [
            "departamento",
            "provincia",
            "distrito",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
            "municipalidad_nombre",
        ]
        if column in bridge.columns
    ]

    aggregations = [
        F.countDistinct("ubigeo").alias("bridge_ubigeo_count"),
        F.first("ubigeo", ignorenulls=True).alias("first_bridge_ubigeo"),
        F.max(F.col("is_valid_ubigeo").cast("int")).alias("has_valid_ubigeo_int"),
        F.max(F.col("has_renamu_match").cast("int")).alias("has_renamu_match_int"),
    ]
    aggregations.extend(
        F.first(column, ignorenulls=True).alias(column) for column in territory_columns
    )

    return (
        bridge.where(nonblank("sec_ejec"))
        .groupBy("sec_ejec")
        .agg(*aggregations)
        .withColumn("has_municipal_bridge", F.lit(True))
        .withColumn(
            "bridge_ubigeo",
            F.when(F.col("bridge_ubigeo_count") == 1, F.col("first_bridge_ubigeo")),
        )
        .withColumn("has_valid_ubigeo", F.col("has_valid_ubigeo_int") == 1)
        .withColumn("has_renamu_match", F.col("has_renamu_match_int") == 1)
        .drop("first_bridge_ubigeo", "has_valid_ubigeo_int", "has_renamu_match_int")
    )


def add_predial_metric_columns(dataframe: Any, columns_by_category: dict[str, list[str]]) -> Any:
    """Agrega métricas prediales derivadas solo desde columnas disponibles."""

    transformed = (
        dataframe.withColumn(
            "predial_collection_total",
            sum_existing_columns(dataframe, columns_by_category["collection"]),
        )
        .withColumn(
            "predial_issue_total",
            sum_existing_columns(dataframe, columns_by_category["issue"]),
        )
        .withColumn(
            "predial_balance_total",
            sum_existing_columns(dataframe, columns_by_category["balance"]),
        )
        .withColumn(
            "taxpayer_count_total",
            sum_existing_columns(dataframe, columns_by_category["taxpayer"]),
        )
        .withColumn(
            "property_count_total",
            sum_existing_columns(dataframe, columns_by_category["property"]),
        )
    )
    return transformed.withColumn(
        "predial_effectiveness_ratio",
        spark_safe_ratio("predial_collection_total", "predial_issue_total"),
    )


def build_fact_predial_compliance(predial: Any, bridge: Any) -> Any:
    """Construye hecho Gold de cumplimiento predial."""

    from pyspark.sql import functions as F

    missing = missing_columns(predial.columns, ["ano_aplicacion", "periodo", "sec_ejec"])
    if missing:
        raise GoldPredialError(f"Predial integrado no tiene columnas requeridas: {missing}")

    columns_by_category = detect_predial_columns(predial.columns)
    bridge_coverage = build_bridge_coverage_by_sec_ejec(bridge)
    base_columns = existing_columns(
        predial.columns,
        [
            *PREDIAL_GRAIN_COLUMNS,
            "source_dataset",
            "integration_grain",
            "source_record_count",
            "active_response_count",
            *columns_by_category["monetary"],
            *columns_by_category["numeric"],
        ],
    )

    fact = predial.select(*base_columns).join(bridge_coverage, on="sec_ejec", how="left")
    fact = (
        fact.withColumn(
            "has_municipal_bridge",
            F.coalesce(F.col("has_municipal_bridge"), F.lit(False)),
        )
        .withColumn(
            "has_valid_ubigeo",
            F.coalesce(F.col("has_valid_ubigeo"), F.lit(False)),
        )
        .withColumn(
            "has_renamu_match",
            F.coalesce(F.col("has_renamu_match"), F.lit(False)),
        )
        .withColumn("integration_quality_status", classify_quality_column())
        .withColumn(
            "effective_ubigeo",
            F.coalesce(F.col("ubigeo"), F.col("bridge_ubigeo")),
        )
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn(
            "gold_grain",
            F.lit("ano_aplicacion_periodo_sec_ejec_formulario_ano_mes_estadistica"),
        )
    )
    return add_predial_metric_columns(fact, columns_by_category)


def build_mart_predial_compliance_overview(fact: Any) -> Any:
    """Construye vista agregada para Power BI predial."""

    from pyspark.sql import functions as F

    group_columns = existing_columns(
        fact.columns,
        [
            "ano_aplicacion",
            "periodo",
            "sec_ejec",
            "effective_ubigeo",
            "departamento",
            "provincia",
            "distrito",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
            "municipalidad_nombre",
            "formulario_id",
            "ano_estadistica",
            "mes_estadistica",
            "has_municipal_bridge",
            "has_valid_ubigeo",
            "has_renamu_match",
            "integration_quality_status",
        ],
    )
    return (
        fact.groupBy(*group_columns)
        .agg(
            F.sum("predial_collection_total").alias("predial_collection_total"),
            F.sum("predial_issue_total").alias("predial_issue_total"),
            F.sum("predial_balance_total").alias("predial_balance_total"),
            F.sum("taxpayer_count_total").alias("taxpayer_count_total"),
            F.sum("property_count_total").alias("property_count_total"),
            F.sum("source_record_count").alias("source_record_count"),
            F.sum("active_response_count").alias("active_response_count"),
        )
        .withColumn(
            "predial_effectiveness_ratio",
            spark_safe_ratio("predial_collection_total", "predial_issue_total"),
        )
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("gold_grain", F.lit("predial_entity_period_coverage"))
    )


def build_mart_predial_ranking(overview: Any) -> Any:
    """Construye ranking predial por recaudación, efectividad y saldo."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    required = ["ano_aplicacion", "periodo", "sec_ejec"]
    missing = missing_columns(overview.columns, required)
    if missing:
        raise GoldPredialError(f"El overview predial no tiene columnas requeridas: {missing}")

    group_columns = existing_columns(
        overview.columns,
        [
            "ano_aplicacion",
            "periodo",
            "sec_ejec",
            "effective_ubigeo",
            "departamento",
            "provincia",
            "distrito",
            "municipalidad_nombre",
            "has_renamu_match",
            "integration_quality_status",
        ],
    )
    aggregated = overview.groupBy(*group_columns).agg(
        F.sum("predial_collection_total").alias("predial_collection_total"),
        F.sum("predial_issue_total").alias("predial_issue_total"),
        F.sum("predial_balance_total").alias("predial_balance_total"),
        F.sum("taxpayer_count_total").alias("taxpayer_count_total"),
        F.sum("property_count_total").alias("property_count_total"),
    )
    ranked = aggregated.withColumn(
        "predial_effectiveness_ratio",
        spark_safe_ratio("predial_collection_total", "predial_issue_total"),
    )
    partition = Window.partitionBy("ano_aplicacion", "periodo")
    return (
        ranked.withColumn(
            "collection_rank_desc",
            F.dense_rank().over(partition.orderBy(F.col("predial_collection_total").desc_nulls_last())),
        )
        .withColumn(
            "collection_rank_asc",
            F.dense_rank().over(partition.orderBy(F.col("predial_collection_total").asc_nulls_last())),
        )
        .withColumn(
            "effectiveness_rank_desc",
            F.dense_rank().over(partition.orderBy(F.col("predial_effectiveness_ratio").desc_nulls_last())),
        )
        .withColumn(
            "balance_rank_desc",
            F.dense_rank().over(partition.orderBy(F.col("predial_balance_total").desc_nulls_last())),
        )
        .withColumn("is_top_collection_candidate", F.col("collection_rank_desc") <= 10)
        .withColumn("is_bottom_collection_candidate", F.col("collection_rank_asc") <= 10)
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def build_fact_predial_integration_coverage(integration_coverage: Any) -> Any:
    """Construye hecho técnico de cobertura predial."""

    from pyspark.sql import functions as F

    required = ["metric_name", "numerator", "denominator", "coverage_percentage"]
    missing = missing_columns(integration_coverage.columns, required)
    if missing:
        raise GoldPredialError(f"Integration coverage no tiene columnas requeridas: {missing}")

    return (
        integration_coverage.where(F.col("metric_name").isin(PREDIAL_COVERAGE_METRICS))
        .select("metric_name", "numerator", "denominator", "coverage_percentage", "description")
        .withColumn("coverage_ratio", spark_safe_ratio("numerator", "denominator"))
        .withColumn("source_dataset", F.lit("silver.integration_coverage"))
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def build_dim_predial_period(fact: Any) -> Any:
    """Construye dimensión ligera de periodo predial."""

    from pyspark.sql import functions as F

    return (
        fact.select(
            "ano_aplicacion",
            "periodo",
            "ano_estadistica",
            "mes_estadistica",
        )
        .dropDuplicates()
        .withColumn(
            "predial_period_key",
            F.concat_ws(
                "_",
                F.col("ano_aplicacion"),
                F.col("periodo"),
                F.coalesce(F.col("ano_estadistica"), F.lit("")),
                F.coalesce(F.col("mes_estadistica"), F.lit("")),
            ),
        )
        .withColumn(
            "predial_period_label",
            F.concat_ws(
                " / ",
                F.concat(F.lit("Aplicación "), F.col("ano_aplicacion")),
                F.concat(F.lit("Periodo "), F.col("periodo")),
                F.concat_ws("-", F.col("ano_estadistica"), F.col("mes_estadistica")),
            ),
        )
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def write_dataset(dataframe: Any, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset Gold predial en Parquet Snappy."""

    mode = "overwrite" if overwrite else "errorifexists"
    dataframe.write.mode(mode).option("compression", "snappy").parquet(str(output_path))


def print_detected_columns(columns_by_category: dict[str, list[str]]) -> None:
    """Imprime columnas prediales detectadas por categoría."""

    print("Columnas prediales detectadas:")
    for category, columns in columns_by_category.items():
        preview = ", ".join(columns[:8])
        suffix = "" if len(columns) <= 8 else f" ... (+{len(columns) - 8})"
        print(f"- {category}: {len(columns)} | {preview}{suffix}")


def print_metric_availability(availability: dict[str, bool]) -> None:
    """Imprime disponibilidad de métricas Gold."""

    print("Métricas Gold prediales:")
    for metric_name, available in availability.items():
        status = "se construirá" if available else "no disponible por falta de columnas base"
        print(f"- {metric_name}: {status}")


def print_dry_run(
    paths: PredialGoldPaths,
    datasets: list[str],
    columns_by_category: dict[str, list[str]],
) -> None:
    """Muestra plan de construcción Gold predial."""

    print("=" * 80)
    print("Plan Gold de cumplimiento predial")
    print(f"Entrada Silver integrada: {paths.input_root}")
    for dataset in INPUT_DATASETS:
        path = paths.input_root / dataset
        print(f"- input {dataset}: {path} | existe={path.exists()}")
    print(f"Salida Gold: {paths.output_root}")
    print("Datasets Gold a crear:")
    for dataset in datasets:
        print(f"- {dataset}: {output_dataset_path(paths.output_root, dataset)}")
    print_detected_columns(columns_by_category)
    print_metric_availability(metric_availability(columns_by_category))
    print("Dry-run finalizado. No se escribirá Parquet en data/gold.")


def write_and_count(
    *,
    dataframe: Any,
    dataset_name: str,
    output_root: Path,
    overwrite: bool,
) -> tuple[str, int]:
    """Escribe un dataset y retorna ruta y conteo."""

    output_path = output_dataset_path(output_root, dataset_name)
    write_dataset(dataframe, output_path, overwrite=overwrite)
    return str(output_path), int(dataframe.count())


def build_predial_gold_marts(
    *,
    dry_run: bool,
    overwrite: bool,
    selected_datasets: list[str] | None = None,
    limit: int | None = None,
    output_subdir: str = SUBJECT_AREA,
) -> dict[str, Any]:
    """Ejecuta o planifica Gold predial."""

    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session

    paths = resolve_paths(output_subdir)
    validate_input_paths(paths)
    datasets = validate_selected_datasets(selected_datasets)

    spark = build_spark_session(
        app_name="GoldPredialComplianceMarts",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "8"},
    )

    try:
        predial_for_detection = spark.read.parquet(str(paths.input_root / "predial_entity_period"))
        columns_by_category = detect_predial_columns(predial_for_detection.columns)

        if dry_run:
            print_dry_run(paths, datasets, columns_by_category)
            print(
                f"- predial_entity_period: filas={predial_for_detection.count()} "
                f"columnas={len(predial_for_detection.columns)}"
            )
            return {"datasets": datasets, "columns_by_category": columns_by_category}

        logger = get_logger(__name__)
        predial = read_input_dataset(spark, paths, "predial_entity_period", limit)
        bridge = read_input_dataset(spark, paths, "municipal_entity_bridge", limit)
        coverage = read_input_dataset(spark, paths, "integration_coverage", None)

        fact = build_fact_predial_compliance(predial, bridge)
        overview = build_mart_predial_compliance_overview(fact)
        ranking = build_mart_predial_ranking(overview)
        coverage_fact = build_fact_predial_integration_coverage(coverage)
        dim_period = build_dim_predial_period(fact)

        built = {
            "fact_predial_compliance": fact,
            "mart_predial_compliance_overview": overview,
            "mart_predial_ranking": ranking,
            "fact_predial_integration_coverage": coverage_fact,
            "dim_predial_period": dim_period,
        }

        outputs: dict[str, str] = {}
        counts: dict[str, int] = {}
        for dataset_name in datasets:
            logger.info("Escribiendo dataset Gold predial %s", dataset_name)
            output_path, count = write_and_count(
                dataframe=built[dataset_name],
                dataset_name=dataset_name,
                output_root=paths.output_root,
                overwrite=overwrite,
            )
            outputs[dataset_name] = output_path
            counts[dataset_name] = count

        coverage_summary = (
            fact.select("sec_ejec", "has_valid_ubigeo", "has_renamu_match")
            .dropDuplicates()
            .agg(
                F.count("sec_ejec").alias("total_sec_ejec"),
                F.sum(F.col("has_valid_ubigeo").cast("int")).alias("with_valid_ubigeo"),
                F.sum(F.col("has_renamu_match").cast("int")).alias("with_renamu_match"),
            )
            .collect()[0]
            .asDict()
        )

        print("=" * 80)
        print("Gold cumplimiento predial finalizado")
        for dataset_name in datasets:
            print(f"- {dataset_name}: filas={counts[dataset_name]} ruta={outputs[dataset_name]}")
        print_detected_columns(columns_by_category)
        print_metric_availability(metric_availability(columns_by_category))
        print("Cobertura predial por sec_ejec distintos:")
        print(f"- total_sec_ejec: {coverage_summary.get('total_sec_ejec')}")
        print(f"- con_ubigeo_valido: {coverage_summary.get('with_valid_ubigeo')}")
        print(f"- con_match_renamu: {coverage_summary.get('with_renamu_match')}")

        return {
            "datasets": outputs,
            "counts": counts,
            "columns_by_category": columns_by_category,
        }
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Construye marts Gold prediales desde Silver integrado."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida entradas y muestra plan sin escribir Parquet Gold.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe salidas Gold existentes.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=None,
        help="Dataset Gold predial a construir. Puede repetirse.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Límite opcional de filas para pruebas locales.",
    )
    parser.add_argument(
        "--output-subdir",
        default=SUBJECT_AREA,
        help="Subcarpeta bajo data/gold para salidas.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        raise GoldPredialError("--limit debe ser un entero positivo.")

    build_predial_gold_marts(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        selected_datasets=args.dataset,
        limit=args.limit,
        output_subdir=args.output_subdir,
    )


if __name__ == "__main__":
    main()
