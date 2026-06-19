"""Construccion de datasets dashboard-ready para Power BI.

Este modulo crea una capa derivada de Gold optimizada para exportacion ligera.
Las salidas Gold agregadas se escriben bajo ``data/gold/powerbi/`` y los
exports finales para Power BI bajo ``powerbi/exports/dashboard/``.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    TimestampType,
)

from src.common.paths import GOLD_DIR, POWERBI_DIR, SILVER_DIR
from src.common.spark_session import build_spark_session
from src.gold.build_revenue_predial_facts import (
    build_siaf_resolution_map,
    derive_date_key,
)


DASHBOARD_DATASETS = [
    "revenue_monthly_dashboard",
    "revenue_source_monthly_dashboard",
    "revenue_source_annual_dashboard",
    "revenue_annual_dashboard",
    "territorial_revenue_dashboard",
    "predial_dashboard",
    "municipal_context_dashboard",
    "municipal_performance_dashboard",
    "audit_dataset_summary_dashboard",
    "audit_integration_coverage_dashboard",
    "audit_quality_results_dashboard",
]

OPTIONAL_DASHBOARD_DATASETS = [
    "revenue_concept_monthly_dashboard",
]

ALL_DASHBOARD_DATASETS = DASHBOARD_DATASETS + OPTIONAL_DASHBOARD_DATASETS

BLOCKED_EXPORT_FILE_NAMES = {
    "fact_siaf_income.csv",
    "mart_municipal_revenue_overview.csv",
}

MUNICIPAL_GRAIN_COLUMNS = [
    "municipality_key",
    "ubigeo6",
    "municipalidad_nombre",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre",
    "tipo_clasificacion_municipal",
    "ambito_municipal",
    "tipomuni_nombre",
]

TIME_MONTHLY_COLUMNS = [
    "anio",
    "mes",
    "anio_mes",
    "trimestre",
    "semestre",
]

REVENUE_SOURCE_COLUMNS = [
    "fuente_financiamiento_codigo",
    "fuente_financiamiento_nombre",
    "rubro_codigo",
    "rubro_nombre",
    "tipo_recurso_codigo",
    "tipo_recurso_nombre",
]

REVENUE_CONCEPT_COLUMNS = [
    "generica_codigo",
    "generica_nombre",
    "subgenerica_codigo",
    "subgenerica_nombre",
    "especifica_codigo",
    "especifica_nombre",
]

PREDIAL_CONTEXT_COLUMNS = [
    "monto_emision_predial_total",
    "monto_recaudacion_predial_total",
    "monto_saldo_predial_total",
    "ratio_recaudacion_emision",
    "numero_predios_total",
    "numero_contribuyentes_predio",
]

MUNICIPAL_CONTEXT_SELECTED_COLUMNS = [
    "total_computadoras_operativas",
    "cuenta_servicio_internet",
    "computadoras_con_acceso_internet",
    "tipo_conexion_internet_codigo",
    "tipo_conexion_internet_nombre",
    "usa_siaf",
    "usa_sistema_recaudacion_tributaria_municipal",
    "usa_sistema_rentas_administracion_tributaria",
    "usa_sistema_catastro",
    "portal_transparencia_actualizado",
    "total_personal_dic_2021",
    "total_personal_mar_2022",
    "tiene_area_ejecucion_coactiva",
    "requiere_asistencia_administracion_tributaria",
    "requiere_asistencia_catastro",
    "requiere_capacitacion_administracion_tributaria",
    "requiere_capacitacion_catastro",
]

SIAF_SILVER_REQUIRED_COLUMNS = [
    "anio",
    "mes",
    "sec_ejec",
    "ubigeo6_ejecutora",
    "fuente_financiamiento_codigo",
    "fuente_financiamiento_nombre",
    "rubro_codigo",
    "rubro_nombre",
    "tipo_recurso_codigo",
    "tipo_recurso_nombre",
    "generica_codigo",
    "generica_nombre",
    "subgenerica_codigo",
    "subgenerica_nombre",
    "especifica_codigo",
    "especifica_nombre",
    "source_resource_key",
    "source_granularity",
    "monto_pia",
    "monto_pim",
    "monto_recaudado",
]

MAX_OPTIONAL_CONCEPT_CSV_ROWS = 1_000_000


class DashboardExportError(ValueError):
    """Error de contrato para la capa dashboard-ready."""


@dataclass(frozen=True)
class DashboardExportPaths:
    """Rutas de entrada y salida para la capa Power BI dashboard-ready."""

    output_root: Path
    export_root: Path
    mart_municipal_revenue_overview_path: Path
    mart_predial_statistics_overview_path: Path
    mart_municipal_context_path: Path
    dim_municipality_path: Path
    dim_geography_path: Path
    dim_time_path: Path
    audit_dataset_summary_path: Path
    audit_integration_coverage_path: Path
    audit_quality_results_path: Path
    silver_siaf_income_root: Path
    map_sec_ejec_ubigeo_path: Path


@dataclass(frozen=True)
class DatasetSpec:
    """Configuracion minima de cada dataset dashboard-ready."""

    dataset_name: str
    description: str
    is_optional: bool = False


@dataclass
class DatasetBuildResult:
    """Resultado operativo por dataset construido o planificado."""

    dataset_name: str
    gold_output_path: Path
    export_output_path: Path
    row_count: int
    columns: list[str]
    export_written: bool
    export_size_bytes: int | None
    skipped: bool = False
    skip_reason: str | None = None
    warnings: list[str] | None = None


DATASET_REGISTRY = {
    "revenue_monthly_dashboard": DatasetSpec(
        dataset_name="revenue_monthly_dashboard",
        description="Resumen mensual agregado por municipalidad para KPIs ejecutivos.",
    ),
    "revenue_source_monthly_dashboard": DatasetSpec(
        dataset_name="revenue_source_monthly_dashboard",
        description="Estructura mensual de ingresos por fuente, rubro y tipo de recurso.",
    ),
    "revenue_source_annual_dashboard": DatasetSpec(
        dataset_name="revenue_source_annual_dashboard",
        description="Estructura anual de ingresos por fuente, rubro y tipo de recurso.",
    ),
    "revenue_annual_dashboard": DatasetSpec(
        dataset_name="revenue_annual_dashboard",
        description="Resumen anual por municipalidad para comparativos y ranking.",
    ),
    "territorial_revenue_dashboard": DatasetSpec(
        dataset_name="territorial_revenue_dashboard",
        description="Resumen territorial anual agregado para mapas y concentracion.",
    ),
    "predial_dashboard": DatasetSpec(
        dataset_name="predial_dashboard",
        description="Indicadores prediales agregados por municipalidad y periodo.",
    ),
    "municipal_context_dashboard": DatasetSpec(
        dataset_name="municipal_context_dashboard",
        description="Contexto institucional municipal derivado de RENAMU.",
    ),
    "municipal_performance_dashboard": DatasetSpec(
        dataset_name="municipal_performance_dashboard",
        description="Segmentacion descriptiva combinando ingresos, predial y contexto.",
    ),
    "audit_dataset_summary_dashboard": DatasetSpec(
        dataset_name="audit_dataset_summary_dashboard",
        description="Resumen tecnico de calidad por dataset.",
    ),
    "audit_integration_coverage_dashboard": DatasetSpec(
        dataset_name="audit_integration_coverage_dashboard",
        description="Cobertura tecnica de integracion municipal.",
    ),
    "audit_quality_results_dashboard": DatasetSpec(
        dataset_name="audit_quality_results_dashboard",
        description="Detalle tecnico de resultados de calidad.",
    ),
    "revenue_concept_monthly_dashboard": DatasetSpec(
        dataset_name="revenue_concept_monthly_dashboard",
        description="Analisis opcional por conceptos presupuestales de ingresos.",
        is_optional=True,
    ),
}


def default_paths() -> DashboardExportPaths:
    """Devuelve rutas vigentes para la construccion dashboard-ready."""

    return DashboardExportPaths(
        output_root=GOLD_DIR / "powerbi",
        export_root=POWERBI_DIR / "exports" / "dashboard",
        mart_municipal_revenue_overview_path=GOLD_DIR / "mart_municipal_revenue_overview",
        mart_predial_statistics_overview_path=GOLD_DIR / "mart_predial_statistics_overview",
        mart_municipal_context_path=GOLD_DIR / "mart_municipal_context",
        dim_municipality_path=GOLD_DIR / "dim_municipality",
        dim_geography_path=GOLD_DIR / "dim_geography",
        dim_time_path=GOLD_DIR / "dim_time",
        audit_dataset_summary_path=GOLD_DIR / "audit_dataset_summary",
        audit_integration_coverage_path=GOLD_DIR / "audit_integration_coverage",
        audit_quality_results_path=GOLD_DIR / "audit_quality_results",
        silver_siaf_income_root=SILVER_DIR / "siaf_income",
        map_sec_ejec_ubigeo_path=SILVER_DIR / "integrated" / "map_sec_ejec_ubigeo",
    )


def validate_selected_datasets(selected_datasets: list[str] | None) -> list[str]:
    """Valida datasets seleccionados desde CLI."""

    if not selected_datasets:
        return ALL_DASHBOARD_DATASETS

    unsupported = [
        dataset for dataset in selected_datasets if dataset not in DATASET_REGISTRY
    ]
    if unsupported:
        raise DashboardExportError(
            f"Datasets dashboard-ready no soportados: {unsupported}. "
            f"Soportados: {ALL_DASHBOARD_DATASETS}."
        )
    return selected_datasets


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta Gold agregada de un dataset soportado."""

    validate_selected_datasets([dataset_name])
    return output_root / dataset_name


def export_dataset_path(export_root: Path, dataset_name: str, output_format: str) -> Path:
    """Construye la ruta de export final para Power BI."""

    validate_output_format(output_format)
    suffix = ".csv" if output_format == "csv" else ".parquet"
    return export_root / f"{dataset_name}{suffix}"


def validate_output_format(output_format: str) -> str:
    """Valida formato de export soportado."""

    normalized = output_format.strip().lower()
    if normalized not in {"csv", "parquet"}:
        raise DashboardExportError(
            f"--output-format no soportado: {output_format}. Use csv o parquet."
        )
    return normalized


def existing_columns(available_columns: list[str], desired_columns: list[str]) -> list[str]:
    """Conserva el orden deseado filtrando columnas existentes."""

    available = set(available_columns)
    return [column for column in desired_columns if column in available]


def missing_columns(available_columns: list[str], required_columns: list[str]) -> list[str]:
    """Devuelve columnas faltantes en un DataFrame."""

    available = set(available_columns)
    return [column for column in required_columns if column not in available]


def require_columns(dataframe: DataFrame, required_columns: list[str], dataset_name: str) -> None:
    """Falla rapido cuando un contrato minimo no se cumple."""

    missing = missing_columns(dataframe.columns, required_columns)
    if missing:
        raise DashboardExportError(
            f"{dataset_name} no tiene columnas requeridas: {missing}"
        )


def read_parquet_dataset(spark: Any, path: Path) -> DataFrame:
    """Lee un dataset Parquet de entrada."""

    return spark.read.parquet(str(path))


def list_siaf_resource_paths(siaf_root: Path) -> list[Path]:
    """Lista recursos Silver SIAF disponibles."""

    if not siaf_root.exists():
        return []
    return sorted(
        path
        for path in siaf_root.iterdir()
        if path.is_dir() and path.name.startswith("resource_key=")
    )


def validate_input_paths(paths: DashboardExportPaths, datasets: list[str]) -> None:
    """Valida entradas minimas requeridas para los datasets solicitados."""

    required_paths = {
        paths.mart_municipal_revenue_overview_path,
        paths.mart_predial_statistics_overview_path,
        paths.mart_municipal_context_path,
        paths.dim_municipality_path,
        paths.dim_geography_path,
        paths.dim_time_path,
        paths.audit_dataset_summary_path,
        paths.audit_integration_coverage_path,
        paths.audit_quality_results_path,
    }

    if any(
        dataset in datasets
        for dataset in [
            "revenue_source_monthly_dashboard",
            "revenue_source_annual_dashboard",
            "revenue_concept_monthly_dashboard",
        ]
    ):
        required_paths.add(paths.map_sec_ejec_ubigeo_path)
        required_paths.add(paths.silver_siaf_income_root)

    missing = [path for path in sorted(required_paths) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"No existen entradas requeridas: {missing}")

    if any(
        dataset in datasets
        for dataset in [
            "revenue_source_monthly_dashboard",
            "revenue_source_annual_dashboard",
            "revenue_concept_monthly_dashboard",
        ]
    ) and not list_siaf_resource_paths(paths.silver_siaf_income_root):
        raise FileNotFoundError(
            f"No hay recursos Silver SIAF bajo {paths.silver_siaf_income_root}."
        )


def safe_ratio_expression(numerator_column: str, denominator_column: str) -> Any:
    """Calcula una razon evitando division por cero."""

    return (
        F.when(
            F.col(denominator_column).isNull() | (F.col(denominator_column) == 0),
            F.lit(None).cast("double"),
        )
        .otherwise(
            (F.col(numerator_column).cast("double") / F.col(denominator_column).cast("double"))
        )
        .cast("double")
    )


def with_revenue_derived_metrics(dataframe: DataFrame) -> DataFrame:
    """Agrega metricas derivadas comunes de ingresos."""

    return (
        dataframe.withColumn(
            "brecha_recaudacion",
            (F.col("monto_pim").cast("double") - F.col("monto_recaudado").cast("double")).cast(
                "double"
            ),
        )
        .withColumn(
            "eficiencia_recaudacion",
            safe_ratio_expression("monto_recaudado", "monto_pim"),
        )
        .withColumn("tiene_recaudacion", F.col("monto_recaudado") > F.lit(0))
    )


def aggregate_revenue(dataframe: DataFrame, group_columns: list[str]) -> DataFrame:
    """Agrega montos de ingresos a un grano objetivo."""

    return with_revenue_derived_metrics(
        dataframe.groupBy(*group_columns).agg(
            F.sum(F.col("monto_pia").cast("double")).alias("monto_pia"),
            F.sum(F.col("monto_pim").cast("double")).alias("monto_pim"),
            F.sum(F.col("monto_recaudado").cast("double")).alias("monto_recaudado"),
        )
    )


def apply_municipal_filters(dataframe: DataFrame) -> DataFrame:
    """Aplica el filtro municipal blindado usado en Gold."""

    filtered = dataframe.filter(F.col("municipality_key").isNotNull())
    if "has_municipality_match" in filtered.columns:
        filtered = filtered.filter(F.col("has_municipality_match") == F.lit(True))
    if "match_status" in filtered.columns:
        filtered = filtered.filter(
            ~F.col("match_status").isin(
                "missing_map",
                "unmatched",
                "invalid_ubigeo",
                "ambiguous_sec_ejec",
            )
        )
    return filtered


def build_revenue_monthly_dashboard(mart_revenue: DataFrame) -> DataFrame:
    """Construye el dataset mensual liviano principal de ingresos."""

    required = MUNICIPAL_GRAIN_COLUMNS + TIME_MONTHLY_COLUMNS + [
        "monto_pia",
        "monto_pim",
        "monto_recaudado",
    ]
    require_columns(mart_revenue, required, "mart_municipal_revenue_overview")
    filtered = apply_municipal_filters(mart_revenue)
    return aggregate_revenue(filtered, MUNICIPAL_GRAIN_COLUMNS + TIME_MONTHLY_COLUMNS)


def build_revenue_annual_dashboard(revenue_monthly: DataFrame) -> DataFrame:
    """Construye el dataset anual liviano principal de ingresos."""

    require_columns(
        revenue_monthly,
        MUNICIPAL_GRAIN_COLUMNS + ["anio", "monto_pia", "monto_pim", "monto_recaudado"],
        "revenue_monthly_dashboard",
    )
    aggregated = aggregate_revenue(revenue_monthly, MUNICIPAL_GRAIN_COLUMNS + ["anio"])
    return aggregated.drop("tiene_recaudacion")


def build_territorial_revenue_dashboard(revenue_annual: DataFrame) -> DataFrame:
    """Construye el dataset territorial anual sin duplicar municipalidades."""

    require_columns(
        revenue_annual,
        [
            "municipality_key",
            "departamento_nombre",
            "provincia_nombre",
            "tipo_clasificacion_municipal",
            "ambito_municipal",
            "anio",
            "monto_pia",
            "monto_pim",
            "monto_recaudado",
        ],
        "revenue_annual_dashboard",
    )

    grouped = (
        revenue_annual.groupBy(
            "departamento_nombre",
            "provincia_nombre",
            "tipo_clasificacion_municipal",
            "ambito_municipal",
            "anio",
        )
        .agg(
            F.countDistinct("municipality_key").alias("total_municipalidades"),
            F.sum("monto_pia").alias("monto_pia"),
            F.sum("monto_pim").alias("monto_pim"),
            F.sum("monto_recaudado").alias("monto_recaudado"),
        )
        .withColumn(
            "brecha_recaudacion",
            (F.col("monto_pim") - F.col("monto_recaudado")).cast("double"),
        )
        .withColumn(
            "eficiencia_recaudacion",
            safe_ratio_expression("monto_recaudado", "monto_pim"),
        )
    )

    annual_window = Window.partitionBy("anio")
    return grouped.withColumn(
        "participacion_recaudacion_anual",
        F.when(
            F.sum("monto_recaudado").over(annual_window) > 0,
            F.col("monto_recaudado") / F.sum("monto_recaudado").over(annual_window),
        ).cast("double"),
    )


def build_predial_dashboard(predial_mart: DataFrame) -> DataFrame:
    """Construye el dataset predial principal agregando cuando existan duplicados."""

    required = MUNICIPAL_GRAIN_COLUMNS + [
        "anio_aplicacion",
        "periodo",
        "periodo_label",
        "periodo_estadistica_tipo",
        "monto_emision_predial_total",
        "monto_recaudacion_predial_total",
        "monto_saldo_predial_total",
        "numero_predios_total",
        "numero_contribuyentes_predio",
    ]
    require_columns(predial_mart, required, "mart_predial_statistics_overview")

    group_columns = MUNICIPAL_GRAIN_COLUMNS + [
        "anio_aplicacion",
        "periodo",
        "periodo_label",
        "periodo_estadistica_tipo",
    ]
    aggregated = predial_mart.groupBy(*group_columns).agg(
        F.sum(F.col("monto_emision_predial_total").cast("double")).alias(
            "monto_emision_predial_total"
        ),
        F.sum(F.col("monto_recaudacion_predial_total").cast("double")).alias(
            "monto_recaudacion_predial_total"
        ),
        F.sum(F.col("monto_saldo_predial_total").cast("double")).alias(
            "monto_saldo_predial_total"
        ),
        F.sum(F.col("numero_predios_total").cast("double")).alias("numero_predios_total"),
        F.sum(F.col("numero_contribuyentes_predio").cast("double")).alias(
            "numero_contribuyentes_predio"
        ),
    )
    return aggregated.withColumn(
        "ratio_recaudacion_emision",
        safe_ratio_expression(
            "monto_recaudacion_predial_total", "monto_emision_predial_total"
        ),
    )


def first_non_null_aggregations(
    dataframe: DataFrame,
    key_columns: list[str],
    value_columns: list[str],
) -> DataFrame:
    """Deduplica una tabla manteniendo la primera observacion no nula por llave."""

    aggregations = [
        F.first(F.col(column), ignorenulls=True).alias(column) for column in value_columns
    ]
    return dataframe.groupBy(*key_columns).agg(*aggregations)


def build_municipal_context_dashboard(context_mart: DataFrame) -> DataFrame:
    """Construye el dataset de contexto municipal con una fila por municipalidad."""

    selected_context_columns = existing_columns(
        context_mart.columns, MUNICIPAL_CONTEXT_SELECTED_COLUMNS
    )
    require_columns(
        context_mart,
        MUNICIPAL_GRAIN_COLUMNS,
        "mart_municipal_context",
    )
    base_columns = MUNICIPAL_GRAIN_COLUMNS
    deduped = first_non_null_aggregations(
        context_mart.select(*base_columns, *selected_context_columns),
        ["municipality_key"],
        [column for column in base_columns if column != "municipality_key"]
        + selected_context_columns,
    )
    return deduped.select("municipality_key", *[column for column in base_columns if column != "municipality_key"], *selected_context_columns)


def build_predial_yearly_latest(predial_dashboard: DataFrame) -> DataFrame:
    """Selecciona el ultimo periodo predial disponible por municipalidad y anio."""

    require_columns(
        predial_dashboard,
        ["municipality_key", "anio_aplicacion", "periodo"] + PREDIAL_CONTEXT_COLUMNS,
        "predial_dashboard",
    )

    latest_window = Window.partitionBy("municipality_key", "anio_aplicacion").orderBy(
        F.col("periodo").desc(),
        F.col("periodo_label").desc(),
    )
    return (
        predial_dashboard.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumnRenamed("anio_aplicacion", "anio")
        .select("municipality_key", "anio", *PREDIAL_CONTEXT_COLUMNS)
    )


def average_available_flags(flag_columns: list[Any]) -> Any:
    """Promedia flags disponibles sin penalizar nulos."""

    numerator = None
    denominator = None
    for expression in flag_columns:
        value = F.when(expression.isNull(), F.lit(None)).otherwise(expression.cast("double"))
        numerator = value if numerator is None else numerator + F.coalesce(value, F.lit(0.0))
        denominator_piece = F.when(value.isNull(), F.lit(0.0)).otherwise(F.lit(1.0))
        denominator = (
            denominator_piece if denominator is None else denominator + denominator_piece
        )
    return F.when(denominator > 0, numerator / denominator).cast("double")


def build_municipal_performance_dashboard(
    revenue_annual: DataFrame,
    predial_dashboard: DataFrame,
    municipal_context: DataFrame,
) -> DataFrame:
    """Construye un dataset integrado para segmentacion descriptiva."""

    require_columns(
        revenue_annual,
        MUNICIPAL_GRAIN_COLUMNS
        + ["anio", "monto_pia", "monto_pim", "monto_recaudado", "eficiencia_recaudacion", "brecha_recaudacion"],
        "revenue_annual_dashboard",
    )
    require_columns(
        municipal_context,
        ["municipality_key"] + [column for column in MUNICIPAL_GRAIN_COLUMNS if column != "municipality_key"],
        "municipal_context_dashboard",
    )

    predial_latest = build_predial_yearly_latest(predial_dashboard)
    context_columns = existing_columns(municipal_context.columns, MUNICIPAL_CONTEXT_SELECTED_COLUMNS)

    joined = (
        revenue_annual.alias("revenue")
        .join(
            municipal_context.select("municipality_key", *context_columns).alias("context"),
            on="municipality_key",
            how="left",
        )
        .join(predial_latest.alias("predial"), on=["municipality_key", "anio"], how="left")
    )

    return (
        joined.withColumn(
            "indice_capacidad_digital",
            average_available_flags(
                [
                    F.col("cuenta_servicio_internet"),
                    F.col("computadoras_con_acceso_internet") > F.lit(0),
                    F.col("portal_transparencia_actualizado"),
                ]
            ),
        )
        .withColumn(
            "indice_capacidad_tributaria",
            average_available_flags(
                [
                    F.col("usa_sistema_recaudacion_tributaria_municipal"),
                    F.col("usa_sistema_rentas_administracion_tributaria"),
                    F.col("usa_sistema_catastro"),
                    F.col("tiene_area_ejecucion_coactiva"),
                ]
            ),
        )
        .withColumn(
            "segmento_desempeno",
            F.when(
                (F.col("eficiencia_recaudacion") >= 0.8)
                & (F.col("indice_capacidad_tributaria") >= 0.75),
                F.lit("alto_desempeno"),
            )
            .when(
                (F.col("eficiencia_recaudacion") >= 0.5)
                & (
                    (F.col("indice_capacidad_digital") >= 0.5)
                    | (F.col("indice_capacidad_tributaria") >= 0.5)
                ),
                F.lit("desempeno_medio"),
            )
            .otherwise(F.lit("oportunidad_mejora")),
        )
        .select(
            "municipality_key",
            *[column for column in MUNICIPAL_GRAIN_COLUMNS if column != "municipality_key"],
            "anio",
            "monto_pia",
            "monto_pim",
            "monto_recaudado",
            "eficiencia_recaudacion",
            "brecha_recaudacion",
            *PREDIAL_CONTEXT_COLUMNS,
            *context_columns,
            "indice_capacidad_digital",
            "indice_capacidad_tributaria",
            "segmento_desempeno",
        )
    )


def build_revenue_source_monthly_dashboard(revenue_source_detail: DataFrame) -> DataFrame:
    """Construye el dataset mensual por fuente, rubro y tipo de recurso."""

    require_columns(
        revenue_source_detail,
        MUNICIPAL_GRAIN_COLUMNS
        + TIME_MONTHLY_COLUMNS
        + REVENUE_SOURCE_COLUMNS
        + ["monto_pia", "monto_pim", "monto_recaudado"],
        "revenue_source_detail",
    )
    aggregated = aggregate_revenue(
        revenue_source_detail,
        MUNICIPAL_GRAIN_COLUMNS + TIME_MONTHLY_COLUMNS + REVENUE_SOURCE_COLUMNS,
    )
    return aggregated.drop("tiene_recaudacion")


def build_revenue_source_annual_dashboard(
    revenue_source_monthly: DataFrame,
) -> DataFrame:
    """Construye el dataset anual por fuente, rubro y tipo de recurso."""

    require_columns(
        revenue_source_monthly,
        MUNICIPAL_GRAIN_COLUMNS
        + ["anio"]
        + REVENUE_SOURCE_COLUMNS
        + ["monto_pia", "monto_pim", "monto_recaudado"],
        "revenue_source_monthly_dashboard",
    )
    aggregated = aggregate_revenue(
        revenue_source_monthly,
        MUNICIPAL_GRAIN_COLUMNS + ["anio"] + REVENUE_SOURCE_COLUMNS,
    )
    return aggregated.drop("tiene_recaudacion")


def build_revenue_concept_monthly_dashboard(
    revenue_source_detail: DataFrame,
) -> DataFrame:
    """Construye el dataset opcional por conceptos presupuestales."""

    required = (
        MUNICIPAL_GRAIN_COLUMNS
        + TIME_MONTHLY_COLUMNS
        + REVENUE_SOURCE_COLUMNS
        + REVENUE_CONCEPT_COLUMNS
        + ["monto_pia", "monto_pim", "monto_recaudado"]
    )
    require_columns(revenue_source_detail, required, "revenue_source_detail")
    aggregated = aggregate_revenue(
        revenue_source_detail,
        MUNICIPAL_GRAIN_COLUMNS
        + TIME_MONTHLY_COLUMNS
        + REVENUE_SOURCE_COLUMNS
        + REVENUE_CONCEPT_COLUMNS,
    )
    return aggregated.drop("tiene_recaudacion")


def build_audit_dashboard(dataframe: DataFrame) -> DataFrame:
    """Curta un dataset de auditoria para exportacion directa."""

    selected_columns = [F.col(column) for column in dataframe.columns]
    return dataframe.select(*selected_columns)


def build_revenue_source_detail(
    siaf_frames: list[DataFrame],
    map_sec_ejec_ubigeo: DataFrame,
    dim_municipality: DataFrame,
    dim_geography: DataFrame,
    dim_time: DataFrame,
) -> DataFrame:
    """Reconstruye detalle municipal con fuente/rubro/tipo desde Silver SIAF curado."""

    if not siaf_frames:
        raise DashboardExportError(
            "No hay datasets Silver SIAF para construir revenue_source_detail."
        )

    municipality_required = [
        "municipality_key",
        "ubigeo6",
        "geography_key",
        "municipalidad_nombre",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
    ]
    geography_required = [
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
    ]
    time_required = ["date_key", "anio_mes", "trimestre", "semestre"]
    require_columns(dim_municipality, municipality_required, "dim_municipality")
    require_columns(dim_geography, geography_required, "dim_geography")
    require_columns(dim_time, time_required, "dim_time")
    require_columns(map_sec_ejec_ubigeo, ["sec_ejec", "municipality_key", "match_status"], "map_sec_ejec_ubigeo")

    normalized_frames: list[DataFrame] = []
    for dataframe in siaf_frames:
        require_columns(dataframe, SIAF_SILVER_REQUIRED_COLUMNS, "silver.siaf_income")
        normalized_frames.append(
            dataframe.select(
                F.col("anio").cast("int").alias("anio"),
                F.col("mes").cast("int").alias("mes"),
                F.col("sec_ejec").cast("string").alias("sec_ejec"),
                F.col("ubigeo6_ejecutora").cast("string").alias("ubigeo6_ejecutora"),
                *[F.col(column).cast("string").alias(column) for column in REVENUE_SOURCE_COLUMNS],
                *[F.col(column).cast("string").alias(column) for column in REVENUE_CONCEPT_COLUMNS],
                F.col("source_resource_key").cast("string").alias("source_resource_key"),
                F.col("source_granularity").cast("string").alias("source_granularity"),
                F.col("monto_pia").cast("double").alias("monto_pia"),
                F.col("monto_pim").cast("double").alias("monto_pim"),
                F.col("monto_recaudado").cast("double").alias("monto_recaudado"),
            )
        )

    detail = normalized_frames[0]
    for dataframe in normalized_frames[1:]:
        detail = detail.unionByName(dataframe)

    municipality_lookup = dim_municipality.select(
        "municipality_key",
        "ubigeo6",
        "geography_key",
        "municipalidad_nombre",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
    ).dropDuplicates(["ubigeo6"])
    geography_lookup = dim_geography.select(
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
    ).dropDuplicates(["geography_key"])
    time_lookup = dim_time.select(
        "date_key",
        "anio_mes",
        "trimestre",
        "semestre",
    ).dropDuplicates(["date_key"])

    dim_validation = municipality_lookup.select(F.col("ubigeo6").alias("dim_ubigeo6"))
    resolution_map = build_siaf_resolution_map(map_sec_ejec_ubigeo)

    joined_dim = detail.join(
        dim_validation,
        detail.ubigeo6_ejecutora == F.col("dim_ubigeo6"),
        how="left",
    )
    is_primary_resolved = (
        F.col("ubigeo6_ejecutora").isNotNull()
        & F.col("ubigeo6_ejecutora").rlike(r"^[0-9]{6}$")
        & F.col("dim_ubigeo6").isNotNull()
    )
    resolved = derive_date_key(joined_dim).join(
        resolution_map.alias("fallback"), on="sec_ejec", how="left"
    )

    enriched = (
        resolved.withColumn(
            "municipality_key",
            F.when(is_primary_resolved, F.col("ubigeo6_ejecutora")).otherwise(
                F.col("fallback.municipality_key")
            ),
        )
        .withColumn(
            "has_municipality_match",
            F.when(is_primary_resolved, F.lit(True)).otherwise(
                F.coalesce(F.col("fallback.has_municipality_match"), F.lit(False))
            ),
        )
        .withColumn(
            "match_status",
            F.when(is_primary_resolved, F.lit("matched")).otherwise(
                F.coalesce(F.col("fallback.match_status"), F.lit("missing_map"))
            ),
        )
        .transform(apply_municipal_filters)
        .join(municipality_lookup, on="municipality_key", how="left")
        .join(geography_lookup, on="geography_key", how="left")
        .join(time_lookup, on="date_key", how="left")
    )

    return enriched.select(
        *MUNICIPAL_GRAIN_COLUMNS,
        *TIME_MONTHLY_COLUMNS,
        *REVENUE_SOURCE_COLUMNS,
        *REVENUE_CONCEPT_COLUMNS,
        "monto_pia",
        "monto_pim",
        "monto_recaudado",
        "source_resource_key",
        "source_granularity",
        "has_municipality_match",
        "match_status",
    )


def write_parquet_dataset(dataframe: DataFrame, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset parquet bajo la capa Gold/powerbi."""

    if output_path.exists():
        if not overwrite:
            raise DashboardExportError(
                f"La salida Gold ya existe: {output_path}. Use --overwrite para reemplazarla."
            )
        shutil.rmtree(output_path)
    dataframe.write.mode("overwrite").parquet(str(output_path))


def prepare_export_dataframe(dataframe: DataFrame) -> DataFrame:
    """Normaliza tipos Spark para exports amigables con Power BI."""

    prepared = dataframe
    for field in dataframe.schema.fields:
        column_name = field.name
        if isinstance(field.dataType, DecimalType):
            prepared = prepared.withColumn(column_name, F.col(column_name).cast("double"))
        elif isinstance(field.dataType, (DoubleType, FloatType)):
            prepared = prepared.withColumn(column_name, F.col(column_name).cast("double"))
        elif isinstance(field.dataType, DateType):
            prepared = prepared.withColumn(
                column_name,
                F.date_format(F.col(column_name), "yyyy-MM-dd"),
            )
        elif isinstance(field.dataType, TimestampType):
            prepared = prepared.withColumn(
                column_name,
                F.date_format(F.col(column_name), "yyyy-MM-dd'T'HH:mm:ss"),
            )
        elif isinstance(field.dataType, BooleanType):
            prepared = prepared.withColumn(
                column_name,
                F.when(F.col(column_name).isNull(), F.lit(None)).otherwise(
                    F.when(F.col(column_name), F.lit("true")).otherwise(F.lit("false"))
                ),
            )
    return prepared


def find_single_output_file(output_dir: Path, suffix: str) -> Path:
    """Localiza el archivo de datos unico generado por Spark."""

    candidates = sorted(
        path
        for path in output_dir.iterdir()
        if path.is_file() and path.name.startswith("part-") and path.name.endswith(suffix)
    )
    if not candidates:
        raise DashboardExportError(
            f"No se encontro archivo de salida {suffix} bajo {output_dir}."
        )
    return candidates[0]


def write_export_file(
    dataframe: DataFrame,
    output_path: Path,
    output_format: str,
    overwrite: bool,
) -> int:
    """Escribe un export final unico para Power BI y devuelve su tamano."""

    if output_path.name in BLOCKED_EXPORT_FILE_NAMES:
        raise DashboardExportError(
            f"Export bloqueado por contrato: {output_path.name}"
        )
    if output_path.exists():
        if not overwrite:
            raise DashboardExportError(
                f"El export ya existe: {output_path}. Use --overwrite para reemplazarlo."
            )
        if output_path.is_dir():
            shutil.rmtree(output_path)
        else:
            output_path.unlink()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    prepared = prepare_export_dataframe(dataframe)
    temp_dir = output_path.parent / f"__tmp_{output_path.stem}"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_format == "csv":
        (
            prepared.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .option("encoding", "UTF-8")
            .option("emptyValue", "")
            .csv(str(temp_dir))
        )
        data_file = find_single_output_file(temp_dir, ".csv")
    else:
        prepared.coalesce(1).write.mode("overwrite").parquet(str(temp_dir))
        data_file = find_single_output_file(temp_dir, ".parquet")
    shutil.move(str(data_file), str(output_path))
    shutil.rmtree(temp_dir)
    return output_path.stat().st_size


def build_source_summary(spark: Any, paths: DashboardExportPaths) -> dict[str, dict[str, Any]]:
    """Resume disponibilidad y columnas relevantes de las fuentes."""

    summary: dict[str, dict[str, Any]] = {}
    source_candidates = {
        "mart_municipal_revenue_overview": (
            paths.mart_municipal_revenue_overview_path,
            REVENUE_SOURCE_COLUMNS + MUNICIPAL_GRAIN_COLUMNS + TIME_MONTHLY_COLUMNS,
        ),
        "mart_predial_statistics_overview": (
            paths.mart_predial_statistics_overview_path,
            PREDIAL_CONTEXT_COLUMNS + ["anio_aplicacion", "periodo", "periodo_label"],
        ),
        "mart_municipal_context": (
            paths.mart_municipal_context_path,
            MUNICIPAL_CONTEXT_SELECTED_COLUMNS + MUNICIPAL_GRAIN_COLUMNS,
        ),
        "audit_dataset_summary": (
            paths.audit_dataset_summary_path,
            [],
        ),
        "audit_integration_coverage": (
            paths.audit_integration_coverage_path,
            [],
        ),
        "audit_quality_results": (
            paths.audit_quality_results_path,
            [],
        ),
        "silver_siaf_income": (
            paths.silver_siaf_income_root,
            REVENUE_SOURCE_COLUMNS + REVENUE_CONCEPT_COLUMNS + ["sec_ejec", "ubigeo6_ejecutora"],
        ),
    }

    for source_name, (path, relevant_columns) in source_candidates.items():
        exists = path.exists()
        item: dict[str, Any] = {"path": str(path), "exists": exists}
        if exists:
            if source_name == "silver_siaf_income":
                resource_paths = list_siaf_resource_paths(path)
                item["resources"] = len(resource_paths)
                if resource_paths:
                    dataframe = read_parquet_dataset(spark, resource_paths[0])
                    item["relevant_columns"] = existing_columns(
                        dataframe.columns, relevant_columns
                    )
                    item["missing_relevant_columns"] = missing_columns(
                        dataframe.columns, relevant_columns
                    )
            else:
                dataframe = read_parquet_dataset(spark, path)
                item["relevant_columns"] = existing_columns(dataframe.columns, relevant_columns)
                item["missing_relevant_columns"] = missing_columns(
                    dataframe.columns, relevant_columns
                )
        summary[source_name] = item
    return summary


def print_dry_run_plan(
    paths: DashboardExportPaths,
    datasets: list[str],
    output_format: str,
    source_summary: dict[str, dict[str, Any]],
) -> None:
    """Imprime el plan de construccion dashboard-ready."""

    print("=" * 80)
    print("Plan dashboard-ready Power BI")
    print(f"Salida Gold agregada: {paths.output_root}")
    print(f"Salida export Power BI: {paths.export_root}")
    print(f"Formato export final: {output_format}")
    print("Advertencia: no se exportaran facts crudas ni marts SIAF crudos como CSV unico.")
    print("Fuentes detectadas:")
    for source_name, details in source_summary.items():
        print(f"- {source_name}: existe={details['exists']} ruta={details['path']}")
        relevant_columns = details.get("relevant_columns")
        if relevant_columns is not None:
            print(f"  columnas_relevantes={relevant_columns}")
        missing_relevant_columns = details.get("missing_relevant_columns")
        if missing_relevant_columns:
            print(f"  advertencia_faltantes={missing_relevant_columns}")
        if "resources" in details:
            print(f"  recursos_siaf={details['resources']}")
    print("Datasets dashboard-ready a crear:")
    for dataset_name in datasets:
        spec = DATASET_REGISTRY[dataset_name]
        print(f"- {dataset_name}: {spec.description}")
        print(f"  gold={output_dataset_path(paths.output_root, dataset_name)}")
        print(f"  export={export_dataset_path(paths.export_root, dataset_name, output_format)}")
    print(
        "Advertencia tecnica: revenue_source_* usa enriquecimiento desde Silver SIAF curado "
        "porque el Gold actual no retiene fuente/rubro/tipo_recurso."
    )


def build_dashboard_export_marts(
    *,
    paths: DashboardExportPaths | None = None,
    selected_datasets: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    output_format: str = "csv",
) -> dict[str, DatasetBuildResult]:
    """Construye datasets dashboard-ready y exports finales para Power BI."""

    resolved_paths = paths or default_paths()
    datasets = validate_selected_datasets(selected_datasets)
    export_format = validate_output_format(output_format)
    validate_input_paths(resolved_paths, datasets)

    spark = build_spark_session(
        app_name="powerbi-dashboard-export-marts",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "8"},
    )

    try:
        source_summary = build_source_summary(spark, resolved_paths)
        if dry_run:
            print_dry_run_plan(resolved_paths, datasets, export_format, source_summary)

        mart_revenue = read_parquet_dataset(spark, resolved_paths.mart_municipal_revenue_overview_path)
        predial_mart = read_parquet_dataset(spark, resolved_paths.mart_predial_statistics_overview_path)
        context_mart = read_parquet_dataset(spark, resolved_paths.mart_municipal_context_path)
        audit_dataset_summary = read_parquet_dataset(spark, resolved_paths.audit_dataset_summary_path)
        audit_integration_coverage = read_parquet_dataset(spark, resolved_paths.audit_integration_coverage_path)
        audit_quality_results = read_parquet_dataset(spark, resolved_paths.audit_quality_results_path)

        revenue_monthly = build_revenue_monthly_dashboard(mart_revenue).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        revenue_annual = build_revenue_annual_dashboard(revenue_monthly).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        territorial_revenue = build_territorial_revenue_dashboard(revenue_annual).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        predial_dashboard = build_predial_dashboard(predial_mart).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        municipal_context = build_municipal_context_dashboard(context_mart).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        municipal_performance = build_municipal_performance_dashboard(
            revenue_annual, predial_dashboard, municipal_context
        ).persist(StorageLevel.MEMORY_AND_DISK)

        revenue_source_detail: DataFrame | None = None
        revenue_source_monthly: DataFrame | None = None
        revenue_source_annual: DataFrame | None = None
        revenue_concept_monthly: DataFrame | None = None
        revenue_source_warnings = [
            "mart_municipal_revenue_overview no conserva fuente/rubro/tipo_recurso; "
            "se usa Silver SIAF curado como enriquecimiento tecnico para estos datasets."
        ]

        if any(
            dataset in datasets
            for dataset in [
                "revenue_source_monthly_dashboard",
                "revenue_source_annual_dashboard",
                "revenue_concept_monthly_dashboard",
            ]
        ):
            dim_municipality = read_parquet_dataset(spark, resolved_paths.dim_municipality_path)
            dim_geography = read_parquet_dataset(spark, resolved_paths.dim_geography_path)
            dim_time = read_parquet_dataset(spark, resolved_paths.dim_time_path)
            map_sec_ejec_ubigeo = read_parquet_dataset(
                spark, resolved_paths.map_sec_ejec_ubigeo_path
            )
            siaf_frames = [
                read_parquet_dataset(spark, path)
                for path in list_siaf_resource_paths(resolved_paths.silver_siaf_income_root)
            ]
            revenue_source_detail = build_revenue_source_detail(
                siaf_frames,
                map_sec_ejec_ubigeo,
                dim_municipality,
                dim_geography,
                dim_time,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            revenue_source_monthly = build_revenue_source_monthly_dashboard(
                revenue_source_detail
            ).persist(StorageLevel.MEMORY_AND_DISK)
            revenue_source_annual = build_revenue_source_annual_dashboard(
                revenue_source_monthly
            ).persist(StorageLevel.MEMORY_AND_DISK)
            revenue_concept_monthly = build_revenue_concept_monthly_dashboard(
                revenue_source_detail
            ).persist(StorageLevel.MEMORY_AND_DISK)

        outputs: dict[str, tuple[DataFrame, list[str]]] = {
            "revenue_monthly_dashboard": (revenue_monthly, []),
            "revenue_annual_dashboard": (revenue_annual, []),
            "territorial_revenue_dashboard": (territorial_revenue, []),
            "predial_dashboard": (predial_dashboard, []),
            "municipal_context_dashboard": (municipal_context, []),
            "municipal_performance_dashboard": (municipal_performance, []),
            "audit_dataset_summary_dashboard": (
                build_audit_dashboard(audit_dataset_summary),
                [],
            ),
            "audit_integration_coverage_dashboard": (
                build_audit_dashboard(audit_integration_coverage),
                [],
            ),
            "audit_quality_results_dashboard": (
                build_audit_dashboard(audit_quality_results),
                [],
            ),
        }
        if revenue_source_monthly is not None and revenue_source_annual is not None:
            outputs["revenue_source_monthly_dashboard"] = (
                revenue_source_monthly,
                revenue_source_warnings,
            )
            outputs["revenue_source_annual_dashboard"] = (
                revenue_source_annual,
                revenue_source_warnings,
            )
        if revenue_concept_monthly is not None:
            outputs["revenue_concept_monthly_dashboard"] = (
                revenue_concept_monthly,
                revenue_source_warnings,
            )

        results: dict[str, DatasetBuildResult] = {}
        resolved_paths.output_root.mkdir(parents=True, exist_ok=True)
        resolved_paths.export_root.mkdir(parents=True, exist_ok=True)

        for dataset_name in datasets:
            if dataset_name == "revenue_concept_monthly_dashboard" and revenue_concept_monthly is not None:
                concept_rows = int(revenue_concept_monthly.count())
                if concept_rows > MAX_OPTIONAL_CONCEPT_CSV_ROWS:
                    gold_output = output_dataset_path(resolved_paths.output_root, dataset_name)
                    export_output = export_dataset_path(
                        resolved_paths.export_root, dataset_name, export_format
                    )
                    if not dry_run:
                        write_parquet_dataset(revenue_concept_monthly, gold_output, overwrite)
                    results[dataset_name] = DatasetBuildResult(
                        dataset_name=dataset_name,
                        gold_output_path=gold_output,
                        export_output_path=export_output,
                        row_count=concept_rows,
                        columns=revenue_concept_monthly.columns,
                        export_written=False,
                        export_size_bytes=None,
                        skipped=True,
                        skip_reason=(
                            "CSV omitido por volumen alto; el dataset opcional se conserva solo en Gold."
                        ),
                        warnings=revenue_source_warnings,
                    )
                    continue

            dataframe, warnings = outputs[dataset_name]
            row_count = int(dataframe.count())
            gold_output = output_dataset_path(resolved_paths.output_root, dataset_name)
            export_output = export_dataset_path(
                resolved_paths.export_root, dataset_name, export_format
            )

            export_size = None
            export_written = False
            if not dry_run:
                write_parquet_dataset(dataframe, gold_output, overwrite)
                export_size = write_export_file(
                    dataframe,
                    export_output,
                    export_format,
                    overwrite,
                )
                export_written = True

            results[dataset_name] = DatasetBuildResult(
                dataset_name=dataset_name,
                gold_output_path=gold_output,
                export_output_path=export_output,
                row_count=row_count,
                columns=dataframe.columns,
                export_written=export_written,
                export_size_bytes=export_size,
                warnings=warnings,
            )

        print("=" * 80)
        print("Datasets dashboard-ready")
        for dataset_name in datasets:
            result = results[dataset_name]
            print(f"- {dataset_name}: filas={result.row_count}")
            print(f"  gold={result.gold_output_path}")
            print(f"  export={result.export_output_path}")
            print(f"  columnas={result.columns}")
            if result.export_size_bytes is not None:
                print(f"  export_size_bytes={result.export_size_bytes}")
            if result.skipped and result.skip_reason:
                print(f"  aviso={result.skip_reason}")
            for warning in result.warnings or []:
                print(f"  warning={warning}")

        print("Confirmaciones de contrato:")
        print("- fact_siaf_income cruda no se exporto.")
        print("- mart_municipal_revenue_overview crudo no se exporto como CSV unico.")
        print("- revenue_monthly_dashboard queda agregado y liviano frente al mart SIAF detallado.")
        if "revenue_source_monthly_dashboard" in results:
            print("- revenue_source_monthly_dashboard conserva fuente/rubro/tipo_recurso.")
        print(f"- powerbi/exports/ permanece ignorado por Git en {POWERBI_DIR / 'exports'}.")

        return results
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI del builder dashboard-ready."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=ALL_DASHBOARD_DATASETS,
        help="Dataset dashboard-ready a construir. Puede repetirse.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Construye y cuenta DataFrames sin escribir salidas.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Reemplaza salidas Gold/export existentes.",
    )
    parser.add_argument(
        "--output-format",
        default="csv",
        choices=["csv", "parquet"],
        help="Formato del export final para Power BI. Gold agregado siempre se escribe en Parquet.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    build_dashboard_export_marts(
        selected_datasets=args.dataset,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        output_format=args.output_format,
    )


if __name__ == "__main__":
    main()
