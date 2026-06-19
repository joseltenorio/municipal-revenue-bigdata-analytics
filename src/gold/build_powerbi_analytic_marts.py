"""Construccion de marts analiticos Gold para consumo Power BI.

Este modulo materializa marts planos usando solo dimensiones y facts Gold.
No construye auditoria Gold, no registra Hive y no exporta artefactos Power BI.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.paths import GOLD_DIR
from src.common.spark_session import build_spark_session
from src.gold.build_municipal_dimensions import RENAMU_CONTEXT_COLUMNS


GOLD_MART_DATASETS = [
    "mart_municipal_revenue_overview",
    "mart_predial_statistics_overview",
    "mart_municipal_context",
    "mart_territorial_summary",
]

MUNICIPALITY_REQUIRED_COLUMNS = [
    "municipality_key",
    "ubigeo6",
    "geography_key",
    "municipalidad_nombre",
    "tipomuni_codigo",
    "tipomuni_nombre",
    "tipo_clasificacion_municipal",
    "ambito_municipal",
    "descripcion_tipo",
]

GEOGRAPHY_REQUIRED_COLUMNS = [
    "geography_key",
    "ubigeo6",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre",
]

TIME_REQUIRED_COLUMNS = [
    "date_key",
    "fecha_mes",
    "anio",
    "mes",
    "anio_mes",
    "trimestre",
    "semestre",
]

SISMEPRE_PERIOD_REQUIRED_COLUMNS = [
    "sismepre_period_key",
    "anio_aplicacion",
    "periodo",
    "anio_estadistica",
    "mes_estadistica",
    "periodo_estadistica_tipo",
    "is_annual_stat_period",
    "periodo_label",
]

FACT_SIAF_REQUIRED_COLUMNS = [
    "municipality_key",
    "sec_ejec",
    "date_key",
    "source_resource_key",
    "source_granularity",
    "monto_pia",
    "monto_pim",
    "monto_recaudado",
    "has_municipality_match",
    "match_status",
]

FACT_PREDIAL_REQUIRED_COLUMNS = [
    "municipality_key",
    "sismepre_period_key",
    "sec_ejec",
    "ubigeo6",
    "formulario_id",
    "monto_emision_predial_total",
    "monto_recaudacion_predial_total",
    "monto_saldo_predial_total",
    "ratio_recaudacion_emision",
    "numero_predios_total",
    "numero_contribuyentes_predio",
]

TERRITORIAL_SUMMARY_CONTEXT_COLUMNS = [
    "usa_siaf",
    "usa_sistema_recaudacion_tributaria_municipal",
    "usa_sistema_catastro",
    "cuenta_servicio_internet",
    "total_computadoras_operativas",
    "total_personal_dic_2021",
    "total_personal_mar_2022",
]


class GoldMartError(ValueError):
    """Error de contrato para marts Gold."""


@dataclass(frozen=True)
class GoldMartPaths:
    """Rutas fisicas de entrada y salida para marts Gold."""

    output_root: Path
    dim_municipality_path: Path
    dim_geography_path: Path
    dim_renamu_context_path: Path
    dim_time_path: Path
    dim_sismepre_period_path: Path
    fact_siaf_income_path: Path
    fact_predial_statistics_path: Path


def default_paths() -> GoldMartPaths:
    """Devuelve rutas Gold vigentes para marts analiticos."""

    return GoldMartPaths(
        output_root=GOLD_DIR,
        dim_municipality_path=GOLD_DIR / "dim_municipality",
        dim_geography_path=GOLD_DIR / "dim_geography",
        dim_renamu_context_path=GOLD_DIR / "dim_renamu_context",
        dim_time_path=GOLD_DIR / "dim_time",
        dim_sismepre_period_path=GOLD_DIR / "dim_sismepre_period",
        fact_siaf_income_path=GOLD_DIR / "fact_siaf_income",
        fact_predial_statistics_path=GOLD_DIR / "fact_predial_statistics",
    )


def utc_now_iso() -> str:
    """Retorna timestamp UTC estable para metadata Gold."""

    return datetime.now(timezone.utc).isoformat()


def existing_columns(available_columns: list[str], desired_columns: list[str]) -> list[str]:
    """Conserva el orden deseado filtrando columnas existentes."""

    available = set(available_columns)
    return [column for column in desired_columns if column in available]


def missing_columns(available_columns: list[str], required_columns: list[str]) -> list[str]:
    """Retorna columnas faltantes en un DataFrame."""

    available = set(available_columns)
    return [column for column in required_columns if column not in available]


def require_columns(dataframe: DataFrame, required_columns: list[str], dataset_name: str) -> None:
    """Falla rapido cuando un dataset Gold no cumple contrato minimo."""

    missing = missing_columns(dataframe.columns, required_columns)
    if missing:
        raise GoldMartError(f"{dataset_name} no tiene columnas requeridas: {missing}")


def validate_selected_datasets(selected_datasets: list[str] | None) -> list[str]:
    """Valida marts seleccionados desde CLI."""

    if not selected_datasets:
        return GOLD_MART_DATASETS

    unsupported = [
        dataset for dataset in selected_datasets if dataset not in GOLD_MART_DATASETS
    ]
    if unsupported:
        supported = ", ".join(GOLD_MART_DATASETS)
        raise GoldMartError(
            f"Datasets Gold no soportados: {unsupported}. Soportados: {supported}."
        )

    return selected_datasets


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta fisica de un mart Gold soportado."""

    validate_selected_datasets([dataset_name])
    return output_root / dataset_name


def read_parquet_dataset(spark: Any, path: Path, limit: int | None = None) -> DataFrame:
    """Lee Parquet Gold con limite opcional para pruebas locales."""

    dataframe = spark.read.parquet(str(path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def required_input_paths(paths: GoldMartPaths, datasets: list[str]) -> list[Path]:
    """Devuelve entradas Gold requeridas por los marts seleccionados."""

    required: list[Path] = []
    if "mart_municipal_revenue_overview" in datasets:
        required.extend(
            [
                paths.fact_siaf_income_path,
                paths.dim_municipality_path,
                paths.dim_geography_path,
                paths.dim_time_path,
            ]
        )
    if "mart_predial_statistics_overview" in datasets:
        required.extend(
            [
                paths.fact_predial_statistics_path,
                paths.dim_municipality_path,
                paths.dim_geography_path,
                paths.dim_sismepre_period_path,
            ]
        )
    if "mart_municipal_context" in datasets or "mart_territorial_summary" in datasets:
        required.extend(
            [
                paths.dim_municipality_path,
                paths.dim_geography_path,
                paths.dim_renamu_context_path,
            ]
        )

    return sorted(set(required))


def validate_input_paths(paths: GoldMartPaths, datasets: list[str]) -> None:
    """Valida existencia minima de entradas Gold antes de construir marts."""

    missing = [path for path in required_input_paths(paths, datasets) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"No existen entradas Gold requeridas: {missing}")


def gold_processed_at_column(*column_names: str) -> Any:
    """Consolida metadata Gold existente preservando la primera no nula."""

    expressions = [F.col(column_name) for column_name in column_names if column_name]
    if not expressions:
        return F.lit(None)
    return F.coalesce(*expressions)


def build_mart_municipal_revenue_overview(
    fact_siaf_income: DataFrame,
    dim_municipality: DataFrame,
    dim_geography: DataFrame,
    dim_time: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye mart plano de ingresos municipales usando Gold facts y dims."""

    require_columns(fact_siaf_income, FACT_SIAF_REQUIRED_COLUMNS, "fact_siaf_income")
    require_columns(dim_municipality, MUNICIPALITY_REQUIRED_COLUMNS, "dim_municipality")
    require_columns(dim_geography, GEOGRAPHY_REQUIRED_COLUMNS, "dim_geography")
    require_columns(dim_time, TIME_REQUIRED_COLUMNS, "dim_time")
    processed_at = processed_at_utc or utc_now_iso()

    municipality = dim_municipality.select(
        "municipality_key",
        "ubigeo6",
        "geography_key",
        "municipalidad_nombre",
        "tipomuni_codigo",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        F.col("gold_processed_at_utc").alias("municipality_gold_processed_at_utc"),
    )
    geography = dim_geography.select(
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        F.col("gold_processed_at_utc").alias("geography_gold_processed_at_utc"),
    )
    time_dimension = dim_time.select(
        "date_key",
        "fecha_mes",
        "anio",
        "mes",
        "anio_mes",
        "trimestre",
        "semestre",
        F.col("gold_processed_at_utc").alias("time_gold_processed_at_utc"),
    )

    return (
        fact_siaf_income.alias("fact")
        .join(municipality.alias("municipality"), on="municipality_key", how="left")
        .join(
            geography.alias("geography"),
            on=F.col("municipality.geography_key") == F.col("geography.geography_key"),
            how="left",
        )
        .join(time_dimension.alias("time"), on="date_key", how="left")
        .withColumn(
            "gold_processed_at_utc",
            F.coalesce(
                gold_processed_at_column(
                    "fact.gold_processed_at_utc",
                    "municipality.municipality_gold_processed_at_utc",
                    "geography.geography_gold_processed_at_utc",
                    "time.time_gold_processed_at_utc",
                ),
                F.lit(processed_at),
            ),
        )
        .select(
            F.col("municipality_key"),
            F.col("municipality.ubigeo6").alias("ubigeo6"),
            F.col("municipality.municipalidad_nombre").alias("municipalidad_nombre"),
            F.col("municipality.geography_key").alias("geography_key"),
            F.col("geography.departamento_nombre").alias("departamento_nombre"),
            F.col("geography.provincia_nombre").alias("provincia_nombre"),
            F.col("geography.distrito_nombre").alias("distrito_nombre"),
            F.col("municipality.tipomuni_codigo").alias("tipomuni_codigo"),
            F.col("municipality.tipomuni_nombre").alias("tipomuni_nombre"),
            F.col("municipality.tipo_clasificacion_municipal").alias(
                "tipo_clasificacion_municipal"
            ),
            F.col("municipality.ambito_municipal").alias("ambito_municipal"),
            F.col("municipality.descripcion_tipo").alias("descripcion_tipo"),
            F.col("date_key"),
            F.col("time.fecha_mes").alias("fecha_mes"),
            F.col("time.anio").alias("anio"),
            F.col("time.mes").alias("mes"),
            F.col("time.anio_mes").alias("anio_mes"),
            F.col("time.trimestre").alias("trimestre"),
            F.col("time.semestre").alias("semestre"),
            F.col("fact.sec_ejec").alias("sec_ejec"),
            F.col("fact.source_resource_key").alias("source_resource_key"),
            F.col("fact.source_granularity").alias("source_granularity"),
            F.col("fact.monto_pia").alias("monto_pia"),
            F.col("fact.monto_pim").alias("monto_pim"),
            F.col("fact.monto_recaudado").alias("monto_recaudado"),
            F.col("fact.has_municipality_match").alias("has_municipality_match"),
            F.col("fact.match_status").alias("match_status"),
            F.col("gold_processed_at_utc"),
        )
    )


def build_mart_predial_statistics_overview(
    fact_predial_statistics: DataFrame,
    dim_municipality: DataFrame,
    dim_geography: DataFrame,
    dim_sismepre_period: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye mart plano predial usando fact Gold y dimensiones Gold."""

    require_columns(
        fact_predial_statistics, FACT_PREDIAL_REQUIRED_COLUMNS, "fact_predial_statistics"
    )
    require_columns(dim_municipality, MUNICIPALITY_REQUIRED_COLUMNS, "dim_municipality")
    require_columns(dim_geography, GEOGRAPHY_REQUIRED_COLUMNS, "dim_geography")
    require_columns(
        dim_sismepre_period, SISMEPRE_PERIOD_REQUIRED_COLUMNS, "dim_sismepre_period"
    )
    processed_at = processed_at_utc or utc_now_iso()

    municipality = dim_municipality.select(
        "municipality_key",
        "ubigeo6",
        "geography_key",
        "municipalidad_nombre",
        "tipomuni_codigo",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        F.col("gold_processed_at_utc").alias("municipality_gold_processed_at_utc"),
    )
    geography = dim_geography.select(
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        F.col("gold_processed_at_utc").alias("geography_gold_processed_at_utc"),
    )
    period_dimension = dim_sismepre_period.select(
        "sismepre_period_key",
        "anio_aplicacion",
        "periodo",
        "anio_estadistica",
        "mes_estadistica",
        "periodo_estadistica_tipo",
        "is_annual_stat_period",
        "periodo_label",
        F.col("gold_processed_at_utc").alias("period_gold_processed_at_utc"),
    )

    return (
        fact_predial_statistics.alias("fact")
        .join(municipality.alias("municipality"), on="municipality_key", how="left")
        .join(
            geography.alias("geography"),
            on=F.col("municipality.geography_key") == F.col("geography.geography_key"),
            how="left",
        )
        .join(period_dimension.alias("period"), on="sismepre_period_key", how="left")
        .withColumn(
            "gold_processed_at_utc",
            F.coalesce(
                gold_processed_at_column(
                    "fact.gold_processed_at_utc",
                    "municipality.municipality_gold_processed_at_utc",
                    "geography.geography_gold_processed_at_utc",
                    "period.period_gold_processed_at_utc",
                ),
                F.lit(processed_at),
            ),
        )
        .select(
            F.col("municipality_key"),
            F.col("fact.ubigeo6").alias("ubigeo6"),
            F.col("municipality.municipalidad_nombre").alias("municipalidad_nombre"),
            F.col("municipality.geography_key").alias("geography_key"),
            F.col("geography.departamento_nombre").alias("departamento_nombre"),
            F.col("geography.provincia_nombre").alias("provincia_nombre"),
            F.col("geography.distrito_nombre").alias("distrito_nombre"),
            F.col("municipality.tipomuni_codigo").alias("tipomuni_codigo"),
            F.col("municipality.tipomuni_nombre").alias("tipomuni_nombre"),
            F.col("municipality.tipo_clasificacion_municipal").alias(
                "tipo_clasificacion_municipal"
            ),
            F.col("municipality.ambito_municipal").alias("ambito_municipal"),
            F.col("municipality.descripcion_tipo").alias("descripcion_tipo"),
            F.col("sismepre_period_key"),
            F.col("period.anio_aplicacion").alias("anio_aplicacion"),
            F.col("period.periodo").alias("periodo"),
            F.col("period.anio_estadistica").alias("anio_estadistica"),
            F.col("period.mes_estadistica").alias("mes_estadistica"),
            F.col("period.periodo_estadistica_tipo").alias("periodo_estadistica_tipo"),
            F.col("period.is_annual_stat_period").alias("is_annual_stat_period"),
            F.col("period.periodo_label").alias("periodo_label"),
            F.col("fact.sec_ejec").alias("sec_ejec"),
            F.col("fact.formulario_id").alias("formulario_id"),
            F.col("fact.monto_emision_predial_total").alias("monto_emision_predial_total"),
            F.col("fact.monto_recaudacion_predial_total").alias(
                "monto_recaudacion_predial_total"
            ),
            F.col("fact.monto_saldo_predial_total").alias("monto_saldo_predial_total"),
            F.col("fact.ratio_recaudacion_emision").alias("ratio_recaudacion_emision"),
            F.col("fact.numero_predios_total").alias("numero_predios_total"),
            F.col("fact.numero_contribuyentes_predio").alias(
                "numero_contribuyentes_predio"
            ),
            F.col("gold_processed_at_utc"),
        )
    )


def build_mart_municipal_context(
    dim_municipality: DataFrame,
    dim_geography: DataFrame,
    dim_renamu_context: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye mart plano de contexto municipal desde dimensiones Gold."""

    require_columns(dim_municipality, MUNICIPALITY_REQUIRED_COLUMNS, "dim_municipality")
    require_columns(dim_geography, GEOGRAPHY_REQUIRED_COLUMNS, "dim_geography")
    require_columns(dim_renamu_context, ["municipality_key", "ubigeo6"], "dim_renamu_context")
    processed_at = processed_at_utc or utc_now_iso()

    renamu_columns = existing_columns(dim_renamu_context.columns, RENAMU_CONTEXT_COLUMNS)

    municipality = dim_municipality.select(
        "municipality_key",
        "ubigeo6",
        "geography_key",
        "municipalidad_nombre",
        "tipomuni_codigo",
        "tipomuni_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "descripcion_tipo",
        F.col("gold_processed_at_utc").alias("municipality_gold_processed_at_utc"),
    )
    geography = dim_geography.select(
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        F.col("gold_processed_at_utc").alias("geography_gold_processed_at_utc"),
    )
    renamu_context = dim_renamu_context.select(
        "municipality_key",
        *renamu_columns,
        F.col("gold_processed_at_utc").alias("renamu_gold_processed_at_utc"),
    )

    return (
        municipality.alias("municipality")
        .join(
            geography.alias("geography"),
            on=F.col("municipality.geography_key") == F.col("geography.geography_key"),
            how="left",
        )
        .join(renamu_context.alias("renamu"), on="municipality_key", how="left")
        .withColumn(
            "gold_processed_at_utc",
            F.coalesce(
                gold_processed_at_column(
                    "municipality.municipality_gold_processed_at_utc",
                    "geography.geography_gold_processed_at_utc",
                    "renamu.renamu_gold_processed_at_utc",
                ),
                F.lit(processed_at),
            ),
        )
        .select(
            F.col("municipality_key"),
            F.col("municipality.ubigeo6").alias("ubigeo6"),
            F.col("municipality.municipalidad_nombre").alias("municipalidad_nombre"),
            F.col("municipality.geography_key").alias("geography_key"),
            F.col("geography.departamento_nombre").alias("departamento_nombre"),
            F.col("geography.provincia_nombre").alias("provincia_nombre"),
            F.col("geography.distrito_nombre").alias("distrito_nombre"),
            F.col("municipality.tipomuni_codigo").alias("tipomuni_codigo"),
            F.col("municipality.tipomuni_nombre").alias("tipomuni_nombre"),
            F.col("municipality.tipo_clasificacion_municipal").alias(
                "tipo_clasificacion_municipal"
            ),
            F.col("municipality.ambito_municipal").alias("ambito_municipal"),
            F.col("municipality.descripcion_tipo").alias("descripcion_tipo"),
            *[F.col(f"renamu.{column}").alias(column) for column in renamu_columns],
            F.col("gold_processed_at_utc"),
        )
        .dropDuplicates(["municipality_key"])
    )


def build_mart_territorial_summary(
    mart_municipal_context: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye resumen territorial estable desde el mart de contexto."""

    context_required = [
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "municipality_key",
    ]
    require_columns(mart_municipal_context, context_required, "mart_municipal_context")
    processed_at = processed_at_utc or utc_now_iso()

    available_context_columns = existing_columns(
        mart_municipal_context.columns, TERRITORIAL_SUMMARY_CONTEXT_COLUMNS
    )
    base = mart_municipal_context.select(
        "geography_key",
        "departamento_nombre",
        "provincia_nombre",
        "distrito_nombre",
        "tipo_clasificacion_municipal",
        "ambito_municipal",
        "municipality_key",
        *available_context_columns,
    ).dropDuplicates(["municipality_key"])

    return (
        base.groupBy(
            "geography_key",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
            "tipo_clasificacion_municipal",
            "ambito_municipal",
        )
        .agg(
            F.countDistinct("municipality_key").alias("total_municipalidades"),
            F.sum(F.when(F.col("usa_siaf"), F.lit(1)).otherwise(F.lit(0))).cast("int").alias(
                "municipalidades_con_siaf"
            )
            if "usa_siaf" in available_context_columns
            else F.lit(None).cast("int").alias("municipalidades_con_siaf"),
            F.sum(
                F.when(
                    F.col("usa_sistema_recaudacion_tributaria_municipal"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("int")
            .alias("municipalidades_con_sistema_recaudacion")
            if "usa_sistema_recaudacion_tributaria_municipal" in available_context_columns
            else F.lit(None).cast("int").alias("municipalidades_con_sistema_recaudacion"),
            F.sum(F.when(F.col("usa_sistema_catastro"), F.lit(1)).otherwise(F.lit(0))).cast(
                "int"
            ).alias("municipalidades_con_catastro")
            if "usa_sistema_catastro" in available_context_columns
            else F.lit(None).cast("int").alias("municipalidades_con_catastro"),
            F.sum(F.when(F.col("cuenta_servicio_internet"), F.lit(1)).otherwise(F.lit(0))).cast(
                "int"
            ).alias("municipalidades_con_internet")
            if "cuenta_servicio_internet" in available_context_columns
            else F.lit(None).cast("int").alias("municipalidades_con_internet"),
            F.sum(F.coalesce(F.col("total_computadoras_operativas"), F.lit(0))).cast("long").alias(
                "total_computadoras_operativas"
            )
            if "total_computadoras_operativas" in available_context_columns
            else F.lit(None).cast("long").alias("total_computadoras_operativas"),
            F.sum(F.coalesce(F.col("total_personal_dic_2021"), F.lit(0))).cast("long").alias(
                "total_personal_dic_2021"
            )
            if "total_personal_dic_2021" in available_context_columns
            else F.lit(None).cast("long").alias("total_personal_dic_2021"),
            F.sum(F.coalesce(F.col("total_personal_mar_2022"), F.lit(0))).cast("long").alias(
                "total_personal_mar_2022"
            )
            if "total_personal_mar_2022" in available_context_columns
            else F.lit(None).cast("long").alias("total_personal_mar_2022"),
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
    )


def write_dataset(dataframe: DataFrame, output_path: Path, overwrite: bool) -> None:
    """Escribe un mart Gold evitando sobrescritura accidental."""

    if output_path.exists():
        if not overwrite:
            raise GoldMartError(
                f"La salida ya existe: {output_path}. Use --overwrite para reemplazarla."
            )
        shutil.rmtree(output_path)

    dataframe.write.mode("overwrite").parquet(str(output_path))


def build_gold_powerbi_analytic_marts(
    *,
    paths: GoldMartPaths | None = None,
    selected_datasets: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Construye fisicamente los marts Gold seleccionados."""

    resolved_paths = paths or default_paths()
    datasets = validate_selected_datasets(selected_datasets)
    validate_input_paths(resolved_paths, datasets)

    spark = build_spark_session(app_name="gold-powerbi-analytic-marts")
    outputs: dict[str, DataFrame] = {}
    try:
        dim_municipality: DataFrame | None = None
        dim_geography: DataFrame | None = None
        dim_renamu_context: DataFrame | None = None
        dim_time: DataFrame | None = None
        dim_sismepre_period: DataFrame | None = None
        fact_siaf_income: DataFrame | None = None
        fact_predial_statistics: DataFrame | None = None

        if any(
            dataset in datasets
            for dataset in [
                "mart_municipal_revenue_overview",
                "mart_predial_statistics_overview",
                "mart_municipal_context",
                "mart_territorial_summary",
            ]
        ):
            dim_municipality = read_parquet_dataset(
                spark, resolved_paths.dim_municipality_path, limit
            )
            dim_geography = read_parquet_dataset(spark, resolved_paths.dim_geography_path, limit)

        if "mart_municipal_context" in datasets or "mart_territorial_summary" in datasets:
            dim_renamu_context = read_parquet_dataset(
                spark, resolved_paths.dim_renamu_context_path, limit
            )
        if "mart_municipal_revenue_overview" in datasets:
            fact_siaf_income = read_parquet_dataset(
                spark, resolved_paths.fact_siaf_income_path, limit
            )
            dim_time = read_parquet_dataset(spark, resolved_paths.dim_time_path, limit)
        if "mart_predial_statistics_overview" in datasets:
            fact_predial_statistics = read_parquet_dataset(
                spark, resolved_paths.fact_predial_statistics_path, limit
            )
            dim_sismepre_period = read_parquet_dataset(
                spark, resolved_paths.dim_sismepre_period_path, limit
            )

        if "mart_municipal_revenue_overview" in datasets:
            if (
                fact_siaf_income is None
                or dim_municipality is None
                or dim_geography is None
                or dim_time is None
            ):
                raise GoldMartError("mart_municipal_revenue_overview requiere fact y dims Gold.")
            outputs["mart_municipal_revenue_overview"] = build_mart_municipal_revenue_overview(
                fact_siaf_income,
                dim_municipality,
                dim_geography,
                dim_time,
            )

        if "mart_predial_statistics_overview" in datasets:
            if (
                fact_predial_statistics is None
                or dim_municipality is None
                or dim_geography is None
                or dim_sismepre_period is None
            ):
                raise GoldMartError(
                    "mart_predial_statistics_overview requiere fact y dims Gold."
                )
            outputs["mart_predial_statistics_overview"] = (
                build_mart_predial_statistics_overview(
                    fact_predial_statistics,
                    dim_municipality,
                    dim_geography,
                    dim_sismepre_period,
                )
            )

        if "mart_municipal_context" in datasets or "mart_territorial_summary" in datasets:
            if (
                dim_municipality is None
                or dim_geography is None
                or dim_renamu_context is None
            ):
                raise GoldMartError("Los marts de contexto requieren dimensiones Gold.")
            outputs["mart_municipal_context"] = build_mart_municipal_context(
                dim_municipality,
                dim_geography,
                dim_renamu_context,
            )

        if "mart_territorial_summary" in datasets:
            outputs["mart_territorial_summary"] = build_mart_territorial_summary(
                outputs["mart_municipal_context"]
            )

        if "mart_municipal_context" not in datasets and "mart_municipal_context" in outputs:
            outputs.pop("mart_municipal_context")

        row_counts = {dataset: dataframe.count() for dataset, dataframe in outputs.items()}
        if dry_run:
            return row_counts

        resolved_paths.output_root.mkdir(parents=True, exist_ok=True)
        for dataset, dataframe in outputs.items():
            write_dataset(
                dataframe,
                output_dataset_path(resolved_paths.output_root, dataset),
                overwrite,
            )
        return row_counts
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI del builder de marts analiticos Gold."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=GOLD_MART_DATASETS,
        help="Mart Gold a construir. Puede repetirse.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Reemplaza salidas Gold existentes.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Construye y cuenta DataFrames sin escribir salidas.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite opcional por dataset de entrada para pruebas locales.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    row_counts = build_gold_powerbi_analytic_marts(
        selected_datasets=args.dataset,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    for dataset, row_count in row_counts.items():
        print(f"{dataset}: {row_count} filas")


if __name__ == "__main__":
    main()
