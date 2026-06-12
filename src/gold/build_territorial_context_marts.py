"""Construcción de marts Gold de contexto territorial.

Este módulo prepara dimensiones y marts territoriales desde RENAMU y Silver
integrado. No cruza fila-a-fila con MEF ni Predial, y solo construye métricas
de capacidad cuando existen columnas reales en los datos.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.common.logger import get_logger
from src.common.paths import SILVER_DIR, get_source_gold_path, get_source_silver_path


SUBJECT_AREA = "territorial_context"
INTEGRATED_SUBDIR = "integrated"

INPUT_DATASETS = [
    "renamu_full",
    "renamu_municipal_context",
    "municipal_entity_bridge",
    "integration_coverage",
]

GOLD_DATASETS = [
    "dim_geography",
    "dim_municipality_context",
    "mart_territorial_context",
    "mart_municipal_capacity",
    "fact_territorial_integration_coverage",
]

TERRITORIAL_COVERAGE_METRICS = [
    "predial_entities_with_valid_ubigeo",
    "predial_entities_with_renamu_match",
    "renamu_ubigeos_without_predial",
    "mef_sec_ejec_with_bridge",
    "mef_sec_ejec_without_bridge",
]

CAPACITY_PATTERNS = {
    "workers": ["p19"],
    "computers": ["p13"],
    "internet": ["p14"],
    "state_systems": ["p16"],
    "municipal_systems": ["p17"],
    "technical_assistance": ["p22_at"],
    "training": ["p22_c"],
    "renamu_income": ["c96"],
    "renamu_expense": ["c97"],
}

RENAMU_FULL_RESOURCE_KEY = "base_renamu_2022"

COMPUTER_QUANTITY_COLUMNS = [
    "p13a_1",
    "p13a_2",
    "p13a_3",
    "p13a_4",
    "p13a_5",
    "p13a_6",
    "p13a_7",
    "p13a_8",
    "p13a_9",
]
INTERNET_CONNECTION_COLUMNS = ["p14a_1", "p14a_2"]
RENT_SYSTEM_COLUMNS = ["p17_1", "p17_2", "p17_3", "p17_4"]
CADASTRE_SYSTEM_COLUMNS = ["p17_5", "p17_6", "p17_7", "p17_8"]
TAX_ASSISTANCE_COLUMNS = ["p22_at1", "p22_at2", "p22_at3", "p22_at4"]
CADASTRE_ASSISTANCE_COLUMNS = ["p22_at5", "p22_at6", "p22_at7", "p22_at8"]
TAX_TRAINING_COLUMNS = ["p22_c1", "p22_c2", "p22_c3", "p22_c4"]
CADASTRE_TRAINING_COLUMNS = ["p22_c5", "p22_c6", "p22_c7", "p22_c8"]


class GoldTerritorialError(Exception):
    """Error controlado durante la construcción Gold territorial."""


@dataclass(frozen=True)
class TerritorialGoldPaths:
    """Rutas de entrada Silver integrada y salida Gold territorial."""

    input_root: Path
    renamu_full_path: Path
    output_root: Path


def utc_now_iso() -> str:
    """Retorna fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def safe_percentage(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Calcula porcentaje evitando división entre cero o nulos."""

    if numerator is None or denominator in (None, 0):
        return None
    return round((float(numerator) / float(denominator)) * 100, 4)


def interpret_tipomuni(value: int | str | None) -> str:
    """Interpreta tipo de municipalidad según diccionario RENAMU."""

    if value is None or str(value).strip() == "":
        return "No informado"
    normalized = str(value).strip()
    mapping = {
        "1": "Provincial",
        "2": "Distrital",
        "3": "Centro Poblado",
    }
    return mapping.get(normalized, "No informado")


def build_geography_key(ubigeo: str | None) -> str | None:
    """Construye llave geográfica desde ubigeo."""

    if ubigeo is None or str(ubigeo).strip() == "":
        return None
    return str(ubigeo).strip()


def existing_columns(columns: Iterable[str], desired_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas deseadas que existen, preservando orden."""

    available = set(columns)
    return [column for column in desired_columns if column in available]


def missing_columns(columns: Iterable[str], required_columns: Iterable[str]) -> list[str]:
    """Devuelve columnas requeridas faltantes."""

    available = set(columns)
    return [column for column in required_columns if column not in available]


def detect_columns_by_patterns(columns: Iterable[str], patterns: Iterable[str]) -> list[str]:
    """Detecta columnas por patrones normalizados."""

    normalized_patterns = [pattern.lower() for pattern in patterns]
    return [
        column
        for column in columns
        if any(pattern in column.lower() for pattern in normalized_patterns)
    ]


def detect_renamu_capacity_columns(columns: Iterable[str]) -> dict[str, list[str]]:
    """Clasifica columnas RENAMU disponibles para contexto y capacidad."""

    column_list = list(columns)
    detected = {
        category: detect_columns_by_patterns(column_list, patterns)
        for category, patterns in CAPACITY_PATTERNS.items()
    }
    detected["renamu_income"] = [
        column for column in detected["renamu_income"] if column.endswith("_decimal")
    ]
    detected["renamu_expense"] = [
        column for column in detected["renamu_expense"] if column.endswith("_decimal")
    ]
    return detected


def capacity_metric_availability(columns_by_category: dict[str, list[str]]) -> dict[str, bool]:
    """Indica qué métricas de capacidad territorial pueden construirse."""

    return {
        "total_personal_dic_2021": "p19d_t" in columns_by_category["workers"],
        "total_personal_mar_2022": "p19m_t" in columns_by_category["workers"],
        "total_computadoras_operativas": bool(
            existing_columns(columns_by_category["computers"], COMPUTER_QUANTITY_COLUMNS)
        ),
        "computadoras_con_internet": "p14a_1" in columns_by_category["internet"],
        "ratio_computadoras_con_internet": bool(
            existing_columns(columns_by_category["computers"], COMPUTER_QUANTITY_COLUMNS)
        )
        and "p14a_1" in columns_by_category["internet"],
        "computadoras_por_trabajador": bool(
            existing_columns(columns_by_category["computers"], COMPUTER_QUANTITY_COLUMNS)
        )
        and "p19m_t" in columns_by_category["workers"],
        "tiene_internet": "p14" in columns_by_category["internet"],
        "tipo_conexion_internet": bool(
            existing_columns(columns_by_category["internet"], INTERNET_CONNECTION_COLUMNS)
        ),
        "tiene_siaf": "p16_1" in columns_by_category["state_systems"],
        "tiene_srtm": "p16_8" in columns_by_category["state_systems"],
        "tiene_sistema_rentas": bool(
            existing_columns(columns_by_category["municipal_systems"], RENT_SYSTEM_COLUMNS)
        ),
        "tiene_catastro": bool(
            existing_columns(columns_by_category["municipal_systems"], CADASTRE_SYSTEM_COLUMNS)
        ),
        "requiere_asistencia_administracion_tributaria": bool(
            existing_columns(columns_by_category["technical_assistance"], TAX_ASSISTANCE_COLUMNS)
        ),
        "requiere_asistencia_catastro": bool(
            existing_columns(columns_by_category["technical_assistance"], CADASTRE_ASSISTANCE_COLUMNS)
        ),
        "requiere_capacitacion_administracion_tributaria": bool(
            existing_columns(columns_by_category["training"], TAX_TRAINING_COLUMNS)
        ),
        "requiere_capacitacion_catastro": bool(
            existing_columns(columns_by_category["training"], CADASTRE_TRAINING_COLUMNS)
        ),
        "renamu_income_total": bool(columns_by_category["renamu_income"]),
        "renamu_expense_total": bool(columns_by_category["renamu_expense"]),
    }


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye ruta Gold para un dataset territorial soportado."""

    if dataset_name not in GOLD_DATASETS:
        raise GoldTerritorialError(f"Dataset Gold territorial no soportado: {dataset_name}")
    return output_root / dataset_name


def validate_selected_datasets(selected: list[str] | None) -> list[str]:
    """Valida selección de datasets Gold territoriales."""

    if not selected:
        return GOLD_DATASETS
    invalid = sorted(set(selected) - set(GOLD_DATASETS))
    if invalid:
        raise GoldTerritorialError(
            f"Datasets Gold territoriales no soportados: {invalid}. "
            f"Disponibles: {GOLD_DATASETS}."
        )
    return selected


def resolve_paths(output_subdir: str) -> TerritorialGoldPaths:
    """Resuelve rutas para Gold territorial."""

    return TerritorialGoldPaths(
        input_root=SILVER_DIR / INTEGRATED_SUBDIR,
        renamu_full_path=get_source_silver_path("renamu")
        / f"resource_key={RENAMU_FULL_RESOURCE_KEY}",
        output_root=get_source_gold_path(output_subdir),
    )


def validate_input_paths(paths: TerritorialGoldPaths) -> None:
    """Valida existencia de entradas Silver integradas."""

    missing = [
        str(paths.input_root / dataset)
        for dataset in INPUT_DATASETS
        if dataset != "renamu_full"
        if not (paths.input_root / dataset).exists()
    ]
    if not paths.renamu_full_path.exists():
        missing.append(str(paths.renamu_full_path))
    if missing:
        raise GoldTerritorialError(
            "Faltan datasets Silver requeridos para Gold territorial: "
            + ", ".join(missing)
        )


def read_input_dataset(
    spark: Any,
    paths: TerritorialGoldPaths,
    dataset_name: str,
    limit: int | None,
) -> Any:
    """Lee un dataset Silver integrado con límite opcional."""

    dataframe = spark.read.parquet(str(paths.input_root / dataset_name))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def read_renamu_full_dataset(spark: Any, paths: TerritorialGoldPaths, limit: int | None) -> Any:
    """Lee RENAMU Silver completo con límite opcional."""

    dataframe = spark.read.parquet(str(paths.renamu_full_path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def nonblank(column_name: str) -> Any:
    """Expresión Spark para texto no vacío."""

    from pyspark.sql import functions as F

    return F.col(column_name).isNotNull() & (F.trim(F.col(column_name)) != "")


def sum_existing_columns(dataframe: Any, columns: list[str]) -> Any:
    """Suma columnas existentes con coalesce a cero."""

    from functools import reduce

    from pyspark.sql import functions as F

    if not columns:
        return F.lit(None).cast("decimal(30,4)")
    expressions = [F.coalesce(F.col(column), F.lit(0)) for column in columns]
    return reduce(lambda left, right: left + right, expressions)


def numeric_column(column_name: str) -> Any:
    """Convierte una columna RENAMU textual a decimal para métricas Gold."""

    from pyspark.sql import functions as F

    cleaned = F.regexp_replace(F.regexp_replace(F.trim(F.col(column_name)), ",", "."), r"[^0-9.-]", "")
    return F.when(cleaned == "", None).otherwise(cleaned.cast("decimal(30,4)"))


def sum_numeric_columns(columns: list[str]) -> Any:
    """Suma columnas textuales convertidas a decimal."""

    from functools import reduce

    from pyspark.sql import functions as F

    if not columns:
        return F.lit(None).cast("decimal(30,4)")
    expressions = [F.coalesce(numeric_column(column), F.lit(0)) for column in columns]
    return reduce(lambda left, right: left + right, expressions)


def safe_ratio_expr(numerator: Any, denominator: Any) -> Any:
    """Calcula ratio Spark evitando división entre cero."""

    from pyspark.sql import functions as F

    return F.when(denominator.isNotNull() & (denominator != 0), numerator / denominator)


def indicator_expr(column_name: str) -> Any:
    """Interpreta valores RENAMU simples como indicador booleano."""

    from pyspark.sql import functions as F

    normalized = F.upper(F.trim(F.col(column_name).cast("string")))
    return normalized.isin("1", "SI", "SÍ", "TRUE", "X")


def any_indicator(columns: list[str]) -> Any:
    """Evalúa si al menos una columna existe con señal afirmativa."""

    from functools import reduce

    from pyspark.sql import functions as F

    if not columns:
        return F.lit(None).cast("boolean")
    expressions = [indicator_expr(column) for column in columns]
    return reduce(lambda left, right: left | right, expressions)


def add_tipomuni_label(dataframe: Any) -> Any:
    """Agrega etiqueta de tipo municipal sin inventar categorías."""

    from pyspark.sql import functions as F

    return dataframe.withColumn(
        "tipomuni_label",
        F.when(F.col("tipomuni_int") == 1, F.lit("Provincial"))
        .when(F.col("tipomuni_int") == 2, F.lit("Distrital"))
        .when(F.col("tipomuni_int") == 3, F.lit("Centro Poblado"))
        .otherwise(F.lit("No informado")),
    )


def build_predial_match_by_ubigeo(bridge: Any) -> Any:
    """Resume presencia predial por ubigeo."""

    from pyspark.sql import functions as F

    required = ["sec_ejec", "ubigeo", "is_valid_ubigeo"]
    missing = missing_columns(bridge.columns, required)
    if missing:
        raise GoldTerritorialError(f"El puente municipal no tiene columnas requeridas: {missing}")

    return (
        bridge.where(nonblank("ubigeo"))
        .groupBy("ubigeo")
        .agg(
            F.countDistinct("sec_ejec").alias("predial_sec_ejec_count"),
            F.max(F.col("is_valid_ubigeo").cast("int")).alias("has_valid_bridge_ubigeo_int"),
        )
        .withColumn("has_predial_match", F.col("predial_sec_ejec_count") > 0)
        .withColumn("has_valid_bridge_ubigeo", F.col("has_valid_bridge_ubigeo_int") == 1)
        .drop("has_valid_bridge_ubigeo_int")
    )


def build_sec_ejec_by_ubigeo(bridge: Any) -> Any:
    """Resume relación sec_ejec -> ubigeo sin asumir equivalencia."""

    from pyspark.sql import functions as F

    required = ["sec_ejec", "ubigeo"]
    missing = missing_columns(bridge.columns, required)
    if missing:
        raise GoldTerritorialError(f"El puente municipal no tiene columnas requeridas: {missing}")

    return (
        bridge.where(nonblank("ubigeo"))
        .groupBy("ubigeo")
        .agg(
            F.countDistinct("sec_ejec").alias("sec_ejec_count"),
            F.first("sec_ejec", ignorenulls=True).alias("first_sec_ejec"),
        )
        .withColumn(
            "sec_ejec",
            F.when(F.col("sec_ejec_count") == 1, F.col("first_sec_ejec")),
        )
        .drop("first_sec_ejec")
    )


def build_dim_geography(renamu: Any) -> Any:
    """Construye dimensión geográfica por ubigeo."""

    from pyspark.sql import functions as F

    required = ["ubigeo", "ccdd", "ccpp", "ccdi"]
    missing = missing_columns(renamu.columns, required)
    if missing:
        raise GoldTerritorialError(f"RENAMU contexto no tiene columnas requeridas: {missing}")

    selected = existing_columns(
        renamu.columns,
        [
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
            "has_complete_territory",
            "is_valid_ubigeo",
        ],
    )
    return (
        renamu.select(*selected)
        .dropDuplicates(["ubigeo"])
        .withColumn("department_key", F.col("ccdd"))
        .withColumn("province_key", F.concat_ws("", F.col("ccdd"), F.col("ccpp")))
        .withColumn("district_key", F.concat_ws("", F.col("ccdd"), F.col("ccpp"), F.col("ccdi")))
        .withColumn("geography_key", F.col("ubigeo"))
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def build_dim_municipality_context(renamu: Any, bridge: Any) -> Any:
    """Construye dimensión municipal contextual por ubigeo."""

    from pyspark.sql import functions as F

    required = ["ubigeo", "idmunici", "tipomuni", "tipomuni_int"]
    missing = missing_columns(renamu.columns, required)
    if missing:
        raise GoldTerritorialError(f"RENAMU contexto no tiene columnas requeridas: {missing}")

    base_columns = existing_columns(
        renamu.columns,
        [
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
            "is_valid_ubigeo",
            "has_complete_territory",
            "has_municipal_identifier",
            "is_valid_tipomuni",
        ],
    )
    sec_ejec = build_sec_ejec_by_ubigeo(bridge)
    predial_match = build_predial_match_by_ubigeo(bridge)
    return (
        add_tipomuni_label(renamu.select(*base_columns).dropDuplicates(["ubigeo"]))
        .join(sec_ejec, on="ubigeo", how="left")
        .join(predial_match, on="ubigeo", how="left")
        .withColumn("has_predial_match", F.coalesce(F.col("has_predial_match"), F.lit(False)))
        .withColumn("predial_sec_ejec_count", F.coalesce(F.col("predial_sec_ejec_count"), F.lit(0)))
        .withColumn("municipality_context_key", F.sha2(F.col("ubigeo"), 256))
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def build_mart_territorial_context(dim_municipality: Any) -> Any:
    """Construye mart agregado territorial para mapas y filtros."""

    from pyspark.sql import functions as F

    group_columns = existing_columns(
        dim_municipality.columns,
        [
            "departamento",
            "provincia",
            "distrito",
            "departamento_normalizado",
            "provincia_normalizada",
            "distrito_normalizado",
            "ubigeo",
            "tipomuni",
            "tipomuni_int",
            "tipomuni_label",
        ],
    )
    return (
        dim_municipality.groupBy(*group_columns)
        .agg(
            F.countDistinct("ubigeo").alias("municipality_count"),
            F.sum(F.col("is_valid_ubigeo").cast("int")).alias("valid_ubigeo_count"),
            F.sum(F.col("has_complete_territory").cast("int")).alias("complete_territory_count"),
            F.sum(F.col("has_predial_match").cast("int")).alias("predial_match_count"),
            F.sum((~F.col("has_predial_match")).cast("int")).alias("without_predial_match_count"),
        )
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
        .withColumn("gold_grain", F.lit("ubigeo_tipomuni_territory"))
    )


def build_mart_municipal_capacity(
    renamu_full: Any,
    dim_municipality: Any,
    columns_by_category: dict[str, list[str]],
) -> Any:
    """Construye mart de capacidad municipal con columnas reales disponibles."""

    from pyspark.sql import functions as F

    base = dim_municipality.select(
        "ubigeo",
        "idmunici",
        "departamento",
        "provincia",
        "distrito",
        "tipomuni",
        "tipomuni_int",
        "tipomuni_label",
        "has_predial_match",
        "predial_sec_ejec_count",
    )
    computer_columns = existing_columns(renamu_full.columns, COMPUTER_QUANTITY_COLUMNS)
    internet_connection_columns = existing_columns(renamu_full.columns, INTERNET_CONNECTION_COLUMNS)
    rent_system_columns = existing_columns(renamu_full.columns, RENT_SYSTEM_COLUMNS)
    cadastre_system_columns = existing_columns(renamu_full.columns, CADASTRE_SYSTEM_COLUMNS)
    tax_assistance_columns = existing_columns(renamu_full.columns, TAX_ASSISTANCE_COLUMNS)
    cadastre_assistance_columns = existing_columns(renamu_full.columns, CADASTRE_ASSISTANCE_COLUMNS)
    tax_training_columns = existing_columns(renamu_full.columns, TAX_TRAINING_COLUMNS)
    cadastre_training_columns = existing_columns(renamu_full.columns, CADASTRE_TRAINING_COLUMNS)

    total_computers = sum_numeric_columns(computer_columns)
    computers_with_internet = (
        numeric_column("p14a_1")
        if "p14a_1" in renamu_full.columns
        else F.lit(None).cast("decimal(30,4)")
    )
    total_personal_mar = (
        numeric_column("p19m_t")
        if "p19m_t" in renamu_full.columns
        else F.lit(None).cast("decimal(30,4)")
    )

    capacity = (
        renamu_full.withColumn(
            "total_personal_dic_2021",
            numeric_column("p19d_t")
            if "p19d_t" in renamu_full.columns
            else F.lit(None).cast("decimal(30,4)"),
        )
        .withColumn("total_personal_mar_2022", total_personal_mar)
        .withColumn("total_computadoras_operativas", total_computers)
        .withColumn("computadoras_con_internet", computers_with_internet)
        .withColumn(
            "ratio_computadoras_con_internet",
            safe_ratio_expr(computers_with_internet, total_computers),
        )
        .withColumn(
            "computadoras_por_trabajador",
            safe_ratio_expr(total_computers, total_personal_mar),
        )
        .withColumn(
            "tiene_internet",
            indicator_expr("p14") if "p14" in renamu_full.columns else F.lit(None).cast("boolean"),
        )
        .withColumn(
            "tipo_conexion_internet",
            F.concat_ws("|", *[F.col(column).cast("string") for column in internet_connection_columns])
            if internet_connection_columns
            else F.lit(None).cast("string"),
        )
        .withColumn(
            "tiene_siaf",
            indicator_expr("p16_1")
            if "p16_1" in renamu_full.columns
            else F.lit(None).cast("boolean"),
        )
        .withColumn(
            "tiene_srtm",
            indicator_expr("p16_8")
            if "p16_8" in renamu_full.columns
            else F.lit(None).cast("boolean"),
        )
        .withColumn("tiene_sistema_rentas", any_indicator(rent_system_columns))
        .withColumn("tiene_catastro", any_indicator(cadastre_system_columns))
        .withColumn(
            "requiere_asistencia_administracion_tributaria",
            any_indicator(tax_assistance_columns),
        )
        .withColumn("requiere_asistencia_catastro", any_indicator(cadastre_assistance_columns))
        .withColumn(
            "requiere_capacitacion_administracion_tributaria",
            any_indicator(tax_training_columns),
        )
        .withColumn("requiere_capacitacion_catastro", any_indicator(cadastre_training_columns))
        .withColumn(
            "renamu_income_total",
            sum_existing_columns(renamu_full, columns_by_category["renamu_income"]),
        )
        .withColumn(
            "renamu_expense_total",
            sum_existing_columns(renamu_full, columns_by_category["renamu_expense"]),
        )
        .select(
            "ubigeo",
            "total_personal_dic_2021",
            "total_personal_mar_2022",
            "total_computadoras_operativas",
            "computadoras_con_internet",
            "ratio_computadoras_con_internet",
            "computadoras_por_trabajador",
            "tiene_internet",
            "tipo_conexion_internet",
            "tiene_siaf",
            "tiene_srtm",
            "tiene_sistema_rentas",
            "tiene_catastro",
            "requiere_asistencia_administracion_tributaria",
            "requiere_asistencia_catastro",
            "requiere_capacitacion_administracion_tributaria",
            "requiere_capacitacion_catastro",
            "renamu_income_total",
            "renamu_expense_total",
        )
    )
    return (
        base.join(capacity, on="ubigeo", how="left")
        .withColumn("has_financial_context", F.col("renamu_income_total").isNotNull() | F.col("renamu_expense_total").isNotNull())
        .withColumn("worker_metric_available", F.lit(bool(columns_by_category["workers"])))
        .withColumn("computer_metric_available", F.lit(bool(columns_by_category["computers"])))
        .withColumn("internet_metric_available", F.lit(bool(columns_by_category["internet"])))
        .withColumn("state_system_metric_available", F.lit(bool(columns_by_category["state_systems"])))
        .withColumn("municipal_system_metric_available", F.lit(bool(columns_by_category["municipal_systems"])))
        .withColumn(
            "technical_assistance_metric_available",
            F.lit(bool(columns_by_category["technical_assistance"])),
        )
        .withColumn("training_metric_available", F.lit(bool(columns_by_category["training"])))
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def build_fact_territorial_integration_coverage(integration_coverage: Any) -> Any:
    """Construye hecho técnico de cobertura territorial."""

    from pyspark.sql import functions as F

    required = ["metric_name", "numerator", "denominator", "coverage_percentage"]
    missing = missing_columns(integration_coverage.columns, required)
    if missing:
        raise GoldTerritorialError(f"Integration coverage no tiene columnas requeridas: {missing}")

    return (
        integration_coverage.where(F.col("metric_name").isin(TERRITORIAL_COVERAGE_METRICS))
        .select("metric_name", "numerator", "denominator", "coverage_percentage", "description")
        .withColumn("coverage_ratio", F.when(F.col("denominator") != 0, F.col("numerator") / F.col("denominator")))
        .withColumn("source_dataset", F.lit("silver.integration_coverage"))
        .withColumn("gold_processed_at_utc", F.lit(utc_now_iso()))
    )


def write_dataset(dataframe: Any, output_path: Path, overwrite: bool) -> None:
    """Escribe un dataset Gold territorial en Parquet Snappy."""

    mode = "overwrite" if overwrite else "errorifexists"
    dataframe.write.mode(mode).option("compression", "snappy").parquet(str(output_path))


def print_detected_columns(columns_by_category: dict[str, list[str]]) -> None:
    """Imprime columnas RENAMU detectadas por categoría."""

    print("Columnas RENAMU detectadas:")
    for category, columns in columns_by_category.items():
        preview = ", ".join(columns[:8])
        suffix = "" if len(columns) <= 8 else f" ... (+{len(columns) - 8})"
        print(f"- {category}: {len(columns)} | {preview}{suffix}")


def print_capacity_availability(availability: dict[str, bool]) -> None:
    """Imprime disponibilidad de métricas de capacidad."""

    print("Métricas de capacidad municipal:")
    for metric_name, available in availability.items():
        status = "se construirá" if available else "no disponible por falta de columnas base"
        print(f"- {metric_name}: {status}")


def print_dry_run(
    paths: TerritorialGoldPaths,
    datasets: list[str],
    columns_by_category: dict[str, list[str]],
) -> None:
    """Muestra plan de construcción Gold territorial."""

    print("=" * 80)
    print("Plan Gold de contexto territorial")
    print(f"Entrada Silver integrada: {paths.input_root}")
    for dataset in INPUT_DATASETS:
        path = paths.renamu_full_path if dataset == "renamu_full" else paths.input_root / dataset
        print(f"- input {dataset}: {path} | existe={path.exists()}")
    print(f"Salida Gold: {paths.output_root}")
    print("Datasets Gold a crear:")
    for dataset in datasets:
        print(f"- {dataset}: {output_dataset_path(paths.output_root, dataset)}")
    print_detected_columns(columns_by_category)
    print_capacity_availability(capacity_metric_availability(columns_by_category))
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


def build_territorial_gold_marts(
    *,
    dry_run: bool,
    overwrite: bool,
    selected_datasets: list[str] | None = None,
    limit: int | None = None,
    output_subdir: str = SUBJECT_AREA,
) -> dict[str, Any]:
    """Ejecuta o planifica Gold territorial."""

    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session

    paths = resolve_paths(output_subdir)
    validate_input_paths(paths)
    datasets = validate_selected_datasets(selected_datasets)

    spark = build_spark_session(
        app_name="GoldTerritorialContextMarts",
        master="local[2]",
        extra_configs={"spark.sql.shuffle.partitions": "8"},
    )

    try:
        renamu_for_detection = spark.read.parquet(str(paths.renamu_full_path))
        columns_by_category = detect_renamu_capacity_columns(renamu_for_detection.columns)

        if dry_run:
            print_dry_run(paths, datasets, columns_by_category)
            print(
                f"- renamu_full: filas={renamu_for_detection.count()} "
                f"columnas={len(renamu_for_detection.columns)}"
            )
            return {"datasets": datasets, "columns_by_category": columns_by_category}

        logger = get_logger(__name__)
        renamu = read_input_dataset(spark, paths, "renamu_municipal_context", limit)
        renamu_full = read_renamu_full_dataset(spark, paths, limit)
        bridge = read_input_dataset(spark, paths, "municipal_entity_bridge", limit)
        coverage = read_input_dataset(spark, paths, "integration_coverage", None)

        dim_geography = build_dim_geography(renamu)
        dim_municipality = build_dim_municipality_context(renamu, bridge)
        territorial_context = build_mart_territorial_context(dim_municipality)
        municipal_capacity = build_mart_municipal_capacity(
            renamu_full,
            dim_municipality,
            columns_by_category,
        )
        coverage_fact = build_fact_territorial_integration_coverage(coverage)

        built = {
            "dim_geography": dim_geography,
            "dim_municipality_context": dim_municipality,
            "mart_territorial_context": territorial_context,
            "mart_municipal_capacity": municipal_capacity,
            "fact_territorial_integration_coverage": coverage_fact,
        }

        outputs: dict[str, str] = {}
        counts: dict[str, int] = {}
        for dataset_name in datasets:
            logger.info("Escribiendo dataset Gold territorial %s", dataset_name)
            output_path, count = write_and_count(
                dataframe=built[dataset_name],
                dataset_name=dataset_name,
                output_root=paths.output_root,
                overwrite=overwrite,
            )
            outputs[dataset_name] = output_path
            counts[dataset_name] = count

        coverage_summary = (
            dim_municipality.agg(
                F.countDistinct("ubigeo").alias("total_ubigeos"),
                F.sum(F.col("is_valid_ubigeo").cast("int")).alias("valid_ubigeos"),
                F.sum(F.col("has_complete_territory").cast("int")).alias("complete_territory"),
                F.sum(F.col("has_predial_match").cast("int")).alias("predial_match"),
            )
            .collect()[0]
            .asDict()
        )

        print("=" * 80)
        print("Gold contexto territorial finalizado")
        for dataset_name in datasets:
            print(f"- {dataset_name}: filas={counts[dataset_name]} ruta={outputs[dataset_name]}")
        print_detected_columns(columns_by_category)
        print_capacity_availability(capacity_metric_availability(columns_by_category))
        print("Cobertura territorial:")
        print(f"- ubigeos_total: {coverage_summary.get('total_ubigeos')}")
        print(f"- ubigeos_validos: {coverage_summary.get('valid_ubigeos')}")
        print(f"- territorio_completo: {coverage_summary.get('complete_territory')}")
        print(f"- con_match_predial: {coverage_summary.get('predial_match')}")

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
        description="Construye marts Gold de contexto territorial desde Silver integrado."
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
        help="Dataset Gold territorial a construir. Puede repetirse.",
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
        raise GoldTerritorialError("--limit debe ser un entero positivo.")

    build_territorial_gold_marts(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        selected_datasets=args.dataset,
        limit=args.limit,
        output_subdir=args.output_subdir,
    )


if __name__ == "__main__":
    main()
