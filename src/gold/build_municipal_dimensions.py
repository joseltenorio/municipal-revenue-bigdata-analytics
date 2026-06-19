"""Construccion de dimensiones Gold municipales.

Este modulo materializa solo las dimensiones base del modelo objetivo Gold.
No construye hechos, marts, auditoria ni registros Hive.
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

from src.common.paths import GOLD_DIR, get_source_silver_path
from src.common.spark_session import build_spark_session


GOLD_DIMENSION_DATASETS = [
    "dim_municipality",
    "dim_geography",
    "dim_renamu_context",
    "dim_time",
    "dim_sismepre_period",
]

RENAMU_CONTEXT_COLUMNS = [
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
]

RENAMU_MUNICIPALITY_COLUMNS = [
    "ubigeo6",
    "idmunici",
    "tipomuni_codigo",
    "tipomuni_nombre",
    "provincia_nombre",
    "distrito_nombre",
]

CLASSIFICATION_COLUMNS = [
    "ubigeo6",
    "tipo_clasificacion_municipal",
    "ambito_municipal",
    "descripcion_tipo",
]

GEOGRAPHY_COLUMNS = [
    "ubigeo6",
    "ccdd",
    "ccpp",
    "ccdi",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre",
]

SISMEPRE_PERIOD_COLUMNS = [
    "anio_aplicacion",
    "periodo",
    "anio_estadistica",
    "mes_estadistica",
    "periodo_estadistica_tipo",
    "is_annual_stat_period",
]


class GoldDimensionError(ValueError):
    """Error de contrato para dimensiones Gold."""


@dataclass(frozen=True)
class GoldDimensionPaths:
    """Rutas fisicas usadas por las dimensiones Gold."""

    output_root: Path
    renamu_context_path: Path
    classification_path: Path
    siaf_income_root: Path
    sismepre_period_path: Path


def default_paths() -> GoldDimensionPaths:
    """Devuelve las rutas Silver y Gold vigentes del modelo dimensional."""

    return GoldDimensionPaths(
        output_root=GOLD_DIR,
        renamu_context_path=get_source_silver_path("renamu") / "resource_key=municipal_context",
        classification_path=get_source_silver_path("municipal_classification")
        / "resource_key=classification_2019",
        siaf_income_root=get_source_silver_path("siaf_income"),
        sismepre_period_path=get_source_silver_path("sismepre")
        / "resource_key=esat_estadistica_atm",
    )


def utc_now_iso() -> str:
    """Timestamp UTC estable para metadata Gold."""

    return datetime.now(timezone.utc).isoformat()


def existing_columns(available_columns: list[str], desired_columns: list[str]) -> list[str]:
    """Conserva el orden deseado filtrando columnas realmente disponibles."""

    available = set(available_columns)
    return [column for column in desired_columns if column in available]


def missing_columns(available_columns: list[str], required_columns: list[str]) -> list[str]:
    """Lista columnas requeridas que no existen en un DataFrame."""

    available = set(available_columns)
    return [column for column in required_columns if column not in available]


def require_columns(dataframe: DataFrame, required_columns: list[str], dataset_name: str) -> None:
    """Falla rapido si falta una columna critica para el contrato Gold."""

    missing = missing_columns(dataframe.columns, required_columns)
    if missing:
        raise GoldDimensionError(f"{dataset_name} no tiene columnas requeridas: {missing}")


def validate_selected_datasets(selected_datasets: list[str] | None) -> list[str]:
    """Valida filtros CLI y devuelve datasets Gold a construir."""

    if not selected_datasets:
        return GOLD_DIMENSION_DATASETS

    unsupported = [
        dataset for dataset in selected_datasets if dataset not in GOLD_DIMENSION_DATASETS
    ]
    if unsupported:
        supported = ", ".join(GOLD_DIMENSION_DATASETS)
        raise GoldDimensionError(
            f"Datasets Gold no soportados: {unsupported}. Soportados: {supported}."
        )

    return selected_datasets


def output_dataset_path(output_root: Path, dataset_name: str) -> Path:
    """Construye la ruta fisica de una dimension Gold soportada."""

    validate_selected_datasets([dataset_name])
    return output_root / dataset_name


def is_valid_ubigeo6(column_name: str = "ubigeo6") -> Any:
    """Expresion Spark para ubigeo peruano de seis digitos."""

    return F.col(column_name).cast("string").rlike(r"^[0-9]{6}$")


def normalize_text(column: Any) -> Any:
    """Normaliza textos maestros sin depender de nombres de otras fuentes."""

    return F.upper(F.regexp_replace(F.trim(column.cast("string")), r"\s+", " "))


def derive_municipalidad_nombre() -> Any:
    """Deriva el nombre municipal maestro desde RENAMU y su tipo municipal.

    La regla evita matching por nombre: usa solo tipo municipal y nombres
    territoriales curados del registro RENAMU.
    """

    tipomuni_codigo = F.col("tipomuni_codigo").cast("string")
    tipomuni_nombre = normalize_text(F.col("tipomuni_nombre"))
    provincia = normalize_text(F.col("provincia_nombre"))
    distrito = normalize_text(F.col("distrito_nombre"))

    return (
        F.when(
            (tipomuni_codigo == F.lit("1")) | (tipomuni_nombre == F.lit("PROVINCIAL")),
            F.concat(F.lit("MUNICIPALIDAD PROVINCIAL DE "), provincia),
        )
        .when(
            (tipomuni_codigo == F.lit("2")) | (tipomuni_nombre == F.lit("DISTRITAL")),
            F.concat(F.lit("MUNICIPALIDAD DISTRITAL DE "), distrito),
        )
        .when(
            (tipomuni_codigo == F.lit("3")) | (tipomuni_nombre == F.lit("CENTRO POBLADO")),
            F.concat(F.lit("MUNICIPALIDAD DE CENTRO POBLADO "), distrito),
        )
        .otherwise(F.concat(F.lit("MUNICIPALIDAD DE "), distrito))
    )


def one_row_per_ubigeo(dataframe: DataFrame) -> DataFrame:
    """Reduce a una fila por ubigeo validando la llave territorial."""

    return dataframe.where(is_valid_ubigeo6()).dropDuplicates(["ubigeo6"])


def build_dim_municipality(
    renamu_context: DataFrame,
    classification: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye la dimension municipal desde RENAMU curado y MEF 2019."""

    require_columns(renamu_context, RENAMU_MUNICIPALITY_COLUMNS, "renamu_context")
    require_columns(classification, CLASSIFICATION_COLUMNS, "classification_2019")

    processed_at = processed_at_utc or utc_now_iso()
    renamu_base = one_row_per_ubigeo(
        renamu_context.select(*RENAMU_MUNICIPALITY_COLUMNS)
    ).withColumn("municipalidad_nombre", derive_municipalidad_nombre())
    classification_base = one_row_per_ubigeo(classification.select(*CLASSIFICATION_COLUMNS))

    return (
        renamu_base.join(classification_base, on="ubigeo6", how="left")
        .withColumn("municipality_key", F.col("ubigeo6"))
        .withColumn("geography_key", F.col("ubigeo6"))
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(
            "municipality_key",
            "ubigeo6",
            "geography_key",
            "idmunici",
            "municipalidad_nombre",
            "tipomuni_codigo",
            "tipomuni_nombre",
            "tipo_clasificacion_municipal",
            "ambito_municipal",
            "descripcion_tipo",
            "gold_processed_at_utc",
        )
    )


def build_dim_geography(
    renamu_context: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye la jerarquia territorial separada de la entidad municipal."""

    require_columns(renamu_context, GEOGRAPHY_COLUMNS, "renamu_context")
    processed_at = processed_at_utc or utc_now_iso()

    return (
        one_row_per_ubigeo(renamu_context.select(*GEOGRAPHY_COLUMNS))
        .withColumn("geography_key", F.col("ubigeo6"))
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(
            "geography_key",
            "ubigeo6",
            "ccdd",
            "ccpp",
            "ccdi",
            "departamento_nombre",
            "provincia_nombre",
            "distrito_nombre",
            "gold_processed_at_utc",
        )
    )


def build_dim_renamu_context(
    renamu_context: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye la dimension de contexto RENAMU con variables seleccionadas."""

    require_columns(renamu_context, ["ubigeo6"], "renamu_context")
    context_columns = existing_columns(renamu_context.columns, RENAMU_CONTEXT_COLUMNS)
    processed_at = processed_at_utc or utc_now_iso()

    return (
        one_row_per_ubigeo(renamu_context.select("ubigeo6", *context_columns))
        .withColumn("municipality_key", F.col("ubigeo6"))
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select("municipality_key", "ubigeo6", *context_columns, "gold_processed_at_utc")
    )


def build_dim_time_from_siaf_frames(
    siaf_frames: list[DataFrame],
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye calendario mensual solo con periodos observados en SIAF."""

    if not siaf_frames:
        raise GoldDimensionError("No hay datasets SIAF para construir dim_time.")

    processed_at = processed_at_utc or utc_now_iso()
    period_frames: list[DataFrame] = []
    for dataframe in siaf_frames:
        require_columns(dataframe, ["anio", "mes"], "siaf_income")
        period_frames.append(
            dataframe.select(
                F.col("anio").cast("int").alias("anio"),
                F.col("mes").cast("int").alias("mes"),
            )
        )

    periods = period_frames[0]
    for dataframe in period_frames[1:]:
        periods = periods.unionByName(dataframe)

    month_text = F.format_string("%02d", F.col("mes"))
    year_text = F.format_string("%04d", F.col("anio"))

    return (
        periods.where(F.col("anio").isNotNull())
        .where(F.col("mes").between(1, 12))
        .dropDuplicates(["anio", "mes"])
        .withColumn("date_key", (F.col("anio") * F.lit(10000) + F.col("mes") * F.lit(100) + F.lit(1)).cast("int"))
        .withColumn("fecha_mes", F.to_date(F.concat_ws("-", year_text, month_text, F.lit("01"))))
        .withColumn("anio_mes", F.concat_ws("-", year_text, month_text))
        .withColumn("trimestre", F.ceil(F.col("mes") / F.lit(3)).cast("int"))
        .withColumn(
            "semestre",
            F.when(F.col("mes") <= F.lit(6), F.lit(1)).otherwise(F.lit(2)).cast("int"),
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(
            "date_key",
            "fecha_mes",
            "anio",
            "mes",
            "anio_mes",
            "trimestre",
            "semestre",
            "gold_processed_at_utc",
        )
        .orderBy("anio", "mes")
    )


def build_dim_sismepre_period(
    sismepre_esat: DataFrame,
    *,
    processed_at_utc: str | None = None,
) -> DataFrame:
    """Construye periodos operativos usando solo el recurso principal SISMEPRE."""

    require_columns(sismepre_esat, SISMEPRE_PERIOD_COLUMNS, "esat_estadistica_atm")
    processed_at = processed_at_utc or utc_now_iso()

    period = (
        sismepre_esat.select(*SISMEPRE_PERIOD_COLUMNS)
        .dropDuplicates(SISMEPRE_PERIOD_COLUMNS)
        .withColumn("anio_aplicacion", F.col("anio_aplicacion").cast("int"))
        .withColumn("periodo", F.col("periodo").cast("int"))
        .withColumn("anio_estadistica", F.col("anio_estadistica").cast("int"))
        .withColumn("mes_estadistica", F.col("mes_estadistica").cast("int"))
        .withColumn("is_annual_stat_period", F.col("is_annual_stat_period").cast("boolean"))
    )

    month_text = F.format_string("%02d", F.coalesce(F.col("mes_estadistica"), F.lit(0)))
    return (
        period.withColumn(
            "sismepre_period_key",
            F.concat_ws(
                "_",
                F.col("anio_aplicacion").cast("string"),
                F.format_string("%02d", F.col("periodo")),
                F.col("anio_estadistica").cast("string"),
                month_text,
            ),
        )
        .withColumn(
            "periodo_label",
            F.concat_ws(
                " ",
                F.lit("Aplicacion"),
                F.col("anio_aplicacion").cast("string"),
                F.lit("- Periodo"),
                F.col("periodo").cast("string"),
                F.lit("- Estadistica"),
                F.col("anio_estadistica").cast("string"),
                month_text,
                F.concat(F.lit("("), F.col("periodo_estadistica_tipo"), F.lit(")")),
            ),
        )
        .withColumn("gold_processed_at_utc", F.lit(processed_at))
        .select(
            "sismepre_period_key",
            "anio_aplicacion",
            "periodo",
            "anio_estadistica",
            "mes_estadistica",
            "periodo_estadistica_tipo",
            "is_annual_stat_period",
            "periodo_label",
            "gold_processed_at_utc",
        )
        .orderBy("anio_aplicacion", "periodo", "anio_estadistica", "mes_estadistica")
    )


def read_parquet_dataset(spark: Any, path: Path, limit: int | None = None) -> DataFrame:
    """Lee Parquet y aplica limite opcional para ejecuciones exploratorias."""

    dataframe = spark.read.parquet(str(path))
    if limit is not None:
        return dataframe.limit(limit)
    return dataframe


def list_siaf_resource_paths(siaf_income_root: Path) -> list[Path]:
    """Lista recursos SIAF Silver disponibles bajo resource_key."""

    if not siaf_income_root.exists():
        return []

    return sorted(
        path
        for path in siaf_income_root.iterdir()
        if path.is_dir() and path.name.startswith("resource_key=")
    )


def required_input_paths(paths: GoldDimensionPaths, datasets: list[str]) -> list[Path]:
    """Devuelve entradas Silver requeridas por las dimensiones seleccionadas."""

    required: list[Path] = []
    if "dim_municipality" in datasets:
        required.extend([paths.renamu_context_path, paths.classification_path])
    if "dim_geography" in datasets or "dim_renamu_context" in datasets:
        required.append(paths.renamu_context_path)
    if "dim_time" in datasets:
        required.append(paths.siaf_income_root)
    if "dim_sismepre_period" in datasets:
        required.append(paths.sismepre_period_path)

    return sorted(set(required))


def validate_input_paths(paths: GoldDimensionPaths, datasets: list[str]) -> None:
    """Valida disponibilidad minima de entradas Silver antes de leer Parquet."""

    missing = [path for path in required_input_paths(paths, datasets) if not path.exists()]
    if missing:
        raise FileNotFoundError(f"No existen entradas Silver requeridas: {missing}")

    if "dim_time" in datasets and not list_siaf_resource_paths(paths.siaf_income_root):
        raise FileNotFoundError(
            f"No hay recursos SIAF Silver bajo {paths.siaf_income_root}."
        )


def write_dimension(dataframe: DataFrame, output_path: Path, overwrite: bool) -> None:
    """Escribe una dimension Gold evitando sobrescritura accidental."""

    if output_path.exists():
        if not overwrite:
            raise GoldDimensionError(
                f"La salida ya existe: {output_path}. Use --overwrite para reemplazarla."
            )
        shutil.rmtree(output_path)

    dataframe.write.mode("overwrite").parquet(str(output_path))


def build_municipal_dimensions(
    *,
    paths: GoldDimensionPaths | None = None,
    selected_datasets: list[str] | None = None,
    overwrite: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Construye fisicamente las dimensiones Gold seleccionadas."""

    resolved_paths = paths or default_paths()
    datasets = validate_selected_datasets(selected_datasets)
    validate_input_paths(resolved_paths, datasets)

    spark = build_spark_session(app_name="gold-municipal-dimensions")
    outputs: dict[str, DataFrame] = {}
    try:
        renamu: DataFrame | None = None
        classification: DataFrame | None = None

        if any(
            dataset in datasets
            for dataset in ["dim_municipality", "dim_geography", "dim_renamu_context"]
        ):
            renamu = read_parquet_dataset(spark, resolved_paths.renamu_context_path, limit)
        if "dim_municipality" in datasets:
            classification = read_parquet_dataset(spark, resolved_paths.classification_path, limit)

        if "dim_municipality" in datasets:
            if renamu is None or classification is None:
                raise GoldDimensionError("dim_municipality requiere RENAMU y clasificacion.")
            outputs["dim_municipality"] = build_dim_municipality(renamu, classification)
        if "dim_geography" in datasets:
            if renamu is None:
                raise GoldDimensionError("dim_geography requiere RENAMU.")
            outputs["dim_geography"] = build_dim_geography(renamu)
        if "dim_renamu_context" in datasets:
            if renamu is None:
                raise GoldDimensionError("dim_renamu_context requiere RENAMU.")
            outputs["dim_renamu_context"] = build_dim_renamu_context(renamu)
        if "dim_time" in datasets:
            siaf_frames = [
                read_parquet_dataset(spark, path, limit)
                for path in list_siaf_resource_paths(resolved_paths.siaf_income_root)
            ]
            outputs["dim_time"] = build_dim_time_from_siaf_frames(siaf_frames)
        if "dim_sismepre_period" in datasets:
            sismepre_esat = read_parquet_dataset(
                spark, resolved_paths.sismepre_period_path, limit
            )
            outputs["dim_sismepre_period"] = build_dim_sismepre_period(sismepre_esat)

        row_counts = {dataset: dataframe.count() for dataset, dataframe in outputs.items()}
        if dry_run:
            return row_counts

        resolved_paths.output_root.mkdir(parents=True, exist_ok=True)
        for dataset, dataframe in outputs.items():
            write_dimension(
                dataframe,
                output_dataset_path(resolved_paths.output_root, dataset),
                overwrite,
            )
        return row_counts
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    """Parsea argumentos CLI del builder Gold dimensional."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=GOLD_DIMENSION_DATASETS,
        help="Dimension Gold a construir. Puede repetirse.",
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
    row_counts = build_municipal_dimensions(
        selected_datasets=args.dataset,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    for dataset, row_count in row_counts.items():
        print(f"{dataset}: {row_count} filas")


if __name__ == "__main__":
    main()
