import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

SOURCE_NAME = "municipal_classification"
SILVER_RESOURCE_KEY = "classification_2019"
EXPECTED_TOTAL_ROWS = 1874
EXPECTED_TYPE_COUNTS = {
    "A": 74,
    "B": 122,
    "C": 42,
    "D": 129,
    "E": 378,
    "F": 509,
    "G": 620,
}
VALID_TYPES = {"A", "B", "C", "D", "E", "F", "G"}
VALID_AMBITOS = {"provincial", "distrital"}

BRONZE_REQUIRED_COLUMNS = [
    "anio",
    "tipo_clasificacion",
    "ambito_municipal",
    "descripcion_tipo",
    "nro",
    "ubigeo",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre"
]

FINAL_COLUMNS = [
    "ubigeo6",
    "tipo_clasificacion_municipal",
    "ambito_municipal",
    "descripcion_tipo",
    "departamento_nombre",
    "provincia_nombre",
    "distrito_nombre",
    "source_row_number",
    "is_valid_ubigeo6",
    "is_valid_tipo_clasificacion_municipal",
    "is_valid_ambito_municipal",
    "is_valid_tipo_ambito_consistency",
    "silver_source_name",
    "silver_resource_key",
    "silver_processed_at_utc"
]

def load_bronze_classification(spark: SparkSession, bronze_path: Path):
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze dataset not found: {bronze_path}")
    return spark.read.parquet(str(bronze_path))

def validate_required_columns(df):
    missing = [c for c in BRONZE_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Bronze columns: {missing}")

def validate_expected_row_count(df):
    count = df.count()
    if count != EXPECTED_TOTAL_ROWS:
        raise ValueError(f"Expected {EXPECTED_TOTAL_ROWS} rows, got {count}.")

def validate_unique_ubigeo6(df):
    if "ubigeo6" in df.columns:
        dup_count = df.groupBy("ubigeo6").count().filter(F.col("count") > 1).count()
        if dup_count > 0:
            raise ValueError(f"Found {dup_count} duplicated ubigeo6")

def validate_type_distribution(df):
    if "tipo_clasificacion_municipal" in df.columns:
        counts = df.groupBy("tipo_clasificacion_municipal").count().collect()
        counts_dict = {row["tipo_clasificacion_municipal"]: row["count"] for row in counts}
        for k, v in EXPECTED_TYPE_COUNTS.items():
            if counts_dict.get(k, 0) != v:
                raise ValueError(f"Expected {v} for type {k}, got {counts_dict.get(k, 0)}")

def validate_hard_quality_rules(df):
    flags = [
        "is_valid_ubigeo6",
        "is_valid_tipo_clasificacion_municipal",
        "is_valid_ambito_municipal",
        "is_valid_tipo_ambito_consistency"
    ]
    for flag in flags:
        invalid_count = df.filter(~F.col(flag)).count()
        if invalid_count > 0:
            raise ValueError(f"Found {invalid_count} rows with {flag} = False")

def transform_municipal_classification_dataframe(df, current_time: datetime):
    validate_required_columns(df)
    
    anio_invalid = df.filter(F.col("anio") != 2019).count()
    if anio_invalid > 0:
        raise ValueError(f"Found {anio_invalid} rows with anio != 2019")

    df = df.withColumnRenamed("ubigeo", "ubigeo6") \
           .withColumnRenamed("tipo_clasificacion", "tipo_clasificacion_municipal") \
           .withColumnRenamed("nro", "source_row_number")

    df = df.withColumn("ubigeo6", F.col("ubigeo6").cast(StringType())) \
           .withColumn("tipo_clasificacion_municipal", F.upper(F.col("tipo_clasificacion_municipal").cast(StringType()))) \
           .withColumn("ambito_municipal", F.lower(F.col("ambito_municipal").cast(StringType()))) \
           .withColumn("descripcion_tipo", F.regexp_replace(F.trim(F.col("descripcion_tipo").cast(StringType())), r"\s+", " ")) \
           .withColumn("departamento_nombre", F.regexp_replace(F.trim(F.col("departamento_nombre").cast(StringType())), r"\s+", " ")) \
           .withColumn("provincia_nombre", F.regexp_replace(F.trim(F.col("provincia_nombre").cast(StringType())), r"\s+", " ")) \
           .withColumn("distrito_nombre", F.regexp_replace(F.trim(F.col("distrito_nombre").cast(StringType())), r"\s+", " ")) \
           .withColumn("source_row_number", F.col("source_row_number").cast(IntegerType()))

    df = df.withColumn("is_valid_ubigeo6", F.col("ubigeo6").rlike(r"^\d{6}$")) \
           .withColumn("is_valid_tipo_clasificacion_municipal", F.col("tipo_clasificacion_municipal").isin(list(VALID_TYPES))) \
           .withColumn("is_valid_ambito_municipal", F.col("ambito_municipal").isin(list(VALID_AMBITOS)))

    df = df.withColumn(
        "is_valid_tipo_ambito_consistency",
        (F.col("tipo_clasificacion_municipal").isin(["A", "B"]) & (F.col("ambito_municipal") == "provincial")) |
        (F.col("tipo_clasificacion_municipal").isin(["C", "D", "E", "F", "G"]) & (F.col("ambito_municipal") == "distrital"))
    )

    df = df.withColumn("is_valid_ubigeo6", F.coalesce(F.col("is_valid_ubigeo6"), F.lit(False))) \
           .withColumn("is_valid_tipo_clasificacion_municipal", F.coalesce(F.col("is_valid_tipo_clasificacion_municipal"), F.lit(False))) \
           .withColumn("is_valid_ambito_municipal", F.coalesce(F.col("is_valid_ambito_municipal"), F.lit(False))) \
           .withColumn("is_valid_tipo_ambito_consistency", F.coalesce(F.col("is_valid_tipo_ambito_consistency"), F.lit(False)))

    df = df.withColumn("silver_source_name", F.lit(SOURCE_NAME)) \
           .withColumn("silver_resource_key", F.lit(SILVER_RESOURCE_KEY)) \
           .withColumn("silver_processed_at_utc", F.lit(current_time.isoformat()))

    return df.select(*FINAL_COLUMNS)

def write_silver_dataset(df, output_dir: Path, overwrite: bool):
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(f"Output directory {output_dir} already exists. Use --overwrite.")
        import shutil
        shutil.rmtree(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").parquet(str(output_dir))
    logger.info(f"Wrote to {output_dir}")

def main():
    parser = argparse.ArgumentParser(description="Transform Municipal Classification to Silver")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent.parent
    bronze_path = project_root / "data" / "bronze" / "municipal_classification" / "data.parquet"
    silver_out = project_root / "data" / "silver" / "municipal_classification" / f"resource_key={SILVER_RESOURCE_KEY}"

    spark = SparkSession.builder \
        .appName("SilverMunicipalClassification") \
        .master("local[*]") \
        .getOrCreate()
        
    # Disable log output
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info(f"Loading Bronze dataset from {bronze_path}")
        df_bronze = load_bronze_classification(spark, bronze_path)
        
        current_time = datetime.now(timezone.utc)
        
        logger.info("Applying transformations")
        df_silver = transform_municipal_classification_dataframe(df_bronze, current_time)
        
        # Cache to run validations
        df_silver.cache()
        
        logger.info("Running hard quality rules")
        validate_expected_row_count(df_silver)
        validate_unique_ubigeo6(df_silver)
        validate_type_distribution(df_silver)
        validate_hard_quality_rules(df_silver)
        
        if args.dry_run:
            logger.info("DRY RUN: Validations passed. Output would be written to:")
            logger.info(f"  {silver_out}")
            return
            
        write_silver_dataset(df_silver, silver_out, args.overwrite)
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
