import pytest
from pathlib import Path
from datetime import datetime, timezone
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.silver.transform_municipal_classification import (  # noqa: E402
    transform_municipal_classification_dataframe,
    validate_required_columns,
    validate_expected_row_count,
    validate_unique_ubigeo6,
    validate_type_distribution,
    validate_hard_quality_rules,
    FINAL_COLUMNS,
    EXPECTED_TOTAL_ROWS
)

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("TestSilverMunicipalClassification") \
        .master("local[1]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()

@pytest.fixture
def valid_bronze_df(spark):
    schema = StructType([
        StructField("anio", IntegerType(), True),
        StructField("tipo_clasificacion", StringType(), True),
        StructField("ambito_municipal", StringType(), True),
        StructField("descripcion_tipo", StringType(), True),
        StructField("nro", IntegerType(), True),
        StructField("ubigeo", StringType(), True),
        StructField("departamento_nombre", StringType(), True),
        StructField("provincia_nombre", StringType(), True),
        StructField("distrito_nombre", StringType(), True)
    ])
    
    data = [
        (2019, "A", "provincial", "Tipo A", 1, "010101", "AMAZONAS", "CHACHAPOYAS", "CHACHAPOYAS"),
        (2019, "B", "provincial", "Tipo B", 2, "010102", "AMAZONAS", "CHACHAPOYAS", "ASUNCION"),
        (2019, "C", "distrital", "Tipo C", 3, "010103", "AMAZONAS", "CHACHAPOYAS", "BALSAS"),
        (2019, "D", "distrital", "Tipo D", 4, "010104", "AMAZONAS", "CHACHAPOYAS", "CHETO"),
        (2019, "E", "distrital", "Tipo E", 5, "010105", "AMAZONAS", "CHACHAPOYAS", "CHILIQUIN"),
        (2019, "F", "distrital", "Tipo F", 6, "010106", "AMAZONAS", "CHACHAPOYAS", "CHUQUIBAMBA"),
        (2019, "G", "distrital", "Tipo G", 7, "010107", "AMAZONAS", "CHACHAPOYAS", "GRANADA")
    ]
    return spark.createDataFrame(data, schema)

def test_transform_municipal_classification_dataframe(spark, valid_bronze_df):
    current_time = datetime.now(timezone.utc)
    silver_df = transform_municipal_classification_dataframe(valid_bronze_df, current_time)
    
    assert silver_df.columns == FINAL_COLUMNS
    
    row = silver_df.collect()[0]
    
    assert row["is_valid_ubigeo6"] is True
    assert row["is_valid_tipo_clasificacion_municipal"] is True
    assert row["is_valid_ambito_municipal"] is True
    assert row["is_valid_tipo_ambito_consistency"] is True

    assert row["silver_source_name"] == "municipal_classification"
    assert row["silver_resource_key"] == "classification_2019"

def test_validate_required_columns(spark, valid_bronze_df):
    validate_required_columns(valid_bronze_df)
    
    invalid_df = valid_bronze_df.drop("anio")
    with pytest.raises(ValueError, match="Missing required Bronze columns"):
        validate_required_columns(invalid_df)

def test_validate_expected_row_count(spark):
    df = spark.createDataFrame([(i,) for i in range(EXPECTED_TOTAL_ROWS)], ["a"])
    validate_expected_row_count(df)
    
    with pytest.raises(ValueError, match="Expected"):
        validate_expected_row_count(spark.createDataFrame([(i,) for i in range(10)], ["a"]))

def test_validate_unique_ubigeo6(spark):
    df = spark.createDataFrame([("010101",), ("010102",)], ["ubigeo6"])
    validate_unique_ubigeo6(df)
    
    df_dup = spark.createDataFrame([("010101",), ("010101",)], ["ubigeo6"])
    with pytest.raises(ValueError, match="Found 1 duplicated ubigeo6"):
        validate_unique_ubigeo6(df_dup)

def test_validate_type_distribution(spark):
    data = []
    data.extend([("A",)] * 74)
    data.extend([("B",)] * 122)
    data.extend([("C",)] * 42)
    data.extend([("D",)] * 129)
    data.extend([("E",)] * 378)
    data.extend([("F",)] * 509)
    data.extend([("G",)] * 620)
    df = spark.createDataFrame(data, ["tipo_clasificacion_municipal"])
    validate_type_distribution(df)
    
    data_invalid = data[:-1]
    df_invalid = spark.createDataFrame(data_invalid, ["tipo_clasificacion_municipal"])
    with pytest.raises(ValueError, match="Expected"):
        validate_type_distribution(df_invalid)

def test_consistency_rules(spark):
    current_time = datetime.now(timezone.utc)
    
    schema = StructType([
        StructField("anio", IntegerType(), True),
        StructField("tipo_clasificacion", StringType(), True),
        StructField("ambito_municipal", StringType(), True),
        StructField("descripcion_tipo", StringType(), True),
        StructField("nro", IntegerType(), True),
        StructField("ubigeo", StringType(), True),
        StructField("departamento_nombre", StringType(), True),
        StructField("provincia_nombre", StringType(), True),
        StructField("distrito_nombre", StringType(), True)
    ])
    
    # A + distrital -> False
    df1 = spark.createDataFrame([(2019, "A", "distrital", "x", 1, "010101", "x", "x", "x")], schema)
    res1 = transform_municipal_classification_dataframe(df1, current_time)
    assert not res1.collect()[0]["is_valid_tipo_ambito_consistency"]
    
    # C + provincial -> False
    df2 = spark.createDataFrame([(2019, "C", "provincial", "x", 2, "010102", "x", "x", "x")], schema)
    res2 = transform_municipal_classification_dataframe(df2, current_time)
    assert not res2.collect()[0]["is_valid_tipo_ambito_consistency"]

def test_anio_must_be_2019(spark):
    current_time = datetime.now(timezone.utc)
    schema = StructType([
        StructField("anio", IntegerType(), True),
        StructField("tipo_clasificacion", StringType(), True),
        StructField("ambito_municipal", StringType(), True),
        StructField("descripcion_tipo", StringType(), True),
        StructField("nro", IntegerType(), True),
        StructField("ubigeo", StringType(), True),
        StructField("departamento_nombre", StringType(), True),
        StructField("provincia_nombre", StringType(), True),
        StructField("distrito_nombre", StringType(), True)
    ])
    df = spark.createDataFrame([(2020, "A", "provincial", "x", 1, "010101", "x", "x", "x")], schema)
    with pytest.raises(ValueError, match="anio != 2019"):
        transform_municipal_classification_dataframe(df, current_time)

def test_validate_hard_quality_rules(spark):
    from pyspark.sql.types import BooleanType
    schema = StructType([
        StructField("is_valid_ubigeo6", BooleanType(), True),
        StructField("is_valid_tipo_clasificacion_municipal", BooleanType(), True),
        StructField("is_valid_ambito_municipal", BooleanType(), True),
        StructField("is_valid_tipo_ambito_consistency", BooleanType(), True)
    ])
    
    df_valid = spark.createDataFrame([(True, True, True, True)], schema)
    validate_hard_quality_rules(df_valid)
    
    df_invalid = spark.createDataFrame([(False, True, True, True)], schema)
    with pytest.raises(ValueError, match="Found 1 rows with is_valid_ubigeo6 = False"):
        validate_hard_quality_rules(df_invalid)
