"""Creación centralizada de SparkSession para el proyecto."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import SparkSession

from src.common.config import get_config_value, load_spark_config


def build_spark_session(
    app_name: str | None = None,
    master: str | None = None,
    extra_configs: dict[str, Any] | None = None,
) -> SparkSession:
    """Crea una SparkSession con configuración base del proyecto.

    La configuración se toma de config/spark.yaml y puede ser sobrescrita por
    variables de entorno o parámetros explícitos.
    """

    spark_config = load_spark_config()

    resolved_app_name = (
        app_name
        or os.getenv("SPARK_APP_NAME")
        or get_config_value(spark_config, "spark.app_name", "MunicipalRevenueLakehouse")
    )

    resolved_master = (
        master
        or os.getenv("SPARK_MASTER")
        or get_config_value(spark_config, "spark.master", "local[*]")
    )

    builder = (
        SparkSession.builder.appName(resolved_app_name)
        .master(resolved_master)
        .config(
            "spark.sql.session.timeZone",
            get_config_value(spark_config, "spark.sql.session_time_zone", "America/Lima"),
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(get_config_value(spark_config, "spark.sql.shuffle_partitions", 8)),
        )
        .config(
            "spark.sql.adaptive.enabled",
            str(get_config_value(spark_config, "spark.sql.adaptive_enabled", True)).lower(),
        )
        .config(
            "spark.sql.caseSensitive",
            str(get_config_value(spark_config, "spark.sql.case_sensitive", False)).lower(),
        )
        .config(
            "spark.sql.parquet.compression.codec",
            get_config_value(spark_config, "spark.parquet.compression", "snappy"),
        )
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    log_level = get_config_value(spark_config, "spark.local.log_level", "WARN")
    spark.sparkContext.setLogLevel(log_level)

    return spark