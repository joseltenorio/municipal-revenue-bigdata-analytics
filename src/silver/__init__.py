"""Módulos de limpieza y estandarización de la capa Silver.

Este paquete agrupa procesos que leen datasets Parquet desde Bronze y generan
datasets Silver con limpieza técnica, tipado semántico inicial y columnas de
metadata propias de la capa.

La capa Silver no debe construir todavía métricas Gold, tablas Hive ni modelos
Power BI. Su objetivo es preparar datos limpios, trazables e integrables para
las etapas analíticas posteriores.
"""
