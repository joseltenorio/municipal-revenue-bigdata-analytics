"""Módulos de construcción de la capa Bronze.

Este paquete agrupa procesos que leen archivos originales desde Landing y
generan datasets Parquet en Bronze, manteniendo la granularidad de origen y
aplicando solo transformaciones técnicas mínimas.

La capa Bronze no debe aplicar reglas de negocio, integración analítica ni
limpieza semántica fuerte. Su objetivo es preservar trazabilidad desde los
archivos originales hacia una representación optimizada en Parquet.
"""