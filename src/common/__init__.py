"""Componentes comunes del proyecto Municipal Revenue Big Data Analytics.

Este paquete centraliza utilidades compartidas como resolución de rutas,
lectura de configuración, logging, auditoría, reintentos y creación de sesiones
Spark.

Las rutas internas del proyecto deben resolverse desde `src.common.paths` y no
desde variables `.env`.
"""