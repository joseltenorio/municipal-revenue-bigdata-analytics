# Alcance del proyecto

## Nombre del proyecto

**Municipal Revenue Big Data Analytics**

## Repositorio

`municipal-revenue-bigdata-analytics`

## Descripción general

Municipal Revenue Big Data Analytics es un proyecto local de ingeniería y analítica de datos orientado al análisis del presupuesto, ejecución de ingresos e impuesto predial de municipalidades peruanas.

El proyecto integra tres fuentes públicas del MEF, SISMERE / Meta del Impuesto Predial e INEI, junto con una fuente manual controlada de categorías municipales entregada como CSV local, para construir un flujo completo de datos basado en arquitectura Medallion, procesamiento con Apache Spark/Python, almacenamiento en Parquet, catálogo SQL con Apache Hive y visualización final en Power BI.

El objetivo no es construir únicamente un dashboard, sino implementar una solución analítica trazable, documentada y defendible desde las perspectivas de Data Engineering, Data Analysis y Data Science inicial.

## Problema de negocio

Las municipalidades peruanas generan y reportan información relacionada con presupuesto, ejecución de ingresos, recaudación y cumplimiento de metas. Sin embargo, estos datos suelen encontrarse distribuidos en fuentes distintas, con formatos heterogéneos y estructuras que requieren exploración, limpieza e integración antes de ser útiles para el análisis.

Este proyecto busca responder preguntas como:

- ¿Qué municipalidades presentan mejor desempeño en ejecución de ingresos?
- ¿Qué brechas existen entre presupuesto, ejecución y cumplimiento de metas?
- ¿Cómo varía el desempeño por departamento, provincia o distrito?
- ¿Qué municipalidades muestran mayores brechas en la meta del impuesto predial?
- ¿Qué contexto territorial puede ayudar a interpretar los resultados municipales?

## Objetivo general

Construir una plataforma analítica local tipo lakehouse para analizar presupuesto, ejecución de ingresos e impuesto predial de municipalidades peruanas, integrando fuentes públicas mediante Apache Spark, Apache Hive, Parquet y Power BI.

## Objetivos específicos

- Explorar fuentes públicas reales y documentar sus métodos de acceso.
- Preservar archivos originales en una zona Landing.
- Convertir las fuentes hacia una capa Bronze en formato Parquet.
- Ejecutar profiling para identificar columnas, tipos, nulos, duplicados, llaves candidatas y problemas de integración.
- Aplicar controles de calidad de datos sobre las capas del lakehouse.
- Limpiar, tipar y estandarizar los datos en una capa Silver.
- Integrar información presupuestal, predial y territorial usando llaves geográficas o administrativas.
- Construir marts analíticos en una capa Gold.
- Registrar tablas externas en Apache Hive para consultar los datos del lakehouse mediante SQL.
- Conectar Power BI preferentemente a tablas Gold expuestas por Hive.
- Documentar hallazgos, limitaciones, decisiones técnicas y conclusiones analíticas.

## Fuentes consideradas

Las fuentes principales del proyecto son:

1. **Presupuesto y ejecución de ingresos - MEF / SIAF**

   Fuente orientada al análisis de presupuesto, ejecución y recaudación municipal.

2. **Seguimiento de la meta del impuesto predial - SISMERE / MEF**

   Fuente orientada al análisis del avance y cumplimiento de la meta relacionada con impuesto predial.

3. **Registro Nacional de Municipalidades RENAMU 2022 - INEI**

   Fuente contextual para enriquecer el análisis territorial y municipal.

4. **Categorías de municipalidades - CSV manual controlado**

   Fuente local entregada como insumo académico para clasificar municipalidades por categoría y habilitar segmentación analítica. A diferencia de las otras tres fuentes, este archivo no se descarga desde web y se versiona de forma controlada en `data/landing/category/`.

## Requerimientos académicos del caso

El proyecto debe cumplir con los siguientes requerimientos:

- Construcción de una arquitectura Medallion.
- Implementación de pipelines de ingesta basados en dicha arquitectura.
- Uso de Apache Spark.
- Uso de Apache Hive.
- Uso de Parquet desde la capa Bronze.
- Control de calidad de datos documentado.
- Profiling de datos documentado.
- Logs de auditoría de ingesta y procesamiento.
- Reporte Power BI con seis páginas orientadas a la toma de decisiones.
- Evidencias de ejecución y validación.
- Documentación técnica profesional.

## Herramientas principales

- Python.
- Apache Spark.
- Apache Hive.
- Hive Metastore.
- HiveServer2.
- Parquet.
- Power BI Desktop.
- Docker y Docker Compose.
- Git y GitHub.

## Alcance incluido

Este proyecto incluye:

- Diseño de una arquitectura local tipo lakehouse.
- Organización de datos por capas: Landing, Bronze, Silver y Gold.
- Preservación de archivos originales en Landing.
- Conversión de fuentes a Parquet desde Bronze.
- Profiling inicial de fuentes.
- Reglas de calidad de datos.
- Auditoría de ingesta y procesamiento.
- Reintentos y manejo de fallos de descarga.
- Limpieza y estandarización de datos.
- Integración de fuentes municipales.
- Creación de tablas externas en Hive.
- Construcción de marts analíticos Gold.
- Diseño de modelo semántico para Power BI.
- Conexión preferente Power BI - Hive mediante ODBC en modo Import.
- Fallback controlado a CSV o Parquet si la conexión local con Hive no es estable.
- Documentación técnica y evidencias para sustentación.

## Fuera de alcance

Este proyecto no incluye:

- Implementación en GCP, BigQuery, Dataflow, Cloud Run o servicios cloud.
- Procesamiento en tiempo real o streaming.
- Orquestación empresarial avanzada con Airflow u otra plataforma similar.
- Gobierno de datos empresarial completo.
- Seguridad avanzada por usuarios, roles o políticas de acceso.
- Machine Learning obligatorio o modelos predictivos complejos.
- Exposición de APIs productivas.
- Despliegue web del dashboard.
- Versionamiento de datos reales dentro de GitHub.

## Criterios de diseño

El proyecto se guiará por los siguientes criterios:

- No subir datos reales descargados, credenciales, `.env`, `.venv`, Parquet, ZIP, XLSX ni logs pesados al repositorio. La excepción controlada es `data/landing/category/CategoriasMunicipalidades.csv`, porque es una fuente manual pequeña no descargable desde web.
- Mantener las rutas internas del proyecto centralizadas en `src/common/paths.py`.
- No usar `.env` para rutas internas como `data/landing`, `data/bronze`, `data/silver` o `data/gold`.
- Definir el modelo Gold después del profiling y la integración Silver, no antes.
- Usar Hive como catálogo SQL real del lakehouse, no como herramienta decorativa.
- Priorizar Power BI conectado a Hive mediante ODBC en modo Import.
- Mantener un fallback controlado a CSV o Parquet solo si Hive-Power BI no es estable.
- Separar la documentación por propósito.
- Evitar sobreingeniería innecesaria.

## Resultado esperado

Al finalizar el proyecto se espera contar con:

- Repositorio profesional y ordenado.
- Arquitectura Medallion implementada localmente.
- Datos procesados en Parquet desde Bronze.
- Capa Silver limpia e integrada.
- Capa Gold orientada a análisis.
- Tablas externas Hive sobre Parquet.
- Dashboard Power BI con seis páginas.
- Reportes de profiling y calidad.
- Auditoría de ingesta y procesamiento.
- Evidencias de Spark, Hive, Parquet, Power BI y ejecución del pipeline.
- Conclusiones analíticas sobre ingresos municipales, cumplimiento predial y brechas territoriales.
