# Municipal Revenue Big Data Analytics

Plataforma analítica local para analizar presupuesto, ejecución de ingresos e impuesto predial de municipalidades peruanas usando Apache Spark, Apache Hive, arquitectura Medallion, archivos Parquet y Power BI.

## Propósito

Este proyecto implementa un flujo analítico de datos orientado a municipalidades peruanas, integrando fuentes públicas relacionadas con presupuesto, ejecución de ingresos, seguimiento de la meta del impuesto predial y contexto municipal.

El objetivo no es construir únicamente un dashboard, sino desarrollar una solución de ingeniería y analítica de datos que permita ingestar, organizar, transformar, validar, consultar y visualizar información municipal de forma trazable.

## Problema analítico

La información municipal se encuentra distribuida en distintas fuentes públicas, con formatos y estructuras que requieren exploración, limpieza e integración antes de ser utilizadas para análisis.

El proyecto busca responder preguntas como:

- ¿Qué municipalidades presentan mejor desempeño en ejecución de ingresos?
- ¿Qué brechas existen entre presupuesto, ejecución y cumplimiento de metas?
- ¿Cómo varía el desempeño municipal por departamento, provincia o distrito?
- ¿Qué municipalidades muestran mayores brechas en la meta del impuesto predial?
- ¿Qué contexto territorial puede ayudar a interpretar los resultados?

## Fuentes consideradas

Las fuentes principales del proyecto son:

- Presupuesto y ejecución de ingresos del MEF / SIAF.
- Seguimiento de la meta del impuesto predial desde SISMERE / MEF.
- Registro Nacional de Municipalidades RENAMU 2022 del INEI.

Estas fuentes serán exploradas, perfiladas y documentadas antes de definir el modelo analítico final.

## Arquitectura general

El proyecto sigue una arquitectura Medallion local:

Fuentes públicas
-> Landing
-> Bronze Parquet
-> Profiling y Quality Gates
-> Silver Parquet
-> Gold Parquet / Marts analíticos
-> Hive External Tables
-> Power BI conectado preferentemente a Hive

La capa Landing conserva archivos originales.
La capa Bronze convierte las fuentes a Parquet y mantiene la granularidad original.
La capa Silver limpia, tipa, estandariza e integra las fuentes.
La capa Gold contiene datasets listos para análisis y consumo desde Power BI.

## Tecnologías principales

- Python
- Apache Spark
- Apache Hive
- Hive Metastore
- HiveServer2
- Parquet
- Power BI Desktop
- Docker y Docker Compose
- Git y GitHub

## Rol de Apache Spark

Apache Spark será usado para procesar los datos por capas, convertir fuentes a Parquet, aplicar transformaciones, ejecutar validaciones y construir datasets analíticos.

## Rol de Apache Hive

Apache Hive funcionará como catálogo SQL del lakehouse. Las tablas externas de Hive apuntarán a archivos Parquet generados por Spark, permitiendo consultar las capas Bronze, Silver y Gold sin mover los datos.

## Rol de Power BI

Power BI consumirá preferentemente las tablas Gold expuestas mediante HiveServer2/ODBC en modo Import. Si la conexión local entre Hive y Power BI no es estable, se usará un fallback controlado mediante exportación de Gold a CSV o Parquet, sin eliminar Hive del flujo técnico.

## Alcance

El proyecto incluye:

- Arquitectura Medallion local.
- Ingesta de fuentes públicas hacia Landing.
- Conversión a Parquet desde Bronze.
- Profiling de datos.
- Reglas de calidad.
- Auditoría de ingesta y procesamiento.
- Transformaciones Silver.
- Marts analíticos Gold.
- Tablas externas en Hive.
- Reporte Power BI con seis páginas.
- Evidencias y documentación técnica.

## Fuera de alcance

El proyecto no incluye:

- Implementación en GCP, BigQuery, Dataflow o servicios cloud.
- Procesamiento en tiempo real.
- Orquestación empresarial avanzada.
- Machine Learning obligatorio.
- Versionamiento de datos reales en GitHub.

## Documentación

La documentación técnica se organizará por propósito:

- `docs/project_scope.md`: alcance, objetivos y requerimientos.
- `docs/architecture.md`: arquitectura Medallion, flujo técnico, rol de Spark, Hive, Parquet y Power BI.
- `docs/data_sources.md`: inventario de fuentes.
- `docs/source_discovery.md`: hallazgos de acceso a fuentes.
- `docs/data_profiling.md`: profiling de datos.
- `docs/data_quality.md`: reglas y resultados de calidad.
- `docs/ingestion_audit.md`: auditoría de ingesta.
- `docs/silver_transformations.md`: reglas de limpieza e integración.
- `docs/hive_model.md`: bases y tablas externas Hive.
- `docs/gold_model.md`: modelo analítico final.
- `docs/powerbi_model.md`: modelo semántico y páginas del reporte.
- `docs/powerbi_hive_connection.md`: conexión Hive - Power BI.
- `docs/final_insights.md`: conclusiones analíticas.
- `docs/execution_guide.md`: guía de ejecución local.

## Estado del proyecto

Proyecto en fase inicial de ingeniería de datos.

La estructura base del repositorio, el alcance analítico, la arquitectura Medallion, el inventario inicial de fuentes, los scripts de discovery, el profiling inicial y la configuración local de Spark/Hive ya se encuentran definidos.

El entorno local fue validado con:

- Docker Compose.
- Apache Spark Master y Spark Worker.
- Apache Hive Metastore.
- HiveServer2.
- Conexión Beeline hacia HiveServer2.

Las siguientes fases se enfocarán en implementar utilidades comunes, ingesta controlada hacia Landing, conversión a Bronze Parquet, reglas de calidad, transformaciones Silver, tablas externas Hive, marts Gold y consumo analítico desde Power BI.
