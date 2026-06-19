# Municipal Revenue Big Data Analytics

Plataforma analítica local para analizar ingresos municipales, cumplimiento sismepre y contexto territorial de municipalidades peruanas usando Apache Spark, Apache Hive, arquitectura Medallion, archivos Parquet y Power BI.

## Propósito

Este proyecto implementa un flujo local de ingeniería y analítica de datos que permite ingestar, organizar, transformar, validar, consultar y visualizar información municipal pública de forma trazable.

El objetivo no es construir únicamente un dashboard, sino desarrollar una solución completa de datos con capas Landing, Bronze, Silver y Gold, catálogo SQL en Hive y consumo analítico desde Power BI.

## Problema Analítico

La información municipal se encuentra distribuida en distintas fuentes públicas, con formatos y estructuras que requieren exploración, limpieza, validación e integración antes de ser utilizadas para análisis.

El proyecto busca responder preguntas como:

- ¿Qué municipalidades presentan mejor desempeño en ejecución de ingresos?
- ¿Qué brechas existen entre presupuesto, ejecución y cumplimiento de metas?
- ¿Cómo varía el desempeño municipal por departamento, provincia o distrito?
- ¿Qué municipalidades muestran mayores brechas en la meta del impuesto sismepre?
- ¿Qué contexto territorial y de capacidad municipal ayuda a interpretar los resultados?

## Fuentes Consideradas

Las fuentes principales del proyecto son:

- Presupuesto y ejecución de ingresos del MEF / SIAF.
- Seguimiento de la meta del impuesto sismepre desde SISMERE / MEF.
- Registro Nacional de Municipalidades RENAMU 2022 del INEI.
- Clasificación Municipal MEF 2019, publicada como siete PDF oficiales A-G.

## Arquitectura General

El proyecto sigue una arquitectura Medallion local:

```text
Fuentes públicas
-> Landing
-> Bronze Parquet
-> Profiling y Quality Gates
-> Silver Parquet
-> Integración Silver
-> Gold Parquet / Marts analíticos
-> Hive External Tables
-> Power BI
```

La capa Landing conserva archivos originales. Bronze convierte fuentes a Parquet manteniendo granularidad. Silver limpia, tipa, estandariza e integra. Gold contiene marts listos para análisis y consumo desde Power BI.

## Tecnologías Principales

- Python
- Apache Spark
- Apache Hive, Hive Metastore y HiveServer2
- Parquet
- Docker y Docker Compose
- Power BI Desktop
- Git y GitHub

## Ejecución Local Desde Cero

La guía completa para clonar, instalar dependencias, levantar Docker, validar Hive, ejecutar/verificar capas y abrir Power BI está en:

```text
docs/execution_guide.md
```

Esa guía está pensada para una persona que recibe el repositorio en una máquina local nueva y necesita preparar Git, Python, Docker Desktop, Hive y Power BI paso a paso.

Resumen mínimo:

```powershell
git clone <URL_DEL_REPOSITORIO>
cd municipal-revenue-bigdata-analytics
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
docker compose up -d
docker compose ps
```

Los datos reales no se versionan. Una persona que clone el repositorio debe ejecutar la ingesta y procesamiento local, o colocar los archivos de datos en las carpetas esperadas antes de construir las capas.

## Rol de Apache Spark

Apache Spark procesa los datos por capas, convierte fuentes a Parquet, aplica transformaciones, ejecuta validaciones y construye datasets analíticos.

## Rol de Apache Hive

Apache Hive funciona como catálogo SQL del lakehouse. Las tablas externas apuntan a archivos Parquet generados por Spark en Bronze, Silver y Gold, permitiendo consultas SQL sin mover ni duplicar los datos.

## Rol de Power BI

Power BI consume preferentemente las tablas Gold expuestas por HiveServer2/ODBC en modo Import. Si la conexión local entre Hive y Power BI no es estable, existe un fallback controlado de exportación Gold a CSV.

## Documentación

- `docs/project_scope.md`: alcance, objetivos y requerimientos.
- `docs/architecture.md`: arquitectura Medallion y flujo técnico.
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
- `docs/execution_guide.md`: guía de instalación y ejecución local desde cero.

## Alcance

El proyecto incluye:

- Arquitectura Medallion local.
- Ingesta de fuentes públicas hacia Landing.
- Conversión a Parquet en Bronze.
- Profiling y reglas de calidad.
- Transformaciones Silver.
- Integración Silver.
- Marts analíticos Gold.
- Tablas externas en Hive.
- Reporte Power BI con conexión preferente a Hive y fallback CSV.
- Evidencias y documentación técnica.

## Fuera de Alcance

El proyecto no incluye:

- Implementación en GCP, BigQuery, Dataflow o servicios cloud.
- Procesamiento en tiempo real.
- Orquestación empresarial avanzada.
- Machine Learning obligatorio.
- Versionamiento de datos reales en GitHub.

## Versionamiento de Datos

El repositorio versiona código, configuración pública, SQL, tests y documentación. No versiona datasets reales ni archivos pesados como CSV, ZIP, PDF, XLSX, Parquet, reportes generados, logs pesados, `.env`, `.venv` o exports Power BI.
