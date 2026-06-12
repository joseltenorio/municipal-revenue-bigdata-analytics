# Modelo Hive del lakehouse

## Propósito

Apache Hive cumple el rol de catálogo SQL del lakehouse local. En este proyecto, Hive no mueve ni duplica datos: registra tablas externas sobre archivos Parquet generados previamente por Spark en las capas Bronze y Silver.

La relación operativa es:

- Spark genera datasets Parquet en `data/bronze`, `data/silver` y, más adelante, `data/gold`.
- Hive registra esos Parquet como tablas externas.
- Beeline permite validar las bases, tablas y consultas SQL.
- Power BI consumirá preferentemente las tablas Gold expuestas por HiveServer2 mediante ODBC/JDBC.

## Bases Hive

Se crean tres bases:

| Base | Estado actual | Uso esperado |
| --- | --- | --- |
| `bronze` | Disponible con tablas externas | Consulta técnica de recursos en granularidad original |
| `silver` | Disponible con tablas externas | Consulta de datos limpios, tipados e integrados |
| `gold` | Disponible con tablas externas | Capa final para marts analíticos y consumo de reportabilidad |

Gold ahora está completamente poblado con tablas externas mapeando los marts de la arquitectura Medallion.

## Tablas Bronze

La base `bronze` registra 25 tablas externas. Cada tabla representa un recurso Bronze específico bajo su `resource_key`.

El diseño usa una tabla por recurso en lugar de una tabla particionada única porque:

- Las fuentes tienen esquemas heterogéneos.
- Predial contiene recursos con estructuras distintas.
- RENAMU es un dataset ancho.
- La validación con `SELECT COUNT(*)` es directa.
- Se evita depender de reparación de particiones con `MSCK REPAIR TABLE`.

Las tablas Bronze conservan la granularidad original de los archivos fuente. No deben usarse como modelo analítico final.

## Tablas Silver

La base `silver` registra 30 tablas externas:

- Recursos Silver de MEF por `resource_key`.
- Recursos Silver de Predial por `resource_key`.
- Recurso Silver de RENAMU.
- Datasets integrados Silver.

Las tablas integradas principales son:

| Tabla | Propósito |
| --- | --- |
| `silver.municipal_entity_bridge` | Puente municipal entre identificadores administrativos y territoriales |
| `silver.mef_municipal_amounts` | Montos MEF agregados con granularidad controlada |
| `silver.predial_entity_period` | Información predial integrada por entidad, periodo, formulario y tiempo estadístico |
| `silver.renamu_municipal_context` | Contexto territorial y municipal desde RENAMU |
| `silver.integration_coverage` | Métricas técnicas de cobertura de cruce entre fuentes |

Estas tablas permiten validar integración y cobertura antes de construir Gold. No constituyen todavía la capa analítica final.

## Gold

La base `gold` registra 15 tablas externas correspondientes a los marts y dimensiones analíticas generados para ingresos municipales, cumplimiento predial y contexto territorial.

El archivo `sql/hive/create_gold_external_tables.sql` define los DDL correspondientes a las siguientes tablas de negocio:

* **Ingresos municipales (`municipal_revenue`):**
  * `gold.dim_municipality`: Dimensión municipal unificada con ubigeos y códigos ejecutores.
  * `gold.dim_time`: Dimensión temporal a nivel anual/mensual.
  * `gold.fact_municipal_income_execution`: Hecho de ejecución de ingresos mensuales y anuales con KPIs de recaudación vs PIA/PIM.
  * `gold.mart_municipal_revenue_overview`: Agregado ejecutivo para análisis visual.
  * `gold.fact_revenue_integration_coverage`: Cobertura técnica de integración presupuestal.
* **Cumplimiento predial (`predial_compliance`):**
  * `gold.dim_predial_period`: Dimensión temporal y de contexto estadístico predial.
  * `gold.fact_predial_compliance`: Hecho de cumplimiento e importes de recaudación y saldos del impuesto predial.
  * `gold.mart_predial_compliance_overview`: Mart agregado de efectividad predial.
  * `gold.mart_predial_ranking`: Ranking municipal según volumen y efectividad de recaudación.
  * `gold.fact_predial_integration_coverage`: Cobertura de la integración predial.
* **Contexto territorial (`territorial_context`):**
  * `gold.dim_geography`: Dimensión geográfica jerárquica a nivel distrital, provincial y departamental.
  * `gold.dim_municipality_context`: Atributos y tipo de municipalidades.
  * `gold.mart_municipal_capacity`: Mart de capacidades institucionales (personal, sistemas, internet, catastro, etc.).
  * `gold.mart_territorial_context`: Agrupador analítico distrital y provincial.
  * `gold.fact_territorial_integration_coverage`: Cobertura del cruce geográfico-territorial.

## Generación de SQL

El SQL de tablas externas se genera con:

```powershell
docker compose run --rm python-app python -m src.hive.generate_external_tables --overwrite-sql --validate-inputs
```

El script:

- Lee Parquet existentes con Spark.
- Infere schemas.
- Traduce tipos Spark a tipos Hive.
- Genera `CREATE EXTERNAL TABLE IF NOT EXISTS`.
- Usa `STORED AS PARQUET`.
- Usa rutas absolutas internas `LOCATION '/app/data/...'`.
- No ejecuta Beeline.
- No modifica archivos Parquet.

Las rutas `/app/data/...` son visibles tanto para HiveServer2 como para Hive Metastore. Para ello, `hive-metastore` monta `./data:/app/data` en Docker Compose.

## Validaciones ejecutadas

Se validó la generación y aplicación del SQL con:

- `create_databases.sql`: correcto.
- `create_bronze_external_tables.sql`: correcto.
- `create_silver_external_tables.sql`: correcto.
- `create_gold_external_tables.sql`: correcto con DDLs dinámicos mapeando los marts Parquet.
- `SHOW DATABASES`: mostró `bronze`, `silver` y `gold`.
- `SHOW TABLES IN bronze`: 25 tablas.
- `SHOW TABLES IN silver`: 30 tablas.
- `SHOW TABLES IN gold`: 15 tablas registradas y funcionales.

También se validaron consultas sobre tablas integradas:

| Consulta | Resultado |
| --- | ---: |
| `SELECT COUNT(*) FROM silver.integration_coverage` | 6 |
| `SELECT COUNT(*) FROM silver.municipal_entity_bridge` | 2598 |
| `SELECT COUNT(*) FROM silver.predial_entity_period` | 133938 |
| `SELECT COUNT(*) FROM silver.renamu_municipal_context` | 1874 |
| `SELECT * FROM silver.mef_municipal_amounts LIMIT 5` | Correcto |

La tabla `silver.mef_municipal_amounts` tiene más de 12 millones de filas, por lo que se validó con `LIMIT 5` en lugar de un conteo completo.

## Interpretación

Hive cataloga de forma completa las tres capas del lakehouse local: Bronze (fuentes crudas), Silver (datos depurados y cruzados) y Gold (marts listos para visualización). 

Power BI consume preferentemente los marts y dimensiones analíticas finales de la capa `gold` expuestos por HiveServer2 mediante el driver ODBC. Si la conexión por Hive experimentase inestabilidad local debido a restricciones de red o controladores locales del sistema anfitrión, Power BI puede utilizar un fallback directo consumiendo los archivos Parquet físicos en `data/gold/` o una exportación local CSV controlada como respaldo de contingencia.

## Limitaciones

- Las tablas Bronze y Silver son para auditoría interna e ingeniería de datos; no deben exponerse al usuario de negocio de Power BI.
- Beeline puede mostrar warnings de Log4j o SLF4J; no afectan la ejecución de consultas.
