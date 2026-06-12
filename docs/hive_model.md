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
| `gold` | Disponible sin tablas | Capa final para marts analíticos futuros |

Gold existe como base para mantener la arquitectura completa, pero todavía no tiene tablas externas porque los marts Gold aún no se han construido.

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

La base `gold` se crea desde `sql/hive/create_databases.sql`.

El archivo `sql/hive/create_gold_external_tables.sql` es un placeholder no-op documentado. No registra tablas porque no existen Parquet Gold en esta etapa.

No se deben inventar tablas Gold antes de construir los marts correspondientes.

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
- `create_gold_external_tables.sql`: correcto como placeholder no-op.
- `SHOW DATABASES`: mostró `bronze`, `silver` y `gold`.
- `SHOW TABLES IN bronze`: 25 tablas.
- `SHOW TABLES IN silver`: 30 tablas.
- `SHOW TABLES IN gold`: 0 tablas, esperado en esta etapa.

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

Hive ya cumple un rol real en la arquitectura del proyecto: cataloga datasets Parquet existentes y permite validarlos mediante SQL.

El estado actual es suficiente para consultar Bronze y Silver desde Hive. Sin embargo, todavía no representa la capa final de consumo analítico, porque Gold no existe. Power BI deberá conectarse preferentemente a tablas Gold cuando los marts estén construidos y registrados.

## Limitaciones

- Gold aún no tiene tablas externas.
- Las tablas Bronze y Silver son útiles para validación técnica, pero no deben exponerse como modelo final de negocio.
- Beeline puede mostrar warnings de Log4j o SLF4J; no fueron bloqueantes durante las validaciones.
- La conexión con Power BI todavía debe validarse después de construir Gold.
