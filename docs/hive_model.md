# Modelo Hive objetivo

## Propósito

Apache Hive funciona como catálogo SQL del lakehouse local.

En este proyecto, Hive no define la lógica de negocio. Sólo registra tablas externas sobre los Parquet generados por Spark para que Power BI y las consultas SQL trabajen sobre nombres estables.

## Capas registradas

| Base | Rol |
| --- | --- |
| `bronze` | Catálogo técnico de recursos crudos preservados |
| `silver` | Catálogo técnico de datasets limpios, tipados e integrados |
| `gold` | Catálogo analítico final para negocio y Power BI |

## Silver objetivo

Silver sigue existiendo como capa técnica de integración y trazabilidad. Para este proyecto, los nombres relevantes a documentar son:

- `silver/municipal_classification/resource_key=classification_2019`
- `silver/sismepre/resource_key=esat_estadistica_atm`
- `silver/renamu/resource_key=municipal_context`
- `silver/map_sec_ejec_ubigeo`

Los otros recursos SISMEPRE y RENAMU pueden permanecer en Silver por trazabilidad, pero no forman parte del Gold inicial ni del dashboard principal.

## Gold objetivo

La capa Gold objetivo se alinea con el modelo dimensional documentado en `docs/gold_model.md`.

### Tablas Gold objetivo

#### Dimensiones y contexto

- `gold.dim_municipality`
- `gold.dim_geography`
- `gold.dim_renamu_context`
- `gold.dim_time`
- `gold.dim_sismepre_period`

#### Hechos

- `gold.fact_siaf_income`
- `gold.fact_predial_statistics`

#### Marts

- `gold.mart_municipal_revenue_overview`
- `gold.mart_predial_statistics_overview`
- `gold.mart_municipal_context`
- `gold.mart_territorial_summary`

#### Auditoría

- `gold.audit_quality_results`
- `gold.audit_dataset_summary`

## Propósito de `map_sec_ejec_ubigeo`

`map_sec_ejec_ubigeo` debe documentarse y conservarse como un mapa técnico Silver.

Sirve para:

- resolver `sec_ejec -> ubigeo6 -> municipality_key`
- conectar SIAF con RENAMU
- conectar SIAF con clasificación municipal oficial
- evitar joins manuales por nombre en Power BI

No debe tratarse como dimensión de negocio ni como tabla principal de consumo.

## Legacy

Las siguientes referencias quedan como legacy o transicionales:

- `silver.municipal_entity_bridge`
- `silver.mef_municipal_amounts`
- `silver.renamu_municipal_context`
- `silver.renamu_full`
- `gold.fact_municipal_income_execution`
- `gold.dim_municipality_context`
- `gold.fact_predial_compliance`
- `gold.mart_municipal_capacity`
- `gold.mart_sismepre_ranking`

También son legacy las referencias a:

- `municipal_categories`
- `categorias_municipalidades`
- `CategoriasMunicipalidades.csv`
- matching manual por nombre como estrategia principal

## Generación de SQL

Las tablas externas de Hive se generan a partir de los Parquet ya materializados por Spark.

Hive debe mantener el contrato de nombres y ubicaciones, pero no debe imponer una lógica de transformación diferente a la documentada en Silver y Gold.

## Consumo

Power BI debe conectarse preferentemente a `gold` mediante HiveServer2/ODBC.

El mapa técnico Silver y las tablas de auditoría no deben exponerse como navegación principal del usuario de negocio.
