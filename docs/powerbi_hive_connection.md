# Conexión Power BI - Hive

## Propósito

Este documento resume la conexión recomendada de Power BI hacia Hive para consumir el modelo Gold objetivo.

## Conexión recomendada

- Base: `gold`
- Motor: HiveServer2
- Puerto: `10000`
- Modo: `Import`

## Tablas objetivo

Power BI debe priorizar:

- `mart_municipal_revenue_overview`
- `mart_predial_statistics_overview`
- `mart_municipal_context`
- `mart_territorial_summary`
- `dim_municipality`
- `dim_geography`
- `dim_time`
- `dim_sismepre_period`
- `dim_renamu_context`
- `audit_quality_results`
- `audit_dataset_summary`
- `audit_integration_coverage`

## Tablas que no deben usarse como navegación principal

- `map_sec_ejec_ubigeo`
- `municipal_entity_bridge`
- `mef_municipal_amounts`
- `renamu_full`
- `renamu_municipal_context`
- `fact_municipal_income_execution`

## Fallback

Si HiveServer2 no está estable, el fallback debe seguir la misma lógica de consumo del modelo objetivo y no reintroducir tablas legacy como navegación principal.
