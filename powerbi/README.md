# Directorio Power BI

Este directorio agrupa recursos de consumo y evidencia para el reporte final.

## Contenido

- `exports/`: CSVs de contingencia, no versionables.
- `screenshots/`: evidencias de conexión, modelo y validación.

## Modelo esperado

Power BI debe construirse sobre:

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

## Legacy

No deben volver como navegación principal:

- `fact_municipal_income_execution`
- `fact_predial_compliance`
- `mart_municipal_capacity`
- `mart_sismepre_ranking`
- `dim_municipality_context`
- `municipal_entity_bridge`
- `renamu_municipal_context`

## Fallback

Si el acceso por Hive falla, la contingencia debe seguir el mismo contrato funcional del modelo objetivo y no reintroducir tablas antiguas como base principal del reporte.
