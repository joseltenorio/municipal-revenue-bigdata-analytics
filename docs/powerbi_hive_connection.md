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
Hive sigue siendo la ruta SQL tÃ©cnica principal del proyecto y no se elimina en esta fase. Las tablas Gold y audit registradas en Hive continÃºan siendo evidencia del catÃ¡logo analÃ­tico local.

Debido a la inestabilidad observada localmente con el driver ODBC de Cloudera sobre HiveServer2, el fallback recomendado para Power BI Desktop deja de ser excepcional y pasa a ser la ruta operativa recomendada para el dashboard.

Ruta recomendada de consumo fallback:

```text
powerbi/exports/dashboard/
```

Builder:

```powershell
python -m src.powerbi.build_dashboard_export_marts --dry-run
python -m src.powerbi.build_dashboard_export_marts --overwrite
```

Datasets recomendados:

- `revenue_monthly_dashboard.csv`
- `revenue_source_monthly_dashboard.csv`
- `revenue_source_annual_dashboard.csv`
- `revenue_annual_dashboard.csv`
- `territorial_revenue_dashboard.csv`
- `predial_dashboard.csv`
- `municipal_context_dashboard.csv`
- `municipal_performance_dashboard.csv`
- `audit_*_dashboard.csv`

Restricciones:

- No se recomienda cargar `fact_siaf_income` completa.
- No se recomienda cargar `mart_municipal_revenue_overview` cruda si conserva millones de filas.
- No se deben reintroducir tablas legacy ni filtros manuales para excluir entidades no municipales.
