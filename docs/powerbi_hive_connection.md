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

## Runner local y fallback operativo

Para refrescar el pipeline antes de abrir Power BI Desktop, la ruta recomendada pasa a ser:

```powershell
docker compose run --rm python-app python -m src.pipeline.run_local_pipeline --stage all --overwrite
```

Si solo se necesita recomponer Gold y luego consumir fallback CSV/dashboard-ready:

```powershell
docker compose run --rm python-app python -m src.pipeline.run_local_pipeline --stage gold --overwrite
```

Si `python-app` no dispone de `beeline`, la etapa Hive del runner fallarÃ¡ con mensaje claro. En ese caso:

1. Ejecutar el runner con `--skip-hive` si solo se quiere refrescar Parquet.
2. Aplicar DDL manualmente desde `hive-server`.

Comandos manuales de fallback:

```powershell
docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /app/sql/hive/create_databases.sql
docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /app/sql/hive/create_silver_external_tables.sql
docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /app/sql/hive/create_gold_external_tables.sql
```
