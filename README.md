# Municipal Revenue Big Data Analytics

Plataforma local de datos para analizar ingresos municipales, estadísticas prediales, contexto RENAMU y clasificación municipal oficial del MEF.

## Estado del proyecto

El proyecto está en fase de alineamiento entre:

- Silver integrado
- Gold dimensional
- Hive como catálogo SQL
- Power BI como capa de consumo

Ya no debe leerse como una arquitectura inicial de Bronze/Silver por fuente. Esa lectura quedó reemplazada por el modelo objetivo documentado en `docs/gold_model.md` y `docs/powerbi_model.md`.

## Arquitectura resumida

```text
Fuentes públicas
-> Landing
-> Bronze
-> Silver por fuente
-> Silver integrado
-> Gold dimensional
-> Hive External Tables
-> Power BI
```

## Fuentes principales

- SIAF / MEF para ingresos municipales.
- SISMEPRE para estadísticas prediales.
- RENAMU para contexto municipal.
- Clasificación municipal oficial MEF 2019.

## Convenciones cerradas

- `municipal_classification` es la fuente vigente de clasificación municipal.
- `municipal_categories` es legacy.
- `map_sec_ejec_ubigeo` es un mapa técnico Silver.
- `dim_municipality` representa la entidad municipal.
- `dim_geography` representa la jerarquía territorial.
- `fact_siaf_income` debe salir con `municipality_key` resuelto.
- El Gold inicial de SISMEPRE usa sólo `silver/sismepre/resource_key=esat_estadistica_atm`.
- RENAMU completo no vuelve a Gold; se separa en `dim_renamu_context`.

## Documentación principal

- `docs/architecture.md`
- `docs/project_scope.md`
- `docs/silver_transformations.md`
- `docs/data_quality.md`
- `docs/gold_model.md`
- `docs/hive_model.md`
- `docs/powerbi_model.md`
- `docs/powerbi_hive_connection.md`

## Ejecución local

La guía completa está en `docs/execution_guide.md`.

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
```

## Versionamiento

El repositorio versiona código, configuración, SQL, tests y documentación.
No versiona datos reales, Parquet, ZIP, CSV generados, logs pesados ni exports de Power BI.

## Power BI dashboard-ready

Hive sigue siendo parte del modelo tÃ©cnico local y del catÃ¡logo SQL del proyecto. Sin embargo, debido a la inestabilidad observada en Power BI Desktop con ODBC + HiveServer2 local para tablas SIAF grandes, el consumo recomendado del dashboard se mueve a datasets exportados y agregados.

Ruta Gold derivada para visualizaciÃ³n:

```text
data/gold/powerbi/
```

Ruta de export final para Power BI Desktop:

```text
powerbi/exports/dashboard/
```

Builder recomendado:

```powershell
python -m src.powerbi.build_dashboard_export_marts --dry-run
python -m src.powerbi.build_dashboard_export_marts --overwrite
```

Datasets principales:

- `revenue_monthly_dashboard`
- `revenue_source_monthly_dashboard`
- `revenue_source_annual_dashboard`
- `predial_dashboard`
- `municipal_context_dashboard`
- `municipal_performance_dashboard`
- `audit_*_dashboard`

## Runner local del pipeline

El proyecto ahora incluye un runner local para refresco ordenado del pipeline:

```powershell
python -m src.pipeline.run_local_pipeline --stage all --overwrite
```

Comportamiento clave:

- `--stage all` corre por defecto desde Silver, luego integration, Gold, Hive y validate.
- `--include-bronze` agrega Bronze al comienzo de `all`.
- `--from-stage gold` permite refrescar desde una etapa concreta hacia adelante.
- `--skip-hive` omite generaciÃ³n/aplicaciÃ³n de DDL Hive y la validaciÃ³n Hive.
- `--skip-validate` omite la validaciÃ³n final.

Ejemplos:

```powershell
docker compose run --rm python-app python -m src.pipeline.run_local_pipeline --stage all --overwrite
docker compose run --rm python-app python -m src.pipeline.run_local_pipeline --stage all --overwrite --include-bronze
docker compose run --rm python-app python -m src.pipeline.run_local_pipeline --stage gold --overwrite
docker compose run --rm python-app python -m src.pipeline.run_local_pipeline --stage validate
```
