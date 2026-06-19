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
