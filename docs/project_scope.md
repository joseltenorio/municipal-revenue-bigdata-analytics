# Alcance del proyecto

## Nombre del proyecto

`municipal-revenue-bigdata-analytics`

## Descripción general

Proyecto local de ingeniería y analítica de datos para revisar ingresos municipales, estadísticas prediales, contexto RENAMU y clasificación municipal oficial MEF 2019.

El alcance actual ya no se describe como una arquitectura inicial de Bronze/Silver por fuente. El foco está en el alineamiento entre Silver integrado, Gold dimensional, Hive y Power BI.

## Fuentes vigentes

1. **SIAF / MEF**
2. **SISMEPRE**
3. **RENAMU 2022**
4. **Clasificación municipal oficial MEF 2019**

## Decisiones cerradas

- `municipal_classification` es la fuente vigente para clasificación municipal.
- `municipal_categories`, `categorias_municipalidades` y `CategoriasMunicipalidades.csv` son legacy.
- `map_sec_ejec_ubigeo` es un mapa técnico Silver.
- `dim_municipality` y `dim_geography` se separan por responsabilidad.
- `fact_siaf_income` debe llegar con `municipality_key` resuelto.
- El Gold inicial de SISMEPRE usa sólo `esat_estadistica_atm`.
- RENAMU completo no vuelve a Gold.

## Objetivo general

Construir una plataforma analítica local basada en Parquet, Hive y Power BI con un modelo dimensional final claro y auditable.

## Fuera de alcance

- Cloud
- Streaming
- ML obligatorio
- Versionamiento de datos reales
- Construcción de Gold fuera del contrato documentado

## Resultado esperado

- Silver integrado estable
- Gold dimensional documentado
- Hive como catálogo SQL
- Power BI sobre marts y dimensiones finales
- Auditoría y calidad separadas del negocio
