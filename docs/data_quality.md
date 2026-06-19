# Calidad y auditoría de datos

## Propósito

Este documento describe la estrategia de calidad y auditoría del proyecto `municipal-revenue-bigdata-analytics`.

La calidad verifica si los datasets cumplen el contrato mínimo para avanzar de capa. La auditoría registra resultados, cobertura y trazabilidad. Son modelos distintos:

- **Calidad**: reglas, severidades y validación.
- **Auditoría**: resumen técnico de resultados por dataset.

## Alcance actual

La calidad se aplica sobre:

- Bronze
- Silver

La auditoría debe mantenerse separada del modelo de negocio Gold.

## Estado de referencia

Los resultados observados siguen siendo útiles como base de control, pero el contrato final de negocio cambió. En particular:

- `municipal_categories` es legacy.
- La clasificación vigente es `silver/municipal_classification/resource_key=classification_2019`.
- El mapa técnico Silver es `data/silver/integrated/map_sec_ejec_ubigeo/`.
- El resumen de cobertura Silver es `data/silver/integrated/integration_coverage/`.
- El Gold inicial no debe depender de `municipal_categories`, `renamu_full` ni `base_renamu_2022`.

## Calidad Silver y Silver integrado

La calidad Silver prioriza estos datasets curados:

- `data/silver/siaf_income/*`
- `data/silver/sismepre/resource_key=esat_estadistica_atm/`
- `data/silver/renamu/resource_key=municipal_context/`
- `data/silver/municipal_classification/resource_key=classification_2019/`
- `data/silver/integrated/map_sec_ejec_ubigeo/`
- `data/silver/integrated/integration_coverage/`

En SISMEPRE, los recursos `respuestas`, `preguntas`, `formulario`, `estadistica`, `ano_aplicacion` y `entidad_estado` pueden existir por trazabilidad, pero no son críticos para el Gold inicial.

La calidad Silver conserva reglas para:

- existencia de ruta y Parquet
- lectura por Spark
- row count positivo
- columnas obligatorias
- tipos y formatos de llaves técnicas
- duplicados por grano
- valores permitidos en `match_status` y `confidence_level`
- llaves integradas como `municipality_key = ubigeo6`
- rangos de `match_rate` e `issue_rate`
- métricas técnicas requeridas en `integration_coverage`

El objetivo no es forzar joins de negocio, sino detectar riesgos antes del Gold dimensional.

### Riesgos a vigilar

- `sec_ejec` no equivale a `ubigeo`
- no usar matching manual por nombre como criterio principal
- no mezclar RENAMU completo dentro de `dim_municipality`
- no exponer `map_sec_ejec_ubigeo` como tabla de negocio
- no promover `municipal_categories` como fuente vigente

## Modelo separado de auditoría

La auditoría Gold debe documentarse con dos datasets:

### `audit_quality_results`

Resultado detallado por regla y dataset.

Campos mínimos:

- `dataset`
- `rule_name`
- `status`
- `severity`
- `message`
- `pass_count`
- `warning_count`
- `fail_count`
- `completeness_score`
- `validity_score`
- `conformity_score`
- `duplicate_rows`
- `null_percentage`
- `row_count`
- `processed_at_utc`

### `audit_dataset_summary`

Resumen por dataset evaluado.

Campos mínimos:

- `dataset`
- `datasets_evaluados`
- `row_count`
- `duplicate_rows`
- `null_percentage`
- `pass_count`
- `warning_count`
- `fail_count`
- `processed_at_utc`

## Interpretación esperada

- `pass_count`, `warning_count` y `fail_count` resumen el estado de validación.
- `completeness_score`, `validity_score` y `conformity_score` ayudan a comparar calidad entre datasets.
- `duplicate_rows` y `null_percentage` permiten rastrear problemas estructurales.
- `row_count` y `datasets_evaluados` hacen explícito el universo revisado.
- `processed_at_utc` permite auditoría temporal.

## Criterio de uso

Las métricas de auditoría no deben usarse como KPIs de negocio.

El dashboard final puede incluir una página de calidad, pero esa página debe consumir las tablas de auditoría, no los hechos de negocio.
