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
- La clasificación vigente es `municipal_classification`.
- El mapa técnico Silver es `map_sec_ejec_ubigeo`.
- El Gold inicial usa `fact_siaf_income` y `fact_predial_statistics`.

## Calidad Silver y Silver integrado

Silver conserva reglas para:

- tipos
- nulos
- duplicados
- montos negativos
- validez territorial
- flags técnicos

El objetivo no es forzar joins de negocio, sino detectar riesgos antes del Gold dimensional.

### Riesgos a vigilar

- `sec_ejec` no equivale a `ubigeo`
- no usar matching manual por nombre como criterio principal
- no mezclar RENAMU completo dentro de `dim_municipality`
- no exponer `map_sec_ejec_ubigeo` como tabla de negocio

## Modelo separado de auditoría

La auditoría Gold debe documentarse con al menos tres datasets:

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

### `audit_municipality_name_comparison`

Comparación técnica y similitud de nombres observados por fuente vs nombre estándar.

Campos mínimos:

- `sec_ejec`
- `ubigeo6`
- `municipality_key`
- `nombre_estandar`
- `nombre_siaf_observado`
- `nombre_sismepre_observado`
- `nombre_renamu_observado`
- `siaf_vs_estandar_match`
- `sismepre_vs_estandar_match`
- `siaf_vs_sismepre_match`
- `similarity_score_siaf`
- `similarity_score_sismepre`
- `issue_type`
- `issue_reason`
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
