# Modelo Gold objetivo

## Propósito

Este documento define la arquitectura objetivo de la capa **Gold** del proyecto `municipal-revenue-bigdata-analytics`.

La arquitectura ya no se organiza como Bronze/Silver por fuente ni como un conjunto de puentes transitorios para negocio. El estado cerrado del proyecto es este:

- **Silver integrado** prepara los datos limpios, tipados y trazables.
- **Gold dimensional** expone las dimensiones, hechos y marts finales.
- **Hive** cataloga las tablas externas.
- **Power BI** consume los marts y dimensiones finales.

Este documento describe el modelo objetivo. No implica que todas las tablas físicas ya estén construidas en este commit.

## Principios cerrados

1. `municipal_categories` es legacy. La fuente vigente es `municipal_classification`, basada en la clasificación municipal oficial MEF 2019.
2. RENAMU completo no debe volver a Gold. El contexto RENAMU se separa en `dim_renamu_context`.
3. `dim_municipality` representa la entidad municipal o institucional. La jerarquía territorial vive en `dim_geography`.
4. `map_sec_ejec_ubigeo` es un mapa técnico Silver, no una dimensión de negocio.
5. `fact_siaf_income` debe salir con `municipality_key` ya resuelto.
6. SISMEPRE inicial solo usa `silver/sismepre/resource_key=esat_estadistica_atm`.
7. La clasificación municipal oficial se resuelve por `ubigeo6`, no por matching manual por nombre.

## Esquema objetivo

```mermaid
erDiagram
    dim_geography ||--o{ dim_municipality : geography_key
    dim_municipality ||--o{ fact_siaf_income : municipality_key
    dim_municipality ||--o{ fact_predial_statistics : municipality_key
    dim_sismepre_period ||--o{ fact_predial_statistics : sismepre_period_key
    dim_time ||--o{ fact_siaf_income : date_key
    dim_municipality ||--o{ dim_renamu_context : municipality_key
    audit_dataset_summary ||--o{ audit_quality_results : dataset
    mart_municipal_revenue_overview }|--|| fact_siaf_income : resume
    mart_predial_statistics_overview }|--|| fact_predial_statistics : resume
    mart_municipal_context }|--|| dim_municipality : resume
    mart_territorial_summary }|--|| dim_geography : resume
```

## Dimensiones y contexto

Estado fisico de este commit:

- Se materializan dimensiones base bajo `data/gold/dim_*`.
- Se materializan facts base bajo `data/gold/fact_*`.
- Se materializan marts analiticos bajo `data/gold/mart_*`.
- No se construyen auditoria Gold ni registros Hive.
- Las dimensiones usan solo Silver curado: `renamu/resource_key=municipal_context`, `municipal_classification/resource_key=classification_2019`, `siaf_income/*` y `sismepre/resource_key=esat_estadistica_atm`.
- `fact_siaf_income` usa `siaf_income/*` y `map_sec_ejec_ubigeo`.
- `fact_predial_statistics` usa solo `sismepre/resource_key=esat_estadistica_atm`.
- Los marts Power BI se construyen solo desde dimensions y facts Gold.

### `dim_municipality`

Representa la entidad municipal/institucional.

Campos objetivo:

- `municipality_key`
- `ubigeo6`
- `geography_key`
- `idmunici`
- `municipalidad_nombre`
- `tipomuni_codigo`
- `tipomuni_nombre`
- `tipo_clasificacion_municipal`
- `ambito_municipal`
- `descripcion_tipo`
- `gold_processed_at_utc`

Reglas:

- Ruta fisica: `data/gold/dim_municipality/`.
- `municipality_key = ubigeo6`.
- `geography_key` puede ser igual a `ubigeo6` para mantener simplicidad.
- La granularidad es una fila por `ubigeo6`.
- No debe repetir departamento, provincia y distrito como atributos principales.
- No debe mezclar contexto territorial con contexto institucional.
- `dim_municipality` tendrá un único campo `municipalidad_nombre` como el nombre estándar usado por Power BI.
- RENAMU será la fuente preferente para construir ese nombre. Si RENAMU no trae un campo explícito de nombre institucional, el nombre puede derivarse de forma controlada usando `tipomuni_codigo`, `tipomuni_nombre`, `provincia_nombre` y/o `distrito_nombre`.
- SIAF y SISMEPRE no definen el nombre oficial de municipalidad en Gold; aportan llaves y hechos, no nombres maestros.

### `dim_geography`

Representa la jerarquía territorial.

Campos objetivo:

- `geography_key`
- `ubigeo6`
- `ccdd`
- `ccpp`
- `ccdi`
- `departamento_nombre`
- `provincia_nombre`
- `distrito_nombre`
- `gold_processed_at_utc`

Reglas:

- Ruta fisica: `data/gold/dim_geography/`.
- `geography_key` puede ser igual a `ubigeo6`.
- Su granularidad es una fila por unidad territorial.
- No debe incorporar atributos institucionales municipales.

### `dim_renamu_context`

Representa el contexto RENAMU seleccionado para negocio.

Campos objetivo:

- `municipality_key`
- `ubigeo6`
- variables RENAMU seleccionadas
- `gold_processed_at_utc`

Reglas:

- Se limita a variables útiles para interpretación de contexto.
- No replica toda la tabla RENAMU.
- No debe insertarse dentro de `dim_municipality`.
- Ruta fisica: `data/gold/dim_renamu_context/`.
- No duplica la jerarquia territorial principal de `dim_geography`.
- No incorpora clasificacion MEF ni metricas SIAF/SISMEPRE.

### `dim_time`

Calendario mensual para SIAF.

Campos objetivo:

- `date_key`
- `fecha_mes`
- `anio`
- `mes`
- `anio_mes`
- `trimestre`
- `semestre`
- `gold_processed_at_utc`

Reglas:

- Ruta fisica: `data/gold/dim_time/`.
- Se construye con periodos observados en `data/silver/siaf_income/*`.
- Grano mensual; no crea un calendario amplio innecesario.

### `dim_sismepre_period`

Calendario operativo de SISMEPRE.

Campos objetivo:

- `sismepre_period_key`
- `anio_aplicacion`
- `periodo`
- `anio_estadistica`
- `mes_estadistica`
- `periodo_estadistica_tipo`
- `is_annual_stat_period`
- `periodo_label`

- `gold_processed_at_utc`

Reglas:

- Ruta fisica: `data/gold/dim_sismepre_period/`.
- Usa solo `data/silver/sismepre/resource_key=esat_estadistica_atm/`.
- No usa los otros recursos SISMEPRE en el Gold inicial.

## Mapa técnico Silver

### `map_sec_ejec_ubigeo`

Mapa técnico de trazabilidad y resolución territorial.

Campos objetivo:

- `sec_ejec`
- `ubigeo6`
- `municipality_key`
- `has_siaf_match`
- `has_sismepre_match`
- `has_renamu_match`
- `has_classification_match`
- `match_status`
- `confidence_level`
- `issue_reason`

Reglas:

- Se documenta como Silver técnico.
- Sirve para resolver `sec_ejec -> ubigeo6 -> municipality_key`.
- No debe resolver `sec_ejec -> nombre SIAF -> nombre SISMEPRE -> fuzzy match`. No se realiza matching manual ni fuzzy matching por nombre como base del modelo final.
- No debe usarse como tabla principal de Power BI.
- No debe exponerse como dimensión de negocio.

## Hechos Gold

### `fact_siaf_income`

Hecho de ingresos y ejecución municipal.

Campos objetivo:

- `municipality_key`
- `sec_ejec`
- `date_key`
- `source_resource_key`
- `source_granularity`
- `monto_pia`
- `monto_pim`
- `monto_recaudado`
- `has_municipality_match`
- `match_status`
- `gold_processed_at_utc`

Reglas:

- Debe salir con `municipality_key` ya resuelto usando `map_sec_ejec_ubigeo`.
- Ruta fisica: `data/gold/fact_siaf_income/`.
- `municipality_key` debe ser el `ubigeo6` resuelto desde el mapa tecnico.
- Si no existe resolucion unica, el registro se conserva con `has_municipality_match = false`.
- Si no existe fila en el mapa tecnico, `match_status` debe quedar como `missing_map`.
- Debe conservar `source_resource_key` y `source_granularity` para trazabilidad.
- No debe incluir nombres observados por fuente ni atributos geograficos, de clasificacion o RENAMU.
- `date_key` debe derivarse con grano mensual; recursos anuales pueden usar enero como convencion estable.
- Power BI no debe depender del mapa técnico como tabla intermedia para análisis normal.

### `fact_predial_statistics`

Hecho inicial de estadísticas SISMEPRE.

Campos objetivo:

- `municipality_key`
- `sismepre_period_key`
- `sec_ejec`
- `ubigeo6`
- `formulario_id`
- `monto_emision_predial_total`
- `monto_recaudacion_predial_total`
- `monto_saldo_predial_total`
- `ratio_recaudacion_emision`
- `numero_predios_total`
- `numero_contribuyentes_predio`
- `gold_processed_at_utc`

Reglas:

- El Gold inicial solo consume `silver/sismepre/resource_key=esat_estadistica_atm`.
- Ruta fisica: `data/gold/fact_predial_statistics/`.
- `municipality_key = ubigeo6`.
- `sismepre_period_key` debe ser compatible con `dim_sismepre_period`.
- `ratio_recaudacion_emision` debe evitar division entre cero devolviendo `null`.
- No incluye nombres observados por fuente, atributos territoriales, clasificacion municipal ni variables RENAMU.
- Los recursos SISMEPRE restantes quedan en Silver por trazabilidad, pero no entran al Gold inicial ni al dashboard principal.

## Marts Gold para Power BI

### `mart_municipal_revenue_overview`

Vista ejecutiva de ingresos municipales.

Uso:

- KPIs principales.
- Tendencias.
- Comparativos por municipio y periodo.

Reglas:

- Ruta fisica: `data/gold/mart_municipal_revenue_overview/`.
- Se construye desde `fact_siaf_income`, `dim_municipality`, `dim_geography` y `dim_time`.
- No expone nombres observados por fuente ni variables extensas de RENAMU.

### `mart_predial_statistics_overview`

Vista ejecutiva de SISMEPRE.

Uso:

- Emisión, recaudación, saldo y ratios.
- Análisis por periodo y entidad municipal.

Reglas:

- Ruta fisica: `data/gold/mart_predial_statistics_overview/`.
- Se construye desde `fact_predial_statistics`, `dim_municipality`, `dim_geography` y `dim_sismepre_period`.
- No usa otros recursos SISMEPRE en el Gold inicial.

### `mart_municipal_context`

Vista de contexto municipal e institucional.

Uso:

- Lectura rápida de clasificación municipal.
- Variables seleccionadas de RENAMU.

Reglas:

- Ruta fisica: `data/gold/mart_municipal_context/`.
- Se construye desde `dim_municipality`, `dim_geography` y `dim_renamu_context`.
- Mantiene una fila por municipalidad.

### `mart_territorial_summary`

Vista de resumen territorial.

Uso:

- Jerarquía geográfica.
- Agregaciones por departamento, provincia y distrito.

Reglas:

- Ruta fisica: `data/gold/mart_territorial_summary/`.
- Se construye desde `mart_municipal_context`.
- En este bloque inicial resume contexto territorial e institucional sin mezclar agregaciones monetarias SIAF/predial.

## Auditoría y calidad

El modelo de auditoría y calidad debe mantenerse separado del modelo de negocio.

### `audit_quality_results`

Resultado detallado por regla.

Campos mínimos:

- `quality_result_key`
- `layer_name`
- `dataset_name`
- `resource_key`
- `check_name`
- `rule_name`
- `rule_category`
- `status`
- `severity`
- `message`
- `metric_name`
- `metric_value`
- `expected_value`
- `actual_value`
- `checked_at_utc`
- `source_file_path`
- `gold_processed_at_utc`

### `audit_dataset_summary`

Resumen por dataset evaluado.

Campos mínimos:

- `dataset_summary_key`
- `layer_name`
- `dataset_name`
- `resource_key`
- `total_checks`
- `pass_count`
- `warning_count`
- `fail_count`
- `error_count`
- `completeness_score`
- `validity_score`
- `conformity_score`
- `quality_score`
- `row_count`
- `duplicate_rows`
- `null_percentage`
- `last_checked_at_utc`
- `gold_processed_at_utc`

### `audit_integration_coverage`

Resumen tecnico Gold derivado de `silver/integrated/integration_coverage`.

Campos mínimos:

- `coverage_scope`
- `source_name`
- `metric_name`
- `metric_value`
- `total_records`
- `matched_records`
- `unmatched_records`
- `match_rate`
- `issue_count`
- `issue_rate`
- `gold_processed_at_utc`

## Legacy y reemplazos

Las siguientes referencias deben considerarse legacy o transición anterior:

- `municipal_entity_bridge`
- `mef_municipal_amounts`
- `renamu_full`
- `renamu_municipal_context`
- `fact_municipal_income_execution`
- `dim_municipality_context`
- `fact_predial_compliance`
- `fact_revenue_integration_coverage`

También quedan legacy las referencias a:

- `municipal_categories`
- `categorias_municipalidades`
- `CategoriasMunicipalidades.csv`
- matching manual por nombre como criterio principal de integración

## Consumo en Hive y Power BI

- Hive registra las tablas Gold como tablas externas.
- Power BI consume preferentemente `mart_municipal_revenue_overview`, `mart_predial_statistics_overview`, `mart_municipal_context` y `mart_territorial_summary`.
- `map_sec_ejec_ubigeo` se mantiene para trazabilidad técnica y depuración.
- Los modelos de auditoría se usan para calidad, no para análisis de negocio.

## Resumen operativo

El modelo objetivo queda cerrado así:

- Silver integrado resuelve y limpia.
- Gold dimensional separa entidad, geografía, RENAMU y tiempo.
- SIAF sale por `fact_siaf_income`.
- SISMEPRE inicial sale por `fact_predial_statistics`.
- RENAMU queda en `dim_renamu_context`.
- La clasificación municipal oficial vive en `dim_municipality`.
- La auditoría vive aparte en `audit_quality_results` y `audit_dataset_summary`.
