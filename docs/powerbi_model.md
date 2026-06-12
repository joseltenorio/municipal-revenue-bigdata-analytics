# Modelo Power BI propuesto

## Propósito

Este documento define una referencia inicial para el futuro modelo Power BI del proyecto.

El reporte todavía no está implementado. Las páginas, relaciones y medidas finales deberán cerrarse después de construir Gold y registrar sus tablas externas en Hive.

## Principio de consumo

Power BI debe consumir preferentemente tablas Gold, no tablas Bronze ni Silver directas.

Bronze y Silver pueden servir para validación técnica, auditoría o análisis exploratorio, pero el modelo final del reporte debe basarse en marts analíticos estables.

La ruta esperada es:

```text
Gold Parquet
-> Hive external tables
-> HiveServer2
-> Power BI Desktop en modo Import
```

## Datasets Gold esperados

El modelo Power BI se apoyará en los datasets propuestos en `docs/gold_model.md`:

- `dim_municipality`
- `dim_geography`
- `dim_time`
- `fact_municipal_income_execution`
- `fact_predial_goal_performance`
- `fact_integration_coverage`
- `mart_municipal_revenue_overview`
- `mart_predial_compliance`
- `mart_territorial_context`

Estos datasets aún no están materializados.

## Páginas candidatas

### Resumen ejecutivo municipal

Objetivo: mostrar una visión general de ingresos municipales, cobertura de integración y brechas principales.

Métricas candidatas:

- PIA total.
- PIM total.
- Recaudación total.
- Avance de recaudación.
- Cobertura de cruce entre fuentes.
- Entidades con y sin match territorial.

### Ejecución de ingresos MEF

Objetivo: analizar presupuesto y recaudación por año, mes, entidad y clasificador presupuestal.

Fuente Gold esperada:

- `fact_municipal_income_execution`
- `mart_municipal_revenue_overview`

Consideraciones:

- MEF debe llegar agregado desde Silver.
- Las entidades sin puente municipal no deben ocultarse sin una regla documentada.
- Los montos negativos deben interpretarse antes de excluirse.

### Desempeño predial

Objetivo: analizar información predial por entidad, periodo, formulario y tiempo estadístico.

Fuente Gold esperada:

- `fact_predial_goal_performance`
- `mart_predial_compliance`

Consideraciones:

- La granularidad predial no se reduce solo a `sec_ejec`.
- Las métricas prediales deben derivarse de columnas validadas y no de respuestas crudas sin tratamiento.

### Contexto territorial

Objetivo: segmentar resultados por departamento, provincia, distrito y tipo de municipalidad.

Fuente Gold esperada:

- `dim_geography`
- `dim_municipality`
- `mart_territorial_context`

Consideraciones:

- `ubigeo` debe ser la llave territorial principal.
- Los nombres territoriales son atributos descriptivos, no llaves principales.

### Cobertura y calidad de integración

Objetivo: hacer visibles las limitaciones de cruce entre MEF, Predial y RENAMU.

Fuente Gold esperada:

- `fact_integration_coverage`

Métricas candidatas:

- Entidades prediales con `sec_ejec`.
- Entidades prediales con `ubigeo` válido.
- Entidades prediales con match RENAMU.
- `sec_ejec` MEF con y sin puente.
- Ubigeos RENAMU sin Predial.

## Medidas

No se definen medidas DAX definitivas en esta etapa.

Las medidas finales deberán documentar:

- Fórmula.
- Tabla base.
- Tratamiento de nulos.
- Tratamiento de montos negativos.
- Tratamiento de registros sin match.
- Nivel de granularidad esperado.

## Relación con Hive

Power BI se conectará preferentemente a HiveServer2 cuando Gold esté disponible como tablas externas en la base `gold`.

En el estado actual:

- HiveServer2 está validado.
- Las bases `bronze`, `silver` y `gold` existen.
- Bronze y Silver tienen tablas externas.
- Gold todavía no tiene tablas externas.

Por tanto, el modelo Power BI queda definido como diseño inicial, no como implementación cerrada.
