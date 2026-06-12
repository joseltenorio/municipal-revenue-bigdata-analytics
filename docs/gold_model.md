# Modelo analítico Gold propuesto

## Propósito

La capa Gold será la capa analítica final del proyecto. Su objetivo será exponer datasets estables, comprensibles y eficientes para Power BI, construidos a partir de los resultados de profiling, reglas de calidad, transformaciones Silver e integración municipal.

Este documento define el modelo propuesto. No implica que Gold ya esté construido.

Gold debe:

- Respetar las granularidades observadas en Silver.
- Evitar joins fila-a-fila entre fuentes incompatibles.
- Conservar información de cobertura de integración.
- Separar dimensiones, hechos y marts según uso analítico.
- Documentar las reglas de agregación que alimenten KPIs.

## Criterios de modelado

Los hallazgos de profiling y calidad condicionan el diseño:

- MEF no debe integrarse fila-a-fila con Predial o RENAMU. La integración debe partir de `mef_municipal_amounts`, que ya agrega montos con granularidad presupuestal controlada.
- Predial conserva una granularidad propia por entidad, periodo, formulario y tiempo estadístico en `predial_entity_period`.
- RENAMU funciona como contexto territorial municipal y usa `ubigeo` como llave territorial principal.
- `sec_ejec` no equivale a `ubigeo`. El cruce debe usar un puente municipal validado.
- La cobertura MEF con puente municipal es parcial: 1485 de 3014 `sec_ejec`, equivalente a 49.2701%.
- La cobertura Predial con RENAMU es razonable, pero no completa: 1110 de 1485 entidades prediales, equivalente a 74.7475%.
- RENAMU contiene 764 ubigeos sin presencia predial observada, equivalente a 40.7684% de sus ubigeos.

Por estos motivos, Gold no debe ocultar faltantes de integración ni excluir registros sin match sin una regla explícita.

## Enfoque propuesto

El modelo Gold debe construirse con:

- Marts analíticos controlados.
- Dimensiones compartidas solo cuando la cobertura lo permita.
- Flags de cobertura para distinguir entidades cruzadas y no cruzadas.
- Granularidades explícitas por tabla.
- Métricas calculadas desde datasets Silver agregados, no desde tablas crudas incompatibles.

La construcción de Gold deberá preservar trazabilidad hacia Silver mediante campos como `source_dataset`, `integration_grain` y timestamps de procesamiento.

## Datasets Gold propuestos

### `dim_municipality`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Representar entidades municipales integrables para análisis. |
| Fuente Silver base | `municipal_entity_bridge` y `renamu_municipal_context`. |
| Granularidad esperada | Una fila por combinación municipal validada, preferentemente por `ubigeo` y referencias disponibles de `sec_ejec`. |
| Llaves candidatas | `ubigeo`, `sec_ejec`, `idmunici`, según disponibilidad y cobertura. |
| Atributos principales | Departamento, provincia, distrito, nombres normalizados, tipo de municipalidad, flags de validez y cobertura. |
| Limitaciones | `sec_ejec` no debe reemplazar `ubigeo`; pueden existir entidades MEF sin puente y ubigeos RENAMU sin Predial. |

### `dim_geography`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Exponer jerarquía territorial para filtros y análisis geográfico. |
| Fuente Silver base | `renamu_municipal_context` y campos territoriales del puente municipal. |
| Granularidad esperada | Una fila por `ubigeo` válido. |
| Llaves candidatas | `ubigeo`, `ccdd`, `ccpp`, `ccdi`. |
| Atributos principales | Departamento, provincia, distrito y versiones normalizadas. |
| Limitaciones | Los nombres territoriales no deben usarse como llave principal porque pueden variar entre fuentes. |

### `dim_time`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Estandarizar análisis temporal en Power BI. |
| Fuente Silver base | Campos temporales de MEF y Predial. |
| Granularidad esperada | Año y mes cuando exista información mensual. |
| Llaves candidatas | `anio`, `mes`, combinaciones de año-periodo según fuente. |
| Atributos principales | Año, mes, trimestre, etiqueta de periodo y granularidad. |
| Limitaciones | Los recursos MEF llamados diarios no tienen una columna real de día observada; no se debe inventar una fecha diaria. |

### `fact_municipal_income_execution`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Analizar presupuesto y recaudación de ingresos municipales. |
| Fuente Silver base | `mef_municipal_amounts`. |
| Granularidad esperada | `source_dataset`, año, mes, `sec_ejec` y clasificadores presupuestales principales. |
| Llaves candidatas | `anio`, `mes`, `sec_ejec`, clasificadores presupuestales y `source_dataset`. |
| Métricas principales | `monto_pia_total`, `monto_pim_total`, `monto_recaudado_total`, conteo de registros fuente. |
| Limitaciones | No todos los `sec_ejec` cruzan con el puente municipal. El hecho debe conservar flags de match o permitir análisis de no cobertura. |

### `fact_predial_goal_performance`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Analizar desempeño predial por entidad, periodo y formulario. |
| Fuente Silver base | `predial_entity_period`. |
| Granularidad esperada | Entidad predial, año de aplicación, periodo, formulario, año estadístico y mes estadístico. |
| Llaves candidatas | `ano_aplicacion`, `periodo`, `sec_ejec`, `formulario_id`, `ano_estadistica`, `mes_estadistica`. |
| Métricas principales | Totales monetarios `mon_*_decimal_total`, totales numéricos `num_*_decimal_total`, conteos fuente y respuestas activas. |
| Limitaciones | La fuente predial no debe colapsarse solo a `sec_ejec`; `respuestas` contiene estados activos e inactivos y no debe usarse cruda como hecho final sin tratamiento. |

### `fact_integration_coverage`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Medir cobertura y brechas de cruce entre MEF, Predial y RENAMU. |
| Fuente Silver base | `integration_coverage`. |
| Granularidad esperada | Una fila por métrica de cobertura. |
| Llaves candidatas | Nombre de métrica o identificador técnico de regla. |
| Métricas principales | Numerador, denominador y porcentaje de cobertura. |
| Limitaciones | Los porcentajes son métricas de calidad de integración, no KPIs de desempeño municipal. |

### `mart_municipal_revenue_overview`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Vista analítica principal de ingresos municipales para Power BI. |
| Fuente Silver base | `mef_municipal_amounts`, `municipal_entity_bridge` y dimensiones Gold. |
| Granularidad esperada | Municipio o entidad, periodo y clasificador agregado según decisión Gold. |
| Métricas principales | PIA total, PIM total, recaudación total, avance de recaudación y variación anual. |
| Limitaciones | Debe exponer cobertura de cruce y no ocultar entidades MEF sin match territorial. |

### `mart_predial_compliance`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Vista analítica de desempeño predial por entidad y periodo. |
| Fuente Silver base | `predial_entity_period` y puente municipal. |
| Granularidad esperada | Entidad predial, periodo, formulario y tiempo estadístico. |
| Métricas principales | Indicadores monetarios y numéricos prediales disponibles, con porcentajes solo cuando estén soportados por columnas y reglas documentadas. |
| Limitaciones | No se deben inventar indicadores de cumplimiento si no están respaldados por la fuente o por reglas Gold explícitas. |

### `mart_territorial_context`

| Criterio | Definición propuesta |
| --- | --- |
| Propósito | Exponer contexto territorial y municipal para segmentación analítica. |
| Fuente Silver base | `renamu_municipal_context` y `municipal_entity_bridge`. |
| Granularidad esperada | `ubigeo` o entidad municipal validada. |
| Atributos principales | Jerarquía territorial, tipo de municipalidad, flags territoriales y cobertura con Predial/MEF. |
| Limitaciones | RENAMU contiene variables amplias de cuestionario; Gold debe seleccionar solo variables justificadas para el análisis. |

## KPIs candidatos

Los siguientes KPIs son candidatos, sujetos a validación durante la construcción Gold:

- PIA total.
- PIM total.
- Recaudación total.
- Avance de recaudación: recaudación total sobre PIM, cuando el denominador sea válido.
- Variación anual de recaudación.
- Ranking municipal por recaudación o avance.
- Indicadores prediales disponibles desde columnas monetarias y numéricas validadas.
- Cobertura de integración MEF-Predial-RENAMU.
- Brechas territoriales por departamentos, provincias o distritos.

No se definen todavía medidas DAX definitivas. Las fórmulas finales deben documentarse cuando Gold y el modelo Power BI estén implementados.

## Decisiones de modelado

- `sec_ejec` no reemplaza `ubigeo`.
- El cruce municipal debe usar `municipal_entity_bridge` y conservar flags de cobertura.
- Los registros sin match pueden ser analíticamente relevantes y no deben descartarse por defecto.
- MEF debe agregarse antes de integrarse con contexto municipal.
- Predial debe conservar su granularidad de formulario y tiempo estadístico.
- RENAMU debe usarse como contexto territorial, no como fuente masiva de indicadores sin selección.
- Power BI debe consumir Gold; Bronze y Silver quedan para validación técnica o auditoría analítica.

## Riesgos

| Riesgo | Impacto |
| --- | --- |
| Cobertura MEF parcial con puente municipal | Gold no puede asumir que todo MEF cruza con Predial o RENAMU. |
| Diferencia de granularidades | Joins directos pueden multiplicar filas o distorsionar métricas. |
| Nombres territoriales variables | Los nombres no son llaves confiables; debe priorizarse `ubigeo`. |
| Llaves candidatas no únicas | Deben documentarse reglas de agregación antes de exponer KPIs. |
| Variables RENAMU numerosas | Seleccionar demasiadas variables puede producir un modelo difícil de usar. |
| Montos negativos MEF | Requieren interpretación presupuestal o contable antes de excluirlos. |

## Relación con Power BI

Gold alimentará las páginas del dashboard Power BI. El diseño inicial considera:

- Resumen municipal de ingresos.
- Análisis presupuestal y recaudación.
- Desempeño predial.
- Contexto territorial.
- Cobertura de integración.

El detalle de páginas, relaciones visuales y medidas se documentará en `docs/powerbi_model.md`. Este documento no afirma que Power BI ya esté implementado.
