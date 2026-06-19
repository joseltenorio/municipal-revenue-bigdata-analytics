# Transformaciones Silver e integración municipal

## Propósito

Este documento describe las reglas de limpieza, tipado, normalización e integración aplicadas en la capa **Silver** del proyecto **Municipal Revenue Big Data Analytics**.

Silver no construye KPIs finales. Esa responsabilidad corresponde a Gold. Silver prepara datos limpios, tipados, trazables e integrables, y registra decisiones necesarias para evitar cruces incorrectos entre fuentes con granularidades distintas.

## Relación entre capas

| Capa | Propósito |
| --- | --- |
| Bronze | Preservar recursos tabulares en Parquet con limpieza técnica mínima y metadata de trazabilidad. |
| Silver | Limpiar, tipar, normalizar y preparar datasets integrables por fuente. |
| Integración Silver | Construir datasets preparatorios y medir cobertura de cruce entre fuentes. |
| Gold | Definir hechos, dimensiones, KPIs finales y modelo analítico para consumo. |

La integración Silver no hace joins crudos fila-a-fila entre SIAF, SISMEPRE y RENAMU. Las fuentes tienen granularidades diferentes y varias llaves candidatas no son únicas. Por eso, la integración usa agregación controlada, contexto territorial y medición explícita de cobertura.

## Transformaciones Silver por fuente

### SIAF ingresos

Script:

```text
src/silver/transform_siaf_income.py
```

Entrada:

```text
data/bronze/siaf_income/resource_key=<resource_key>/
```

Salida:

```text
data/silver/siaf_income/resource_key=<resource_key>/
```

Reglas aplicadas:

- Mantener un dataset por `resource_key`.
- Aplicar `trim` técnico a columnas string.
- Preservar códigos como string.
- Crear `anio` desde `ano_doc`.
- Crear `mes` desde `mes_doc`.
- Crear montos decimales:
  - `monto_pia_decimal`
  - `monto_pim_decimal`
  - `monto_recaudado_decimal`
- Crear `bronze_processed_at_timestamp`.
- Agregar metadata Silver:
  - `silver_source_name`
  - `silver_resource_key`
  - `silver_source_year`
  - `silver_source_granularity`
  - `silver_processed_at_utc`
- Crear flags técnicos:
  - `is_valid_anio`
  - `is_valid_mes`
  - `is_valid_monto_pia`
  - `is_valid_monto_pim`
  - `is_valid_monto_recaudado`
  - `has_complete_executora_location`

Hallazgos relevantes:

- La llave presupuestal preliminar no fue única en varios recursos.
- Los duplicados no fueron exactos.
- En los grupos duplicados varían montos, territorio o nombre de ejecutora.
- En recursos `daily` no se observó una columna real de día; la estructura disponible mantiene año y mes.
- No se debe integrar MEF fila-a-fila con SISMEPRE o RENAMU.

Decisión de integración:

MEF se integra mediante `siaf_municipal_amounts`, un dataset agregado por recurso, año, mes, `sec_ejec` y clasificadores presupuestales. Los montos se suman de forma controlada porque los duplicados por llave candidata reflejan granularidad más fina o cambios de atributos, no duplicados exactos.

Los montos negativos se mantienen como valores observados y se tratan como `WARNING` de calidad. No se eliminan automáticamente porque pueden representar ajustes, anulaciones o semántica contable que debe validarse antes de Gold.

### SISMEPRE

Script:

```text
src/silver/transform_sismepre.py
```

Entrada:

```text
data/bronze/sismepre/resource_key=<resource_key>/
```

Salida:

```text
data/silver/sismepre/resource_key=<resource_key>/
```

Reglas aplicadas:

- Mantener las siete tablas sismeprees por separado.
- No hacer joins entre tablas en la transformación por fuente.
- Aplicar `trim` técnico.
- Preservar identificadores como string.
- Crear columnas enteras progresivas:
  - `ano_aplicacion_int`
  - `periodo_int`
  - `ano_estadistica_int`
  - `mes_estadistica_int`
- Crear auxiliares tipadas para respuestas:
  - `respuesta_decimal_value`
  - `respuesta_entero_value`
  - `respuesta_fecha_value`
- Crear auxiliares decimales para columnas `mon_*` y `num_*` cuando existen.
- Crear flags técnicos según recurso:
  - `is_valid_ano_aplicacion`
  - `is_valid_periodo`
  - `is_valid_mes_estadistica`
  - `is_valid_ubigeo`
  - `is_valid_respuesta_decimal`
  - `is_valid_respuesta_entero`
  - `has_required_relationship_keys`
  - `has_complete_territory`

Hallazgos relevantes:

- `esat_estadistica_atm` no es único por `ano_aplicacion`, `periodo`, `sec_ejec`.
- La granularidad real de `esat_estadistica_atm` incluye al menos `formulario_id`, `ano_estadistica`, `mes_estadistica` y métricas monetarias/numéricas.
- `estadistica` requiere `ano_estadistica` y `mes_estadistica` para interpretar su granularidad.
- `respuestas` contiene registros activos e inactivos mediante `estado_registro`.
- `respuestas` no debe usarse como tabla final cruda sin tratar `estado_registro`.

Decisión de integración:

SISMEPRE se integra mediante `sismepre_entity_period`, preservando granularidad por entidad, periodo, formulario y tiempo estadístico. No se colapsa todo a `sec_ejec`, porque eso perdería granularidad real de la fuente.

Cuando se usa `respuestas`, la integración considera registros activos (`estado_registro = 'A'`) como resumen auxiliar, no como tabla final fila-a-fila.

### RENAMU

Script:

```text
src/silver/transform_renamu.py
```

Entrada:

```text
data/bronze/renamu/resource_key=base_renamu_2022/
```

Salida:

```text
data/silver/renamu/resource_key=base_renamu_2022/
```

Reglas aplicadas:

- Conservar RENAMU como dataset ancho.
- Preservar todas las columnas originales.
- Crear `anio` desde `ano`.
- Crear `tipomuni_int`.
- Normalizar territorio sin reemplazar nombres originales:
  - `departamento_normalizado`
  - `provincia_normalizada`
  - `distrito_normalizado`
- Validar campos territoriales:
  - `is_valid_ubigeo`
  - `is_valid_ccdd`
  - `is_valid_ccpp`
  - `is_valid_ccdi`
  - `has_complete_territory`
  - `has_municipal_identifier`
  - `is_valid_tipomuni`
- Aceptar `tipomuni` con valores `1`, `2` y `3`.
- Crear auxiliares decimales para columnas financieras `c96*` y `c97*`.

Decisión de integración:

RENAMU se usa como contexto municipal territorial mediante `renamu_municipal_context`. La llave territorial principal es `ubigeo`. No se seleccionan variables RENAMU arbitrarias para KPIs en Silver.

## Integración Silver

Script:

```text
src/silver/integrate_municipal_sources.py
```

Notebook de apoyo:

```text
notebooks/03_integration_analysis.ipynb
```

Salidas locales no versionables:

```text
data/silver/integrated/municipal_entity_bridge/
data/silver/integrated/siaf_municipal_amounts/
data/silver/integrated/sismepre_entity_period/
data/silver/integrated/renamu_municipal_context/
data/silver/integrated/integration_coverage/
```

### Datasets integrados

| Dataset | Propósito | Granularidad |
| --- | --- | --- |
| `municipal_entity_bridge` | Puente municipal entre `sec_ejec` y `ubigeo`, construido principalmente desde SISMEPRE y validado contra RENAMU. | Mapeo `sec_ejec`/`ubigeo` observado. |
| `siaf_municipal_amounts` | Montos SIAF agregados para evitar integración fila-a-fila. | Recurso MEF, año, mes, `sec_ejec` y clasificadores presupuestales. |
| `sismepre_entity_period` | Métricas sismeprees por entidad, formulario y tiempo estadístico. | `ano_aplicacion`, `periodo`, `sec_ejec`, `ubigeo`, `formulario_id`, `ano_estadistica`, `mes_estadistica`. |
| `renamu_municipal_context` | Contexto territorial y municipal RENAMU. | `ubigeo`. |
| `integration_coverage` | Métricas de cobertura de cruce entre fuentes. | Métrica de cobertura. |

### Resultado observado

| Dataset | Filas | Columnas |
| --- | ---: | ---: |
| `municipal_entity_bridge` | 2,598 | 15 |
| `siaf_municipal_amounts` | 12,129,286 | 23 |
| `sismepre_entity_period` | 133,938 | 37 |
| `renamu_municipal_context` | 1,874 | 109 |
| `integration_coverage` | 6 | 6 |

### Cobertura observada

| Métrica | Resultado | Cobertura |
| --- | ---: | ---: |
| `total_sismepre_sec_ejec_entities` | 1,485 / 1,485 | 100.0000% |
| `sismepre_entities_with_valid_ubigeo` | 1,113 / 1,485 | 74.9495% |
| `sismepre_entities_with_renamu_match` | 1,110 / 1,485 | 74.7475% |
| `siaf_sec_ejec_with_bridge` | 1,485 / 3,014 | 49.2701% |
| `siaf_sec_ejec_without_bridge` | 1,529 / 3,014 | 50.7299% |
| `renamu_ubigeos_without_sismepre` | 764 / 1,874 | 40.7684% |

## Interpretación de problemas de integración

### `sec_ejec` no equivale a `ubigeo`

`sec_ejec` identifica unidades ejecutoras o entidades administrativas. `ubigeo` identifica territorio. La integración no asume equivalencia directa entre ambos campos.

Por eso se construye `municipal_entity_bridge` como puente observado `sec_ejec -> ubigeo`, usando SISMEPRE como fuente principal del mapeo y RENAMU como referencia territorial.

### Cobertura SIAF limitada

Solo 1,485 de 3,014 `sec_ejec` MEF cruzan con el puente municipal observado. Esto implica que Gold no debe asumir que todo MEF puede cruzarse con SISMEPRE o RENAMU.

Para análisis posteriores se debe modelar explícitamente:

- MEF con puente territorial.
- MEF sin puente territorial.
- Posibles fuentes complementarias de mapeo `sec_ejec -> ubigeo`.

### Cobertura SISMEPRE con RENAMU razonable pero incompleta

SISMEPRE tiene 1,485 entidades con `sec_ejec`, de las cuales 1,113 tienen `ubigeo` válido y 1,110 cruzan con RENAMU. La cobertura es útil para integración territorial, pero no completa.

Gold debe considerar entidades sismeprees sin match RENAMU y evitar descartes silenciosos.

### RENAMU contiene municipios sin SISMEPRE

RENAMU tiene 764 ubigeos sin presencia en el puente sismepre observado. Esto puede representar municipios fuera del universo sismepre disponible, diferencias de cobertura de fuente o diferencias de identificación.

Estos casos deben conservarse para análisis de cobertura y no eliminarse antes de definir el modelo analítico.

## Decisiones para Gold

Gold debe construirse sobre datasets agregados y con cobertura explícita:

- Usar `siaf_municipal_amounts`, no filas crudas MEF, para hechos presupuestales preliminares.
- Usar `sismepre_entity_period`, no `respuestas` crudas, para métricas sismeprees preparadas.
- Usar `renamu_municipal_context` como dimensión/contexto territorial.
- Usar `municipal_entity_bridge` como puente validado, no como verdad definitiva.
- Incluir métricas de cobertura y faltantes para no ocultar entidades sin match.
- Definir KPIs solo después de validar granularidad, llaves y semántica de montos.

## Criterio de versionamiento

No versionar salidas locales integradas:

```text
data/silver/integrated/
```

Sí versionar:

```text
src/silver/integrate_municipal_sources.py
notebooks/03_integration_analysis.ipynb
docs/silver_transformations.md
```

Los Parquet integrados pueden regenerarse ejecutando el pipeline Silver correspondiente.

## Estrategia de rediseño Silver posterior al profiling

El modelo Silver vigente se considera una base de transición. Antes de redefinir dimensiones, hechos y marts Gold, se debe revisar el profiling Bronze generado por `src.quality.profile_bronze_datasets`.

La nueva estrategia de modelado se organizará por fuentes reales:

| Fuente | Nombre técnico | Rol en Silver |
| --- | --- | --- |
| SIAF ingresos | `siaf_income` | Hecho financiero de ingresos y recaudación presupuestal. |
| SISMEPRE | `sismepre` | Hechos y cuestionarios de meta del impuesto predial. |
| RENAMU | `renamu` | Contexto institucional y territorial municipal. |
| Clasificación municipal MEF | `municipal_classification` | Segmentación oficial A-G por `ubigeo`; no debe degradarse a matching por nombre. |

Salidas Silver objetivo:

```text
silver/siaf_income
silver/sismepre/estadistica_atm
silver/sismepre/formulario
silver/sismepre/preguntas
silver/sismepre/respuestas
silver/renamu/full_clean
silver/renamu/municipal_context
silver/municipal_classification
silver/integrated/municipal_entity_bridge
```

El puente municipal debe conservar evidencia de cruce:

```text
sec_ejec
ubigeo6
idmunici
municipalidad_nombre_normalizado
categoria_municipal
categoria_match_method
categoria_match_distance
categoria_match_status
categoria_match_is_ambiguous
```

Ninguna dimensión Gold debe asumir que `sec_ejec`, `ubigeo`, `idmunici` y nombre municipal son equivalentes directos.
