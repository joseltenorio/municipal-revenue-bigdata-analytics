# Profiling de datos

## Propósito del documento

Este documento describe la estrategia de profiling para el proyecto **Municipal Revenue Big Data Analytics**.

El profiling permite inspeccionar estructura, columnas, nulos, duplicados, tipos observados y valores representativos antes de tomar decisiones de limpieza, integración y modelado. En este proyecto se complementa con motores de calidad sobre Bronze y Silver.

El profiling no reemplaza las reglas de calidad. El profiling explora y ayuda a diseñar reglas; la calidad ejecuta validaciones reproducibles sobre datasets procesados.

## Relación entre discovery, profiling y calidad

| Actividad | Propósito | Capa principal |
| --- | --- | --- |
| Discovery | Confirmar acceso, disponibilidad, formatos y recursos candidatos. | Fuentes públicas |
| Profiling inicial | Inspeccionar archivos locales y estructuras preservadas. | Landing / Bronze |
| Calidad Bronze | Validar contrato técnico mínimo de Parquet preservado. | Bronze |
| Calidad Silver | Validar limpieza, tipado, flags y riesgos antes de integración. | Silver |

El profiling inicial estuvo orientado a Landing y Bronze. Después de generar Silver, la validación Silver aporta una lectura complementaria sobre nulos críticos, duplicados por llaves candidatas, flags, montos negativos y parseos fallidos.

## Alcance actual

El alcance actual cubre:

- Archivos locales de Landing para MEF, Predial y RENAMU.
- Recursos Bronze Parquet generados para las tres fuentes.
- Recursos Silver Parquet ya limpiados y tipados por fuente.
- Resultados locales de calidad Bronze y Silver.

No se integran fuentes en este documento. Tampoco se define el modelo Gold final.

## Script de profiling

El script principal de profiling es:

```text
src/quality/profile_sources.py
```

Este script:

- Recorre archivos locales dentro de Landing.
- Soporta archivos `.csv`, `.txt`, `.xlsx`, `.xls`, `.json` y `.parquet`.
- Lee una cantidad máxima controlada de filas.
- Resume métricas por archivo y columna.
- Puede generar `reports/profiling_summary.json`.
- No descarga datos externos.
- No transforma archivos.
- No construye Bronze, Silver ni Gold.

El reporte de profiling es local y no debe versionarse si contiene información derivada de datos reales.

## Notebook de apoyo

El notebook de apoyo es:

```text
notebooks/02_data_profiling.ipynb
```

El notebook resume resultados locales de calidad Bronze y Silver cuando existen:

```text
data/quality/bronze_quality_results.jsonl
data/quality/silver_quality_results.jsonl
```

El notebook no debe guardar CSV, HTML ni outputs pesados. Su objetivo es facilitar lectura agregada de resultados para revisar riesgos de integración.

## Resultados de calidad usados como profiling operativo

### Bronze

| Métrica | Resultado |
| --- | ---: |
| Recursos evaluados | 25 |
| Resultados de calidad | 275 |
| `PASS` | 220 |
| `WARNING` | 55 |
| `FAIL` | 0 |

Warnings Bronze:

| Regla | `WARNING` | Lectura |
| --- | ---: | --- |
| `invalid_percentage` | 25 | No hay columnas candidatas de porcentaje en Bronze. |
| `invalid_ubigeo` | 23 | No todos los recursos tienen `ubigeo`. |
| `invalid_year` | 7 | Algunos recursos no exponen año con nombres genéricos. |

### Silver

| Métrica | Resultado |
| --- | ---: |
| Recursos evaluados | 25 |
| Resultados de calidad | 403 |
| `PASS` | 347 |
| `WARNING` | 56 |
| `FAIL` | 0 |

Warnings Silver principales:

| Regla | `WARNING` | Lectura |
| --- | ---: | --- |
| `negative_amounts` | 17 | Montos negativos MEF requieren interpretación presupuestal o contable. |
| `candidate_key_duplicates` | 19 | Las llaves candidatas todavía son hipótesis. |
| `mef_candidate_key_duplicates` | 16 | La llave presupuestal MEF debe revisarse antes de integración. |
| `predial_candidate_key_duplicates` | 3 | Algunas llaves prediales requieren ajuste o más granularidad. |
| `predial_parse_failures` | 1 | Hay un parseo puntual que debe revisarse antes de Gold. |

No hubo `FAIL` en Silver. Esto indica que los datasets Silver son técnicamente utilizables para análisis de integración, aunque conservan riesgos que deben tratarse antes del modelo final.

## Cómo estos hallazgos orientan integración

El profiling y la calidad ayudan a decidir:

- **Llaves candidatas:** los duplicados por llaves preliminares muestran que las llaves actuales no deben asumirse definitivas.
- **Campos críticos:** los nulos críticos y flags indican qué columnas requieren revisión antes de integrarse.
- **Riesgos territoriales:** las reglas de ubigeo, territorio y `tipomuni` orientan cruces entre MEF, Predial y RENAMU.
- **Riesgos numéricos:** montos negativos y parseos fallidos deben interpretarse antes de construir métricas Gold.
- **Granularidad:** los duplicados funcionales pueden indicar que falta incluir periodo, clasificador, entidad, pregunta u otra dimensión en la llave.
- **Modelo Gold posterior:** las reglas actuales ayudan a separar hechos, dimensiones y catálogos, pero todavía no los definen.

Los hallazgos de duplicados por llaves candidatas motivaron una integración Silver controlada, documentada en `docs/silver_transformations.md`. Esa integración evita joins crudos fila-a-fila, agrega MEF por granularidad presupuestal, conserva la granularidad predial por entidad/formulario/tiempo estadístico y usa RENAMU como contexto territorial por `ubigeo`.

La cobertura de cruce se mide explícitamente en `integration_coverage`. Estos porcentajes deben interpretarse como calidad de integración y no como KPIs de negocio.

## Hallazgos por fuente

### SIAF ingresos

MEF cuenta con 17 recursos Silver. Los hallazgos principales son:

- Los recursos pasan validaciones técnicas de existencia, lectura, filas, columnas y metadata.
- Existen columnas tipadas para año, mes y montos.
- Los flags técnicos fueron generados.
- Hay `WARNING` por montos negativos en todos los recursos MEF evaluados.
- Hay duplicados por llave candidata presupuestal en la mayoría de recursos.

Implicancia: antes de integrar o modelar Gold, se debe validar la semántica de montos negativos y refinar la llave presupuestal con la granularidad real de MEF.

### SISMEPRE

Predial cuenta con 7 recursos Silver relacionados entre sí. Los hallazgos principales son:

- Los recursos pasan validaciones técnicas.
- Las tablas se mantienen separadas y no integradas.
- Hay duplicados por llaves candidatas en algunos recursos.
- Existe un hallazgo puntual de parseo.

Implicancia: la integración predial debe revisar relaciones entre formularios, preguntas, respuestas, entidades y estadísticas antes de definir hechos o dimensiones.

### RENAMU

RENAMU cuenta con 1 recurso Silver ancho. Los hallazgos principales son:

- El recurso pasa validaciones técnicas.
- Las columnas territoriales normalizadas y flags esperados están presentes.
- La validación acepta `tipomuni` con valores `1`, `2` y `3`.
- No se observaron `WARNING` principales en las reglas resumidas para RENAMU.

Implicancia: RENAMU puede usarse como fuente contextual para integración territorial, manteniendo cuidado por su estructura ancha de cuestionario.

## Limitaciones

- Los resultados reflejan corridas locales y deben regenerarse si cambian datos o reglas.
- Los `WARNING` no prueban errores de negocio; indican riesgos o hipótesis pendientes.
- Las llaves candidatas todavía no son llaves finales.
- No se evalúa todavía consistencia entre fuentes.
- No se define todavía el modelo Gold.
- Los diccionarios oficiales apoyan interpretación semántica, pero no sustituyen validación con datos reales.

## Estado actual

Estado: profiling inicial y calidad Bronze/Silver disponibles localmente.

El proyecto está preparado para analizar integración de fuentes, usando como insumos:

- Contratos Bronze y Silver.
- Resultados de calidad sin `FAIL`.
- Warnings sobre montos, duplicados candidatos y parseos.
- Diccionarios locales como referencia semántica.
- Notebook de apoyo para revisar resultados agregados sin versionar outputs reales.
