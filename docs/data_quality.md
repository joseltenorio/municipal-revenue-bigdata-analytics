# Calidad de datos

## Propósito del documento

Este documento describe la estrategia de calidad de datos aplicada al proyecto **Municipal Revenue Big Data Analytics**.

La calidad de datos verifica que las capas procesadas del lakehouse sean legibles, trazables y suficientemente consistentes para avanzar entre etapas. En Bronze se valida el contrato técnico mínimo de preservación y trazabilidad. En Silver se valida el resultado posterior a limpieza, tipado y estandarización por fuente, antes de integrar SIAF, SISMEPRE, RENAMU y la clasificación municipal oficial del MEF.

El objetivo no es corregir datos dentro del motor de calidad ni construir el modelo analítico final. El objetivo es medir riesgos, dejar evidencia reproducible y separar problemas técnicos bloqueantes de hallazgos de datos que requieren interpretación.

## Alcance actual

La calidad actual cubre dos capas:

| Capa | Propósito | Recursos evaluados |
| --- | --- | ---: |
| Bronze | Validar existencia, lectura, filas, columnas y metadata técnica de los Parquet preservados. | 26 |
| Silver | Validar datasets limpios/tipados por fuente antes de integración. | Por redefinir |

Fuentes evaluadas:

| Fuente | Descripción | Recursos |
| --- | --- | ---: |
| `siaf_income` | Presupuesto y ejecución de ingresos SIAF. | 17 |
| `sismepre` | Seguimiento de la meta del impuesto sismepre. | 7 |
| `renamu` | Registro Nacional de Municipalidades 2022. | 1 |
| `municipal_classification` | Clasificación Municipal MEF 2019. | 1 |

## Configuración

Las reglas se parametrizan en:

```text
config/quality_rules.yaml
```

La configuración mantiene secciones separadas:

| Sección | Uso |
| --- | --- |
| `quality.bronze` | Reglas técnicas y progresivas para Bronze. |
| `quality.silver` | Reglas de validación y profiling operativo para Silver. |

Los estados válidos son `PASS`, `WARNING` y `FAIL`. No se usa `SKIPPED`. Cuando una regla no puede evaluarse por falta de columnas en una capa temprana, se registra como `WARNING` con una explicación clara.

## Calidad Bronze

### Scripts

Validación Bronze:

```text
src/quality/run_quality_checks.py
```

Reporte Bronze:

```text
src/quality/generate_quality_report.py
```

Outputs locales:

```text
data/quality/bronze_quality_results.jsonl
reports/data_quality_report.html
```

Estos archivos son derivados locales y no deben versionarse.

### Reglas Bronze

| Regla | Severidad | Descripción |
| --- | --- | --- |
| `dataset_path_exists` | `FAIL` | Verifica que exista la ruta del recurso Bronze. |
| `parquet_files_exist` | `FAIL` | Verifica que existan archivos Parquet. |
| `dataset_readable` | `FAIL` | Verifica lectura con Spark. |
| `row_count_positive` | `FAIL` | Verifica al menos una fila. |
| `column_count_positive` | `FAIL` | Verifica al menos una columna. |
| `bronze_metadata_columns_present` | `FAIL` | Verifica metadata técnica común. |
| `fully_null_columns` | `WARNING` | Detecta columnas completamente nulas. |
| `exact_duplicate_rows` | `WARNING` | Detecta duplicados exactos. |
| `invalid_year` | `WARNING` | Evalúa año si existe columna candidata. |
| `invalid_ubigeo` | `WARNING` | Evalúa formato de ubigeo si existe columna candidata. |
| `expected_columns_present` | `FAIL` | Evalúa el contrato Bronze esperado para fuentes con esquema definido. |
| `valid_tipo_clasificacion` | `FAIL` | Evalúa que `tipo_clasificacion` solo contenga valores `A` a `G`. |
| `expected_total_rows` | `FAIL` | Evalúa el total oficial esperado cuando existe referencia externa. |
| `expected_rows_by_tipo` | `FAIL` | Evalúa los conteos oficiales por tipo municipal. |
| `duplicate_key_check_anio_ubigeo` | `FAIL` | Evalúa ausencia de duplicados por `anio + ubigeo`. |
| `invalid_percentage` | `WARNING` | Evalúa porcentajes si existen columnas candidatas. |

### Resultado observado Bronze

| Métrica | Resultado |
| --- | ---: |
| Recursos Bronze evaluados | 25 de 25 |
| Resultados generados | 275 |
| `PASS` | 220 |
| `WARNING` | 55 |
| `FAIL` | 0 |

Warnings Bronze observados:

| Regla | `WARNING` | Interpretación |
| --- | ---: | --- |
| `invalid_percentage` | 25 | No se encontraron columnas candidatas de porcentaje en Bronze. |
| `invalid_ubigeo` | 23 | No todos los recursos Bronze contienen `ubigeo`. |
| `invalid_year` | 7 | Algunos recursos sismeprees no exponen año con nombres candidatos genéricos. |

No hubo `FAIL`, por lo que Bronze cumple el contrato técnico mínimo para continuar hacia limpieza y tipado Silver.

## Calidad Silver

La validación Silver ocurre después de limpiar, tipar y estandarizar cada fuente, y antes de integrar datasets municipales. Su función es comprobar que las transformaciones Silver produjeron columnas técnicas esperadas, flags de validez y señales de riesgo para integración.

### Scripts

Validación Silver:

```text
src/quality/run_silver_quality_checks.py
```

Reporte Silver:

```text
src/quality/generate_silver_quality_report.py
```

Configuración:

```text
config/quality_rules.yaml
```

Sección:

```text
quality.silver
```

Outputs locales:

```text
data/quality/silver_quality_results.jsonl
reports/silver_quality_report.html
```

Estos outputs son regenerables y no deben versionarse.

### Reglas Silver

| Grupo | Reglas |
| --- | --- |
| Existencia y lectura | `dataset_path_exists`, `parquet_files_exist`, `dataset_readable` |
| Estructura básica | `row_count_positive`, `column_count_positive` |
| Contrato Silver | `silver_metadata_columns_present`, `expected_typed_columns_present`, `expected_flags_present` |
| Nulos | `critical_nulls` |
| Duplicados | `exact_duplicate_rows`, `candidate_key_duplicates`, `siaf_candidate_key_duplicates`, `sismepre_candidate_key_duplicates`, `renamu_ubigeo_duplicates`, `renamu_idmunici_duplicates` |
| Flags | `invalid_boolean_flags`, `invalid_siaf_flags` |
| Calidad numérica | `negative_amounts`, `sismepre_parse_failures`, `renamu_financial_parse_failures` |
| Territorio RENAMU | `renamu_territory_nulls`, `renamu_tipomuni_invalid_values` |
| Referencias semánticas | `dictionary_reference_missing` |

### Resultado observado Silver

| Métrica | Resultado |
| --- | ---: |
| Recursos Silver evaluados | 25 de 25 |
| Resultados generados | 403 |
| `PASS` | 347 |
| `WARNING` | 56 |
| `FAIL` | 0 |

Resultados por fuente:

| Fuente | Resultados |
| --- | ---: |
| `siaf_income` | 272 |
| `sismepre` | 112 |
| `renamu` | 19 |

Warnings Silver principales:

| Regla | `WARNING` | Interpretación |
| --- | ---: | --- |
| `negative_amounts` | 17 | Hay montos negativos en recursos SIAF. Requieren interpretación presupuestal o contable; no deben bloquearse automáticamente. |
| `candidate_key_duplicates` | 19 | Las llaves candidatas preliminares todavía no identifican unicidad completa. Esto sugiere granularidad más fina o llaves incompletas. |
| `siaf_candidate_key_duplicates` | 16 | La llave presupuestal SIAF propuesta es hipótesis funcional y debe revisarse antes de integrar o modelar Gold. |
| `sismepre_candidate_key_duplicates` | 3 | Algunas tablas sismeprees requieren revisar llaves relacionales o granularidad. |
| `sismepre_parse_failures` | 1 | Existe un hallazgo puntual de parseo en SISMEPRE que debe revisarse antes de usar el campo en Gold. |

No hubo `FAIL`. Por tanto, Silver queda técnicamente validada para avanzar hacia análisis de integración, manteniendo los `WARNING` como riesgos explícitos.

## Interpretación de warnings

Los `WARNING` no bloquean automáticamente el avance. En esta etapa representan señales de revisión:

- Los montos negativos de SIAF pueden ser válidos según semántica presupuestal, ajustes o anulaciones. Deben revisarse con criterio contable antes de convertirlos en errores.
- Los duplicados por llave candidata no implican necesariamente filas incorrectas. Pueden indicar que la llave propuesta todavía no incluye todos los campos de granularidad.
- Los duplicados funcionales deben analizarse antes de integrar fuentes o construir hechos Gold.
- Los parseos fallidos puntuales deben revisarse antes de usar los campos tipados en métricas finales.
- La validación territorial RENAMU no mostró fallos bloqueantes, pero sigue siendo clave para integración por ubigeo.

## Criterio de avance

Bronze y Silver no registran `FAIL` en las corridas locales observadas. Esto permite continuar hacia integración de fuentes con una base técnicamente validada.

La siguiente fase debe usar estos hallazgos para:

- Refinar llaves candidatas.
- Definir reglas de nulos críticos por dataset integrado.
- Confirmar granularidad real de SIAF y SISMEPRE.
- Revisar montos negativos antes de métricas finales.
- Revisar parseos puntuales de SISMEPRE.
- Validar consistencia territorial entre fuentes.

La integración Silver ya trata estos warnings de forma conservadora: SIAF se agrega en `siaf_municipal_amounts`, SISMEPRE conserva granularidad de entidad/formulario/tiempo estadístico en `sismepre_entity_period`, y el cruce territorial se apoya en `municipal_entity_bridge` sin asumir que `sec_ejec` equivale a `ubigeo`. Los detalles y coberturas observadas se documentan en `docs/silver_transformations.md`.

## Criterio de versionamiento

No versionar outputs locales derivados:

```text
data/quality/bronze_quality_results.jsonl
data/quality/silver_quality_results.jsonl
reports/data_quality_report.html
reports/silver_quality_report.html
```

Sí versionar la documentación y notebooks de apoyo cuando no contengan outputs pesados:

```text
docs/data_quality.md
docs/data_profiling.md
notebooks/02_data_profiling.ipynb
```

## Estado actual

Estado: calidad Bronze y Silver implementada, ejecutada localmente y documentada.

Resumen:

- Bronze: 275 resultados, 220 `PASS`, 55 `WARNING`, 0 `FAIL`.
- Silver: 403 resultados, 347 `PASS`, 56 `WARNING`, 0 `FAIL`.
- Los 26 recursos esperados fueron evaluados en ambas capas.
- No hubo errores técnicos bloqueantes.
- Los riesgos pendientes se concentran en llaves candidatas, montos negativos SIAF y un parseo puntual SISMEPRE.
