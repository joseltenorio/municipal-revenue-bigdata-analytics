# Calidad de datos

## Propósito del documento

Este documento describe la estrategia de calidad de datos aplicada al proyecto **Municipal Revenue Big Data Analytics**.

La calidad de datos permite verificar que las fuentes convertidas a Bronze Parquet sean legibles, trazables y suficientemente consistentes para continuar hacia las etapas de limpieza, tipado semántico e integración en Silver.

El objetivo de esta etapa no es corregir datos ni construir el modelo analítico final. El objetivo es medir el estado técnico de la capa Bronze, identificar riesgos iniciales y documentar qué aspectos deberán resolverse posteriormente en Silver.

## Alcance actual

El alcance actual de calidad corresponde a la capa **Bronze**.

Se evaluaron los datasets Parquet generados para las tres fuentes principales del proyecto:

| Fuente         |                                 Descripción | Recursos Bronze evaluados |
| -------------- | ------------------------------------------: | ------------------------: |
| `mef_income`   |     Presupuesto y ejecución de ingresos MEF |                        17 |
| `predial_goal` | Seguimiento de la meta del impuesto predial |                         7 |
| `renamu`       |   Registro Nacional de Municipalidades 2022 |                         1 |
| **Total**      |                                             |                    **25** |

La evaluación actual se concentra en controles técnicos y reglas progresivas. No exige todavía columnas de negocio como PIA, PIM, recaudación, cumplimiento predial o variables específicas de RENAMU, porque esas definiciones corresponden a Silver y al modelado analítico posterior.

## Archivo de configuración

Las reglas de calidad se parametrizan en:

```text
config/quality_rules.yaml
```

Este archivo dejó de ser un borrador de discovery y ahora funciona como configuración progresiva de calidad Bronze.

La configuración actual define:

- Estados válidos: `PASS`, `WARNING`, `FAIL`.
- Fuentes Bronze evaluadas: `mef_income`, `predial_goal`, `renamu`.
- Recursos esperados por fuente.
- Metadata técnica obligatoria.
- Reglas técnicas mínimas.
- Reglas progresivas para detectar riesgos iniciales.

No se utiliza el estado `SKIPPED`. Cuando una regla no puede evaluarse porque la columna necesaria todavía no existe en Bronze, se registra como `WARNING` con `evaluated=false`.

## Script principal

El script principal de calidad es:

```text
src/quality/run_quality_checks.py
```

Este script:

- Lee la configuración de calidad.
- Identifica recursos Bronze esperados.
- Valida existencia de rutas y archivos Parquet.
- Lee datasets Parquet con Spark.
- Ejecuta reglas técnicas y progresivas.
- Registra resultados con `run_id`.
- Escribe resultados locales en formato JSON Lines cuando se ejecuta en modo real.
- Soporta modo `--dry-run` para validar el plan sin leer Parquet ni generar archivos de salida.

## Generación de reporte

El reporte HTML se genera con:

```text
src/quality/generate_quality_report.py
```

Este script lee los resultados locales de calidad y construye un reporte HTML resumido.

Por defecto usa:

```text
data/quality/bronze_quality_results.jsonl
reports/data_quality_report.html
```

Estos archivos son derivados de datos reales y no deben versionarse en Git.

## Comandos de ejecución

Validación sin escritura:

```powershell
docker compose run --rm python-app python -m src.quality.run_quality_checks --dry-run
```

Ejecución real de quality checks:

```powershell
docker compose run --rm python-app python -m src.quality.run_quality_checks
```

Generación del reporte HTML:

```powershell
docker compose run --rm python-app python -m src.quality.generate_quality_report
```

## Outputs locales

La ejecución real genera los siguientes archivos locales:

| Archivo                                     | Propósito                                            | Versionar |
| ------------------------------------------- | ---------------------------------------------------- | --------- |
| `data/quality/bronze_quality_results.jsonl` | Resultados detallados de calidad por recurso y regla | No        |
| `reports/data_quality_report.html`          | Reporte HTML resumido de calidad                     | No        |

Estos outputs pueden regenerarse cuando cambien los datos Bronze o las reglas de calidad.

## Estados de resultado

| Estado    | Interpretación                                                                                             |
| --------- | ---------------------------------------------------------------------------------------------------------- |
| `PASS`    | La regla fue evaluada correctamente y no se detectó incumplimiento.                                        |
| `WARNING` | La regla detectó una condición que requiere revisión o no pudo evaluarse por falta de columnas candidatas. |
| `FAIL`    | La regla técnica obligatoria falló y debe corregirse antes de continuar con confianza.                     |

Una regla con `WARNING` no bloquea necesariamente el avance. En esta etapa, varios `WARNING` representan limitaciones esperadas de Bronze, especialmente cuando una columna de negocio todavía no está normalizada o no existe en todos los recursos.

## Reglas implementadas

| Regla                             | Tipo       | Severidad esperada | Descripción                                                         | Acción posterior                                              |
| --------------------------------- | ---------- | -----------------: | ------------------------------------------------------------------- | ------------------------------------------------------------- |
| `dataset_path_exists`             | Técnica    |    `FAIL` si falla | Verifica que exista la ruta Bronze del recurso esperado.            | Corregir generación Bronze o configuración del recurso.       |
| `parquet_files_exist`             | Técnica    |    `FAIL` si falla | Verifica que existan archivos `.parquet` dentro del recurso Bronze. | Regenerar Bronze para el recurso afectado.                    |
| `dataset_readable`                | Técnica    |    `FAIL` si falla | Verifica que Spark pueda leer el dataset Parquet.                   | Revisar escritura Parquet, permisos o corrupción de archivos. |
| `row_count_positive`              | Técnica    |    `FAIL` si falla | Verifica que el recurso tenga al menos una fila.                    | Revisar fuente, descarga o conversión Bronze.                 |
| `column_count_positive`           | Técnica    |    `FAIL` si falla | Verifica que el recurso tenga al menos una columna.                 | Revisar lectura de origen y conversión.                       |
| `bronze_metadata_columns_present` | Técnica    |    `FAIL` si falla | Verifica metadata común de trazabilidad Bronze.                     | Corregir builders Bronze.                                     |
| `fully_null_columns`              | Progresiva |          `WARNING` | Detecta columnas completamente nulas.                               | Evaluar descarte, imputación o interpretación en Silver.      |
| `exact_duplicate_rows`            | Progresiva |          `WARNING` | Detecta duplicados exactos a nivel de fila completa.                | Evaluar deduplicación o preservación en Silver.               |
| `invalid_year`                    | Progresiva |          `WARNING` | Valida año cuando existe una columna candidata.                     | Normalizar año o periodo en Silver.                           |
| `invalid_ubigeo`                  | Progresiva |          `WARNING` | Valida formato de ubigeo cuando existe la columna.                  | Estandarizar ubigeo y llaves territoriales en Silver.         |
| `invalid_percentage`              | Progresiva |          `WARNING` | Valida porcentajes cuando existen columnas candidatas.              | Tipar porcentajes, avances o tasas en Silver.                 |

## Metadata Bronze obligatoria

La metadata común esperada en los recursos Bronze es:

| Columna                   | Descripción                                    |
| ------------------------- | ---------------------------------------------- |
| `bronze_source_name`      | Nombre lógico de la fuente.                    |
| `bronze_resource_key`     | Identificador técnico del recurso convertido.  |
| `bronze_source_file_name` | Nombre del archivo de origen usado en Landing. |
| `bronze_source_file_path` | Ruta local del archivo de origen.              |
| `bronze_processed_at_utc` | Fecha y hora de procesamiento Bronze en UTC.   |

Algunas fuentes agregan metadata adicional según su naturaleza:

| Fuente         | Metadata adicional                                |
| -------------- | ------------------------------------------------- |
| `mef_income`   | `bronze_source_year`, `bronze_source_granularity` |
| `predial_goal` | `bronze_source_role`, `bronze_source_priority`    |
| `renamu`       | `bronze_source_year`                              |

## Resultado observado de ejecución

La ejecución real de quality checks sobre Bronze evaluó correctamente los 25 recursos esperados.

Resumen general:

| Métrica                      | Resultado |
| ---------------------------- | --------: |
| Resultados generados         |       275 |
| Recursos Bronze evaluados    |  25 de 25 |
| Reglas aplicadas por recurso |        11 |
| Errores de lectura Parquet   |         0 |
| `PASS`                       |       220 |
| `WARNING`                    |        55 |
| `FAIL`                       |         0 |

Resumen por fuente:

| Fuente         | Resultados |
| -------------- | ---------: |
| `mef_income`   |        187 |
| `predial_goal` |         77 |
| `renamu`       |         11 |

Cada regla generó 25 resultados, uno por cada recurso Bronze esperado.

## Interpretación de resultados

El resultado general es favorable para continuar hacia Silver:

- No se detectaron recursos Bronze faltantes.
- No se detectaron errores de lectura Parquet.
- No se detectaron fallos técnicos obligatorios.
- Los 25 recursos esperados fueron evaluados.
- Las reglas técnicas de existencia, lectura, filas, columnas y metadata pasaron correctamente.
- Los `WARNING` se concentran en reglas progresivas que dependen de columnas de negocio no uniformes entre fuentes.

No se registraron `FAIL`, por lo que la capa Bronze cumple el contrato técnico mínimo para continuar con profiling complementario, limpieza y estandarización en Silver.

## Warnings observados

Los `WARNING` se distribuyen en tres reglas:

| Regla                | Cantidad de `WARNING` | Interpretación                                                                           |
| -------------------- | --------------------: | ---------------------------------------------------------------------------------------- |
| `invalid_percentage` |                    25 | La regla no encontró columnas candidatas de porcentaje en los recursos Bronze evaluados. |
| `invalid_ubigeo`     |                    23 | La regla no pudo evaluarse en recursos donde no existe una columna `ubigeo`.             |
| `invalid_year`       |                     7 | La regla no encontró columna candidata de año en los recursos prediales.                 |

Estos `WARNING` no significan necesariamente que los datos sean incorrectos. En varios casos indican que Bronze conserva la estructura original sin forzar una estandarización semántica.

## Hallazgos por fuente

### MEF ingresos

La fuente `mef_income` presenta 17 recursos Bronze evaluados correctamente.

Hallazgos principales:

- Todos los recursos esperados existen en Bronze.
- Los datasets Parquet son legibles con Spark.
- La metadata Bronze común está presente.
- La fuente mantiene recursos por año y granularidad.
- Las reglas progresivas de ubigeo y porcentaje no son plenamente evaluables en esta etapa.
- La interpretación de montos, periodos, clasificadores y posibles llaves queda pendiente para Silver.

Acciones esperadas en Silver:

- Tipar años, periodos y montos.
- Identificar columnas de municipalidad, clasificación presupuestal e indicadores de ejecución.
- Estandarizar campos territoriales si existen.
- Evaluar llaves candidatas y duplicados con criterios de negocio.

### Meta predial

La fuente `predial_goal` presenta 7 recursos Bronze evaluados correctamente.

Hallazgos principales:

- Las tablas prediales fueron evaluadas como recursos separados.
- Los datasets Parquet son legibles con Spark.
- La metadata Bronze común está presente.
- Se observaron `WARNING` en `invalid_year` porque no se encontraron columnas candidatas de año con los nombres esperados por la regla.
- Algunas tablas no contienen `ubigeo`, por lo que `invalid_ubigeo` no se evalúa en todos los recursos.
- Las relaciones entre tablas prediales todavía no se interpretan en Bronze.

Acciones esperadas en Silver:

- Identificar llaves entre tablas prediales.
- Determinar qué tablas funcionan como hechos, catálogos o estructuras auxiliares.
- Tipar campos de avance, cumplimiento, preguntas, respuestas y años.
- Estandarizar campos territoriales o códigos de entidad si existen.
- Resolver reglas de negocio predial después de revisar diccionarios y relaciones.

### RENAMU

La fuente `renamu` presenta 1 recurso Bronze evaluado correctamente:

```text
resource_key=base_renamu_2022
```

Hallazgos principales:

- El dataset Bronze RENAMU existe y es legible.
- La metadata Bronze común está presente.
- La fuente se mantiene como tabla ancha contextual.
- No se seleccionan todavía variables útiles para análisis.
- No se interpreta todavía el cuestionario RENAMU.
- La normalización de ubigeo, departamentos, provincias, distritos y variables relevantes queda pendiente para Silver.

Acciones esperadas en Silver:

- Normalizar `ubigeo`, `ccdd`, `ccpp`, `ccdi` si están disponibles.
- Seleccionar variables RENAMU útiles para contexto municipal.
- Estandarizar nombres territoriales.
- Evaluar cobertura de cruce con MEF y meta predial.
- Documentar variables descartadas o mantenidas.

## Relación con Silver

Los resultados de calidad Bronze sustentan el inicio de Silver, pero no reemplazan las reglas de limpieza.

Silver deberá encargarse de:

- Tipado semántico de años, meses, montos y porcentajes.
- Normalización de ubigeos y nombres geográficos.
- Identificación de llaves candidatas reales.
- Reglas de negocio específicas por fuente.
- Detección de duplicados con llaves funcionales, no solo duplicados exactos.
- Integración entre MEF, predial y RENAMU.
- Separación entre hechos, dimensiones, catálogos o marts analíticos.

## Limitaciones de la evaluación actual

La evaluación actual tiene las siguientes limitaciones:

- Bronze prioriza preservación y trazabilidad, no limpieza final.
- Algunas reglas no se evalúan si la columna candidata no existe.
- No se validan todavía montos negativos, porque los campos numéricos de negocio aún no han sido tipados.
- No se evalúa todavía integridad entre fuentes.
- No se valida todavía consistencia territorial entre MEF, predial y RENAMU.
- No se determinan todavía llaves definitivas.
- No se define todavía el modelo Gold.

Estas limitaciones son esperadas en esta etapa y se abordarán durante Silver, integración y modelado analítico.

## Criterio de versionamiento

Los resultados locales de calidad y reportes generados no deben incluirse en Git.

No versionar:

```text
data/quality/bronze_quality_results.jsonl
reports/data_quality_report.html
```

Sí versionar:

```text
docs/data_quality.md
docs/data_profiling.md
```

## Estado actual

Estado: controles de calidad Bronze implementados y ejecutados localmente.

Resultado general:

- 25 de 25 recursos Bronze evaluados.
- 275 resultados generados.
- 220 `PASS`.
- 55 `WARNING`.
- 0 `FAIL`.
- 0 errores de lectura.
- Reporte HTML local generado.

La capa Bronze queda validada técnicamente y lista para iniciar la etapa Silver, donde se realizarán limpieza, tipado semántico, normalización territorial e integración de fuentes.
