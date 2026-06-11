# Diccionario de datos

## Propósito del documento

Este documento define un diccionario inicial de datos para el proyecto **Municipal Revenue Big Data Analytics**.

En esta etapa, el diccionario funciona como una hipótesis de trabajo basada en las fuentes esperadas. No representa todavía un contrato definitivo de columnas, tipos ni reglas de negocio.

El diccionario será actualizado después de:

- Discovery de fuentes.
- Profiling inicial.
- Construcción de Bronze.
- Reglas de calidad.
- Transformaciones Silver.
- Modelo Gold final.

## Criterio de uso

Este documento no debe confundirse con el reporte de profiling.

- El diccionario de datos describe campos esperados, significado y uso analítico.
- El profiling documenta valores observados, nulos, duplicados, tipos inferidos y problemas detectados.
- La calidad documenta reglas, severidades y resultados de validación.

## Convenciones preliminares de nombres

Los nombres técnicos finales se definirán durante Bronze y Silver. Como criterio general:

- Se usarán nombres en minúsculas.
- Se evitarán tildes y caracteres especiales.
- Se reemplazarán espacios por guiones bajos.
- Se preferirán nombres descriptivos y consistentes.
- Se mantendrán columnas originales relevantes cuando aporten trazabilidad.
- Se agregarán columnas de metadata técnica por capa.

Ejemplos:

| Nombre original posible | Nombre técnico esperado |
| ----------------------- | ----------------------- |
| Año                     | anio                    |
| Departamento            | departamento            |
| Provincia               | provincia               |
| Distrito                | distrito                |
| Municipalidad           | municipalidad           |
| Ubigeo                  | ubigeo                  |
| PIA                     | pia                     |
| PIM                     | pim                     |
| Ejecución               | ejecucion               |
| Avance %                | porcentaje_avance       |

## Campos comunes esperados

| Campo técnico             | Descripción                      | Tipo esperado | Capa esperada           | Observación                    |
| ------------------------- | -------------------------------- | ------------- | ----------------------- | ------------------------------ |
| source_system             | Sistema o fuente de origen       | string        | Bronze, Silver, Gold    | Metadata técnica               |
| source_file_name          | Nombre del archivo original      | string        | Bronze, Silver          | Permite trazabilidad           |
| ingestion_date            | Fecha de ingesta                 | date          | Bronze, Silver          | Fecha de carga al lakehouse    |
| run_id                    | Identificador único de ejecución | string        | Bronze, Silver, Quality | Permite auditoría              |
| processing_timestamp      | Fecha y hora de procesamiento    | timestamp     | Silver, Gold            | Metadata de transformación     |
| anio                      | Año del registro                 | integer       | Bronze, Silver, Gold    | Debe validarse en profiling    |
| departamento              | Departamento                     | string        | Silver, Gold            | Debe estandarizarse            |
| provincia                 | Provincia                        | string        | Silver, Gold            | Debe estandarizarse            |
| distrito                  | Distrito                         | string        | Silver, Gold            | Debe estandarizarse            |
| ubigeo                    | Código geográfico                | string        | Silver, Gold            | Llave candidata de integración |
| municipalidad             | Nombre de municipalidad          | string        | Bronze, Silver, Gold    | Puede requerir normalización   |
| municipalidad_normalizada | Nombre estandarizado             | string        | Silver, Gold            | Apoyo para cruces              |

## Fuente MEF: Presupuesto y ejecución de ingresos

### Descripción

Fuente esperada para analizar presupuesto y ejecución de ingresos municipales.

### Campos preliminares

| Campo técnico esperado | Descripción                           | Tipo esperado | Uso analítico                    | Observación                            |
| ---------------------- | ------------------------------------- | ------------- | -------------------------------- | -------------------------------------- |
| anio                   | Año presupuestal                      | integer       | Filtro temporal y análisis anual | Confirmar nombre real                  |
| periodo                | Periodo de registro, si existe        | string        | Análisis temporal más granular   | Puede no estar disponible              |
| departamento           | Departamento                          | string        | Segmentación territorial         | Puede venir como texto o código        |
| provincia              | Provincia                             | string        | Segmentación territorial         | Puede venir como texto o código        |
| distrito               | Distrito                              | string        | Segmentación territorial         | Puede venir como texto o código        |
| ubigeo                 | Código geográfico                     | string        | Integración territorial          | Confirmar disponibilidad               |
| codigo_entidad         | Código de entidad pública             | string        | Llave administrativa candidata   | Puede ser clave más estable que nombre |
| municipalidad          | Nombre de municipalidad o entidad     | string        | Análisis municipal               | Requiere normalización                 |
| rubro_ingreso          | Rubro o categoría de ingreso          | string        | Segmentación por tipo de ingreso | Confirmar estructura real              |
| clasificador_ingreso   | Clasificador presupuestal             | string        | Análisis presupuestal detallado  | Puede ser jerárquico                   |
| pia                    | Presupuesto institucional de apertura | decimal       | KPI presupuestal                 | Convertir a numérico en Silver         |
| pim                    | Presupuesto institucional modificado  | decimal       | KPI presupuestal                 | Convertir a numérico en Silver         |
| ejecucion_ingresos     | Monto ejecutado o recaudado           | decimal       | KPI principal                    | Nombre exacto pendiente                |
| porcentaje_ejecucion   | Avance porcentual de ejecución        | decimal       | KPI derivado o fuente            | Validar rango 0 a 100                  |

### Reglas preliminares asociadas

- `anio` no debe estar vacío.
- Los montos deben poder convertirse a tipo numérico.
- `pia`, `pim` y `ejecucion_ingresos` no deberían ser negativos, salvo justificación documentada.
- `porcentaje_ejecucion` debería estar entre 0 y 100 si representa un porcentaje simple.
- Debe evaluarse si `codigo_entidad` o `ubigeo` funciona como llave principal.

## Fuente SISMERE / MEF: Meta del impuesto predial

### Descripción

Fuente esperada para analizar avance, cumplimiento y brechas de la meta del impuesto predial.

### Campos preliminares

| Campo técnico esperado    | Descripción                            | Tipo esperado | Uso analítico              | Observación              |
| ------------------------- | -------------------------------------- | ------------- | -------------------------- | ------------------------ |
| anio                      | Año de seguimiento                     | integer       | Filtro temporal            | Confirmar disponibilidad |
| departamento              | Departamento                           | string        | Segmentación territorial   | Debe estandarizarse      |
| provincia                 | Provincia                              | string        | Segmentación territorial   | Debe estandarizarse      |
| distrito                  | Distrito                               | string        | Segmentación territorial   | Debe estandarizarse      |
| ubigeo                    | Código geográfico                      | string        | Integración territorial    | Confirmar disponibilidad |
| codigo_municipalidad      | Código administrativo de municipalidad | string        | Llave candidata            | Confirmar existencia     |
| municipalidad             | Nombre de municipalidad                | string        | Análisis municipal         | Requiere normalización   |
| meta_predial              | Valor de meta predial                  | decimal       | KPI base                   | Confirmar unidad         |
| avance_predial            | Valor de avance predial                | decimal       | KPI base                   | Confirmar unidad         |
| porcentaje_avance_predial | Porcentaje de avance                   | decimal       | KPI de cumplimiento        | Validar rango esperado   |
| estado_cumplimiento       | Estado de cumplimiento                 | string        | Clasificación de desempeño | Puede ser derivado       |
| grupo_municipal           | Grupo o clasificación municipal        | string        | Segmentación               | Confirmar si existe      |

### Reglas preliminares asociadas

- `anio` no debe estar vacío.
- `municipalidad` debe estar presente.
- `porcentaje_avance_predial` debe ser numérico.
- Los porcentajes deben validarse antes de asumir rango.
- Debe evaluarse si la fuente permite calcular brecha de cumplimiento.
- Debe evaluarse si el cumplimiento se toma de la fuente o se calcula en Gold.

## Fuente RENAMU 2022

### Descripción

Fuente esperada para agregar contexto territorial y municipal al análisis.

### Campos preliminares

| Campo técnico esperado    | Descripción                            | Tipo esperado | Uso analítico         | Observación                               |
| ------------------------- | -------------------------------------- | ------------- | --------------------- | ----------------------------------------- |
| ubigeo                    | Código geográfico                      | string        | Llave territorial     | Debe preservarse con ceros a la izquierda |
| departamento              | Departamento                           | string        | Jerarquía territorial | Debe estandarizarse                       |
| provincia                 | Provincia                              | string        | Jerarquía territorial | Debe estandarizarse                       |
| distrito                  | Distrito                               | string        | Jerarquía territorial | Debe estandarizarse                       |
| municipalidad             | Nombre de municipalidad                | string        | Contexto municipal    | Requiere normalización                    |
| tipo_municipalidad        | Tipo o categoría de municipalidad      | string        | Segmentación          | Confirmar disponibilidad                  |
| variables_gestion         | Variables de gestión municipal         | mixed         | Contexto analítico    | Se seleccionarán después del profiling    |
| variables_servicios       | Variables de servicios municipales     | mixed         | Contexto analítico    | Se seleccionarán después del profiling    |
| variables_infraestructura | Variables de infraestructura municipal | mixed         | Contexto analítico    | Se seleccionarán después del profiling    |

### Reglas preliminares asociadas

- `ubigeo` debe conservarse como texto.
- `ubigeo` debe mantener longitud válida según estructura territorial.
- Las variables RENAMU deben seleccionarse con criterio analítico.
- No se debe cargar todo RENAMU a Gold si no aporta al análisis.
- Debe documentarse cualquier variable descartada.

## Campos derivados esperados en Silver o Gold

Los siguientes campos no necesariamente existen en las fuentes. Podrían crearse durante Silver o Gold si los datos lo permiten.

| Campo técnico                   | Descripción                              | Tipo esperado | Capa probable | Observación                          |
| ------------------------------- | ---------------------------------------- | ------------- | ------------- | ------------------------------------ |
| brecha_ejecucion                | Diferencia entre presupuesto y ejecución | decimal       | Gold          | Fórmula depende de campos reales     |
| ratio_ejecucion                 | Ejecución sobre presupuesto              | decimal       | Gold          | Puede calcularse con PIM como base   |
| brecha_predial                  | Diferencia entre meta y avance predial   | decimal       | Gold          | Depende de unidad de medida          |
| clasificacion_desempeno_predial | Categoría de desempeño predial           | string        | Gold          | Regla a definir después de profiling |
| ranking_ejecucion_municipal     | Ranking por ejecución o avance           | integer       | Gold          | Orientado a dashboard                |
| ranking_cumplimiento_predial    | Ranking por cumplimiento predial         | integer       | Gold          | Orientado a dashboard                |
| tiene_cruce_renamu              | Indicador de cruce con RENAMU            | boolean       | Silver, Gold  | Útil para auditoría de integración   |

## Columnas de auditoría de ingesta

| Campo técnico              | Descripción                       | Tipo esperado | Uso                               |
| -------------------------- | --------------------------------- | ------------- | --------------------------------- |
| run_id                     | Identificador único de ejecución  | string        | Trazabilidad                      |
| source_name                | Nombre lógico de fuente           | string        | Auditoría                         |
| source_url                 | URL o referencia de origen        | string        | Auditoría                         |
| access_method              | Método de acceso                  | string        | API, CSV, ZIP o descarga manual   |
| started_at                 | Inicio de ejecución               | timestamp     | Auditoría                         |
| finished_at                | Fin de ejecución                  | timestamp     | Auditoría                         |
| duration_seconds           | Duración de ejecución             | decimal       | Auditoría                         |
| attempt_number             | Número de intento                 | integer       | Control de reintentos             |
| max_attempts               | Máximo de intentos configurados   | integer       | Control de reintentos             |
| retry_count                | Cantidad de reintentos realizados | integer       | Auditoría                         |
| http_status_code           | Código HTTP, si aplica            | integer       | Diagnóstico                       |
| error_type                 | Tipo de error                     | string        | Diagnóstico                       |
| error_message              | Mensaje de error                  | string        | Diagnóstico                       |
| downloaded_file_name       | Archivo descargado                | string        | Trazabilidad                      |
| downloaded_file_size_bytes | Tamaño del archivo                | integer       | Validación técnica                |
| checksum_sha256            | Hash de integridad                | string        | Validación técnica                |
| records_detected           | Registros detectados, si aplica   | integer       | Validación inicial                |
| final_status               | Estado final                      | string        | SUCCESS, FAILED o PARTIAL_SUCCESS |

## Uso de diccionarios oficiales de las fuentes

Algunas fuentes públicas, especialmente las publicadas en el portal de datos abiertos del MEF, incluyen una sección de diccionario de datos por recurso o archivos específicos de diccionario en formato CSV.

Estos diccionarios oficiales serán usados como insumo para validar y enriquecer este documento cuando se confirme el recurso final de cada fuente.

Criterio de uso:

- El diccionario oficial de la fuente se considerará referencia primaria para nombres originales, tipos publicados y descripciones oficiales.
- El profiling local validará cómo llegan realmente los datos al descargarlos o consultarlos.
- El diccionario técnico del proyecto documentará los nombres normalizados usados en Bronze, Silver y Gold.
- Si existe diferencia entre tipo oficial y tipo observado, se documentará en profiling y se resolverá en Silver.
- No se reemplazará el profiling por el diccionario oficial, porque el profiling permite detectar nulos, duplicados, valores inesperados, cambios de estructura y problemas de integración.

Estado actual:

- MEF ingresos: pendiente de seleccionar recurso final y extraer diccionario oficial asociado.
- Meta predial: pendiente de seleccionar recurso final y extraer diccionario oficial asociado.
- RENAMU 2022: pendiente de confirmar si el archivo descargable incluye diccionario, ficha técnica o metadatos separados.

## Decisiones pendientes

Las siguientes decisiones se tomarán después de discovery y profiling:

- Columnas reales de cada fuente.
- Tipos reales observados.
- Llaves candidatas definitivas.
- Granularidad real de MEF.
- Granularidad real de meta predial.
- Estructura útil de RENAMU.
- Variables RENAMU que pasarán a Silver y Gold.
- Reglas finales de calidad por fuente.
- KPIs definitivos.
- Modelo Gold final.
- Relaciones del modelo Power BI.

## Actualización por capacidad de profiling

Durante el commit `feat(profiling): add raw data profiling and document results` se agregó una capacidad inicial para perfilar archivos locales en Landing mediante:

`src/quality/profile_sources.py`

El diccionario de datos se mantiene como borrador porque aún no se han descargado ni perfilado archivos reales de las fuentes finales.

Los campos definidos en este documento deberán validarse contra los resultados de:

- `reports/profiling_summary.json`
- `docs/data_profiling.md`
- Diccionarios oficiales de las fuentes
- Resultados de discovery
- Reglas de calidad
- Transformaciones Bronze y Silver

No se debe asumir que los nombres técnicos preliminares coinciden exactamente con los nombres de columnas observados en las fuentes reales.

### Criterio profesional

- El diccionario describe nombres técnicos esperados y significado analítico.
- El profiling documenta evidencia observada en archivos locales.
- Los diccionarios oficiales ayudan a interpretar campos publicados por las instituciones.
- Las reglas finales de calidad se definirán después de observar datos reales.
- Bronze debe conservar trazabilidad hacia nombres originales.
- Silver definirá nombres normalizados, tipos corregidos y llaves candidatas.

## Estado del diccionario

Estado actual: **borrador inicial**.
