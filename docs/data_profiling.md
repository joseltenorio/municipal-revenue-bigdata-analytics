# Profiling de datos

## Propósito del documento

Este documento describe la estrategia de profiling para el proyecto **Municipal Revenue Big Data Analytics**.

El profiling permite inspeccionar la estructura de los datos antes de aplicar limpieza, tipado semántico, integración y modelado analítico. Su función principal es aportar evidencia para decidir reglas de calidad, transformaciones Silver, llaves candidatas y criterios posteriores de modelado Gold.

En este proyecto, el profiling se complementa con los controles de calidad sobre Bronze. El profiling observa estructura, tipos, nulos, duplicados y valores de muestra; la calidad evalúa reglas técnicas y progresivas sobre datasets Parquet ya generados.

## Relación entre discovery, profiling y calidad

El proyecto separa tres actividades relacionadas pero distintas:

| Actividad      | Propósito                                                                                   | Capa principal   |
| -------------- | ------------------------------------------------------------------------------------------- | ---------------- |
| Discovery      | Confirmar acceso, URLs, disponibilidad, formatos y recursos candidatos.                     | Fuentes públicas |
| Profiling      | Inspeccionar estructura, columnas, tipos inferidos, nulos, duplicados y valores de muestra. | Landing / Bronze |
| Quality checks | Validar reglas técnicas y progresivas sobre datasets procesados.                            | Bronze           |

El discovery confirma cómo se accede a las fuentes. El profiling analiza cómo vienen los datos. Los quality checks verifican si los datasets Bronze cumplen condiciones mínimas para continuar.

## Alcance del profiling inicial

El profiling inicial busca responder:

- Qué archivos existen localmente en Landing.
- Cuántas filas y columnas tiene cada archivo.
- Qué tipos de datos se infieren en una lectura exploratoria.
- Qué columnas presentan nulos.
- Qué columnas tienen valores únicos en la muestra.
- Qué archivos presentan duplicados exactos.
- Qué valores de muestra aparecen por columna.
- Qué problemas de lectura aparecen por formato, separador o codificación.
- Qué campos podrían funcionar como llaves candidatas.
- Qué riesgos existen para integrar fuentes municipales.

## Archivo principal

El script principal de profiling es:

```text
src/quality/profile_sources.py
```

Este script:

- Recorre archivos locales dentro de Landing.
- Soporta archivos `.csv`, `.txt`, `.xlsx`, `.xls`, `.json` y `.parquet`.
- Lee una cantidad máxima controlada de filas por archivo.
- Genera un resumen técnico por archivo y columna.
- Escribe un reporte JSON en `reports/profiling_summary.json`.
- No descarga datos externos.
- No transforma archivos.
- No genera Bronze, Silver ni Gold.

## Comando de ejecución

Ejecución por defecto:

```powershell
python -m src.quality.profile_sources
```

Ejecución indicando directorio y salida:

```powershell
python -m src.quality.profile_sources `
  --input-dir "data/landing" `
  --output "reports/profiling_summary.json" `
  --max-rows 10000
```

## Reporte generado

El reporte de profiling se genera localmente en:

```text
reports/profiling_summary.json
```

El archivo contiene:

- Fecha de generación.
- Cantidad de perfiles generados.
- Lista de archivos perfilados.
- Métricas por archivo.
- Métricas por columna.
- Errores de lectura, si existieran.

El reporte JSON generado localmente no debe tratarse como dato fuente. Puede regenerarse cuando cambien los archivos locales. Si contiene información derivada de datos reales, no debe versionarse en Git.

## Métricas calculadas

Por archivo:

| Métrica          | Descripción                                  |
| ---------------- | -------------------------------------------- |
| `file_name`      | Nombre del archivo local.                    |
| `file_extension` | Extensión del archivo.                       |
| `row_count`      | Cantidad de filas leídas.                    |
| `column_count`   | Cantidad de columnas.                        |
| `duplicate_rows` | Duplicados exactos detectados en la muestra. |
| `error`          | Error de lectura, si ocurre.                 |

Por columna:

| Métrica          | Descripción                                |
| ---------------- | ------------------------------------------ |
| `column_name`    | Nombre original de la columna.             |
| `inferred_dtype` | Tipo inferido por la lectura exploratoria. |
| `non_null_count` | Cantidad de valores no nulos.              |
| `null_count`     | Cantidad de valores nulos.                 |
| `null_rate`      | Proporción de nulos.                       |
| `unique_count`   | Cantidad de valores únicos.                |
| `sample_values`  | Valores de muestra no nulos.               |

## Criterios de interpretación

El profiling debe interpretarse con cautela:

- Los tipos inferidos pueden cambiar cuando se procese el archivo completo.
- Un campo único en una muestra no necesariamente es una llave definitiva.
- Un campo sin nulos en una muestra puede tener nulos al leer el archivo completo.
- Los valores de muestra no representan la distribución completa.
- Las reglas de calidad definitivas se ajustan después de observar datos reales.
- Los diccionarios oficiales ayudan a interpretar campos, pero no reemplazan el profiling local.
- La validación de negocio debe realizarse después del tipado y estandarización en Silver.

## Relación con fuentes del proyecto

El discovery identificó recursos directos para las fuentes principales:

- MEF ingresos: CSV por año, recursos recientes diarios o mensuales y diccionario CSV.
- Meta predial: múltiples CSV temáticos y diccionarios.
- RENAMU 2022: ZIP completo, CSV principal extraído y diccionario PDF.

Después de la ingesta y conversión a Bronze, el proyecto cuenta con datasets Parquet para las tres fuentes:

| Fuente         | Capa Bronze                 | Uso actual                                |
| -------------- | --------------------------- | ----------------------------------------- |
| `mef_income`   | `data/bronze/mef_income/`   | Presupuesto y ejecución de ingresos.      |
| `predial_goal` | `data/bronze/predial_goal/` | Seguimiento de meta del impuesto predial. |
| `renamu`       | `data/bronze/renamu/`       | Contexto municipal RENAMU 2022.           |

## Relación con calidad Bronze

La calidad Bronze se ejecuta con:

```text
src/quality/run_quality_checks.py
```

El reporte HTML se genera con:

```text
src/quality/generate_quality_report.py
```

La ejecución real de calidad sobre Bronze produjo:

| Métrica                    | Resultado |
| -------------------------- | --------: |
| Recursos Bronze evaluados  |  25 de 25 |
| Resultados de calidad      |       275 |
| `PASS`                     |       220 |
| `WARNING`                  |        55 |
| `FAIL`                     |         0 |
| Errores de lectura Parquet |         0 |

Estos resultados indican que la capa Bronze cumple el contrato técnico mínimo: existen las rutas esperadas, los archivos Parquet son legibles, los datasets tienen filas y columnas, y la metadata técnica está presente.

## Hallazgos preliminares de calidad

Los `WARNING` observados se relacionan principalmente con reglas progresivas que no pudieron evaluarse por ausencia de columnas candidatas en algunos recursos Bronze.

| Regla                | Resultado observado | Interpretación                                                                                        |
| -------------------- | ------------------: | ----------------------------------------------------------------------------------------------------- |
| `invalid_percentage` |        25 `WARNING` | No se encontraron columnas candidatas de porcentaje en los recursos Bronze evaluados.                 |
| `invalid_ubigeo`     |        23 `WARNING` | No todos los recursos contienen columna `ubigeo`.                                                     |
| `invalid_year`       |         7 `WARNING` | Los recursos prediales no tienen una columna candidata de año con los nombres esperados por la regla. |

Estos hallazgos no representan fallos técnicos de Bronze. Reflejan que las fuentes conservan estructuras heterogéneas y que la normalización semántica debe resolverse en Silver.

## Hallazgos por fuente

### MEF ingresos

La fuente MEF se encuentra disponible en Bronze como recursos separados por año y granularidad.

Hallazgos preliminares:

- Los recursos Bronze esperados fueron evaluados.
- Los datasets Parquet son legibles.
- La metadata técnica está presente.
- Las reglas de porcentaje y ubigeo no son plenamente evaluables en esta etapa.
- La interpretación de montos, periodos, clasificadores y campos territoriales requiere tipado y análisis posterior.

Implicancias para Silver:

- Estandarizar año, mes o periodo.
- Convertir montos a numéricos.
- Identificar campos de municipalidad, clasificador y ejecución.
- Evaluar llaves candidatas de análisis presupuestal.
- Validar duplicados con reglas de negocio, no solo duplicados exactos.

### Meta predial

La fuente predial se encuentra disponible en Bronze como siete tablas separadas.

Hallazgos preliminares:

- Los recursos Bronze esperados fueron evaluados.
- Los datasets Parquet son legibles.
- La metadata técnica está presente.
- Algunas reglas progresivas no se evalúan porque no todas las tablas tienen año o ubigeo.
- Las relaciones entre tablas prediales todavía no se interpretan en Bronze.

Implicancias para Silver:

- Identificar llaves entre tablas prediales.
- Determinar tablas principales, catálogos y estructuras auxiliares.
- Tipar campos de avance, cumplimiento, preguntas, respuestas y periodos.
- Estandarizar códigos de entidad o campos territoriales.
- Preparar una estructura integrable para análisis de cumplimiento predial.

### RENAMU 2022

La fuente RENAMU se encuentra disponible en Bronze como un recurso principal:

```text
resource_key=base_renamu_2022
```

Hallazgos preliminares:

- El recurso Bronze existe y es legible.
- La metadata técnica está presente.
- La fuente conserva una estructura amplia de variables municipales.
- No se seleccionan todavía variables útiles para análisis.
- La interpretación de variables depende del diccionario oficial y del objetivo analítico.

Implicancias para Silver:

- Normalizar `ubigeo`, `ccdd`, `ccpp` y `ccdi` si están disponibles.
- Estandarizar departamento, provincia y distrito.
- Seleccionar variables contextuales relevantes.
- Separar variables útiles de campos auxiliares o de cuestionario.
- Evaluar cobertura de cruce con MEF y meta predial.

## Relación con el diccionario de datos

El diccionario de datos mantiene el contrato técnico de datasets y columnas relevantes por capa.

El profiling y la calidad aportan evidencia para actualizarlo posteriormente:

- Nombres reales de columnas.
- Columnas que requieren tipado.
- Columnas candidatas para llaves.
- Campos territoriales.
- Variables de contexto RENAMU.
- Riesgos de integración.
- Campos que podrían descartarse o conservarse en Silver.

En esta etapa no se debe convertir el diccionario en un reporte exhaustivo de profiling. Los hallazgos extensos deben mantenerse en documentos de profiling, calidad o análisis de integración.

## Relación con Silver

Los hallazgos actuales orientan el siguiente bloque de trabajo: limpieza y estandarización en Silver.

Silver deberá resolver:

- Tipado semántico de años, meses, montos y porcentajes.
- Normalización de ubigeos y nombres geográficos.
- Identificación de llaves candidatas.
- Reglas de nulos críticos.
- Reglas de duplicados por entidad, periodo o clasificación.
- Integración entre MEF, predial y RENAMU.
- Selección de variables útiles para análisis.

## Limitaciones actuales

El profiling y la calidad Bronze presentan limitaciones esperadas:

- Bronze conserva datos con limpieza mínima.
- No todos los recursos tienen las mismas columnas.
- Algunas reglas progresivas no son evaluables hasta Silver.
- Todavía no se validan reglas de negocio completas.
- No se evalúa integridad entre fuentes.
- No se define todavía el modelo Gold.
- No se han documentado todavía llaves definitivas de integración.

Estas limitaciones no bloquean el avance. Sirven para justificar el trabajo de Silver.

## Estado actual

Estado: profiling implementado y calidad Bronze ejecutada localmente.

Resultados actuales:

- Landing ya cuenta con fuentes descargadas localmente.
- Bronze ya cuenta con Parquet para MEF, meta predial y RENAMU.
- Quality checks Bronze evaluaron 25 de 25 recursos.
- No se detectaron `FAIL`.
- Los `WARNING` corresponden principalmente a reglas progresivas no evaluables por ausencia de columnas candidatas.
- La información obtenida sustenta el inicio de la etapa Silver.

El proyecto queda preparado para continuar con limpieza, tipado semántico e integración de fuentes municipales.
