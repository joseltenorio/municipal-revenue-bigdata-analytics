# Discovery de fuentes

## Propósito del documento

Este documento registra los hallazgos de discovery para las fuentes públicas del proyecto **Municipal Revenue Big Data Analytics**.

El objetivo de esta etapa es confirmar cómo se accede realmente a cada fuente antes de implementar la ingesta definitiva hacia Landing.

Esta documentación no reemplaza el profiling. El discovery se enfoca en acceso, disponibilidad, formato, estabilidad, tamaño aproximado, tipo de contenido y riesgos de descarga. El profiling se enfocará luego en columnas, tipos, nulos, duplicados, llaves candidatas y distribución de valores.

## Alcance del discovery

El discovery inicial busca responder:

- Qué fuente pública se usará.
- Qué institución la publica.
- Qué método de acceso es viable.
- Qué formatos están disponibles.
- Qué recursos directos se pueden probar.
- Qué riesgos existen para la ingesta.
- Si se requiere CSV, ZIP, XLSX, PDF, API o descarga manual controlada.
- Qué aspectos deben confirmarse antes de construir Landing y Bronze.

## Scripts de discovery

Se agregan los siguientes scripts:

| Script                                   | Fuente                                  | Propósito                                                            |
| ---------------------------------------- | --------------------------------------- | -------------------------------------------------------------------- |
| `src/ingestion/download_mef_income.py`   | Presupuesto y ejecución de ingresos MEF | Validar acceso inicial a recursos candidatos del portal MEF          |
| `src/ingestion/download_predial_goal.py` | Meta del impuesto predial               | Validar acceso inicial a recursos candidatos asociados a MEF/SISMERE |
| `src/ingestion/download_renamu.py`       | RENAMU 2022                             | Validar acceso inicial a recursos candidatos del INEI                |

Estos scripts todavía no implementan la descarga final. Su función es ejecutar pruebas livianas de conectividad y metadatos técnicos.

## Criterio técnico de los scripts

Los scripts de discovery:

- Ejecutan solicitudes `HEAD` cuando es posible.
- Usan `GET` como fallback si el servidor no permite `HEAD`.
- Registran estado HTTP.
- Registran tipo de contenido.
- Registran tamaño declarado por el servidor, si existe.
- Registran URL final después de redirecciones.
- Registran errores de conexión o timeout.
- No descargan archivos completos.
- No escriben datos reales en el repositorio.
- No generan archivos Parquet, CSV, ZIP ni XLSX.

## Comandos de prueba

Ejemplos de ejecución local:

```powershell
python -m src.ingestion.download_mef_income
python -m src.ingestion.download_predial_goal
python -m src.ingestion.download_renamu
```

También se puede evaluar una URL específica:

```powershell
python -m src.ingestion.download_mef_income --url "https://fs.datosabiertos.mef.gob.pe/datastorefiles/2012-Ingreso.csv"
python -m src.ingestion.download_predial_goal --url "https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_estadistica.csv"
python -m src.ingestion.download_renamu --url "https://www.inei.gob.pe/media/DATOS_ABIERTOS/RENAMU/DATA/2022.zip"
```

## Criterio de no versionamiento

Durante discovery no se deben subir al repositorio:

- CSV descargados.
- ZIP descargados.
- XLSX descargados.
- Parquet generados.
- Logs pesados.
- Archivos temporales.
- Credenciales.
- `.env` real.

Cualquier muestra local debe permanecer fuera de Git.

## Fuente MEF: Presupuesto y ejecución de ingresos

### Institución

Ministerio de Economía y Finanzas del Perú.

### Uso esperado

Esta fuente será usada para analizar presupuesto, ejecución de ingresos, avance presupuestal y diferencias territoriales entre municipalidades.

### Página del dataset

`https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-ingreso`

### Hallazgo de discovery

La página del dataset contiene recursos CSV por año, recursos recientes con granularidad diaria o mensual y un diccionario de datos.

Recursos representativos validados:

| Recurso                    |            Tipo | Estado HTTP | Tipo de contenido | Tamaño declarado | URL                                                                           |
| -------------------------- | --------------: | ----------: | ----------------- | ---------------: | ----------------------------------------------------------------------------- |
| `2012-Ingreso.csv`         |       CSV anual |         200 | `text/csv`        |  367063258 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/2012-Ingreso.csv`         |
| `2026-Ingreso-Diario.csv`  |      CSV diario |         200 | `text/csv`        |  190675218 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/2026-Ingreso-Diario.csv`  |
| `Ingresos_Diccionario.csv` | Diccionario CSV |         200 | `text/csv`        |       3574 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/Ingresos_Diccionario.csv` |

### Método de acceso preliminar

Método observado: `csv`.

El acceso directo a archivos CSV es viable. La ingesta definitiva deberá definir el rango temporal y granularidad objetivo antes de descargar archivos completos.

### Riesgos

- Los archivos principales pueden ser grandes.
- Puede existir más de un recurso por año para periodos recientes.
- La fuente debe diferenciar ingresos de gasto.
- El portal puede cambiar nombres o disponibilidad de archivos.
- Es necesario confirmar columnas reales con profiling.
- Es necesario confirmar si existe ubigeo, código de entidad o ambos.
- No se debe cargar todo con pandas si el archivo supera tamaños razonables; Spark será preferible en etapas de procesamiento.

### Decisión provisional

Usar los recursos CSV como fuente principal de MEF ingresos. Para la primera ingesta se priorizarán archivos anuales o un rango temporal controlado. Los recursos diarios o mensuales recientes quedan registrados como candidatos, pero no como prioridad inicial.

## Fuente MEF / SISMERE: Meta del impuesto predial

### Institución

Ministerio de Economía y Finanzas del Perú.

### Uso esperado

Esta fuente será usada para analizar avance, cumplimiento y brechas de la meta del impuesto predial.

### Página del dataset

`https://datosabiertos.mef.gob.pe/dataset/seguimiento-de-la-meta-del-impuesto-predial`

### Hallazgo de discovery

La página contiene múltiples CSV temáticos y diccionarios. No se trata de una única tabla plana.

Recursos representativos validados:

| Recurso                             |            Tipo | Estado HTTP | Tipo de contenido | Tamaño declarado | URL                                                                                    |
| ----------------------------------- | --------------: | ----------: | ----------------- | ---------------: | -------------------------------------------------------------------------------------- |
| `rentas_estadistica.csv`            |    CSV temático |         200 | `text/csv`        |       9281 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_estadistica.csv`            |
| `rentas_preguntas.csv`              |    CSV temático |         200 | `text/csv`        |     201938 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_preguntas.csv`              |
| `rentas_formulario.csv`             |    CSV temático |         200 | `text/csv`        |      11933 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_formulario.csv`             |
| `rentas_respuestas_diccionario.csv` | Diccionario CSV |         200 | `text/csv`        |        628 bytes | `https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_respuestas_diccionario.csv` |

### Método de acceso preliminar

Método observado: `csv`.

El acceso directo a archivos CSV es viable.

### Riesgos

- La fuente está distribuida en varias tablas.
- No se debe asumir una única tabla final.
- Puede requerir integración entre tablas de preguntas, respuestas, formularios, estadísticas y estados.
- Los diccionarios son necesarios para interpretar algunas estructuras.
- Puede faltar una llave geográfica limpia.
- Puede requerir normalización fuerte de municipalidades.
- Las reglas de Silver dependerán del profiling real.

### Decisión provisional

Bronze deberá preservar las tablas fuente candidatas sin integrarlas prematuramente. Silver deberá seleccionar las estructuras útiles para KPIs de avance, cumplimiento y brecha predial.

## Fuente RENAMU 2022

### Institución

Instituto Nacional de Estadística e Informática.

### Uso esperado

RENAMU será usada como fuente contextual para enriquecer el análisis municipal y territorial.

### Página del dataset

`https://www.datosabiertos.gob.pe/dataset/registro-nacional-de-municipalidades-renamu-2022-instituto-nacional-de-estadistica-e`

### Hallazgo de discovery

La página del dataset contiene diccionario PDF, muestra XLSX y data completa en ZIP.

Durante las pruebas automatizadas, la URL de catálogo puede presentar timeout. Sin embargo, los recursos directos del INEI respondieron correctamente para el ZIP completo y el diccionario PDF.

Recursos representativos validados:

| Recurso                  |            Tipo | Estado HTTP | Tipo de contenido          | Tamaño declarado | URL                                                                                    |
| ------------------------ | --------------: | ----------: | -------------------------- | ---------------: | -------------------------------------------------------------------------------------- |
| `2022.zip`               |    ZIP completo |         200 | `application/zip`          |    1919681 bytes | `https://www.inei.gob.pe/media/DATOS_ABIERTOS/RENAMU/DATA/2022.zip`                    |
| `Diccionario.pdf`        | Diccionario PDF |         200 | `application/pdf`          |     718281 bytes | `https://www.inei.gob.pe/media/DATOS_ABIERTOS/RENAMU/DICCIONARIO/2022/Diccionario.pdf` |
| `BD_Muestra_2022_0.xlsx` |    Muestra XLSX |         418 | `text/html; charset=utf-8` |    No disponible | `https://www.datosabiertos.gob.pe/sites/default/files/BD_Muestra_2022_0.xlsx`          |

### Método de acceso preliminar

Método observado prioritario: `zip`.

El ZIP completo y el diccionario PDF son accesibles desde Python. La muestra XLSX no se considera prioritaria porque respondió con HTTP 418.

### Riesgos

- La estructura interna del ZIP debe confirmarse después de la descarga local.
- Puede estar distribuida en varios archivos o tablas.
- Puede requerir seleccionar variables útiles.
- Puede tener diccionario separado.
- Las variables pueden ser numerosas.
- No todas las variables serán relevantes para Gold.
- Se debe conservar `ubigeo` como texto para no perder ceros a la izquierda.
- La página del catálogo puede ser menos estable que los recursos directos.

### Decisión provisional

RENAMU se usará como fuente contextual. La ingesta definitiva deberá priorizar el ZIP completo y conservar el diccionario PDF como referencia documental. La muestra XLSX queda registrada como recurso observado, pero no será usada como fuente principal.

## Implicancias para la ingesta

| Fuente       | Resultado observado                                                            | Implicancia para la ingesta                                                                            |
| ------------ | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| MEF ingresos | CSV directos por año, recursos recientes diarios o mensuales y diccionario CSV | Definir rango temporal y granularidad objetivo antes de descargar. Considerar tamaño alto de archivos. |
| Meta predial | Múltiples CSV temáticos y diccionarios                                         | Tratar como conjunto de tablas relacionadas. No forzar una única tabla plana desde Bronze.             |
| RENAMU 2022  | ZIP completo y diccionario PDF disponibles; muestra XLSX con HTTP 418          | Priorizar ZIP completo y diccionario PDF. No depender de la muestra XLSX.                              |

## Estado actualizado después de prueba local

| Fuente       | Estado actualizado                                       | Próxima acción                                                               |
| ------------ | -------------------------------------------------------- | ---------------------------------------------------------------------------- |
| MEF ingresos | Recursos CSV directos identificados y validados          | Definir años y granularidad a descargar en ingesta definitiva.               |
| Meta predial | Recursos CSV temáticos identificados y validados         | Definir qué tablas se conservarán en Bronze y cuáles serán usadas en Silver. |
| RENAMU 2022  | ZIP completo y diccionario PDF identificados y validados | Implementar descarga controlada del ZIP completo en la fase de ingesta.      |

## Conclusión general del discovery local

Las pruebas actualizadas confirman que:

- MEF ingresos dispone de recursos CSV directos por año, además de recursos recientes diarios o mensuales.
- La fuente de meta predial dispone de múltiples CSV temáticos y diccionarios.
- RENAMU 2022 dispone de ZIP completo y diccionario PDF accesibles desde Python.
- La muestra XLSX de RENAMU respondió con HTTP 418, por lo que no será prioritaria.
- Los archivos principales de MEF ingresos pueden ser grandes y requieren una estrategia de ingesta cuidadosa.
- No se descargaron ni versionaron datos reales durante esta etapa.
- El siguiente paso será implementar la ingesta hacia Landing usando estos recursos candidatos, con auditoría y reintentos en commits posteriores.
