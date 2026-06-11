# Discovery de fuentes

## Propósito del documento

Este documento registra los hallazgos de discovery para las fuentes públicas del proyecto **Municipal Revenue Big Data Analytics**.

El objetivo de esta etapa es confirmar cómo se accede realmente a cada fuente antes de implementar la ingesta definitiva hacia Landing.

Esta documentación no reemplaza el profiling. El discovery se enfoca en acceso, disponibilidad, formato, estabilidad, tamaño aproximado, tipo de contenido y riesgos de descarga. El profiling se enfocará luego en columnas, tipos, nulos, duplicados, llaves candidatas y distribución de valores.

## Estado actual del documento

Actualmente, el discovery ya permitió identificar recursos directos para las fuentes principales.

Además, la fuente MEF de presupuesto y ejecución de ingresos ya cuenta con una descarga controlada hacia Landing usando recursos definidos en `config/sources.yaml`.

La ingesta predial y RENAMU todavía se mantienen pendientes.

## Alcance del discovery

El discovery inicial buscó responder:

- Qué fuente pública se usará.
- Qué institución la publica.
- Qué método de acceso es viable.
- Qué formatos están disponibles.
- Qué recursos directos se pueden probar.
- Qué riesgos existen para la ingesta.
- Si se requiere CSV, ZIP, XLSX, PDF, API o descarga manual controlada.
- Qué aspectos deben confirmarse antes de construir Landing y Bronze.

## Scripts usados durante discovery

Durante la etapa de discovery se agregaron los siguientes scripts de prueba:

| Script                                   | Fuente                                  | Propósito inicial                                                    |
| ---------------------------------------- | --------------------------------------- | -------------------------------------------------------------------- |
| `src/ingestion/download_mef_income.py`   | Presupuesto y ejecución de ingresos MEF | Validar acceso inicial a recursos candidatos del portal MEF          |
| `src/ingestion/download_predial_goal.py` | Meta del impuesto predial               | Validar acceso inicial a recursos candidatos asociados a MEF/SISMERE |
| `src/ingestion/download_renamu.py`       | RENAMU 2022                             | Validar acceso inicial a recursos candidatos del INEI                |

Estos scripts permitieron probar conectividad, estado HTTP, tipo de contenido y tamaño declarado de recursos candidatos.

Posteriormente, `src/ingestion/download_mef_income.py` evolucionó de script de prueba hacia ingesta controlada para la fuente MEF. Los scripts de predial y RENAMU aún se mantienen como base para sus ingestas posteriores.

## Criterio técnico del discovery

Las pruebas de discovery se guiaron por los siguientes criterios:

- Ejecutar solicitudes `HEAD` cuando fuera posible.
- Usar `GET` como fallback si el servidor no permitía `HEAD`.
- Registrar estado HTTP.
- Registrar tipo de contenido.
- Registrar tamaño declarado por el servidor, si existía.
- Registrar URL final después de redirecciones.
- Registrar errores de conexión o timeout.
- No transformar datos.
- No generar Bronze.
- No generar Silver.
- No generar Gold.
- No versionar archivos descargados.

## Comandos de prueba usados durante discovery

Ejemplos de ejecución local durante la etapa de discovery:

```powershell
python -m src.ingestion.download_mef_income
python -m src.ingestion.download_predial_goal
python -m src.ingestion.download_renamu
```

Ejemplos de evaluación de URLs específicas durante discovery:

```powershell
python -m src.ingestion.download_mef_income --url "https://fs.datosabiertos.mef.gob.pe/datastorefiles/2012-Ingreso.csv"
python -m src.ingestion.download_predial_goal --url "https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_estadistica.csv"
python -m src.ingestion.download_renamu --url "https://www.inei.gob.pe/media/DATOS_ABIERTOS/RENAMU/DATA/2022.zip"
```

Estos comandos pertenecen a la etapa de discovery. En el estado actual, el script MEF ya no se usa solamente como probador de URLs, sino como ingestor controlado hacia Landing.

## Fuente MEF: Presupuesto y ejecución de ingresos

### Institución

Ministerio de Economía y Finanzas del Perú.

### Uso esperado

Esta fuente será usada para analizar presupuesto, ejecución de ingresos, avance presupuestal y diferencias territoriales entre municipalidades.

### Página del dataset

`https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-ingreso`

### Hallazgo de discovery

La página del dataset contiene recursos CSV para el periodo 2012-2026 y un diccionario de datos.

El patrón observado es:

| Grupo de recursos                   |   Periodo | Granularidad |
| ----------------------------------- | --------: | ------------ |
| Archivos `YYYY-Ingreso.csv`         | 2012-2024 | Anual        |
| Archivos `YYYY-Ingreso-Mensual.csv` | 2025-2026 | Mensual      |
| Archivos `YYYY-Ingreso-Diario.csv`  | 2025-2026 | Diaria       |
| `Ingresos_Diccionario.csv`          | No aplica | Diccionario  |

### Método de acceso observado

Método observado: `csv`.

El acceso directo a archivos CSV es viable. Actualmente, la descarga se implementa mediante streaming usando las URLs configuradas en `config/sources.yaml`.

### Recursos representativos validados

| Recurso                    | Tipo            | Estado observado | Uso                                                           |
| -------------------------- | --------------- | ---------------: | ------------------------------------------------------------- |
| `2012-Ingreso.csv`         | CSV anual       |              200 | Validar acceso a un archivo anual histórico                   |
| `2026-Ingreso-Diario.csv`  | CSV diario      |              200 | Validar existencia de recurso vigente con granularidad diaria |
| `Ingresos_Diccionario.csv` | Diccionario CSV |              200 | Validar acceso al diccionario de datos                        |

Estos recursos no son los únicos disponibles. El inventario completo de recursos MEF se mantiene en `config/sources.yaml`.

### Riesgos identificados

- Los archivos principales pueden ser grandes.
- La fuente combina granularidad anual, mensual y diaria.
- No se debe mezclar granularidad sin una decisión analítica explícita.
- Puede existir variación de estructura entre años o granularidades.
- Es necesario confirmar columnas reales con profiling.
- Es necesario confirmar si existe ubigeo, código de entidad o ambos.
- No se debe cargar todo con pandas si el archivo supera tamaños razonables.
- Spark será preferible para Bronze y capas posteriores.

### Decisión derivada del discovery

Usar los recursos CSV como fuente principal de MEF ingresos.

La descarga se implementa de forma controlada. El usuario puede descargar un recurso, un año, una granularidad o todos los recursos, pero la descarga masiva no debe ocurrir por accidente.

## Actualización por ingesta inicial MEF

Actualmente, el script MEF permite ingesta hacia Landing.

Script principal:

`src/ingestion/download_mef_income.py`

Configuración usada:

`config/sources.yaml`

Destino local:

`data/landing/mef_income/`

Capacidades implementadas:

- Listar recursos MEF configurados.
- Descargar un recurso específico.
- Descargar por año.
- Descargar por granularidad.
- Descargar todos los recursos configurados de forma explícita.
- Incluir recursos documentales, como diccionario de datos.
- Ejecutar `--dry-run` para validar disponibilidad sin descargar.
- Descargar por streaming.
- Generar metadata básica por archivo.
- Calcular checksum SHA256.
- Evitar transformación de datos en Landing.

Ejemplos:

```powershell
python -m src.ingestion.download_mef_income --list-resources
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
python -m src.ingestion.download_mef_income --resource dictionary
python -m src.ingestion.download_mef_income --year 2024 --dry-run
python -m src.ingestion.download_mef_income --granularity annual --dry-run
python -m src.ingestion.download_mef_income --all-resources --include-documentation
```

Criterios aplicados:

- Landing conserva archivos originales.
- No se limpian columnas.
- No se renombran columnas.
- No se convierten tipos.
- No se genera Parquet.
- No se interpreta semántica de negocio.
- Los archivos descargados no deben subirse a Git.
- La auditoría completa con reintentos se incorporará posteriormente.

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

| Recurso                             | Tipo            | Estado observado | Uso                                      |
| ----------------------------------- | --------------- | ---------------: | ---------------------------------------- |
| `rentas_estadistica.csv`            | CSV temático    |              200 | Tabla candidata para análisis            |
| `rentas_preguntas.csv`              | CSV temático    |              200 | Tabla candidata o referencia estructural |
| `rentas_formulario.csv`             | CSV temático    |              200 | Tabla candidata o referencia estructural |
| `rentas_respuestas_diccionario.csv` | Diccionario CSV |              200 | Referencia documental                    |

### Método de acceso observado

Método observado: `csv`.

El acceso directo a archivos CSV es viable.

### Riesgos identificados

- La fuente está distribuida en varias tablas.
- No se debe asumir una única tabla final.
- Puede requerir integración entre tablas de preguntas, respuestas, formularios, estadísticas y estados.
- Los diccionarios son necesarios para interpretar algunas estructuras.
- Puede faltar una llave geográfica limpia.
- Puede requerir normalización fuerte de municipalidades.
- Las reglas de Silver dependerán del profiling real.

### Decisión derivada del discovery

Bronze deberá preservar las tablas fuente candidatas sin integrarlas prematuramente. Silver deberá seleccionar las estructuras útiles para KPIs de avance, cumplimiento y brecha predial.

### Estado de ingesta

La ingesta predial todavía está pendiente.

Posteriormente se implementará una descarga controlada hacia:

`data/landing/predial_goal/`

## Fuente RENAMU 2022

### Institución

Instituto Nacional de Estadística e Informática.

### Uso esperado

RENAMU será usada como fuente contextual para enriquecer el análisis municipal y territorial.

### Página del dataset

`https://www.datosabiertos.gob.pe/dataset/registro-nacional-de-municipalidades-renamu-2022-instituto-nacional-de-estadistica-e`

### Hallazgo de discovery

La página del dataset contiene diccionario PDF, muestra XLSX y data completa en ZIP.

Durante pruebas automatizadas, algunas páginas del catálogo pueden presentar timeout o comportamiento especial frente a solicitudes automáticas. Sin embargo, los recursos directos del INEI respondieron correctamente para el ZIP completo y el diccionario PDF.

Recursos representativos validados:

| Recurso                  | Tipo            | Estado observado | Uso                               |
| ------------------------ | --------------- | ---------------: | --------------------------------- |
| `2022.zip`               | ZIP completo    |              200 | Fuente principal para ingesta     |
| `Diccionario.pdf`        | Diccionario PDF |              200 | Referencia documental             |
| `BD_Muestra_2022_0.xlsx` | Muestra XLSX    |              418 | Recurso observado, no prioritario |

### Método de acceso observado

Método observado prioritario: `zip`.

El ZIP completo y el diccionario PDF son accesibles desde recursos directos. La muestra XLSX no se considera prioritaria porque respondió con HTTP 418.

### Riesgos identificados

- La estructura interna del ZIP debe confirmarse después de la descarga local.
- Puede estar distribuida en varios archivos o tablas.
- Puede requerir seleccionar variables útiles.
- Puede tener diccionario separado.
- Las variables pueden ser numerosas.
- No todas las variables serán relevantes para Gold.
- Se debe conservar `ubigeo` como texto para no perder ceros a la izquierda.
- La página del catálogo puede ser menos estable que los recursos directos.

### Decisión derivada del discovery

RENAMU se usará como fuente contextual. La ingesta definitiva deberá priorizar el ZIP completo y conservar el diccionario PDF como referencia documental.

### Estado de ingesta

La descarga y extracción RENAMU todavía está pendiente.

Posteriormente se implementará una descarga, preservación y extracción controlada hacia:

`data/landing/renamu/`

## Implicancias para la ingesta

| Fuente       | Resultado observado                        | Implicancia para la ingesta                                |
| ------------ | ------------------------------------------ | ---------------------------------------------------------- |
| MEF ingresos | CSV directos 2012-2026 y diccionario CSV   | Ingesta controlada implementada; no transformar en Landing |
| Meta predial | Múltiples CSV temáticos y diccionarios     | Tratar como conjunto de tablas relacionadas                |
| RENAMU 2022  | ZIP completo y diccionario PDF disponibles | Descargar y extraer en una etapa específica                |

## Estado actualizado por fuente

| Fuente       | Estado actualizado                           | Próxima acción                                                    |
| ------------ | -------------------------------------------- | ----------------------------------------------------------------- |
| MEF ingresos | Ingesta controlada hacia Landing disponible  | Descargar recursos necesarios localmente y luego construir Bronze |
| Meta predial | Recursos CSV temáticos identificados         | Implementar descarga controlada hacia Landing                     |
| RENAMU 2022  | ZIP completo y diccionario PDF identificados | Implementar descarga, preservación y extracción controlada        |

## Criterio de no versionamiento

No se deben subir al repositorio:

- CSV descargados.
- ZIP descargados.
- XLSX descargados.
- PDF descargados si se consideran datos o documentación pesada.
- Parquet generados.
- Logs pesados.
- Archivos temporales.
- Credenciales.
- `.env` real.
- Metadata local generada junto a archivos descargados, si queda dentro de carpetas ignoradas.

Los archivos descargados deben permanecer en `data/landing/` de forma local y fuera de Git.

## Conclusión general del discovery

Las pruebas realizadas confirman que:

- MEF ingresos dispone de recursos CSV directos para el periodo 2012-2026.
- MEF ingresos combina archivos anuales, mensuales y diarios.
- Meta predial dispone de múltiples CSV temáticos y diccionarios.
- RENAMU 2022 dispone de ZIP completo y diccionario PDF accesibles desde recursos directos.
- Algunas páginas o muestras pueden presentar comportamiento especial frente a solicitudes automatizadas.
- Los archivos principales de MEF pueden ser grandes y requieren una estrategia de descarga cuidadosa.
- La ingesta MEF ya está disponible hacia Landing de forma controlada.
- Las ingestas predial y RENAMU quedan pendientes.
- No se debe versionar ningún archivo real descargado.
