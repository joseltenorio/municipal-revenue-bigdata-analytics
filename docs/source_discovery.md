# Discovery de fuentes

## Propósito del documento

Este documento registra los hallazgos de discovery para las fuentes públicas del proyecto **Municipal Revenue Big Data Analytics**.

El objetivo de esta etapa es confirmar cómo se accede realmente a cada fuente antes de consolidar la ingesta hacia Landing, construir Bronze Parquet y definir reglas de calidad, limpieza e integración.

Esta documentación no reemplaza el profiling. El discovery se enfoca en acceso, disponibilidad, formato, estabilidad, tamaño aproximado, tipo de contenido y riesgos de descarga. El profiling se enfocará posteriormente en columnas, tipos de datos, nulos, duplicados, llaves candidatas y distribución de valores.

## Estado actual del documento

El discovery permitió identificar recursos directos para las tres fuentes principales del proyecto:

- MEF de presupuesto y ejecución de ingresos.
- MEF / SISMERE de seguimiento de meta del impuesto predial.
- INEI RENAMU 2022.

En el estado actual, las tres fuentes cuentan con procesos de ingesta controlada hacia Landing usando recursos centralizados en `config/sources.yaml`.

Los scripts de ingesta incorporan validación de disponibilidad, descarga por streaming, generación de metadata local, cálculo de checksum, auditoría básica, reintentos HTTP y fallback de validación cuando corresponde.

La ingesta conserva los archivos originales en Landing, no transforma datos de negocio, no genera Bronze y no debe versionar archivos reales descargados.

## Alcance del discovery

El discovery inicial buscó responder:

- Qué fuente pública se usará.
- Qué institución publica la fuente.
- Qué método de acceso es viable.
- Qué formatos están disponibles.
- Qué recursos directos se pueden probar.
- Qué riesgos existen para la ingesta.
- Si se requiere CSV, ZIP, XLSX, PDF, API o descarga manual controlada.
- Qué aspectos deben confirmarse antes de construir Landing y Bronze.

## Scripts usados durante discovery e ingesta inicial

Durante la etapa inicial se agregaron scripts de validación y descarga para las fuentes principales:

| Script                                   | Fuente                                  | Estado actual                                             |
| ---------------------------------------- | --------------------------------------- | --------------------------------------------------------- |
| `src/ingestion/download_mef_income.py`   | Presupuesto y ejecución de ingresos MEF | Ingesta controlada hacia Landing disponible               |
| `src/ingestion/download_predial_goal.py` | Meta del impuesto predial               | Ingesta controlada hacia Landing disponible               |
| `src/ingestion/download_renamu.py`       | RENAMU 2022                             | Descarga y extracción controlada hacia Landing disponible |

Estos scripts permiten validar conectividad, estado HTTP, tipo de contenido y tamaño declarado de recursos candidatos.

Además, incorporan una capa común de auditoría local, reintentos HTTP y fallback de validación. La auditoría se registra en formato JSON Lines dentro de:

`data/quality/ingestion_audit.jsonl`

Esta auditoría permite conservar evidencia local de inicio, fin y resultado de recursos procesados, sin versionar archivos descargados ni archivos de auditoría generados.

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

Estos comandos pertenecen a la etapa de discovery inicial. En el estado actual, los scripts de MEF ingresos y meta predial ya no se usan únicamente como probadores de URLs, sino como procesos de descarga controlada hacia Landing.

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

El acceso directo a archivos CSV es viable. La descarga se realiza mediante streaming usando las URLs configuradas en `config/sources.yaml`.

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
- La descarga completa debe ejecutarse localmente, de forma controlada y fuera de Git.

### Decisión derivada del discovery

Usar los recursos CSV como fuente principal de MEF ingresos.

La descarga se implementa de forma controlada. Se puede descargar un recurso, un año, una granularidad o todos los recursos configurados de forma explícita, pero la descarga masiva no debe ocurrir por accidente.

## Ingesta inicial MEF hacia Landing

La fuente MEF de presupuesto y ejecución de ingresos cuenta con ingesta controlada hacia Landing.

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
- Incluir recursos documentales, como el diccionario de datos.
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

La fuente debe manejarse como un conjunto de tablas relacionadas que se preservan inicialmente en Landing y Bronze. La integración analítica se definirá después del profiling y durante la construcción de Silver.

### Método de acceso observado

Método observado: `csv`.

El acceso directo a archivos CSV es viable para las tablas principales y la mayoría de diccionarios.

### Recursos identificados

| Recurso                                       | Tipo            | Estado observado | Uso                                         |
| --------------------------------------------- | --------------- | ---------------: | ------------------------------------------- |
| `rentas_preguntas.csv`                        | CSV temático    |              200 | Tabla candidata o referencia estructural    |
| `rentas_estadistica.csv`                      | CSV temático    |              200 | Tabla candidata para análisis predial       |
| `rentas_formulario.csv`                       | CSV temático    |              200 | Tabla candidata o referencia estructural    |
| `rentas_esat_estadistica_atm.csv`             | CSV temático    |              200 | Tabla candidata para análisis o contexto    |
| `rentas_respuestas.csv`                       | CSV temático    |              200 | Tabla candidata para análisis o integración |
| `rentas_ano_aplicacion.csv`                   | CSV temático    |              200 | Referencia temporal o estructural           |
| `rentas_entidad_estado.csv`                   | CSV temático    |              200 | Posible tabla de estado por entidad         |
| `rentas_ano_aplicacion_diccionario.csv`       | Diccionario CSV |              200 | Referencia documental                       |
| `rentas_preguntas_diccionario.csv`            | Diccionario CSV |              200 | Referencia documental                       |
| `rentas_estadistica_diccionario.csv`          | Diccionario CSV |              200 | Referencia documental                       |
| `rentas_entidad_estado_diccionario.csv`       | Diccionario CSV |              404 | Recurso observado no habilitado             |
| `rentas_formulario_diccionario.csv`           | Diccionario CSV |      Por validar | Referencia documental                       |
| `rentas_esat_estadistica_atm_diccionario.csv` | Diccionario CSV |      Por validar | Referencia documental                       |
| `rentas_respuestas_diccionario.csv`           | Diccionario CSV |      Por validar | Referencia documental                       |

Durante la validación de ingesta predial, el recurso candidato `rentas_entidad_estado_diccionario.csv` respondió con HTTP 404 en la URL directa evaluada. Por ese motivo debe mantenerse registrado como recurso observado, pero no habilitado para descarga automática hasta confirmar una URL válida.

### Riesgos identificados

- La fuente está distribuida en varias tablas.
- No se debe asumir una única tabla final.
- Puede requerir integración entre tablas de preguntas, respuestas, formularios, estadísticas y estados.
- Los diccionarios son necesarios para interpretar algunas estructuras.
- Algunos diccionarios pueden no estar disponibles en la URL directa inicialmente identificada.
- Puede faltar una llave geográfica limpia.
- Puede requerir normalización fuerte de municipalidades.
- Las reglas de Silver dependerán del profiling real.

### Decisión derivada del discovery

Bronze deberá preservar las tablas fuente candidatas sin integrarlas prematuramente. Silver deberá seleccionar las estructuras útiles para KPIs de avance, cumplimiento y brecha predial.

Los recursos que no respondan correctamente durante validación se conservan como observados, pero no se habilitan para descarga automática hasta confirmar una URL válida.

### Ingesta inicial predial hacia Landing

La fuente de seguimiento de meta del impuesto predial cuenta con ingesta controlada hacia Landing.

Script principal:

`src/ingestion/download_predial_goal.py`

Configuración usada:

`config/sources.yaml`

Destino local:

`data/landing/predial_goal/`

Capacidades implementadas:

- Listar recursos prediales configurados.
- Descargar un recurso específico.
- Descargar recursos principales de ingesta.
- Incluir recursos documentales habilitados.
- Ejecutar `--dry-run` para validar disponibilidad sin descargar.
- Descargar por streaming.
- Generar metadata básica por archivo.
- Calcular checksum SHA256.
- Evitar transformación de datos en Landing.

Ejemplos:

```powershell
python -m src.ingestion.download_predial_goal --list-resources
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
python -m src.ingestion.download_predial_goal --all-enabled --dry-run
python -m src.ingestion.download_predial_goal --all-enabled
```

Criterios aplicados:

- Landing conserva archivos originales.
- No se limpian columnas.
- No se renombran columnas.
- No se convierten tipos.
- No se genera Parquet.
- No se integran tablas prediales.
- No se interpreta semántica de negocio.
- Los archivos descargados no deben subirse a Git.
- La auditoría completa con reintentos se incorporará posteriormente.

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

### Recursos representativos validados

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

RENAMU se usará como fuente contextual. La ingesta deberá priorizar el ZIP completo y conservar el diccionario PDF como referencia documental.

### Ingesta inicial RENAMU hacia Landing

La fuente RENAMU 2022 cuenta con descarga y extracción controlada hacia Landing.

Script principal:

`src/ingestion/download_renamu.py`

Configuración usada:

`config/sources.yaml`

Destino local:

`data/landing/renamu/`

Directorio de extracción:

`data/landing/renamu/extracted/`

Capacidades implementadas:

- Listar recursos RENAMU configurados.
- Descargar el ZIP completo.
- Descargar el diccionario PDF como documentación de referencia.
- Mantener la muestra XLSX como recurso observado no habilitado.
- Ejecutar `--dry-run` para validar disponibilidad sin descargar.
- Descargar por streaming.
- Generar metadata básica por archivo descargado.
- Calcular checksum SHA256.
- Extraer el ZIP completo de forma controlada.
- Generar metadata de extracción.
- Evitar transformación de datos en Landing.

Ejemplos:

```powershell id="bpjlrt"
python -m src.ingestion.download_renamu --list-resources
python -m src.ingestion.download_renamu --resource full_zip --dry-run
python -m src.ingestion.download_renamu --all-enabled --dry-run
python -m src.ingestion.download_renamu --all-enabled --extract
```

Criterios aplicados:

- Landing conserva archivos originales.
- La extracción se realiza dentro de `data/landing/renamu/extracted/`.
- No se limpian columnas.
- No se renombran columnas.
- No se convierten tipos.
- No se genera Parquet.
- No se seleccionan variables analíticas.
- No se interpreta semántica de negocio.
- Los archivos descargados y extraídos no deben subirse a Git.
- La auditoría completa con reintentos se incorporará posteriormente.

## Implicancias para la ingesta

| Fuente       | Resultado observado                        | Implicancia para la ingesta                                                   |
| ------------ | ------------------------------------------ | ----------------------------------------------------------------------------- |
| MEF ingresos | CSV directos 2012-2026 y diccionario CSV   | Ingesta controlada disponible; no transformar en Landing                      |
| Meta predial | Múltiples CSV temáticos y diccionarios     | Ingesta controlada disponible; preservar como conjunto de tablas relacionadas |
| RENAMU 2022  | ZIP completo y diccionario PDF disponibles | Implementar descarga y extracción controlada                                  |

## Estado actualizado por fuente

| Fuente       | Estado actualizado                           | Próxima acción                                                    |
| ------------ | -------------------------------------------- | ----------------------------------------------------------------- |
| MEF ingresos | Ingesta controlada hacia Landing disponible  | Descargar recursos necesarios localmente y luego construir Bronze |
| Meta predial | Ingesta controlada hacia Landing disponible  | Validar recursos habilitados y luego construir Bronze             |
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
- La mayoría de recursos prediales principales responde correctamente desde URLs directas.
- El recurso candidato `rentas_entidad_estado_diccionario.csv` respondió con HTTP 404 y queda no habilitado hasta confirmar una URL válida.
- RENAMU 2022 dispone de ZIP completo y diccionario PDF accesibles desde recursos directos.
- Algunas páginas o muestras pueden presentar comportamiento especial frente a solicitudes automatizadas.
- Los archivos principales de MEF pueden ser grandes y requieren una estrategia de descarga cuidadosa.
- Las ingestas MEF, predial y RENAMU ya están disponibles hacia Landing de forma controlada.
- Los procesos de ingesta incorporan auditoría básica, reintentos HTTP y fallback de validación.
- No se debe versionar ningún archivo real descargado ni archivos locales de auditoría generados.

La siguiente etapa operativa será ejecutar una descarga local completa y controlada de las fuentes necesarias, revisar la auditoría generada y luego perfilar los archivos descargados antes de construir Bronze Parquet.

## Fuente manual fuera de discovery web: categorías municipales

`CategoriasMunicipalidades.csv` no se evalúa mediante discovery HTTP porque no proviene de un portal público ni de una URL descargable. Se registra como fuente manual controlada en `config/sources.yaml`, se conserva en `data/landing/category/` y se valida operativamente durante la construcción Bronze.
