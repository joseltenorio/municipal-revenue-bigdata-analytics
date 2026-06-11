# Fuentes de datos

## Propósito del documento

Este documento describe las fuentes públicas consideradas para el proyecto **Municipal Revenue Big Data Analytics**.

El objetivo es identificar el origen, uso analítico, método de acceso observado, formato esperado, recursos disponibles, riesgos y criterios de uso para los procesos de ingesta hacia Landing.

La información sobre columnas, tipos de datos, llaves, granularidad analítica y reglas de negocio aún debe confirmarse mediante profiling, construcción de Bronze, validaciones de calidad y transformaciones Silver.

## Estado actual del documento

Actualmente, el proyecto cuenta con:

- Inventario de fuentes principales.
- Recursos directos identificados para MEF, meta predial y RENAMU.
- Descarga controlada implementada para la fuente MEF de presupuesto y ejecución de ingresos.
- Descarga controlada implementada para la fuente de seguimiento de meta del impuesto predial.
- Descarga y extracción controlada implementada para RENAMU 2022.
- Validación de disponibilidad, descarga por streaming, metadata local, checksum, auditoría básica, reintentos HTTP y fallback de validación en los procesos de ingesta.
- Conversión a Bronze Parquet pendiente.
- Profiling real pendiente sobre los archivos descargados localmente.

Este documento no representa todavía el modelo final de datos. Su función es documentar las fuentes y criterios de uso antes de definir contratos definitivos de Bronze, Silver y Gold.

## Resumen de fuentes

| Fuente                                   | Institución   | Uso principal                                             | Método observado               | Estado actual                                             |
| ---------------------------------------- | ------------- | --------------------------------------------------------- | ------------------------------ | --------------------------------------------------------- |
| Presupuesto y ejecución de ingresos      | MEF / SIAF    | Análisis presupuestal y ejecución de ingresos municipales | CSV directo                    | Ingesta controlada hacia Landing disponible               |
| Seguimiento de meta del impuesto predial | MEF / SISMERE | Análisis de avance y cumplimiento de meta predial         | CSV directo                    | Ingesta controlada hacia Landing disponible               |
| RENAMU 2022                              | INEI          | Contexto territorial y municipal                          | ZIP completo y diccionario PDF | Descarga y extracción controlada hacia Landing disponible |

## Fuente 1: Presupuesto y ejecución de ingresos - MEF / SIAF

### Descripción

Fuente pública relacionada con información presupuestal y ejecución de ingresos. Permitirá analizar el comportamiento de los ingresos municipales, su ejecución, variaciones por periodo y posibles diferencias territoriales entre municipalidades.

Esta fuente es central para el proyecto porque alimentará los futuros análisis de presupuesto, recaudación, ejecución, ranking municipal y brechas territoriales.

### Institución responsable

Ministerio de Economía y Finanzas del Perú.

### Página del dataset

`https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-ingreso`

### Uso analítico esperado

Esta fuente será usada para responder preguntas como:

- ¿Qué municipalidades tienen mayor presupuesto o ejecución de ingresos?
- ¿Qué municipalidades presentan mayor o menor avance de ejecución?
- ¿Cómo varía la ejecución por departamento, provincia o distrito?
- ¿Qué diferencias existen entre presupuesto inicial, presupuesto modificado y ejecución?
- ¿Qué comportamiento se observa entre periodos anuales, mensuales o diarios, si la granularidad disponible lo permite?

### Campos esperados

Los campos exactos se confirmarán durante profiling y construcción de Bronze. De forma preliminar se esperan variables como:

- Año o periodo.
- Nivel de gobierno.
- Departamento.
- Provincia.
- Distrito.
- Municipalidad o entidad.
- Código de entidad.
- Rubro, clasificador o concepto de ingreso.
- Presupuesto institucional de apertura.
- Presupuesto institucional modificado.
- Ejecución o recaudación.
- Porcentaje de avance.

### Método de acceso observado

Método observado: CSV directo.

Durante la etapa de revisión de fuentes se identificaron archivos CSV directos publicados por el portal de datos abiertos del MEF. La configuración de estos recursos se centraliza en:

`config/sources.yaml`

La descarga controlada de esta fuente se implementa mediante:

`src/ingestion/download_mef_income.py`

El script consume los recursos configurados en `sources.yaml`, descarga por streaming y guarda los archivos originales en:

`data/landing/mef_income/`

### Recursos identificados

La fuente contiene recursos CSV para el periodo 2012-2026 y un diccionario de datos.

| Grupo de recursos                       |   Periodo | Granularidad | Uso esperado                                                       |
| --------------------------------------- | --------: | ------------ | ------------------------------------------------------------------ |
| `2012-Ingreso.csv` a `2024-Ingreso.csv` | 2012-2024 | Anual        | Base histórica principal para análisis presupuestal y de ejecución |
| `2025-Ingreso-Mensual.csv`              |      2025 | Mensual      | Recurso reciente con granularidad mensual                          |
| `2025-Ingreso-Diario.csv`               |      2025 | Diaria       | Recurso reciente con granularidad diaria                           |
| `2026-Ingreso-Mensual.csv`              |      2026 | Mensual      | Recurso vigente con granularidad mensual                           |
| `2026-Ingreso-Diario.csv`               |      2026 | Diaria       | Recurso vigente con granularidad diaria                            |
| `Ingresos_Diccionario.csv`              | No aplica | Diccionario  | Referencia documental para interpretar columnas de la fuente       |

### Criterio de ingesta implementado

La descarga MEF se implementa como ingesta controlada.

El script permite:

- Listar recursos configurados.
- Descargar un recurso específico.
- Descargar recursos por año.
- Descargar recursos por granularidad.
- Descargar todos los recursos configurados si se solicita explícitamente.
- Descargar el diccionario como recurso documental.
- Ejecutar validaciones sin descarga mediante `--dry-run`.
- Registrar metadata básica por archivo descargado.

La descarga no se ejecuta de forma masiva por defecto. Esta decisión evita descargas accidentales de archivos grandes y permite controlar qué periodo se usa para pruebas, profiling y construcción de Bronze.

### Ejemplos operativos

Listar recursos configurados:

```powershell
python -m src.ingestion.download_mef_income --list-resources
```

Validar el diccionario sin descargarlo:

```powershell
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
```

Descargar el diccionario:

```powershell
python -m src.ingestion.download_mef_income --resource dictionary
```

Validar un año específico:

```powershell
python -m src.ingestion.download_mef_income --year 2024 --dry-run
```

Descargar todos los recursos MEF configurados, incluyendo documentación:

```powershell
python -m src.ingestion.download_mef_income --all-resources --include-documentation
```

### Riesgos identificados

- Los archivos principales pueden ser grandes.
- La fuente combina recursos históricos anuales con recursos recientes mensuales y diarios.
- No se debe mezclar automáticamente granularidad anual, mensual y diaria sin una regla analítica definida.
- Puede haber cambios de estructura entre años o entre granularidades.
- Es necesario confirmar columnas reales mediante profiling.
- Es necesario confirmar si existe ubigeo, código de entidad o ambos.
- No se debe procesar con pandas si el archivo supera tamaños razonables; Spark será preferible para Bronze, Silver y Gold.
- Los archivos descargados no deben versionarse en Git.

### Criterio inicial de uso

Esta fuente será la base principal para el análisis de presupuesto y ejecución de ingresos municipales.

Landing conservará los archivos originales. Posteriormente, Bronze convertirá la fuente a Parquet. Silver limpiará, tipará y estandarizará columnas. Gold definirá los indicadores finales después del profiling e integración.

La granularidad analítica final se decidirá después de observar los datos reales. En principio, la serie anual 2012-2024 es candidata para análisis histórico, mientras que los recursos 2025-2026 mensuales o diarios pueden servir para análisis reciente si se decide incorporar esa granularidad.

## Fuente 2: Seguimiento de meta del impuesto predial - MEF / SISMERE

### Descripción

Fuente pública orientada al seguimiento de la meta vinculada al impuesto predial. Permitirá analizar avance, cumplimiento, brechas y desempeño de municipalidades respecto a la meta predial.

Esta fuente complementará el análisis de ingresos municipales al permitir comparar ejecución presupuestal con desempeño de gestión y cumplimiento de la meta predial.

### Institución responsable

Ministerio de Economía y Finanzas del Perú.

### Página del dataset

`https://datosabiertos.mef.gob.pe/dataset/seguimiento-de-la-meta-del-impuesto-predial`

### Uso analítico esperado

Esta fuente será usada para responder preguntas como:

- ¿Qué municipalidades cumplen la meta del impuesto predial?
- ¿Cuáles presentan mayor brecha de cumplimiento?
- ¿Cómo se distribuye el avance predial por territorio?
- ¿Existe relación entre ejecución de ingresos y cumplimiento predial?
- ¿Qué variables o tablas de la fuente explican mejor el avance de la meta predial?

### Campos esperados

Los campos exactos se confirmarán durante profiling y construcción de Bronze. De forma preliminar se esperan variables como:

- Año.
- Departamento.
- Provincia.
- Distrito.
- Municipalidad.
- Código de municipalidad o ubigeo.
- Meta programada.
- Avance logrado.
- Porcentaje de avance.
- Estado de cumplimiento.
- Preguntas, formularios, respuestas o variables temáticas asociadas al seguimiento.

### Método de acceso observado

Método observado: CSV directo.

Durante la revisión de fuentes se identificó que la fuente no corresponde a una única tabla plana, sino a un conjunto de CSV temáticos y diccionarios.

La configuración de estos recursos se centraliza en:

`config/sources.yaml`

La descarga controlada de esta fuente se implementa mediante:

`src/ingestion/download_predial_goal.py`

El script consume los recursos configurados en `sources.yaml`, descarga por streaming y guarda los archivos originales en:

`data/landing/predial_goal/`

### Recursos identificados

| Recurso                                       | Tipo            | Uso esperado                                      |
| --------------------------------------------- | --------------- | ------------------------------------------------- |
| `rentas_preguntas.csv`                        | CSV temático    | Tabla candidata o referencia estructural          |
| `rentas_estadistica.csv`                      | CSV temático    | Tabla candidata para análisis predial             |
| `rentas_formulario.csv`                       | CSV temático    | Tabla candidata o referencia estructural          |
| `rentas_esat_estadistica_atm.csv`             | CSV temático    | Tabla candidata para análisis o contexto          |
| `rentas_respuestas.csv`                       | CSV temático    | Tabla candidata para análisis o integración       |
| `rentas_ano_aplicacion.csv`                   | CSV temático    | Referencia temporal o estructural                 |
| `rentas_entidad_estado.csv`                   | CSV temático    | Posible tabla de estado por entidad               |
| `rentas_ano_aplicacion_diccionario.csv`       | Diccionario CSV | Referencia documental                             |
| `rentas_preguntas_diccionario.csv`            | Diccionario CSV | Referencia documental                             |
| `rentas_estadistica_diccionario.csv`          | Diccionario CSV | Referencia documental                             |
| `rentas_entidad_estado_diccionario.csv`       | Diccionario CSV | Recurso observado no habilitado por respuesta 404 |
| `rentas_formulario_diccionario.csv`           | Diccionario CSV | Referencia documental                             |
| `rentas_esat_estadistica_atm_diccionario.csv` | Diccionario CSV | Referencia documental                             |
| `rentas_respuestas_diccionario.csv`           | Diccionario CSV | Referencia documental                             |

### Criterio de ingesta implementado

La descarga predial se implementa como ingesta controlada.

El script permite:

- Listar recursos configurados.
- Descargar un recurso específico.
- Descargar recursos principales de ingesta.
- Incluir recursos documentales habilitados.
- Ejecutar validaciones sin descarga mediante `--dry-run`.
- Registrar metadata básica por archivo descargado.

La fuente predial debe preservarse inicialmente como conjunto de archivos relacionados. La integración de tablas y la selección de estructuras útiles se definirá después del profiling y durante Silver.

Los recursos que no respondan correctamente durante validación quedan registrados como observados, pero no se habilitan para descarga automática hasta confirmar una URL válida.

### Ejemplos operativos

Listar recursos configurados:

```powershell
python -m src.ingestion.download_predial_goal --list-resources
```

Validar un recurso principal:

```powershell
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
```

Validar recursos habilitados sin descargar:

```powershell
python -m src.ingestion.download_predial_goal --all-enabled --dry-run
```

Descargar recursos habilitados:

```powershell
python -m src.ingestion.download_predial_goal --all-enabled
```

### Riesgos identificados

- No es una única tabla plana.
- La fuente contiene varias tablas temáticas.
- Puede requerir integración entre tablas.
- Puede requerir interpretación mediante diccionarios.
- Algunos recursos documentales pueden no estar disponibles en la URL directa inicialmente identificada.
- Puede faltar una llave geográfica limpia.
- Puede requerir normalización fuerte de municipalidades.
- Las reglas de Bronze y Silver dependerán del profiling real.
- Los archivos descargados no deben versionarse en Git.

### Criterio inicial de uso

Esta fuente será la base principal para construir indicadores de cumplimiento y brecha predial.

Bronze deberá preservar las tablas fuente necesarias sin integrarlas prematuramente. Silver definirá qué tablas son útiles para análisis y cómo se relacionan entre sí.

## Fuente 3: RENAMU 2022 - INEI

### Descripción

El Registro Nacional de Municipalidades 2022 contiene información contextual de municipalidades peruanas. Será usado para enriquecer el análisis territorial y proporcionar variables descriptivas del contexto municipal.

### Institución responsable

Instituto Nacional de Estadística e Informática del Perú.

### Página del dataset

`https://www.datosabiertos.gob.pe/dataset/registro-nacional-de-municipalidades-renamu-2022-instituto-nacional-de-estadistica-e`

### Uso analítico esperado

Esta fuente será usada para responder preguntas como:

- ¿Qué características municipales pueden ayudar a interpretar diferencias de desempeño?
- ¿Cómo se distribuyen los resultados por departamento, provincia o distrito?
- ¿Qué variables contextuales pueden complementar el análisis presupuestal y predial?
- ¿Qué municipalidades tienen características similares y comportamientos distintos?

### Campos esperados

Los campos exactos se confirmarán durante profiling. De forma preliminar se esperan variables como:

- Ubigeo.
- Departamento.
- Provincia.
- Distrito.
- Municipalidad.
- Tipo de municipalidad.
- Variables administrativas.
- Variables de servicios municipales.
- Variables de infraestructura o gestión municipal.
- Variables territoriales relevantes.

### Método de acceso observado

Método observado prioritario: ZIP completo.

### Recursos representativos identificados

| Recurso                  | Tipo         | Uso esperado                                             |
| ------------------------ | ------------ | -------------------------------------------------------- |
| `2022.zip`               | ZIP completo | Fuente principal para ingesta RENAMU                     |
| `Diccionario.pdf`        | PDF          | Referencia documental                                    |
| `BD_Muestra_2022_0.xlsx` | XLSX         | Recurso observado, no prioritario por respuesta HTTP 418 |

### Estado de ingesta

La fuente RENAMU 2022 cuenta con descarga y extracción controlada hacia:

`data/landing/renamu/`

El ZIP completo se conserva como archivo original y su contenido se extrae dentro de:

`data/landing/renamu/extracted/`

La ingesta no transforma datos de negocio, no selecciona variables analíticas y no genera Bronze.

### Riesgos identificados

- La estructura interna del ZIP debe confirmarse después de la descarga local.
- Puede estar distribuida en varios archivos o tablas temáticas.
- El diccionario de datos está separado del archivo principal.
- Puede contener columnas extensas o poco estandarizadas.
- Los datos categóricos pueden requerir interpretación.
- Se deben seleccionar variables relevantes para no sobrecargar Gold.
- La página del catálogo puede ser menos estable que los recursos directos.
- Se debe conservar `ubigeo` como texto para no perder ceros a la izquierda.

### Criterio inicial de uso

RENAMU será usada como fuente contextual. No debe dominar el modelo analítico, sino enriquecer la interpretación territorial y municipal.

La ingesta deberá priorizar el ZIP completo y conservar el diccionario PDF como referencia documental.

## Criterios generales de ingesta

Las fuentes se trabajarán bajo los siguientes criterios:

- Landing conservará archivos originales.
- Bronze convertirá las fuentes a Parquet.
- Silver limpiará, tipará e integrará.
- Gold se diseñará después del profiling y Silver.
- Hive expondrá tablas externas sobre Parquet.
- Power BI consumirá preferentemente tablas Gold desde Hive.
- Si una fuente requiere descarga manual controlada, esa decisión deberá quedar documentada y auditada.
- Los diccionarios se registran como recursos de referencia, no como hechos analíticos principales.
- Las muestras se registran solo como recursos observados o de validación ligera, no como fuente principal si existe data completa.
- Los archivos descargados hacia Landing no deben subirse al repositorio.

## Riesgos generales

Los principales riesgos de las fuentes son:

- Cambios en los portales públicos.
- Descargas inestables.
- Archivos grandes.
- Columnas cambiantes.
- Diferencias de granularidad.
- Falta de llaves consistentes.
- Diferencias entre ubigeo, código de entidad y nombre municipal.
- Formatos numéricos heterogéneos.
- Variables contextuales RENAMU distribuidas en múltiples archivos.
- Recursos documentales con URLs no disponibles o modificadas.
- Acceso automatizado bloqueado, lento o condicionado por el portal.

## Decisiones pendientes

Después de la identificación de recursos y la implementación de ingesta controlada para MEF y predial, todavía quedan decisiones de implementación y modelado antes de construir Bronze, Silver y Gold.

Decisiones pendientes:

- Definir qué rango temporal MEF se usará para profiling completo.
- Definir si el análisis final trabajará solo granularidad anual o también granularidad mensual/diaria reciente.
- Confirmar la disponibilidad final de los diccionarios prediales que no respondan correctamente durante validación.
- Definir qué tablas de meta predial se conservarán completas en Bronze.
- Definir qué tablas de meta predial serán necesarias para Silver y Gold.
- Implementar descarga y extracción de RENAMU.
- Confirmar la estructura interna del ZIP completo de RENAMU 2022.
- Seleccionar variables RENAMU relevantes para contexto municipal.
- Confirmar columnas reales, tipos de datos y llaves candidatas mediante profiling.
- Evaluar cobertura de cruce entre MEF, meta predial y RENAMU.
- Definir modelo Gold final y modelo Power BI después de Silver.

## Actualización por identificación de recursos directos

Se identificaron recursos descargables directos para las fuentes principales del proyecto.

Hallazgos principales:

- MEF ingresos dispone de CSV directos para el periodo 2012-2026 y un diccionario CSV.
- Meta predial dispone de múltiples CSV temáticos y diccionarios.
- RENAMU 2022 dispone de ZIP completo y diccionario PDF accesibles desde recursos directos.
- Algunas páginas o muestras pueden presentar comportamientos especiales frente a solicitudes automáticas.
- Los archivos principales de MEF pueden ser grandes y requieren estrategia de descarga controlada.

## Actualización por ingesta MEF hacia Landing

Existe una descarga controlada para la fuente MEF de presupuesto y ejecución de ingresos.

Estado técnico actual:

- Fuente: MEF ingresos.
- Método implementado: CSV directo.
- Configuración de recursos: `config/sources.yaml`.
- Script de ingesta: `src/ingestion/download_mef_income.py`.
- Destino local: `data/landing/mef_income/`.
- Transformación de negocio: no aplica en Landing.
- Conversión a Parquet: pendiente para Bronze.
- Auditoría completa y reintentos: pendiente para una etapa posterior.

El script descarga archivos por streaming y genera metadata básica local por archivo descargado. Esta metadata permite validar origen, tamaño y checksum sin modificar el contenido original.

La ingesta MEF no descarga predial ni RENAMU. Esas fuentes se gestionan mediante sus propios procesos de ingesta.

## Actualización por ingesta predial hacia Landing

Existe una descarga controlada para la fuente de seguimiento de meta del impuesto predial.

Estado técnico actual:

- Fuente: meta predial.
- Método implementado: CSV directo.
- Configuración de recursos: `config/sources.yaml`.
- Script de ingesta: `src/ingestion/download_predial_goal.py`.
- Destino local: `data/landing/predial_goal/`.
- Transformación de negocio: no aplica en Landing.
- Conversión a Parquet: pendiente para Bronze.
- Integración de tablas: pendiente para Silver.
- Auditoría completa y reintentos: pendiente para una etapa posterior.

El archivo `config/sources.yaml` registra los recursos prediales observados en el portal, incluyendo tablas temáticas y diccionarios.

La fuente predial debe preservarse inicialmente como conjunto de archivos relacionados. La integración y selección de tablas analíticas se realizará después del profiling y durante Silver.

Los recursos documentales que no respondan correctamente durante validación quedan registrados como observados, pero no se habilitan para descarga automática hasta confirmar una URL válida.

## Actualización por ingesta RENAMU hacia Landing

Existe una descarga y extracción controlada para la fuente RENAMU 2022.

Estado técnico actual:

- Fuente: RENAMU 2022.
- Método implementado: ZIP completo y diccionario PDF.
- Configuración de recursos: `config/sources.yaml`.
- Script de ingesta: `src/ingestion/download_renamu.py`.
- Destino local: `data/landing/renamu/`.
- Directorio de extracción: `data/landing/renamu/extracted/`.
- Transformación de negocio: no aplica en Landing.
- Conversión a Parquet: pendiente para Bronze.
- Selección de variables RENAMU: pendiente para Silver.
- Auditoría completa y reintentos: pendiente para una etapa posterior.

El script descarga los recursos RENAMU por streaming, conserva los archivos originales, genera metadata básica local y permite extraer el ZIP completo dentro de Landing.

La fuente RENAMU debe preservarse inicialmente como fuente contextual. La selección de variables útiles para análisis municipal se realizará después del profiling y durante Silver.

Los archivos descargados, extraídos y su metadata local no deben subirse al repositorio.

## Resultado esperado de esta etapa

En el estado actual, el proyecto cuenta con tres fuentes preparadas para descargarse hacia Landing de forma controlada:

- MEF ingresos, mediante `src/ingestion/download_mef_income.py`.
- Meta predial, mediante `src/ingestion/download_predial_goal.py`.
- RENAMU 2022, mediante `src/ingestion/download_renamu.py`.

Las tres fuentes tienen sus recursos centralizados en `config/sources.yaml`, se descargan como archivos originales hacia Landing, no se transforman en esta etapa y no deben versionarse en GitHub.

Los procesos de ingesta incorporan validación de disponibilidad, descarga por streaming, metadata local por archivo, checksum, auditoría básica, reintentos HTTP y fallback de validación cuando corresponde.

La fuente MEF ingresos queda preparada para descarga explícita por recurso, año, granularidad o descarga completa solicitada de forma explícita, debido al tamaño de sus archivos históricos y recientes.

La fuente predial queda preparada para descarga de sus tablas temáticas y diccionarios habilitados, conservando como observados los recursos que no respondan correctamente durante validación.

La fuente RENAMU queda preparada para descargar el ZIP completo, conservar el diccionario PDF como referencia documental y extraer el contenido del ZIP dentro de Landing sin transformar los datos.

Las siguientes etapas técnicas serán:

- Ejecutar una descarga local completa y controlada de las fuentes necesarias.
- Revisar la auditoría local generada por los procesos de ingesta.
- Perfilar archivos descargados en Landing.
- Convertir fuentes Landing hacia Bronze Parquet.
