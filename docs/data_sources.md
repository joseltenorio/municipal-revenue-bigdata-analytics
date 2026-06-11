# Fuentes de datos

## Propósito del documento

Este documento describe las fuentes públicas consideradas para el proyecto **Municipal Revenue Big Data Analytics**.

El objetivo es identificar el origen, uso analítico, método de acceso observado, formato esperado, riesgos y criterios iniciales de uso antes de implementar los procesos de ingesta hacia Landing.

La información documentada en esta etapa sigue siendo preliminar respecto a estructura interna de columnas, tipos y llaves. Esos detalles se confirmarán durante profiling, Bronze y Silver.

## Resumen de fuentes

| Fuente                                   | Institución   | Uso principal                                             | Método observado               | Estado actual                                                         |
| ---------------------------------------- | ------------- | --------------------------------------------------------- | ------------------------------ | --------------------------------------------------------------------- |
| Presupuesto y ejecución de ingresos      | MEF / SIAF    | Análisis presupuestal y ejecución de ingresos municipales | CSV directo                    | Recursos CSV representativos identificados y validados                |
| Seguimiento de meta del impuesto predial | MEF / SISMERE | Análisis de avance y cumplimiento de meta predial         | CSV directo                    | Múltiples CSV temáticos y diccionarios identificados                  |
| RENAMU 2022                              | INEI          | Contexto territorial y municipal                          | ZIP completo y diccionario PDF | ZIP completo y diccionario PDF validados; muestra XLSX no prioritaria |

## Fuente 1: Presupuesto y ejecución de ingresos - MEF / SIAF

### Descripción

Fuente pública relacionada con información presupuestal y ejecución de ingresos. Permitirá analizar el comportamiento de los ingresos municipales, su ejecución y posibles diferencias territoriales.

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

### Campos esperados

Los campos exactos se confirmarán durante profiling. De forma preliminar se esperan variables como:

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

Recursos representativos validados:

| Recurso                    | Tipo            | Uso esperado                                                    |
| -------------------------- | --------------- | --------------------------------------------------------------- |
| `2012-Ingreso.csv`         | CSV anual       | Recurso anual representativo                                    |
| `2026-Ingreso-Diario.csv`  | CSV diario      | Recurso reciente observado, no prioritario para primera ingesta |
| `Ingresos_Diccionario.csv` | Diccionario CSV | Referencia documental                                           |

### Riesgos identificados

- Archivos principales grandes.
- Variación de granularidad entre archivos anuales, mensuales y diarios.
- Posible cambio de estructura entre años.
- Necesidad de definir rango temporal antes de descargar.
- Posible diferencia entre código de entidad, ubigeo y nombre municipal.
- Necesidad de profiling para confirmar columnas reales.

### Criterio inicial de uso

Esta fuente será la base principal para el análisis de presupuesto y ejecución de ingresos municipales. No se transformará en Landing. Se convertirá a Parquet en Bronze y se limpiará en Silver.

Para la primera ingesta se priorizarán archivos anuales o un rango temporal controlado. Los recursos diarios o mensuales recientes quedan registrados, pero no se priorizan inicialmente por tamaño y granularidad.

## Fuente 2: Seguimiento de meta del impuesto predial - MEF / SISMERE

### Descripción

Fuente pública orientada al seguimiento de la meta vinculada al impuesto predial. Permitirá analizar avance, cumplimiento, brechas y desempeño de municipalidades respecto a la meta predial.

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

### Campos esperados

Los campos exactos se confirmarán durante profiling. De forma preliminar se esperan variables como:

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

Recursos representativos validados:

| Recurso                             | Tipo            | Uso esperado                             |
| ----------------------------------- | --------------- | ---------------------------------------- |
| `rentas_estadistica.csv`            | CSV temático    | Tabla candidata para análisis            |
| `rentas_preguntas.csv`              | CSV temático    | Tabla candidata o referencia estructural |
| `rentas_formulario.csv`             | CSV temático    | Tabla candidata o referencia estructural |
| `rentas_respuestas_diccionario.csv` | Diccionario CSV | Referencia documental                    |

### Riesgos identificados

- No es una única tabla plana.
- La fuente contiene varias tablas temáticas.
- Puede requerir integración entre tablas.
- Puede requerir interpretación mediante diccionarios.
- Puede faltar una llave geográfica limpia.
- Puede requerir normalización fuerte de municipalidades.
- Las reglas de Silver dependerán de profiling real.

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

Recursos representativos validados:

| Recurso                  | Tipo         | Uso esperado                                   |
| ------------------------ | ------------ | ---------------------------------------------- |
| `2022.zip`               | ZIP completo | Fuente principal para ingesta                  |
| `Diccionario.pdf`        | PDF          | Referencia documental                          |
| `BD_Muestra_2022_0.xlsx` | XLSX         | Recurso observado, no prioritario por HTTP 418 |

### Riesgos identificados

- La estructura interna del ZIP debe confirmarse después de la descarga local.
- Puede estar distribuida en varios archivos o tablas temáticas.
- Diccionario de datos separado del archivo principal.
- Columnas con nombres extensos o poco estandarizados.
- Datos categóricos con códigos que requieren interpretación.
- Necesidad de seleccionar solo variables relevantes para no sobrecargar el modelo Gold.
- La muestra XLSX no es prioritaria porque respondió con HTTP 418.
- La página del catálogo puede ser menos estable que los recursos directos.

### Criterio inicial de uso

RENAMU será usada como fuente contextual. No debe dominar el modelo analítico, sino enriquecer la interpretación territorial y municipal.

La ingesta definitiva deberá priorizar el ZIP completo y conservar el diccionario PDF como referencia documental.

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

## Riesgos generales

Los principales riesgos de las fuentes son:

- Cambios en los portales públicos.
- Descargas inestables.
- Archivos grandes.
- Columnas cambiantes.
- Falta de llaves consistentes.
- Diferencias entre ubigeo, código de entidad y nombre municipal.
- Formatos numéricos heterogéneos.
- Variables contextuales RENAMU distribuidas en múltiples archivos.
- Acceso automatizado bloqueado, lento o condicionado por el portal.

## Decisiones pendientes después del discovery

Después del discovery inicial, ya se identificaron recursos descargables directos para las tres fuentes principales. Sin embargo, todavía quedan decisiones de implementación antes de construir Landing, Bronze, Silver y Gold.

Decisiones pendientes:

- Definir el rango temporal de MEF ingresos que será descargado.
- Decidir si la ingesta de MEF ingresos usará solo archivos anuales o también recursos diarios o mensuales recientes.
- Definir qué tablas de meta predial se conservarán completas en Bronze.
- Definir qué tablas de meta predial serán necesarias para Silver y Gold.
- Confirmar la estructura interna del ZIP completo de RENAMU 2022 después de su descarga local.
- Seleccionar variables RENAMU relevantes para contexto municipal.
- Confirmar columnas reales, tipos de datos y llaves candidatas mediante profiling.
- Definir granularidad final de análisis después de observar los datos.
- Evaluar cobertura de cruce entre MEF, meta predial y RENAMU.
- Definir modelo Gold final y modelo Power BI después de Silver.

## Actualización por discovery de recursos directos

Durante el commit `feat(discovery): add source probing scripts and findings` se identificaron recursos descargables directos para las fuentes principales del proyecto.

### MEF - Presupuesto y ejecución de ingreso

Se identificó la página del dataset:

`https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-ingreso`

La fuente contiene archivos CSV por año, recursos recientes con granularidad diaria o mensual y un diccionario CSV.

Recursos representativos validados:

- `2012-Ingreso.csv`
- `2026-Ingreso-Diario.csv`
- `Ingresos_Diccionario.csv`

La fuente se mantiene como principal para análisis presupuestal y ejecución de ingresos, pero la ingesta definitiva deberá definir rango temporal y granularidad objetivo debido al tamaño de los archivos.

### MEF / SISMERE - Seguimiento de la Meta del Impuesto Predial

Se identificó la página del dataset:

`https://datosabiertos.mef.gob.pe/dataset/seguimiento-de-la-meta-del-impuesto-predial`

La fuente contiene múltiples CSV temáticos y diccionarios. No debe tratarse como una única tabla plana.

Recursos representativos validados:

- `rentas_estadistica.csv`
- `rentas_preguntas.csv`
- `rentas_formulario.csv`
- `rentas_respuestas_diccionario.csv`

Bronze deberá preservar las tablas fuente necesarias y Silver definirá las estructuras finales para análisis de avance, cumplimiento y brechas.

### INEI - RENAMU 2022

Se identificó la página del dataset:

`https://www.datosabiertos.gob.pe/dataset/registro-nacional-de-municipalidades-renamu-2022-instituto-nacional-de-estadistica-e`

Recursos representativos validados:

- `2022.zip`
- `Diccionario.pdf`
- `BD_Muestra_2022_0.xlsx`

El ZIP completo y el diccionario PDF respondieron correctamente desde Python. La muestra XLSX respondió con HTTP 418, por lo que no se considera prioritaria para la ingesta. La ingesta definitiva deberá priorizar el ZIP completo y conservar el diccionario PDF como referencia documental.

## Resultado esperado de esta etapa

Al finalizar la etapa de inventario y discovery, el proyecto cuenta con fuentes priorizadas, recursos directos validados, riesgos de acceso identificados y decisiones pendientes claras antes de construir Landing, Bronze, Silver y Gold.
