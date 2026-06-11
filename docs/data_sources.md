# Fuentes de datos

## Propósito del documento

Este documento describe las fuentes públicas consideradas para el proyecto **Municipal Revenue Big Data Analytics**.

El objetivo es identificar el origen, uso analítico, método probable de acceso, formato esperado, riesgos y criterios iniciales de uso antes de implementar los procesos de ingesta.

La información documentada en esta etapa es preliminar. Los detalles técnicos definitivos se confirmarán durante la fase de discovery y profiling.

## Resumen de fuentes

| Fuente                                   | Institución   | Uso principal                                             | Método esperado                             | Estado                 |
| ---------------------------------------- | ------------- | --------------------------------------------------------- | ------------------------------------------- | ---------------------- |
| Presupuesto y ejecución de ingresos      | MEF / SIAF    | Análisis presupuestal y ejecución de ingresos municipales | CSV, API o descarga desde portal público    | Pendiente de discovery |
| Seguimiento de meta del impuesto predial | SISMERE / MEF | Análisis de avance y cumplimiento de meta predial         | API, CSV o descarga desde portal público    | Pendiente de discovery |
| RENAMU 2022                              | INEI          | Contexto territorial y municipal                          | ZIP, XLSX, CSV o descarga manual controlada | Pendiente de discovery |

## Fuente 1: Presupuesto y ejecución de ingresos - MEF / SIAF

### Descripción

Fuente pública relacionada con información presupuestal y ejecución de ingresos. Permitirá analizar el comportamiento de los ingresos municipales, su ejecución y posibles diferencias territoriales.

### Institución responsable

Ministerio de Economía y Finanzas del Perú.

### Uso analítico esperado

Esta fuente será usada para responder preguntas como:

- ¿Qué municipalidades tienen mayor presupuesto o ejecución de ingresos?
- ¿Qué municipalidades presentan mayor o menor avance de ejecución?
- ¿Cómo varía la ejecución por departamento, provincia o distrito?
- ¿Qué diferencias existen entre presupuesto inicial, presupuesto modificado y ejecución?

### Campos esperados

Los campos exactos se confirmarán durante discovery y profiling. De forma preliminar se esperan variables como:

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

### Método de acceso esperado

El método definitivo se confirmará en discovery. Posibles métodos:

- Descarga CSV desde portal público.
- Consulta mediante API pública.
- Descarga manual controlada si el portal no ofrece acceso estable.
- Archivo comprimido si la fuente se distribuye como paquete.

### Riesgos identificados

- Cambios en estructura o nombres de columnas.
- Archivos grandes.
- Posible sobrecarga o inestabilidad del portal.
- Formatos numéricos con separadores locales.
- Diferencias entre códigos de entidad y ubigeo.
- Granularidad distinta a la requerida para análisis municipal.

### Criterio inicial de uso

Esta fuente será la base principal para el análisis de presupuesto y ejecución de ingresos municipales. No se transformará en Landing. Se convertirá a Parquet en Bronze y se limpiará en Silver.

## Fuente 2: Seguimiento de meta del impuesto predial - SISMERE / MEF

### Descripción

Fuente pública orientada al seguimiento de la meta vinculada al impuesto predial. Permitirá analizar avance, cumplimiento, brechas y desempeño de municipalidades respecto a la meta predial.

### Institución responsable

Ministerio de Economía y Finanzas del Perú, mediante SISMERE u otro portal asociado al seguimiento de metas.

### Uso analítico esperado

Esta fuente será usada para responder preguntas como:

- ¿Qué municipalidades cumplen la meta del impuesto predial?
- ¿Cuáles presentan mayor brecha de cumplimiento?
- ¿Cómo se distribuye el avance predial por territorio?
- ¿Existe relación entre ejecución de ingresos y cumplimiento predial?

### Campos esperados

Los campos exactos se confirmarán durante discovery y profiling. De forma preliminar se esperan variables como:

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
- Clasificación o grupo municipal, si existe.

### Método de acceso esperado

El método definitivo se confirmará en discovery. Posibles métodos:

- API pública.
- Descarga CSV.
- Descarga desde visor o portal público.
- Descarga manual controlada si la fuente no expone un endpoint estable.

### Riesgos identificados

- Endpoint inestable o con límites de consulta.
- Cambios en estructura de respuesta.
- Datos disponibles solo mediante interfaz web.
- Diferencias de nombres municipales respecto a MEF o RENAMU.
- Porcentajes almacenados como texto.
- Falta de ubigeo o códigos administrativos consistentes.

### Criterio inicial de uso

Esta fuente será la base principal para construir indicadores de cumplimiento y brecha predial. Se cruzará con la fuente de ingresos y con RENAMU si existen llaves suficientes.

## Fuente 3: RENAMU 2022 - INEI

### Descripción

El Registro Nacional de Municipalidades 2022 contiene información contextual de municipalidades peruanas. Será usado para enriquecer el análisis territorial y proporcionar variables descriptivas del contexto municipal.

### Institución responsable

Instituto Nacional de Estadística e Informática del Perú.

### Uso analítico esperado

Esta fuente será usada para responder preguntas como:

- ¿Qué características municipales pueden ayudar a interpretar diferencias de desempeño?
- ¿Cómo se distribuyen los resultados por departamento, provincia o distrito?
- ¿Qué variables contextuales pueden complementar el análisis presupuestal y predial?
- ¿Qué municipalidades tienen características similares y comportamientos distintos?

### Campos esperados

Los campos exactos se confirmarán durante discovery y profiling. De forma preliminar se esperan variables como:

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

### Método de acceso esperado

El método definitivo se confirmará en discovery. Posibles métodos:

- Descarga ZIP.
- Archivo XLSX.
- Archivo CSV.
- Descarga manual controlada desde el portal del INEI.

### Riesgos identificados

- Fuente distribuida en varios archivos o tablas temáticas.
- Diccionarios de datos separados del archivo principal.
- Columnas con nombres extensos o poco estandarizados.
- Datos categóricos con códigos que requieren interpretación.
- Necesidad de seleccionar solo variables relevantes para no sobrecargar el modelo Gold.

### Criterio inicial de uso

RENAMU será usada como fuente contextual. No debe dominar el modelo analítico, sino enriquecer la interpretación territorial y municipal.

## Criterios generales de ingesta

Las fuentes se trabajarán bajo los siguientes criterios:

- Landing conservará archivos originales.
- Bronze convertirá las fuentes a Parquet.
- Silver limpiará, tipará e integrará.
- Gold se diseñará después del profiling y Silver.
- Hive expondrá tablas externas sobre Parquet.
- Power BI consumirá preferentemente tablas Gold desde Hive.

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

## Decisiones pendientes

Las siguientes decisiones se tomarán después de discovery y profiling:

- Método real de acceso por fuente.
- Formato exacto de descarga.
- Columnas finales disponibles.
- Tipos de datos reales.
- Llaves candidatas.
- Granularidad por fuente.
- Cobertura de cruce entre MEF, SISMERE y RENAMU.
- Variables RENAMU que serán usadas en Silver y Gold.
- Modelo analítico final para Power BI.

## Resultado esperado de esta etapa

Al finalizar la etapa de inventario inicial, el proyecto cuenta con una visión clara de las fuentes que serán exploradas y de los riesgos que deben validarse antes de construir Bronze, Silver y Gold.
