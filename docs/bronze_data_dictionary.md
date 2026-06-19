# Diccionario de datos Bronze

## Propósito

Este documento define el diccionario técnico de la capa **Bronze** del proyecto **Municipal Revenue Big Data Analytics**.

La capa Bronze conserva los datos provenientes de las fuentes originales en formato Parquet, con normalización técnica mínima y metadata de trazabilidad. No representa todavía el modelo analítico final, ni debe usarse para KPIs definitivos sin pasar por Silver.

## Alcance actual

La capa Bronze cubre cuatro familias de fuentes:

| Fuente técnica | Fuente real | Tipo de origen | Uso principal |
| -------------- | ----------- | -------------- | ------------- |
| `siaf_income` | Consulta de ingresos SIAF / MEF | Web / CSV descargado | Ingresos municipales y clasificadores presupuestales |
| `sismepre` | SISMEPRE / Rentas municipales | Web / CSV descargado | Estadística predial, formularios, preguntas y respuestas |
| `renamu` | Registro Nacional de Municipalidades 2022 | Web / ZIP/CSV descargado | Contexto institucional, territorial y operativo municipal |
| `municipal_classification` | Clasificación Municipal MEF 2019 | PDF oficial + CSV extraído | Segmentación oficial por tipo municipal A-G |

## Criterios Bronze

Bronze debe:

- Convertir archivos fuente a Parquet.
- Mantener granularidad original por recurso.
- Preservar columnas de origen, con normalización técnica de nombres cuando corresponda.
- Agregar metadata técnica de ingestión/procesamiento.
- Evitar transformaciones semánticas fuertes.
- Evitar joins entre fuentes.
- Evitar cálculos analíticos definitivos.
- Servir como punto de partida para profiling, calidad y diseño Silver.

Bronze no debe:

- Corregir montos.
- Interpretar códigos como categorías finales.
- Resolver nombres municipales ambiguos.
- Crear dimensiones ni hechos.
- Filtrar definitivamente registros activos/inactivos, salvo reglas técnicas explícitas.
- Asumir que una columna candidata es llave final sin validación posterior.

## Metadata técnica esperada

Cada dataset Bronze debe incluir metadata técnica equivalente a:

| Columna | Descripción |
| ------- | ----------- |
| `source_name` | Familia de fuente técnica. |
| `resource_key` | Recurso específico dentro de la fuente. |
| `ingestion_timestamp` / equivalente | Momento de procesamiento o carga. |
| `source_file` / equivalente | Archivo o recurso de origen cuando esté disponible. |
| `extraction_date` / equivalente | Fecha de extracción cuando aplique. |

La nomenclatura exacta puede variar según el script Bronze, pero la intención es mantener trazabilidad suficiente para reproducir y auditar el flujo.

---

# 1. Fuente Bronze: `siaf_income`

## Descripción

`siaf_income` contiene información de ingresos públicos registrada en SIAF y publicada por el MEF. Para el proyecto, se usa como fuente financiera principal de ingresos municipales.

## Recursos Bronze esperados

| Recurso | Descripción |
| ------- | ----------- |
| `annual_2012` a `annual_2024` | Archivos anuales históricos. |
| `monthly_2025` | Archivo mensual 2025. |
| `daily_2025` | Archivo de actualización 2025, aunque el diccionario solo declara año y mes. |
| `monthly_2026` | Archivo mensual 2026. |
| `daily_2026` | Archivo de actualización 2026, aunque el diccionario solo declara año y mes. |
| `dictionary` | Diccionario de variables de ingresos. |

## Columnas oficiales según diccionario

| Columna original | Tipo fuente | Descripción funcional |
| ---------------- | ---------- | --------------------- |
| `ANO_DOC` | Numérico | Año del documento de recaudación. |
| `MES_DOC` | Numérico | Mes del documento de recaudación. |
| `NIVEL_GOBIERNO` | Carácter | Código del nivel de gobierno: `E`, `R`, `M`. |
| `NIVEL_GOBIERNO_NOMBRE` | Carácter | Nombre del nivel de gobierno. |
| `SECTOR` | Carácter | Código de sector. |
| `SECTOR_NOMBRE` | Carácter | Nombre del sector. |
| `PLIEGO` | Carácter | Código de pliego. |
| `PLIEGO_NOMBRE` | Carácter | Nombre del pliego. |
| `SEC_EJEC` | Numérico | Código de gobierno local / entidad ejecutora. |
| `EJECUTORA` | Carácter | Código de ejecutora. |
| `EJECUTORA_NOMBRE` | Carácter | Nombre de ejecutora. |
| `DEPARTAMENTO_EJECUTORA` | Carácter | Código de departamento de la ejecutora. |
| `DEPARTAMENTO_EJECUTORA_NOMBRE` | Carácter | Nombre de departamento de la ejecutora. |
| `PROVINCIA_EJECUTORA` | Carácter | Código de provincia de la ejecutora. |
| `PROVINCIA_EJECUTORA_NOMBRE` | Carácter | Nombre de provincia de la ejecutora. |
| `DISTRITO_EJECUTORA` | Carácter | Código de distrito de la ejecutora. |
| `DISTRITO_EJECUTORA_NOMBRE` | Carácter | Nombre de distrito de la ejecutora. |
| `FUENTE_FINANCIAMIENTO` | Carácter | Código de fuente de financiamiento. |
| `FUENTE_FINANCIAMIENTO_NOMBRE` | Carácter | Nombre de fuente de financiamiento. |
| `RUBRO` | Carácter | Código de rubro. |
| `RUBRO_NOMBRE` | Carácter | Nombre de rubro. |
| `TIPO_RECURSO` | Carácter | Código de tipo de recurso. |
| `TIPO_RECURSO_NOMBRE` | Carácter | Nombre de tipo de recurso. |
| `GENERICA` | Carácter | Código de genérica de ingreso. |
| `GENERICA_NOMBRE` | Carácter | Nombre de genérica de ingreso. |
| `SUBGENERICA` | Carácter | Código de subgenérica. |
| `SUBGENERICA_NOMBRE` | Carácter | Nombre de subgenérica. |
| `SUBGENERICA_DET` | Carácter | Código de subgenérica detalle. |
| `SUBGENERICA_DET_NOMBRE` | Carácter | Nombre de subgenérica detalle. |
| `ESPECIFICA` | Carácter | Código de específica. |
| `ESPECIFICA_NOMBRE` | Carácter | Nombre de específica. |
| `ESPECIFICA_DET` | Carácter | Código de específica detalle. |
| `ESPECIFICA_DET_NOMBRE` | Carácter | Nombre de específica detalle. |
| `MONTO_PIA` | Numérico | Presupuesto institucional de apertura. |
| `MONTO_PIM` | Numérico | Presupuesto institucional modificado. |
| `MONTO_RECAUDADO` | Numérico | Monto recaudado. |

## Observaciones Bronze

- Aunque algunos códigos estén marcados como numéricos, en Silver deberán tratarse como identificadores de texto cuando puedan tener ceros a la izquierda.
- `daily_2025` y `daily_2026` no deben asumirse como granularidad diaria definitiva si no existe una columna de día.
- Bronze no filtra todavía `NIVEL_GOBIERNO = M`; esa decisión corresponde a Silver/Gold.

---

# 2. Fuente Bronze: `sismepre`

## Descripción

`sismepre` contiene información del sistema de rentas municipales, principalmente relacionada con impuesto predial, formularios, preguntas, respuestas y estadística ATM.

## Recursos Bronze esperados

| Recurso | Descripción |
| ------- | ----------- |
| `ano_aplicacion` | Catálogo operativo de años de aplicación. |
| `entidad_estado` | Estado de entidades por año/periodo. |
| `esat_estadistica_atm` | Estadística principal predial/ATM. |
| `estadistica` | Catálogo de año, mes, formulario y periodo estadístico. |
| `formulario` | Catálogo de formularios. |
| `preguntas` | Catálogo de preguntas de cuestionario. |
| `respuestas` | Respuestas registradas por entidad/pregunta. |
| `*_dictionary` | Diccionarios fuente asociados cuando estén disponibles. |

## 2.1. `ano_aplicacion`

| Columna original | Descripción |
| ---------------- | ----------- |
| `ANO_APLICACION` | Año de aplicación. |
| `ANO_APLICACION_INICIO` | Año inicial de registro. |
| `ANO_APLICACION_FIN` | Año final de registro. |
| `FECHA_CIERRE` | Fecha de cierre. |
| `ESTADO` | Estado del registro: activo/inactivo. |
| `PERIODO` | Periodo. |
| `FECHA_PRES_OFICIO` | Fecha de presentación de oficio. |
| `FECHA_INI_CIERRE` | Fecha de inicio de cierre. |
| `FECHA_ING` | Fecha de ingreso. |

## 2.2. `entidad_estado`

| Columna original | Descripción |
| ---------------- | ----------- |
| `SEC_EJEC` | Código de entidad / gobierno local. |
| `ANO_APLICACION` | Año de aplicación. |
| `USUARIO_CREACION_FECHA` | Fecha de creación. |
| `ESTADO` | Estado operativo del registro. |
| `USUARIO_ENVIO_ID` | Usuario de envío. |
| `USUARIO_FECHA_ENVIO` | Fecha de envío. |
| `CORREO` | Correo registrado. |
| `ORIGEN_INFORMACION` | Origen de la información. |
| `CLASIFICACION` | Clasificación de la entidad. |
| `PERIODO` | Periodo. |
| `TIPO_META` | Tipo de meta. |
| `IND_RESOL_ALCAL_ADJUNTO` | Indicador de resolución de alcaldía adjunta. |
| `FECHA_RESOL_ALCAL_ADJUNTO` | Fecha de resolución de alcaldía adjunta. |

## 2.3. `esat_estadistica_atm`

| Columna original | Descripción |
| ---------------- | ----------- |
| `SEC_EJEC` | Identificador del gobierno local. |
| `UBIGEO` | Ubigeo de la municipalidad. |
| `DEPARTAMENTO` | Código de departamento. |
| `DEPARTAMENTO_NOMBRE` | Nombre de departamento. |
| `PROVINCIA` | Código de provincia. |
| `PROVINCIA_NOMBRE` | Nombre de provincia. |
| `DISTRITO` | Código de distrito. |
| `DISTRITO_NOMBRE` | Nombre de distrito. |
| `MUNICIPALIDAD_NOMBRE` | Nombre de la municipalidad. |
| `ANO_APLICACION` | Año de aplicación. |
| `PERIODO` | Periodo. |
| `ANO_ESTADISTICA` | Año estadístico. |
| `MES_ESTADISTICA` | Mes estadístico; puede incluir `13` como periodo anual/cierre. |
| `MON_EMISIONPREDIAL_AFECTO` | Monto de emisión predial afecto. |
| `MON_EMISIONPREDIAL_EXON` | Monto de emisión predial exonerado. |
| `NUM_EMISIONPREDIAL_AFECTO` | Número de emisiones prediales afectas. |
| `NUM_EMISIONPREDIAL_EXON` | Número de emisiones prediales exoneradas. |
| `NUM_EMISIONPREDIAL_CASA` | Número de emisiones prediales de casa habitación. |
| `NUM_EMISIONPREDIAL_OTROS` | Número de emisiones prediales de otros usos. |
| `MON_BASEIMPONIBLE_AFECTO` | Base imponible afecta. |
| `MON_BASEIMPONIBLE_EXON` | Base imponible exonerada. |
| `MON_AUTOAVALUO_INAFECTO` | Autovalúo inafecto. |
| `MON_RECAUDACTUAL_ORDIN` | Recaudación actual ordinaria. |
| `MON_RECAUDACTUAL_COAC` | Recaudación actual coactiva. |
| `MON_RECAUDANTER_ORDI` | Recaudación anterior ordinaria. |
| `MON_RECAUDANTER_COAC` | Recaudación anterior coactiva. |
| `MON_INICIALADULTOMAYOR` | Monto inicial adulto mayor. |
| `MON_PREDIALADULTOMAYOR` | Monto predial adulto mayor. |
| `NUM_CONTRIBADULTOMAYOR` | Número de contribuyentes adulto mayor. |
| `MON_RECUADADULTOMAYOR` | Recaudación adulto mayor. |
| `MON_SALDOPREDIAL_ORD` | Saldo predial ordinario. |
| `MON_SALDOPREDIAL_COAC` | Saldo predial coactivo. |
| `NUM_INAFECTOS` | Número de inafectos. |
| `TIPO_META` | Tipo de meta. |
| `NUM_CONTRIPREDIO` | Número de contribuyentes predio. |
| `FLAG_EMILIQUIDA` | Flag de emisión/liquidación. |
| `NUM_PREDIOUSOCH` | Número de predios de uso casa habitación. |
| `NUM_PREDIOOTROUSO` | Número de predios de otro uso. |
| `NUM_PREDIOTOTAL` | Número total de predios. |
| `FLAG_EMISION_INICIAL` | Flag de emisión inicial. |
| `FORMULARIO_ID` | Identificador del formulario. |
| `MES_ESTADISTICA` | Mes estadístico. |
| `MON_EMISIONPREDIAL_INSO` | Monto de emisión predial insoluto. |

## 2.4. `estadistica`

| Columna original | Descripción |
| ---------------- | ----------- |
| `ANO_APLICACION` | Año de aplicación. |
| `PERIODO` | Periodo. |
| `FORMULARIO_ID` | Identificador del formulario. |
| `ANO_ESTADISTICA` | Año estadístico. |
| `MES_ESTADISTICA` | Mes estadístico. |
| `ESTADO_REGISTRO` | Estado del registro. |
| `ANO_ESTADISTICA_DESC` | Descripción del año estadístico. |

## 2.5. `formulario`

| Columna original | Descripción |
| ---------------- | ----------- |
| `ANO_APLICACION` | Año de aplicación. |
| `PERIODO` | Periodo. |
| `FORMULARIO_ID` | Identificador del formulario. |
| `ORDEN_FORMULARIO` | Orden de formulario. |
| `TITULO` | Título del formulario. |
| `SUB_TITULO` | Subtítulo del formulario. |
| `ABREVIATURA` | Abreviatura. |
| `CLASIFICACION` | Clasificación. |
| `TIPO_FORMULARIO` | Tipo de formulario. |
| `ESTADO_REGISTRO` | Estado del registro. |

## 2.6. `preguntas`

| Columna original | Descripción |
| ---------------- | ----------- |
| `ANO_APLICACION` | Año de aplicación. |
| `PERIODO` | Periodo. |
| `FORMULARIO_ID` | Identificador del formulario. |
| `PREGUNTA_ID` | Identificador de pregunta. |
| `PREGUNTA_PADRE_ID` | Identificador de pregunta padre. |
| `ORDEN_PREGUNTA` | Orden de pregunta. |
| `DESCRIPCION` | Texto de la pregunta. |
| `OBJETO_ACTIVO` | Indicador de objeto activo. |
| `TIPO_CUESTIONARIO_ID` | Tipo de cuestionario. |
| `RESPUESTA` | Configuración o respuesta esperada según pregunta. |
| `RANGO_INI` | Rango inicial permitido. |
| `RANGO_FIN` | Rango final permitido. |
| `TEXTO_APOYO` | Texto de apoyo. |
| `TEXTO_LECTURA` | Texto de lectura. |
| `ESTADO_REGISTRO` | Estado del registro. |

## 2.7. `respuestas`

| Columna original | Descripción |
| ---------------- | ----------- |
| `SEC_EJEC` | Código de entidad / gobierno local. |
| `ANO_APLICACION` | Año de aplicación. |
| `PERIODO` | Periodo. |
| `FORMULARIO_ID` | Identificador del formulario. |
| `PREGUNTA_ID` | Identificador de pregunta. |
| `RESPUESTA_ID` | Identificador de respuesta. |
| `RESPUESTA_TEXTO` | Respuesta textual. |
| `RESPUESTA_DECIMAL` | Respuesta decimal. |
| `RESPUESTA_ENTERO` | Respuesta entera. |
| `RESPUESTA_FECHA` | Respuesta fecha. |
| `ESTADO_REGISTRO` | Estado del registro. |

## Observaciones Bronze SISMEPRE

- `esat_estadistica_atm` es la tabla candidata principal para análisis predial.
- `respuestas` debe conservarse en formato largo en Bronze.
- La interpretación de `MES_ESTADISTICA = 13` se realizará en Silver.
- Los filtros por `ESTADO_REGISTRO` o `ESTADO` no deben aplicarse de forma definitiva en Bronze.

---

# 3. Fuente Bronze: `renamu`

## Descripción

`renamu` contiene el Registro Nacional de Municipalidades 2022. Es una fuente ancha con muchas columnas tipo `Pxx`, por lo que Bronze conserva el dataset completo y Silver decidirá subconjuntos analíticos.

## Recurso Bronze esperado

| Recurso | Descripción |
| ------- | ----------- |
| `base_renamu_2022` | Base RENAMU 2022 completa. |
| `dictionary_pdf` | Diccionario oficial RENAMU 2022. |

## Variables globales principales

| Columna original | Descripción |
| ---------------- | ----------- |
| `Año` | Año en que se ejecuta el estudio. |
| `idmunici` | Código de la municipalidad. |
| `ccdd` | Código de departamento. |
| `ccpp` | Código de provincia. |
| `ccdi` | Código de distrito. |
| `Ubigeo` | Código ubigeo. |
| `Departamento` | Nombre del departamento. |
| `Provincia` | Nombre de la provincia. |
| `Distrito` | Nombre del distrito. |
| `Tipomuni` | Tipo de municipalidad: provincial, distrital o centro poblado. |

## Campos RENAMU relevantes para Silver futuro

| Campo | Uso preliminar |
| ----- | -------------- |
| `P14` | Servicio de internet municipal. |
| `P14A_1` | Computadoras con acceso a internet. |
| `P14A_2` | Tipo de conexión a internet. |
| `P16_4` | Uso de SIAF. |
| `P16_5` | Uso de sistema de recaudación tributaria municipal. |
| `P17_7` | Sistema de rentas / administración tributaria. |
| `P17_8` | Sistema de catastro. |
| `P18` | Estado del portal de transparencia. |
| `P18_Portal` | URL del portal de transparencia. |
| `P19D_T` | Total de personal al 31 de diciembre de 2021. |
| `P19M_T` | Total de personal al 31 de marzo de 2022. |
| `P20` | Personal con discapacidad; no es total de personal. |
| `P22_AT2` | Requiere asistencia técnica en administración tributaria municipal. |
| `P22_AT3` | Requiere asistencia técnica en catastro urbano/rural. |
| `P22_C2` | Requiere capacitación en administración tributaria municipal. |
| `P22_C3` | Requiere capacitación en catastro urbano/rural. |

## Observaciones Bronze RENAMU

- Bronze no debe reducir RENAMU a las columnas anteriores.
- La lista anterior solo documenta campos útiles para el futuro Silver.
- La fuente completa debe conservarse porque hay más variables que podrían ser necesarias para análisis posteriores.
- Muchos campos `Pxx` son códigos de alternativas; no deben transformarse automáticamente a booleanos sin revisar el diccionario.

---

# 4. Fuente Bronze: `municipal_classification`

## Descripción

`municipal_classification` consolida los siete PDF oficiales de Clasificación Municipal 2019 publicados por el MEF. Bronze preserva una fila por municipalidad clasificada y conserva metadata técnica del PDF origen.

## Recurso Bronze esperado

| Recurso | Descripción |
| ------- | ----------- |
| `municipal_classification` | Dataset consolidado con tipos A-G y metadata de trazabilidad. |

## Columnas Bronze consolidadas

| Columna original | Descripción |
| ---------------- | ----------- |
| `anio` | Año lógico del dataset. Valor esperado: `2019`. |
| `tipo_clasificacion` | Tipo oficial `A` a `G`. |
| `ambito_municipal` | Ambito oficial: `provincial` o `distrital`. |
| `descripcion_tipo` | Descripción oficial del tipo municipal. |
| `nro` | Orden mostrado en el PDF. |
| `ubigeo` | Ubigeo textual de 6 dígitos. |
| `departamento_nombre` | Nombre de departamento. |
| `provincia_nombre` | Nombre de provincia. |
| `distrito_nombre` | Nombre de distrito. |
| `bronze_source_name` | Valor esperado: `municipal_classification`. |
| `bronze_resource_key` | PDF origen: `tipo_a` a `tipo_g`. |
| `bronze_source_file_name` | Nombre local del PDF descargado. |
| `bronze_source_file_path` | Ruta local del PDF en Landing. |
| `bronze_source_url` | URL oficial del PDF. |
| `bronze_source_page_url` | Página oficial del MEF. |
| `bronze_processed_at_utc` | Timestamp UTC de construcción Bronze. |

## Observaciones Bronze

- Landing conserva PDFs originales y CSV extraídos locales, pero esos artefactos no deben versionarse.
- Bronze consolida los siete tipos en un único dataset, no en siete tablas separadas.
- `ubigeo` debe mantenerse como texto para no perder ceros a la izquierda.
- La integración posterior debe hacerse por `ubigeo` o `ubigeo6`, no por nombre municipal.
- Los conteos oficiales validados son A=74, B=122, C=42, D=129, E=378, F=509, G=620, total 1874.

---

# 5. Exclusiones del diccionario Bronze

Este documento no define todavía:

- Columnas finales Silver.
- Columnas finales Gold.
- Dimensiones y hechos.
- KPIs Power BI.
- Reglas definitivas de matching municipal.
- Reglas finales de deduplicación.
- Correcciones semánticas de datos.

Esas decisiones se documentarán después del profiling y durante los commits de Silver.

## Relación con otros documentos

| Documento | Rol |
| --------- | --- |
| `docs/data_dictionary.md` | Índice y diccionario maestro progresivo. |
| `docs/bronze_data_dictionary.md` | Contrato técnico de la capa Bronze. |
| `docs/data_profiling.md` | Observaciones de valores, nulos, duplicados, tipos y llaves candidatas. |
| `docs/data_quality.md` | Reglas y resultados de calidad. |
| `docs/silver_transformations.md` | Decisiones de limpieza y tipado Silver. |
| `docs/gold_model.md` | Modelo analítico final. |
