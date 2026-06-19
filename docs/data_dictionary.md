# Diccionario de datos

## Propósito del documento

Este documento define el diccionario de datos progresivo del proyecto **Municipal Revenue Big Data Analytics**.

En esta etapa, el documento cumple dos funciones:

1. Mantener el diccionario inicial de campos esperados y conceptos analíticos.
2. Documentar el contrato técnico de los datasets disponibles en la capa Bronze.

Este documento no representa todavía el modelo analítico final, ni define reglas definitivas de calidad, ni reemplaza el profiling de datos.

El diccionario será actualizado progresivamente después de:

- Discovery de fuentes.
- Profiling inicial.
- Construcción de Bronze.
- Reglas de calidad.
- Transformaciones Silver.
- Modelo Gold final.

## Criterio de uso

Este documento no debe confundirse con el reporte de profiling.

- El diccionario de datos describe campos esperados, significado, origen y uso analítico.
- El contrato Bronze documenta datasets Parquet generados, rutas, metadata técnica y exclusiones.
- El profiling documenta valores observados, nulos, duplicados, tipos inferidos y problemas detectados.
- La calidad documenta reglas, severidades y resultados de validación.
- Silver documentará limpieza, tipado semántico e integración.
- Gold documentará hechos, dimensiones, marts y KPIs finales.

## Convenciones preliminares de nombres

Como criterio general del proyecto:

- Se usarán nombres en minúsculas.
- Se evitarán tildes y caracteres especiales en nombres técnicos.
- Se reemplazarán espacios por guiones bajos.
- Se preferirán nombres descriptivos y consistentes.
- Se mantendrán columnas originales relevantes cuando aporten trazabilidad.
- Se agregarán columnas de metadata técnica por capa.

Ejemplos:

| Nombre original posible | Nombre técnico esperado |
| ----------------------- | ----------------------- |
| Año                     | `anio`                  |
| Departamento            | `departamento`          |
| Provincia               | `provincia`             |
| Distrito                | `distrito`              |
| Municipalidad           | `municipalidad`         |
| Monto Ejecutado         | `monto_ejecutado`       |
| Porcentaje de Avance    | `porcentaje_avance`     |

En Bronze, la normalización de nombres es técnica. La interpretación semántica definitiva de cada campo se realizará después de profiling y durante Silver.

## Capas del diccionario

El diccionario del proyecto se organizará progresivamente por capa:

| Capa    | Propósito del diccionario                                                        |
| ------- | -------------------------------------------------------------------------------- |
| Landing | Identificar archivos originales y documentación fuente.                          |
| Bronze  | Documentar datasets Parquet generados, rutas, `resource_key` y metadata técnica. |
| Silver  | Documentar campos limpios, tipados, estandarizados e integrables.                |
| Gold    | Documentar marts, KPIs, granularidad analítica y campos finales para Power BI.   |

El estado actual de este documento se enfoca en el contrato Bronze.

## Contrato técnico de Bronze

La capa Bronze contiene recursos tabulares seleccionados desde Landing convertidos a Parquet.

Bronze garantiza:

- Escritura en formato Parquet.
- Organización por fuente y por `resource_key`.
- Normalización técnica de nombres de columnas.
- Metadata técnica de procesamiento.
- Conservación de la granularidad original de cada recurso.
- Separación de recursos sin integración entre fuentes.

Bronze no garantiza todavía:

- Tipado semántico definitivo.
- Conversión final de montos, porcentajes, fechas o ubigeos.
- Reglas de calidad definitivas.
- Identificación final de llaves primarias o foráneas.
- Integración entre SIAF, SISMEPRE y RENAMU.
- Modelo de hechos y dimensiones.
- Modelo Gold.
- Tablas externas Hive.
- Modelo Power BI.

Bronze evita inferencia agresiva y prioriza preservar valores de origen sin aplicar limpieza fuerte de negocio.

## Organización física Bronze

Las salidas Bronze se organizan bajo la siguiente convención:

```text
data/bronze/<source_name>/resource_key=<valor>/
```

La carpeta `resource_key=<valor>` identifica el recurso lógico convertido desde Landing. Esta organización no debe interpretarse como particionamiento analítico definitivo; solo separa datasets por recurso de origen.

## Metadata técnica Bronze

### Metadata común

| Campo                     | Tipo esperado    | Descripción                                    |
| ------------------------- | ---------------- | ---------------------------------------------- |
| `bronze_source_name`      | string           | Nombre lógico de la fuente procesada.          |
| `bronze_resource_key`     | string           | Identificador lógico del recurso convertido.   |
| `bronze_source_file_name` | string           | Nombre del archivo fuente leído desde Landing. |
| `bronze_source_file_path` | string           | Ruta local del archivo fuente en Landing.      |
| `bronze_processed_at_utc` | string/timestamp | Fecha y hora UTC de procesamiento Bronze.      |

### Metadata específica de SIAF ingresos

| Campo                       | Tipo esperado | Descripción                                                    |
| --------------------------- | ------------- | -------------------------------------------------------------- |
| `bronze_source_year`        | string/int    | Año asociado al recurso MEF, cuando aplica.                    |
| `bronze_source_granularity` | string        | Granularidad configurada del recurso: anual, mensual o diaria. |

### Metadata específica de SISMEPRE

| Campo                    | Tipo esperado | Descripción                                                     |
| ------------------------ | ------------- | --------------------------------------------------------------- |
| `bronze_source_role`     | string        | Rol configurado del recurso predial convertido.                 |
| `bronze_source_priority` | string        | Prioridad operativa configurada para el recurso, cuando aplica. |

### Metadata específica de RENAMU

| Campo                | Tipo esperado | Descripción                                |
| -------------------- | ------------- | ------------------------------------------ |
| `bronze_source_year` | string/int    | Año asociado al recurso RENAMU convertido. |

La columna `run_id` pertenece a la auditoría de ingesta. No se documenta como columna Bronze porque los builders actuales no la incorporan al Parquet Bronze.

## Datasets Bronze por fuente

## 1. SIAF ingresos

### Descripción

Fuente orientada al análisis de presupuesto y ejecución de ingresos municipales.

En Bronze, cada recurso MEF seleccionado desde Landing se convierte a un dataset Parquet independiente bajo `resource_key`.

### Ruta Bronze

```text
data/bronze/siaf_income/resource_key=<resource_key>/
```

### Recursos Bronze convertidos

| Resource key   | Granularidad | Año  | Observación                                |
| -------------- | ------------ | ---- | ------------------------------------------ |
| `annual_2012`  | Anual        | 2012 | Recurso anual MEF.                         |
| `annual_2013`  | Anual        | 2013 | Recurso anual MEF.                         |
| `annual_2014`  | Anual        | 2014 | Recurso anual MEF.                         |
| `annual_2015`  | Anual        | 2015 | Recurso anual MEF.                         |
| `annual_2016`  | Anual        | 2016 | Recurso anual MEF.                         |
| `annual_2017`  | Anual        | 2017 | Recurso anual MEF.                         |
| `annual_2018`  | Anual        | 2018 | Recurso anual MEF.                         |
| `annual_2019`  | Anual        | 2019 | Recurso anual MEF.                         |
| `annual_2020`  | Anual        | 2020 | Recurso anual MEF.                         |
| `annual_2021`  | Anual        | 2021 | Recurso anual MEF.                         |
| `annual_2022`  | Anual        | 2022 | Recurso anual MEF.                         |
| `annual_2023`  | Anual        | 2023 | Recurso anual MEF.                         |
| `annual_2024`  | Anual        | 2024 | Recurso anual MEF.                         |
| `monthly_2025` | Mensual      | 2025 | Recurso reciente con granularidad mensual. |
| `daily_2025`   | Diaria       | 2025 | Recurso reciente con granularidad diaria.  |
| `monthly_2026` | Mensual      | 2026 | Recurso reciente con granularidad mensual. |
| `daily_2026`   | Diaria       | 2026 | Recurso reciente con granularidad diaria.  |

### Origen Landing

```text
data/landing/siaf_income/
```

### Recurso excluido

| Archivo                    | Motivo                                                                                                            |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `Ingresos_Diccionario.csv` | Diccionario de datos. Se conserva como documentación de fuente, pero no se convierte como tabla principal Bronze. |

### Metadata Bronze MEF

| Campo                       | Descripción                                  |
| --------------------------- | -------------------------------------------- |
| `bronze_source_name`        | Valor esperado: `siaf_income`.                |
| `bronze_resource_key`       | Identificador del recurso MEF convertido.    |
| `bronze_source_file_name`   | Nombre del CSV fuente.                       |
| `bronze_source_file_path`   | Ruta local del CSV en Landing.               |
| `bronze_source_year`        | Año asociado al recurso.                     |
| `bronze_source_granularity` | Granularidad: `annual`, `monthly` o `daily`. |
| `bronze_processed_at_utc`   | Fecha y hora UTC del procesamiento Bronze.   |

### Rol en el lakehouse

SIAF ingresos es la fuente principal para el análisis presupuestal y de ejecución de ingresos. En Bronze se conserva como datasets separados por recurso temporal, sin integración ni limpieza semántica definitiva.

### Decisiones pendientes

Quedan para profiling, calidad y Silver:

- Confirmar columnas reales por recurso.
- Evaluar consistencia entre años y granularidades.
- Tipar montos y periodos.
- Identificar llaves candidatas.
- Normalizar municipalidades o códigos administrativos.
- Definir qué granularidad alimentará Gold.

## 2. SISMEPRE

### Descripción

Fuente orientada al seguimiento de la meta del impuesto predial. A diferencia de SIAF ingresos, esta fuente no se trata como una única tabla plana; está compuesta por varias tablas fuente relacionadas.

En Bronze, cada tabla fuente se conserva por separado bajo su propio `resource_key`. No se integran tablas, no se definen hechos ni dimensiones y no se decide todavía el modelo analítico.

### Ruta Bronze

```text
data/bronze/sismepre/resource_key=<resource_key>/
```

### Recursos Bronze convertidos

| Resource key           | Archivo Landing                   | Rol Bronze   | Observación                                                       |
| ---------------------- | --------------------------------- | ------------ | ----------------------------------------------------------------- |
| `preguntas`            | `rentas_preguntas.csv`            | Tabla fuente | Tabla de preguntas o estructura asociada a formularios prediales. |
| `estadistica`          | `rentas_estadistica.csv`          | Tabla fuente | Tabla estadística predial.                                        |
| `formulario`           | `rentas_formulario.csv`           | Tabla fuente | Tabla de formularios.                                             |
| `esat_estadistica_atm` | `rentas_esat_estadistica_atm.csv` | Tabla fuente | Tabla estadística ATM asociada a la fuente predial.               |
| `respuestas`           | `rentas_respuestas.csv`           | Tabla fuente | Tabla de respuestas prediales.                                    |
| `ano_aplicacion`       | `rentas_ano_aplicacion.csv`       | Tabla fuente | Tabla relacionada con años de aplicación.                         |
| `entidad_estado`       | `rentas_entidad_estado.csv`       | Tabla fuente | Tabla de entidades y estados asociados.                           |

### Origen Landing

```text
data/landing/sismepre/
```

### Recursos excluidos

Los recursos con rol `dictionary` se conservan como documentación de fuente, pero no se convierten como tablas principales Bronze:

| Resource key excluido             | Motivo                   |
| --------------------------------- | ------------------------ |
| `ano_aplicacion_dictionary`       | Diccionario de columnas. |
| `preguntas_dictionary`            | Diccionario de columnas. |
| `estadistica_dictionary`          | Diccionario de columnas. |
| `entidad_estado_dictionary`       | Diccionario de columnas. |
| `formulario_dictionary`           | Diccionario de columnas. |
| `esat_estadistica_atm_dictionary` | Diccionario de columnas. |
| `respuestas_dictionary`           | Diccionario de columnas. |

### Metadata Bronze Predial

| Campo                     | Descripción                                   |
| ------------------------- | --------------------------------------------- |
| `bronze_source_name`      | Valor esperado: `sismepre`.               |
| `bronze_resource_key`     | Identificador del recurso predial convertido. |
| `bronze_source_file_name` | Nombre del CSV fuente.                        |
| `bronze_source_file_path` | Ruta local del CSV en Landing.                |
| `bronze_source_role`      | Rol configurado del recurso convertido.       |
| `bronze_source_priority`  | Prioridad configurada, cuando aplica.         |
| `bronze_processed_at_utc` | Fecha y hora UTC del procesamiento Bronze.    |

### Rol en el lakehouse

La fuente predial aporta información para analizar avance, cumplimiento y brechas relacionadas con la meta del impuesto predial. En Bronze se conserva la estructura original por tablas fuente, evitando integraciones prematuras.

### Decisiones pendientes

Quedan para profiling, calidad y Silver:

- Determinar la relación real entre `respuestas`, `preguntas`, `formulario`, `estadistica` y `esat_estadistica_atm`.
- Identificar llaves candidatas.
- Determinar si alguna tabla actúa como hecho, catálogo operativo o dimensión futura.
- Tipar porcentajes, avances, años y posibles identificadores.
- Evaluar nulos, duplicados y consistencia entre tablas.
- Definir el modelo analítico predial en Gold.

## 3. RENAMU 2022

### Descripción

RENAMU 2022 funciona como fuente contextual municipal. Su objetivo posterior será enriquecer el análisis territorial y municipal con variables institucionales, administrativas o de contexto.

En Bronze se convierte únicamente el CSV tabular principal extraído desde Landing. No se convierten ZIPs ni PDFs como tablas.

### Ruta Bronze

```text
data/bronze/renamu/resource_key=base_renamu_2022/
```

### Recurso Bronze convertido

| Resource key       | Archivo Landing          | Año  | Observación                                                                                               |
| ------------------ | ------------------------ | ---- | --------------------------------------------------------------------------------------------------------- |
| `base_renamu_2022` | `Base_RENAMU_2022_f.csv` | 2022 | CSV principal extraído desde el ZIP RENAMU 2022. Es una tabla ancha con una cantidad elevada de columnas. |

### Origen Landing

```text
data/landing/renamu/extracted/783-Modulo1726/Base_RENAMU_2022_f.csv
```

### Características de lectura

| Criterio          | Valor                                                             |
| ----------------- | ----------------------------------------------------------------- |
| Separador         | `;`                                                               |
| Encoding esperado | `UTF-8`                                                           |
| Estructura        | Tabla ancha                                                       |
| Uso en Bronze     | Conversión técnica a Parquet sin selección semántica de variables |

No se copia en este documento el listado completo de columnas de RENAMU. El detalle exhaustivo de columnas corresponde al profiling o a documentación ampliada posterior.

### Recursos excluidos

| Archivo o recurso                          | Motivo                                                                    |
| ------------------------------------------ | ------------------------------------------------------------------------- |
| `2022.zip`                                 | Archivo comprimido original. Se conserva en Landing como fuente original. |
| `Diccionario.pdf`                          | Documento de referencia. No es tabla Bronze.                              |
| `2.Diccionario de Datos - RENAMU 2022.pdf` | Diccionario incluido en el ZIP. No es tabla Bronze.                       |
| Otros archivos no tabulares extraídos      | No forman parte del dataset Bronze principal.                             |

### Metadata Bronze RENAMU

| Campo                     | Descripción                                |
| ------------------------- | ------------------------------------------ |
| `bronze_source_name`      | Valor esperado: `renamu`.                  |
| `bronze_resource_key`     | Valor esperado: `base_renamu_2022`.        |
| `bronze_source_file_name` | Nombre del CSV fuente.                     |
| `bronze_source_file_path` | Ruta local del CSV extraído en Landing.    |
| `bronze_source_year`      | Año de la fuente: 2022.                    |
| `bronze_processed_at_utc` | Fecha y hora UTC del procesamiento Bronze. |

### Rol en el lakehouse

RENAMU se usará como fuente de contexto municipal. En Bronze se conserva como tabla ancha, sin seleccionar variables analíticas ni interpretar todavía cuestionarios, módulos o codificaciones internas.

### Decisiones pendientes

Quedan para profiling, calidad y Silver:

- Identificar columnas relevantes para contexto territorial.
- Reconocer posibles campos de ubigeo, departamento, provincia, distrito o municipalidad.
- Evaluar nulos y consistencia de campos geográficos.
- Seleccionar variables útiles para análisis territorial.
- Tipar campos categóricos y numéricos relevantes.
- Reducir o estructurar la tabla para uso analítico posterior.

## 4. Clasificación Municipal MEF 2019

### Descripción

`municipal_classification` consolida los siete PDF oficiales de Clasificación Municipal 2019 publicados por el MEF. Bronze mantiene una fila por municipalidad clasificada y conserva metadata técnica del PDF origen.

### Ruta Bronze

```text
data/bronze/municipal_classification/
```

### Recurso Bronze convertido

| Resource key | Archivo Landing | Año | Observación |
| ------------ | --------------- | --- | ----------- |
| `municipal_classification` | `tipo_a.pdf` ... `tipo_g.pdf` | 2019 | Dataset consolidado desde siete PDF oficiales A-G con CSV intermedios regenerables en Landing. |

### Metadata Bronze

| Campo | Descripción |
| ----- | ----------- |
| `bronze_source_name` | Valor esperado: `municipal_classification`. |
| `bronze_resource_key` | PDF origen: `tipo_a` a `tipo_g`. |
| `bronze_source_file_name` | Nombre local del PDF descargado. |
| `bronze_source_file_path` | Ruta local del PDF en Landing. |
| `bronze_source_url` | URL oficial del PDF. |
| `bronze_source_page_url` | Página oficial del MEF. |
| `bronze_processed_at_utc` | Fecha y hora UTC del procesamiento Bronze. |

### Contrato funcional mínimo

- `anio = 2019` para todas las filas.
- `ubigeo` como string de 6 dígitos.
- `tipo_clasificacion` limitado a `A`..`G`.
- `ambito_municipal` limitado a `provincial` o `distrital`.
- Conteos oficiales validados: A=74, B=122, C=42, D=129, E=378, F=509, G=620, total 1874.
- La integración posterior debe realizarse por `ubigeo` o `ubigeo6`.

## Exclusiones generales del contrato Bronze

No forman parte de los datasets Bronze:

| Recurso                              | Motivo                                                             |
| ------------------------------------ | ------------------------------------------------------------------ |
| CSV de diccionario                   | Documentación técnica de columnas, no tabla principal de análisis. |
| PDFs                                 | Documentación de fuente, no dataset tabular Bronze.                |
| ZIPs                                 | Archivos originales comprimidos, preservados en Landing.           |
| `*.metadata.json`                    | Metadata local de descarga.                                        |
| `*.part`                             | Archivos temporales de descarga incompleta.                        |
| `data/quality/ingestion_audit.jsonl` | Auditoría local de ingesta.                                        |
| Logs locales                         | Evidencia operativa local, no dataset Bronze.                      |
| Reportes generados                   | Artefactos regenerables, no fuente Bronze.                         |
| Archivos Parquet reales              | Salidas locales del lakehouse, no deben versionarse en GitHub.     |

## Relación con profiling

El contrato Bronze documenta qué datasets existen y cómo se organizan técnicamente.

El profiling posterior debe documentar:

- Cantidad de filas por dataset.
- Cantidad de columnas por dataset.
- Tipos inferidos.
- Nulos.
- Duplicados.
- Valores frecuentes.
- Llaves candidatas.
- Problemas de lectura o codificación.
- Campos útiles para Silver y Gold.

No se deben mezclar resultados detallados de profiling dentro de este contrato Bronze.

## Relación con calidad de datos

Las reglas de calidad se definirán y documentarán después de observar los datos Bronze.

Ejemplos de validaciones futuras:

- Columnas críticas presentes.
- Nulos en campos relevantes.
- Duplicados por llave candidata.
- Años fuera de rango.
- Porcentajes fuera de 0 a 100.
- Montos negativos, si aplica.
- Ubigeos vacíos o inválidos.
- Consistencia entre fuentes integrables.

Este documento no registra resultados de calidad porque esa responsabilidad corresponde a `docs/data_quality.md`.

## Relación con Silver

Silver será responsable de:

- Limpiar datos.
- Convertir tipos semánticos.
- Estandarizar fechas, años, meses, montos y porcentajes.
- Normalizar nombres de municipalidades.
- Evaluar ubigeos y llaves administrativas.
- Integrar fuentes cuando corresponda.
- Documentar reglas de transformación.

Bronze no debe adelantar esas decisiones.

## Datasets Silver integrados

La integración Silver prepara datasets técnicos para resolver llaves y dejar trazabilidad antes del Gold dimensional.

Salidas locales no versionables:

```text
data/silver/integrated/map_sec_ejec_ubigeo/
data/silver/integrated/siaf_municipal_amounts/
data/silver/integrated/municipal_context/
data/silver/integrated/integration_coverage/
```

| Dataset | Propósito |
| --- | --- |
| `map_sec_ejec_ubigeo` | Mapa técnico para resolver `sec_ejec -> ubigeo6 -> municipality_key`. |
| `siaf_municipal_amounts` | Montos MEF agregados por recurso, año, mes y entidad. |
| `municipal_context` | Contexto RENAMU seleccionado para negocio. |
| `integration_coverage` | Métricas técnicas de cobertura y calidad del cruce. |

## Relación con Gold y Power BI

La capa Gold define el contrato analítico final. Su modelo objetivo está documentado en `docs/gold_model.md`.

## Contrato objetivo Gold

La capa Gold contiene dimensiones, hechos, marts y auditoría separados.

### Dimensiones

- `gold.dim_municipality`
- `gold.dim_geography`
- `gold.dim_renamu_context`
- `gold.dim_time`
- `gold.dim_sismepre_period`

### Hechos

- `gold.fact_siaf_income`
- `gold.fact_predial_statistics`

### Marts

- `gold.mart_municipal_revenue_overview`
- `gold.mart_predial_statistics_overview`
- `gold.mart_municipal_context`
- `gold.mart_territorial_summary`

### Auditoría

- `gold.audit_quality_results`
- `gold.audit_dataset_summary`

## Legacy explícito

Las siguientes referencias se consideran legacy o transicionales:

- `municipal_entity_bridge`
- `mef_municipal_amounts`
- `renamu_full`
- `renamu_municipal_context`
- `dim_municipality_context`
- `fact_municipal_income_execution`
- `fact_predial_compliance`
- `fact_revenue_integration_coverage`
- `fact_predial_integration_coverage`
- `fact_territorial_integration_coverage`
- `municipal_categories`
- `categorias_municipalidades`
- `CategoriasMunicipalidades.csv`

## Documentos específicos por capa

El diccionario maestro se complementa con:

- `docs/bronze_data_dictionary.md`
- `docs/silver_transformations.md`
- `docs/gold_model.md`
- `docs/data_profiling.md`
- `docs/data_quality.md`
