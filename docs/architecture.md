# Arquitectura del proyecto

## Nombre del proyecto

**Municipal Revenue Big Data Analytics**

## Propósito de la arquitectura

La arquitectura del proyecto está diseñada para construir un flujo local de ingeniería y analítica de datos que permita procesar, validar, integrar y analizar información municipal peruana proveniente de fuentes públicas.

El proyecto no se limita a construir un reporte final. Su objetivo es implementar un lakehouse analítico local con capas claramente separadas, trazabilidad de datos, procesamiento distribuido con Apache Spark, almacenamiento en Parquet, catálogo SQL con Apache Hive y visualización final en Power BI.

## Enfoque general

El proyecto sigue una arquitectura Medallion:

```text
Fuentes públicas
-> Landing
-> Bronze Parquet
-> Profiling y Quality Gates
-> Silver Parquet
-> Gold Parquet / Marts analíticos
-> Hive External Tables
-> Power BI conectado preferentemente a Hive
```

Esta arquitectura separa los datos según su nivel de procesamiento, calidad y orientación analítica.

## Fuentes públicas

Las fuentes consideradas son:

- Presupuesto y ejecución de ingresos del MEF / SIAF.
- Seguimiento de la meta del impuesto predial desde SISMERE / MEF.
- Registro Nacional de Municipalidades RENAMU 2022 del INEI.

Cada fuente se incorpora al lakehouse después de confirmar su método real de acceso, formato, estructura, granularidad y limitaciones.

## Capa Landing

La capa Landing conserva los archivos originales tal como se obtienen desde las fuentes públicas.

Puede contener archivos como:

- CSV.
- ZIP.
- XLSX.
- JSON.
- PDF de documentación o diccionarios.
- Archivos extraídos desde paquetes comprimidos.
- Metadata local de descarga.

Características principales:

- No transforma datos.
- No cambia nombres de columnas.
- No corrige tipos.
- No elimina registros.
- Conserva evidencia del origen.
- Sirve como respaldo para reprocesamiento.

Los datos reales de Landing no se versionan en GitHub. Solo se mantiene la estructura de carpetas mediante archivos `.gitkeep`.

## Capa Bronze

La capa Bronze convierte recursos tabulares seleccionados desde Landing hacia formato Parquet.

Bronze representa una capa técnica y trazable: mantiene la granularidad original de cada fuente, evita inferencia agresiva de tipos y prioriza preservar los valores de origen sin aplicar tipado semántico fuerte, reglas de negocio ni integración entre fuentes.

Objetivos de Bronze:

- Estandarizar el almacenamiento físico en Parquet.
- Mantener la granularidad original de la fuente.
- Aplicar limpieza técnica mínima.
- Normalizar nombres técnicos de columnas.
- Agregar metadata técnica de procesamiento.
- Organizar las salidas por fuente y por `resource_key`.
- Preparar los datos para profiling, calidad y procesamiento posterior.

### Organización física Bronze

Las salidas Bronze se organizan en carpetas por fuente y por recurso:

```text
data/bronze/<source_name>/resource_key=<valor>/
```

Esta organización por `resource_key` no representa todavía un particionamiento analítico definitivo. Su objetivo es separar físicamente cada recurso convertido desde Landing y mantener trazabilidad entre archivo origen, recurso lógico y dataset Parquet generado.

Rutas Bronze actuales:

```text
data/bronze/mef_income/
data/bronze/predial_goal/
data/bronze/renamu/
```

### Contrato Bronze por fuente

| Fuente       | Ruta Bronze                                             | Criterio de organización                                 | Estado                  |
| ------------ | ------------------------------------------------------- | -------------------------------------------------------- | ----------------------- |
| MEF ingresos | `data/bronze/mef_income/resource_key=<resource_key>/`   | Un dataset Parquet por recurso MEF convertido            | Implementado localmente |
| Meta predial | `data/bronze/predial_goal/resource_key=<resource_key>/` | Un dataset Parquet por tabla fuente predial seleccionada | Implementado localmente |
| RENAMU 2022  | `data/bronze/renamu/resource_key=base_renamu_2022/`     | Un dataset Parquet para el CSV principal extraído        | Implementado localmente |

### Transformaciones permitidas en Bronze

En Bronze se permiten únicamente transformaciones técnicas mínimas:

- Conversión desde CSV u otro recurso tabular seleccionado hacia Parquet.
- Normalización técnica de nombres de columnas a un formato estable para Spark, Parquet y consultas posteriores.
- Resolución determinística de nombres de columnas duplicados después de normalizar.
- Inclusión de metadata técnica de procesamiento.
- Separación física de datasets mediante carpetas `resource_key=<valor>`.
- Escritura con compresión Parquet, según configuración del proceso Spark.

### Transformaciones no aplicadas en Bronze

Bronze no debe aplicar:

- Limpieza de negocio profunda.
- Conversión semántica definitiva de tipos.
- Corrección de montos, porcentajes, fechas, ubigeos o nombres de municipalidades.
- Eliminación agresiva de registros.
- Integración entre fuentes.
- Unión de tablas prediales.
- Selección de variables analíticas finales.
- Definición de hechos y dimensiones.
- Definición del modelo Gold.
- Creación de tablas externas Hive.
- Diseño del modelo Power BI.

Estas decisiones corresponden a etapas posteriores del proyecto: profiling, calidad, Silver, Gold, Hive y Power BI.

### Metadata técnica Bronze

La metadata Bronze permite rastrear cada dataset generado hacia su fuente original. Las columnas agregadas dependen de cada builder, pero siguen un criterio común.

Metadata común:

| Columna                   | Descripción                                       |
| ------------------------- | ------------------------------------------------- |
| `bronze_source_name`      | Nombre lógico de la fuente procesada.             |
| `bronze_resource_key`     | Identificador lógico del recurso convertido.      |
| `bronze_source_file_name` | Nombre del archivo de origen leído desde Landing. |
| `bronze_source_file_path` | Ruta local del archivo origen en Landing.         |
| `bronze_processed_at_utc` | Fecha y hora UTC de procesamiento Bronze.         |

Metadata adicional para MEF ingresos:

| Columna                     | Descripción                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `bronze_source_year`        | Año asociado al recurso MEF, cuando aplica.                  |
| `bronze_source_granularity` | Granularidad declarada del recurso: anual, mensual o diaria. |

Metadata adicional para meta predial:

| Columna                  | Descripción                                                     |
| ------------------------ | --------------------------------------------------------------- |
| `bronze_source_role`     | Rol configurado del recurso predial convertido.                 |
| `bronze_source_priority` | Prioridad operativa configurada para el recurso, cuando aplica. |

Metadata adicional para RENAMU:

| Columna              | Descripción                                |
| -------------------- | ------------------------------------------ |
| `bronze_source_year` | Año asociado al recurso RENAMU convertido. |

La columna `run_id` forma parte de la auditoría de ingesta, pero no se documenta como columna Bronze porque los builders Bronze actuales no la agregan al dataset Parquet.

### Recursos excluidos de Bronze

Bronze convierte únicamente recursos tabulares seleccionados para procesamiento. No convierte como tablas principales:

- Diccionarios CSV de columnas.
- PDFs de documentación.
- ZIPs originales.
- Metadata local de descarga.
- Archivos `.part`.
- Auditoría local de ingesta.
- Logs locales.
- Reportes generados.

Estos recursos pueden conservarse en Landing o en carpetas locales de calidad/evidencia, pero no forman parte del contrato de datasets Bronze.

### Relación entre Landing y Bronze

| Criterio                       | Landing                                                 | Bronze                                     |
| ------------------------------ | ------------------------------------------------------- | ------------------------------------------ |
| Propósito                      | Preservar archivos originales                           | Convertir recursos seleccionados a Parquet |
| Formato                        | CSV, ZIP, PDF, JSON, archivos extraídos, metadata local | Parquet                                    |
| Transformación                 | Ninguna                                                 | Técnica mínima                             |
| Nombres de columnas            | Originales                                              | Normalizados técnicamente                  |
| Tipado semántico               | No aplica                                               | No se aplica tipado fuerte de negocio      |
| Integración                    | No aplica                                               | No integra fuentes                         |
| Versionamiento de datos reales | No se versionan                                         | No se versionan                            |
| Uso posterior                  | Respaldo y reprocesamiento                              | Profiling, calidad y Silver                |

Bronze cumple el requisito de trabajar con Parquet desde una etapa temprana del pipeline, sin adelantar decisiones que corresponden a Silver, Gold, Hive o Power BI.

## Profiling y Quality Gates

Después de Bronze se ejecutan procesos de profiling y calidad para entender la estructura real de los datos antes de tomar decisiones de modelado.

El profiling evalúa:

- Columnas disponibles.
- Tipos inferidos.
- Conteo de registros.
- Porcentaje de nulos.
- Duplicados.
- Valores únicos.
- Valores frecuentes.
- Llaves candidatas.
- Problemas de integración.
- Riesgos para Silver y Gold.

Los quality gates evalúan reglas como:

- Existencia de columnas críticas.
- Nulos en campos relevantes.
- Duplicados por llave candidata.
- Montos negativos.
- Años fuera de rango.
- Porcentajes fuera de 0 a 100.
- Ubigeos vacíos o inválidos.

El objetivo de esta etapa es evitar que Silver y Gold se diseñen con supuestos no validados.

## Capa Silver

La capa Silver contiene datos limpios, tipados, estandarizados e integrables.

Objetivos de Silver:

- Corregir tipos de datos.
- Normalizar montos, porcentajes, fechas y periodos.
- Estandarizar nombres de municipalidades.
- Normalizar ubigeos y jerarquías territoriales.
- Seleccionar variables relevantes.
- Identificar registros problemáticos.
- Preparar llaves de integración entre fuentes.

En Silver se integran progresivamente las fuentes usando llaves geográficas o administrativas, según lo que confirme el profiling.

Posibles llaves de integración:

- Ubigeo.
- Código de entidad.
- Nombre normalizado de municipalidad.
- Combinación de departamento, provincia y distrito.

La integración debe documentar cobertura, registros que no cruzan y limitaciones.

## Capa Gold

La capa Gold contiene datasets listos para análisis y consumo desde Power BI.

Objetivos de Gold:

- Crear marts analíticos.
- Definir KPIs.
- Consolidar información municipal.
- Facilitar consultas desde Hive.
- Alimentar el modelo semántico de Power BI.

Marts esperados:

- Mart de ingresos municipales.
- Mart de cumplimiento predial.
- Mart de contexto territorial.
- Mart o vista consolidada para análisis ejecutivo, si los datos lo justifican.

El modelo Gold no se define de forma definitiva al inicio del proyecto. Se decidirá después del profiling y de la integración Silver.

Opciones posibles:

- Modelo estrella.
- Copo de nieve parcial.
- Marts planos.
- Combinación pragmática según la granularidad real de los datos.

## Apache Spark

Apache Spark es el motor principal de procesamiento del proyecto.

Se utilizará para:

- Leer archivos desde Landing.
- Convertir datos a Parquet en Bronze.
- Ejecutar transformaciones Silver.
- Construir marts Gold.
- Calcular métricas de calidad.
- Procesar volúmenes de datos mayores a los que serían cómodos con herramientas manuales.

El procesamiento será batch. No se contempla streaming.

## Apache Hive

Apache Hive funcionará como catálogo SQL del lakehouse.

Se utilizará para:

- Crear bases de datos lógicas para Bronze, Silver y Gold.
- Registrar tablas externas sobre archivos Parquet.
- Consultar datos procesados mediante SQL.
- Validar conteos y consultas analíticas.
- Servir como punto de conexión preferente para Power BI mediante HiveServer2/ODBC.

Hive no moverá los datos ni reemplazará a Spark. Su función será catalogar y exponer los Parquet como tablas consultables.

## Parquet

Parquet será el formato principal de almacenamiento desde la capa Bronze.

Razones para usar Parquet:

- Formato columnar.
- Mejor compresión que CSV en escenarios analíticos.
- Mejor rendimiento para lectura selectiva de columnas.
- Compatibilidad con Spark y Hive.
- Adecuado para pipelines batch y lakehouse local.

Landing puede conservar formatos originales como CSV, ZIP o PDF, pero Bronze, Silver y Gold trabajarán con Parquet.

En Spark, un dataset Parquet normalmente se materializa como una carpeta con archivos `part-*.parquet` y archivos auxiliares de ejecución. Por ello, no se debe esperar necesariamente un único archivo `.parquet` por recurso.

## Power BI

Power BI será la herramienta de visualización y análisis final.

La conexión recomendada será:

```text
Gold Parquet
-> Hive External Tables
-> HiveServer2
-> ODBC/JDBC
-> Power BI Desktop en modo Import
```

El modo Import es el recomendado porque el proyecto es batch y no requiere consultas en tiempo real.

Si la conexión local entre Power BI y Hive no es estable, se usará un fallback controlado exportando Gold a CSV o Parquet. Este fallback no reemplaza Hive; solo asegura la entrega final del dashboard.

## Flujo de datos

El flujo técnico esperado es:

1. Las fuentes públicas se descargan o recolectan hacia Landing.
2. Landing preserva archivos originales.
3. Spark convierte recursos seleccionados desde Landing hacia Bronze Parquet.
4. Se ejecuta profiling y calidad sobre Bronze.
5. Spark limpia y estandariza datos hacia Silver Parquet.
6. Spark integra fuentes municipales en Silver.
7. Se define el modelo Gold según evidencia real.
8. Spark construye marts Gold en Parquet.
9. Hive registra tablas externas sobre Bronze, Silver y Gold.
10. Power BI consume preferentemente tablas Gold desde Hive.
11. Se generan evidencias, reportes e insights finales.

## Auditoría y trazabilidad

La arquitectura considera auditoría técnica de ingesta y procesamiento.

La auditoría debe permitir conocer:

- Qué fuente fue procesada.
- Cuándo inició y terminó la ejecución.
- Cuántos intentos de descarga se realizaron.
- Qué errores ocurrieron.
- Qué archivo se obtuvo.
- Qué tamaño tuvo el archivo.
- Qué checksum permite validar integridad.
- Cuál fue el estado final de la ejecución.

Esta auditoría complementa la calidad de datos y permite defender la trazabilidad del pipeline.

La auditoría local de ingesta se conserva fuera del contrato de datasets Bronze y no debe versionarse si contiene salidas generadas por ejecuciones reales.

## Organización local de rutas

Las rutas internas del proyecto se resuelven desde `src/common/paths.py` usando `pathlib`.

No se usará `.env` para definir rutas internas como:

- `data/landing`
- `data/bronze`
- `data/silver`
- `data/gold`
- `data/quality`

El archivo `.env.example` se reserva para variables dependientes del entorno, como nivel de logs, Spark master, host de Hive y puerto de HiveServer2.

## Relación entre componentes

| Componente    | Responsabilidad                                                           |
| ------------- | ------------------------------------------------------------------------- |
| Landing       | Preservar archivos originales                                             |
| Bronze        | Convertir recursos tabulares seleccionados a Parquet con metadata técnica |
| Profiling     | Entender estructura, calidad y riesgos de los datos                       |
| Quality Gates | Validar reglas mínimas antes de avanzar                                   |
| Silver        | Limpiar, tipar, estandarizar e integrar                                   |
| Gold          | Construir datasets analíticos listos para BI                              |
| Spark         | Procesar datos por capas                                                  |
| Hive          | Catalogar Parquet como tablas SQL externas                                |
| Power BI      | Visualizar indicadores y construir el reporte final                       |

## Decisiones clave

- El proyecto es local, no cloud.
- Bronze, Silver y Gold usarán Parquet.
- Landing conserva los archivos originales.
- Bronze organiza sus salidas por `resource_key`, sin definir todavía particionamiento analítico final.
- Bronze evita inferencia agresiva y no aplica tipado semántico fuerte de negocio.
- Silver limpia e integra.
- Gold se decide después del profiling y Silver.
- Hive se usa como catálogo SQL real.
- Power BI consume preferentemente desde Hive.
- CSV o Parquet para Power BI solo se usan como fallback controlado.
- Las rutas internas no dependen de `.env`.
- Los datos reales no se versionan en GitHub.

## Resultado esperado de la arquitectura

La arquitectura debe permitir demostrar:

- Ingeniería de datos con Spark, Hive y Parquet.
- Separación clara de capas Medallion.
- Trazabilidad desde fuente hasta dashboard.
- Calidad de datos documentada.
- Auditoría de ingesta y procesamiento.
- Modelo analítico construido con evidencia real.
- Power BI conectado preferentemente a una capa SQL mediante Hive.
