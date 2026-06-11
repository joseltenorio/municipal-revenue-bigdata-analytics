# Arquitectura del proyecto

## Nombre del proyecto

**Municipal Revenue Big Data Analytics**

## Propósito de la arquitectura

La arquitectura del proyecto está diseñada para construir un flujo local de ingeniería y analítica de datos que permita procesar, validar, integrar y analizar información municipal peruana proveniente de fuentes públicas.

El proyecto no se limita a construir un reporte final. Su objetivo es implementar un lakehouse analítico local con capas claramente separadas, trazabilidad de datos, procesamiento distribuido con Apache Spark, almacenamiento en Parquet, catálogo SQL con Apache Hive y visualización final en Power BI.

## Enfoque general

El proyecto sigue una arquitectura Medallion:

Fuentes públicas
-> Landing
-> Bronze Parquet
-> Profiling y Quality Gates
-> Silver Parquet
-> Gold Parquet / Marts analíticos
-> Hive External Tables
-> Power BI conectado preferentemente a Hive

Esta arquitectura separa los datos según su nivel de procesamiento, calidad y orientación analítica.

## Fuentes públicas

Las fuentes consideradas son:

- Presupuesto y ejecución de ingresos del MEF / SIAF.
- Seguimiento de la meta del impuesto predial desde SISMERE / MEF.
- Registro Nacional de Municipalidades RENAMU 2022 del INEI.

Cada fuente será primero explorada para confirmar su método real de acceso, formato, estructura, granularidad y limitaciones.

## Capa Landing

La capa Landing conserva los archivos originales tal como se obtienen desde las fuentes públicas.

Puede contener archivos como:

- CSV.
- ZIP.
- XLSX.
- JSON.
- Archivos extraídos desde paquetes comprimidos.

Características principales:

- No transforma datos.
- No cambia nombres de columnas.
- No corrige tipos.
- No elimina registros.
- Conserva evidencia del origen.
- Sirve como respaldo para reprocesamiento.

Los datos reales de Landing no se versionan en GitHub. Solo se mantiene la estructura de carpetas mediante archivos `.gitkeep`.

## Capa Bronze

La capa Bronze convierte las fuentes originales hacia formato Parquet.

Objetivos de Bronze:

- Estandarizar el almacenamiento físico en Parquet.
- Mantener la granularidad original de la fuente.
- Aplicar limpieza técnica mínima.
- Agregar metadata de ingesta.
- Preparar los datos para profiling, calidad y procesamiento posterior.

Transformaciones permitidas en Bronze:

- Conversión a Parquet.
- Normalización técnica de nombres de columnas.
- Inclusión de columnas de metadata como fuente, archivo origen, fecha de ingesta y `run_id`.
- Particionamiento básico si corresponde.

Transformaciones no recomendadas en Bronze:

- Limpieza de negocio profunda.
- Eliminación agresiva de registros.
- Integración entre fuentes.
- Corrección compleja de valores.
- Definición del modelo analítico final.

Bronze cumple el requisito de trabajar con Parquet desde una etapa temprana del pipeline.

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

Landing puede conservar formatos originales como CSV o ZIP, pero Bronze, Silver y Gold trabajarán con Parquet.

## Power BI

Power BI será la herramienta de visualización y análisis final.

La conexión recomendada será:

Gold Parquet
-> Hive External Tables
-> HiveServer2
-> ODBC/JDBC
-> Power BI Desktop en modo Import

El modo Import es el recomendado porque el proyecto es batch y no requiere consultas en tiempo real.

Si la conexión local entre Power BI y Hive no es estable, se usará un fallback controlado exportando Gold a CSV o Parquet. Este fallback no reemplaza Hive; solo asegura la entrega final del dashboard.

## Flujo de datos

El flujo técnico esperado es:

1. Las fuentes públicas se descargan o recolectan hacia Landing.
2. Landing preserva archivos originales.
3. Spark convierte Landing hacia Bronze Parquet.
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

| Componente    | Responsabilidad                                     |
| ------------- | --------------------------------------------------- |
| Landing       | Preservar archivos originales                       |
| Bronze        | Convertir fuentes a Parquet con metadata técnica    |
| Profiling     | Entender estructura, calidad y riesgos de los datos |
| Quality Gates | Validar reglas mínimas antes de avanzar             |
| Silver        | Limpiar, tipar, estandarizar e integrar             |
| Gold          | Construir datasets analíticos listos para BI        |
| Spark         | Procesar datos por capas                            |
| Hive          | Catalogar Parquet como tablas SQL externas          |
| Power BI      | Visualizar indicadores y construir el reporte final |

## Decisiones clave

- El proyecto es local, no cloud.
- Bronze, Silver y Gold usarán Parquet.
- Landing conserva los archivos originales.
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
