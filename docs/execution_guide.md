# Guía de ejecución local

## Propósito

Este documento describe la preparación, ejecución y validación del entorno local del proyecto **Municipal Revenue Big Data Analytics**.

La guía permite reproducir el entorno técnico necesario para trabajar con Python, Apache Spark, Apache Hive, Parquet y Power BI Desktop en una máquina local.

El documento se enfoca en la operación del entorno. La arquitectura, fuentes de datos, profiling, calidad, modelado y Power BI se documentan en archivos técnicos separados.

## Alcance

Esta guía cubre:

- Preparación del entorno Python.
- Instalación de dependencias.
- Validación de rutas internas.
- Ejecución de scripts de discovery.
- Ejecución del profiling inicial.
- Levantamiento del entorno Spark y Hive con Docker Compose.
- Validación de Spark Master y Spark Worker.
- Validación de Hive Metastore y HiveServer2.
- Consideraciones iniciales para conexión futura con Power BI.

No cubre:

- Ingesta final de fuentes hacia Landing.
- Construcción de capas Bronze, Silver y Gold.
- Creación de tablas externas Hive.
- Construcción del reporte Power BI.
- Ejecución completa del pipeline analítico final.

Estas actividades se incorporarán a la guía cuando las respectivas capacidades estén disponibles en el proyecto.

## Requisitos previos

Se recomienda contar con:

- Windows 10 o Windows 11.
- Git.
- Python 3.11.
- Docker Desktop.
- PowerShell.
- Visual Studio Code.
- Power BI Desktop.
- Conexión a internet para instalar dependencias y descargar imágenes Docker.

## Estructura esperada del repositorio

El repositorio debe estar clonado localmente y mantener una estructura similar a:

```text
municipal-revenue-bigdata-analytics/
|-- config/
|-- data/
|-- docs/
|-- evidence/
|-- logs/
|-- notebooks/
|-- powerbi/
|-- reports/
|-- src/
|-- tests/
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
|-- requirements-dev.txt
|-- .env.example
```

Los datos reales no deben versionarse. Las carpetas de datos se conservan en Git mediante archivos `.gitkeep`.

## Entorno Python local

Desde la raíz del proyecto, crear el entorno virtual:

```powershell
py -m venv .venv
```

Activar el entorno:

```powershell
.venv\Scripts\Activate.ps1
```

Actualizar `pip`:

```powershell
python -m pip install --upgrade pip
```

Instalar dependencias de desarrollo:

```powershell
pip install -r requirements-dev.txt
```

Si el sistema no reconoce `python`, se puede usar `py`:

```powershell
py -m pip install --upgrade pip
py -m pip install -r requirements-dev.txt
```

Validar versión de Python:

```powershell
python --version
```

Validar dependencias principales:

```powershell
python -c "import pandas; import pyarrow; import pyspark; print('Dependencias principales disponibles')"
```

## Variables de entorno

El archivo versionado es:

```text
.env.example
```

Este archivo contiene variables públicas y mínimas del entorno local:

```env
PROJECT_NAME=municipal-revenue-bigdata-analytics
ENVIRONMENT=local
LOG_LEVEL=INFO

SPARK_APP_NAME=MunicipalRevenueLakehouse
SPARK_MASTER=local[*]

HIVE_HOST=localhost
HIVE_PORT=10000
HIVE_DATABASE_BRONZE=bronze
HIVE_DATABASE_SILVER=silver
HIVE_DATABASE_GOLD=gold

POWERBI_CONNECTION_MODE=hive_import
```

El archivo `.env` real no debe subirse al repositorio.

Las rutas internas del proyecto no se definen en `.env`. Se resuelven desde:

```text
src/common/paths.py
```

Este criterio evita rutas absolutas dependientes de una máquina específica y mantiene el proyecto portable.

## Validación de rutas internas

Ejecutar:

```powershell
python -c "from src.common.paths import PROJECT_ROOT, LANDING_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR; print(PROJECT_ROOT); print(LANDING_DIR); print(BRONZE_DIR); print(SILVER_DIR); print(GOLD_DIR)"
```

El resultado debe mostrar rutas ubicadas dentro del repositorio local.

## Discovery inicial de fuentes

Los scripts de discovery e ingesta inicial validan conectividad y metadatos técnicos de fuentes candidatas. En el estado actual del proyecto, estos scripts también están preparados para descarga controlada hacia Landing, pero para validación inicial se recomienda usar `--dry-run`.

Ejecutar validaciones sin descarga pesada:

```powershell
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
python -m src.ingestion.download_renamu --all-enabled --dry-run
```

Resultado esperado:

- Estado HTTP de recursos seleccionados.
- Tipo de contenido.
- Tamaño declarado, si existe.
- Validación de reintentos HTTP y fallback de `HEAD` a `GET` cuando corresponda.
- Registro de eventos de inicio y fin en la auditoría local.
- Ausencia de archivos descargados cuando se usa `--dry-run`.

Los hallazgos específicos de acceso a fuentes se documentan en:

```text
docs/source_discovery.md
```

## Validación de ingesta con auditoría y reintentos

Antes de ejecutar descargas completas hacia Landing, se recomienda validar los scripts de ingesta en modo `--dry-run`.

Estos comandos validan disponibilidad, estado HTTP, tipo de contenido, tamaño declarado, reintentos y fallback de validación sin descargar archivos pesados.

### Validar MEF ingresos

```powershell
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
```

### Validar meta predial

```powershell
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
```

### Validar RENAMU 2022

```powershell
python -m src.ingestion.download_renamu --all-enabled --dry-run
```

### Revisar auditoría local

```powershell
Get-Content data/quality/ingestion_audit.jsonl -Tail 20
```

En modo `--dry-run`, la auditoría registra principalmente eventos de inicio y fin de ejecución. Los resultados de recursos descargados se registran cuando se ejecuta una descarga real.

### Validar que no se versionen datos ni auditorías locales

```powershell
git status --short
```

No deben aparecer archivos como:

```text
data/landing/
data/quality/ingestion_audit.jsonl
*.csv
*.zip
*.xlsx
*.pdf
*.parquet
```

Si aparecen archivos de datos o auditoría local en `git status`, se debe revisar `.gitignore` antes de hacer commit.

### Criterio operativo

La descarga completa de fuentes debe ejecutarse solo después de validar:

- Configuración de fuentes en `config/sources.yaml`.
- Política de reintentos en `config/retry_policy.yaml`.
- Auditoría local en `config/audit.yaml`.
- Funcionamiento de los scripts con `--dry-run`.
- Exclusión de archivos reales mediante `.gitignore`.

La ingesta hacia Landing no transforma datos, no genera Bronze y no interpreta columnas de negocio.

## Profiling inicial

El profiling inicial analiza archivos locales disponibles en Landing.

Ejecutar:

```powershell
python -m src.quality.profile_sources
```

Si todavía no existen archivos en `data/landing`, el script indicará que no encontró archivos soportados y generará un reporte local vacío.

También puede indicarse una carpeta específica:

```powershell
python -m src.quality.profile_sources --input-dir data/landing --max-rows 10000
```

El reporte local se genera en:

```text
reports/profiling_summary.json
```

Los reportes generados localmente no deben subirse si contienen resultados pesados o derivados de datos reales.

## Validación de Docker Desktop

Antes de usar Docker Compose, validar que Docker Desktop esté activo:

```powershell
docker version
```

La salida debe mostrar información de `Client` y `Server`.

También se puede validar con:

```powershell
docker info
```

Si solo aparece información de `Client` y falla la conexión con `Server`, Docker Desktop no está corriendo correctamente o el backend Linux no está activo.

## Ejecución con Docker Compose

Los comandos de Docker Compose deben ejecutarse desde la raíz del proyecto, donde se encuentra `docker-compose.yml`.

Validar la configuración efectiva:

```powershell
docker compose config
```

Este comando no levanta contenedores. Solo muestra la configuración interpretada por Docker Compose.

Descargar imágenes base:

```powershell
docker compose pull
```

Imágenes principales usadas por el entorno:

- `apache/spark:4.0.3-python3`
- `apache/hive:4.1.0`
- Imagen local construida para `python-app`

Construir imagen Python del proyecto:

```powershell
docker compose build python-app
```

Para una reconstrucción limpia:

```powershell
docker compose build --no-cache python-app
```

Levantar servicios:

```powershell
docker compose up -d
```

Verificar estado:

```powershell
docker compose ps
```

Servicios esperados:

| Servicio           | Puerto | Uso                            |
| ------------------ | -----: | ------------------------------ |
| Spark Master       |   7077 | Coordinación del clúster Spark |
| Spark Master UI    |   8080 | Interfaz web de Spark Master   |
| Spark Worker UI    |   8081 | Interfaz web del worker Spark  |
| Hive Metastore     |   9083 | Catálogo de metadatos Hive     |
| HiveServer2        |  10000 | Conexión JDBC/ODBC             |
| HiveServer2 Web UI |  10002 | Interfaz web de HiveServer2    |

El contenedor `municipal_revenue_python` puede finalizar con estado `Exited (0)` porque ejecuta `python --version` y termina correctamente. Ese comportamiento no representa un error.

## Validación de Apache Spark

Abrir en navegador:

```text
http://localhost:8080
```

Resultado esperado:

- Spark Master visible.
- Estado activo.
- Worker registrado.

Abrir también:

```text
http://localhost:8081
```

Resultado esperado:

- Spark Worker visible.
- Cores disponibles.
- Memoria asignada.

También pueden revisarse logs:

```powershell
docker logs municipal_revenue_spark_master --tail 80
docker logs municipal_revenue_spark_worker --tail 80
```

## Validación de Apache Hive

Revisar que los servicios estén activos:

```powershell
docker compose ps -a
```

Revisar logs del metastore:

```powershell
docker logs municipal_revenue_hive_metastore --tail 80
```

Revisar logs de HiveServer2:

```powershell
docker logs municipal_revenue_hive_server --tail 80
```

Mensajes esperados en HiveServer2:

```text
ThriftBinaryCLIService on port 10000
Service:HiveServer2 is started
Web UI has started on port 10002
```

## Conexión con Beeline

Con HiveServer2 activo, conectarse mediante Beeline:

```powershell
docker exec -it municipal_revenue_hive_server beeline -u "jdbc:hive2://localhost:10000/"
```

Resultado esperado:

```text
Connected to: Apache Hive
Driver: Hive JDBC
Beeline version
```

Validar bases disponibles:

```sql
SHOW DATABASES;
```

Resultado esperado inicial:

```text
default
```

Salir de Beeline:

```sql
!quit
```

## Rol de Hive en el proyecto

Hive funciona como catálogo SQL del lakehouse local.

En fases posteriores se crearán:

- Base `bronze`.
- Base `silver`.
- Base `gold`.
- Tablas externas sobre archivos Parquet.
- Consultas de validación.

En esta etapa se valida que Hive Metastore y HiveServer2 puedan levantarse y aceptar conexiones.

## Consideraciones para Power BI

La conexión recomendada para Power BI será:

```text
Gold Parquet
-> Hive External Tables
-> HiveServer2
-> Driver ODBC/JDBC
-> Power BI Desktop en modo Import
```

En esta etapa todavía no se conecta Power BI porque aún no existen tablas Gold.

Cuando las tablas Gold estén disponibles en Hive, se probará la conexión usando:

- Host: `localhost`.
- Puerto: `10000`.
- Base: `gold`.
- Modo recomendado: `Import`.

Si la conexión local Power BI - Hive no es estable, se usará un fallback controlado exportando Gold a CSV o Parquet. Este fallback no reemplaza el uso de Hive.

## Detención de servicios

Detener servicios sin borrar volúmenes:

```powershell
docker compose down
```

Detener servicios y eliminar volúmenes locales asociados:

```powershell
docker compose down -v
```

Usar `-v` solo si se desea limpiar completamente el estado local de los servicios. Si ya existen tablas Hive o metadata útil, no usar `-v`.

## Limpieza de imágenes y espacio

Ver imágenes descargadas:

```powershell
docker images
```

Ver uso de espacio:

```powershell
docker system df
```

Limpiar recursos no usados:

```powershell
docker system prune
```

No usar `docker system prune -a` sin revisar, porque puede borrar imágenes de otros proyectos que luego tendrían que descargarse nuevamente.

## Validaciones recomendadas por etapa

### Antes de ingesta

Validar scripts de ingesta en modo `--dry-run`:

```powershell
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
python -m src.ingestion.download_renamu --all-enabled --dry-run
```

Revisar auditoría local:

```powershell
Get-Content data/quality/ingestion_audit.jsonl -Tail 20
```

### Antes de Bronze

Confirmar existencia de archivos locales en Landing:

```powershell
Get-ChildItem data/landing -Recurse
```

### Antes de Hive

Confirmar existencia de archivos Parquet en Bronze, Silver o Gold:

```powershell
Get-ChildItem data -Recurse -Filter *.parquet
```

### Antes de Power BI

Confirmar que HiveServer2 está activo:

```powershell
docker compose ps
```

Validar conexión con Beeline:

```powershell
docker exec -it municipal_revenue_hive_server beeline -u "jdbc:hive2://localhost:10000/"
```

## Problemas comunes

### Docker Desktop no está iniciado

Síntoma:

```text
failed to connect to the docker API
dockerDesktopLinuxEngine
```

Acción recomendada:

- Abrir Docker Desktop.
- Esperar a que el motor Docker esté activo.
- Ejecutar `docker version` y `docker info`.

### Puerto ocupado

Síntoma:

```text
port is already allocated
```

Acción recomendada:

- Verificar si otro contenedor o programa usa el puerto.
- Revisar contenedores activos con `docker ps`.
- Detener servicios anteriores si corresponde.

### Imagen Python falla por OpenJDK

Síntoma:

```text
Package openjdk-17-jre-headless is not available
```

Acción recomendada:

Usar una base estable en el `Dockerfile`:

```dockerfile
FROM python:3.11-slim-bookworm
```

### Hive Metastore falla por schema

Síntoma:

```text
Version information not found in metastore
```

Acción recomendada en una primera ejecución limpia:

```powershell
docker compose down -v
docker compose up -d
```

También se recomienda no usar `IS_RESUME: "true"` en la primera inicialización del entorno.

### HiveServer2 rechaza conexión

Síntoma:

```text
Could not open client transport
Connection refused
```

Acciones recomendadas:

- Esperar unos segundos después de levantar contenedores.
- Verificar estado con `docker compose ps -a`.
- Revisar logs de `municipal_revenue_hive_server`.
- Revisar logs de `municipal_revenue_hive_metastore`.
- Confirmar que HiveServer2 indique en logs que inició el servicio en el puerto `10000`.

### Warnings de SLF4J o Log4j

Durante Beeline pueden aparecer mensajes como:

```text
SLF4J: Class path contains multiple SLF4J bindings
```

Estos mensajes son advertencias de librerías Java y no necesariamente indican error.

El criterio de éxito es que Beeline muestre:

```text
Connected to: Apache Hive
```

## Criterios de versionamiento

No subir al repositorio:

- `.env`
- `.venv`
- Archivos CSV reales.
- Archivos ZIP reales.
- Archivos XLSX reales.
- Archivos Parquet.
- Logs pesados.
- Reportes generados con datos reales.
- Exports pesados de Power BI.

Sí subir:

- Código fuente.
- Documentación.
- Configuración pública.
- `.env.example`.
- `.gitkeep`.
- SQL.
- Evidencias controladas y ligeras cuando correspondan.

## Validación mínima del entorno local

Para considerar validado el entorno local, se debe comprobar:

- `docker compose config` ejecuta sin errores.
- `docker compose pull` descarga imágenes principales.
- `docker compose build python-app` construye la imagen Python.
- `docker compose up -d` levanta servicios.
- Spark Master responde en `http://localhost:8080`.
- Spark Worker responde en `http://localhost:8081`.
- Hive Metastore permanece en estado `Up`.
- HiveServer2 permanece en estado `Up`.
- Beeline conecta a `jdbc:hive2://localhost:10000/`.
- `SHOW DATABASES;` responde correctamente.

## Estado del entorno

El entorno local queda preparado para las siguientes fases del proyecto:

- Descarga local completa y controlada hacia Landing.
- Revisión de auditoría de ingesta generada localmente.
- Conversión a Bronze Parquet.
- Ejecución de reglas de calidad.
- Transformaciones Silver.
- Registro de tablas externas Hive.
- Construcción de marts Gold.
- Consumo analítico desde Power BI.
