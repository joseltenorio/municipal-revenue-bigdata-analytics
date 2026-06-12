# Guía de Ejecución Local de Punta a Punta

## Propósito

Esta guía explica cómo preparar y usar el proyecto **Municipal Revenue Big Data Analytics** desde cero en una máquina local. Está pensada para una persona que acaba de clonar el repositorio y todavía no tiene configurados Git, Python, Docker, Hive ni Power BI.

El documento cubre instalación, validación del entorno, ejecución modular por capas, registro de tablas Hive y opciones de consumo desde Power BI. No reemplaza la documentación especializada de arquitectura, calidad, modelo Gold o Power BI; funciona como guía operativa central.

## Alcance

Esta guía cubre:

- Requisitos previos de software.
- Clonado del repositorio.
- Entorno virtual Python en Windows PowerShell.
- Variables locales.
- Docker Compose, Spark, Hive Metastore y HiveServer2.
- Validaciones mínimas del entorno.
- Ejecución modular del flujo por capas.
- Registro de tablas externas Hive.
- Consumo desde Power BI con ODBC y fallback CSV.
- Problemas comunes.

No cubre:

- Explicación detallada del modelo semántico Power BI.
- Diseño visual del dashboard.
- Reglas internas completas de calidad.
- Documentación exhaustiva de cada columna.

Para esos temas usar:

- `docs/architecture.md`
- `docs/data_quality.md`
- `docs/gold_model.md`
- `docs/hive_model.md`
- `docs/powerbi_model.md`
- `docs/powerbi_hive_connection.md`
- `powerbi/README.md`

## 1. Requisitos Previos

Instalar en la máquina local:

- **Git** para clonar el repositorio.
- **Python 3.11** o una versión compatible con las dependencias del proyecto.
- **Docker Desktop** con backend Linux activo.
- **PowerShell** en Windows.
- **Power BI Desktop** para abrir o construir el reporte.
- **Driver ODBC de Apache Hive** o compatible, preferentemente de 64 bits, para conectar Power BI con HiveServer2.
- **Visual Studio Code**, opcional pero recomendado.

También se requiere conexión a internet para:

- Clonar el repositorio.
- Instalar dependencias Python.
- Descargar imágenes Docker la primera vez.
- Descargar fuentes públicas si se ejecuta la ingesta real.

## 2. Clonar el Repositorio

Desde PowerShell:

```powershell
git clone <URL_DEL_REPOSITORIO>
cd municipal-revenue-bigdata-analytics
```

Todos los comandos siguientes deben ejecutarse desde la raíz del proyecto, donde se encuentran `README.md`, `docker-compose.yml`, `requirements.txt` y `requirements-dev.txt`.

Validar ubicación:

```powershell
Get-Location
```

La ruta debe terminar en:

```text
municipal-revenue-bigdata-analytics
```

## 3. Crear el Entorno Virtual Python

Crear `.venv`:

```powershell
py -m venv .venv
```

Activar el entorno:

```powershell
.venv\Scripts\Activate.ps1
```

Si PowerShell bloquea la activación por política de ejecución, abrir una consola como usuario normal y ejecutar:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Luego activar nuevamente:

```powershell
.venv\Scripts\Activate.ps1
```

Actualizar `pip`:

```powershell
python -m pip install --upgrade pip
```

Instalar dependencias de ejecución:

```powershell
pip install -r requirements.txt
```

Instalar dependencias de desarrollo y validación:

```powershell
pip install -r requirements-dev.txt
```

Validar Python:

```powershell
python --version
```

Validar dependencias principales:

```powershell
python -c "import pyarrow; import pyspark; import yaml; print('Dependencias principales disponibles')"
```

## 4. Configurar Variables Locales

El archivo versionado es:

```text
.env.example
```

Si se necesita un `.env` local:

```powershell
Copy-Item .env.example .env
```

El archivo `.env` real no debe subirse al repositorio.

Las rutas internas del proyecto no se configuran en `.env`. Se resuelven de forma centralizada en:

```text
src/common/paths.py
```

Ese módulo deriva rutas desde la raíz del repositorio, por ejemplo:

- `data/landing`
- `data/bronze`
- `data/silver`
- `data/gold`
- `data/quality`
- `reports`
- `powerbi`

Validar rutas:

```powershell
python -c "from src.common.paths import PROJECT_ROOT, LANDING_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR; print(PROJECT_ROOT); print(LANDING_DIR); print(BRONZE_DIR); print(SILVER_DIR); print(GOLD_DIR)"
```

Todas las rutas deben estar dentro del repositorio local.

## 5. Levantar Servicios Docker

Validar Docker Desktop:

```powershell
docker version
docker info
```

Si `docker info` no responde, abrir Docker Desktop y esperar a que el motor Linux esté activo.

Validar configuración Docker Compose:

```powershell
docker compose config
```

Levantar servicios:

```powershell
docker compose up -d
```

La primera ejecución puede tardar porque Docker descargará imágenes base y construirá la imagen local cuando corresponda.

Validar contenedores:

```powershell
docker compose ps
```

Servicios y puertos principales:

| Servicio | Puerto | Uso |
| --- | ---: | --- |
| Spark Master | 7077 | Coordinación del clúster Spark |
| Spark Master UI | 8080 | Interfaz web del master |
| Spark Worker UI | 8081 | Interfaz web del worker |
| Hive Metastore | 9083 | Catálogo de metadatos Hive |
| HiveServer2 | 10000 | Conexión JDBC/ODBC |
| HiveServer2 Web UI | 10002 | Interfaz web de HiveServer2 |

Abrir en navegador:

```text
http://localhost:8080
http://localhost:8081
```

El contenedor `python-app` puede finalizar con `Exited (0)` si solo ejecuta una validación corta. Eso no indica error.

## 6. Validar HiveServer2 con Beeline

Con Docker activo:

```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
```

Si el entorno está recién levantado y todavía no se aplicaron DDLs, puede aparecer solo `default`. Después de registrar tablas externas deben aparecer:

```text
bronze
silver
gold
```

Validar tablas Gold, si ya existen Parquet Gold y DDL aplicado:

```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN gold;"
```

Si no aparecen tablas Gold, revisar:

- Que `data/gold` exista y contenga Parquet generados.
- Que se haya ejecutado la generación de SQL Hive.
- Que se haya aplicado `sql/hive/create_gold_external_tables.sql`.

## 7. Ejecutar Tests

Ejecutar la suite disponible:

```powershell
python -m pytest
```

Para validaciones más acotadas, se pueden ejecutar tests por tema:

```powershell
python -m pytest tests/test_paths.py tests/test_config.py
python -m pytest tests/test_hive_external_tables.py
python -m pytest tests/test_gold_municipal_revenue.py tests/test_gold_predial_compliance.py tests/test_gold_territorial_context.py
```

Los tests no reemplazan la ejecución real del pipeline, pero permiten validar helpers, configuración y contratos de scripts.

## 8. Flujo Conceptual por Capas

El proyecto se ejecuta por módulos. No hay un único runner global para todo el lakehouse, por lo que el orden recomendado es:

1. Landing / Ingestion.
2. Bronze.
3. Quality Bronze.
4. Silver.
5. Quality Silver.
6. Integración Silver.
7. Gold.
8. Generación y registro de tablas Hive.
9. Power BI.

Los comandos siguientes son los módulos reales disponibles en el repositorio.

## 9. Landing e Ingesta

Validar fuentes sin descargar:

```powershell
python -m src.ingestion.run_all_ingestion --dry-run
```

Validar fuentes individuales:

```powershell
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
python -m src.ingestion.download_renamu --all-enabled --dry-run
```

Ejecutar ingesta real hacia `data/landing`:

```powershell
python -m src.ingestion.run_all_ingestion
```

La ingesta real descarga archivos locales. Esos archivos no deben versionarse.

## 10. Construir Bronze

Primero validar planes sin escribir:

```powershell
python -m src.bronze.build_bronze_mef_income --dry-run
python -m src.bronze.build_bronze_predial_goal --dry-run
python -m src.bronze.build_bronze_renamu --dry-run
```

Ejecutar construcción real, si los datos Landing ya existen:

```powershell
python -m src.bronze.build_bronze_mef_income
python -m src.bronze.build_bronze_predial_goal
python -m src.bronze.build_bronze_renamu
```

Las salidas se escriben en:

```text
data/bronze/
```

## 11. Ejecutar Calidad Bronze

Validar plan:

```powershell
python -m src.quality.run_quality_checks --dry-run
```

Ejecutar calidad real:

```powershell
python -m src.quality.run_quality_checks
```

Generar reporte HTML local:

```powershell
python -m src.quality.generate_quality_report
```

Salidas locales:

```text
data/quality/bronze_quality_results.jsonl
reports/data_quality_report.html
```

Estas salidas no deben versionarse.

## 12. Construir Silver

Validar planes:

```powershell
python -m src.silver.transform_mef_income --dry-run
python -m src.silver.transform_predial_goal --dry-run
python -m src.silver.transform_renamu --dry-run
```

Ejecutar transformaciones reales:

```powershell
python -m src.silver.transform_mef_income --overwrite
python -m src.silver.transform_predial_goal --overwrite
python -m src.silver.transform_renamu --overwrite
```

Construir integración Silver:

```powershell
python -m src.silver.integrate_municipal_sources --dry-run
python -m src.silver.integrate_municipal_sources --overwrite
```

Las salidas se escriben en:

```text
data/silver/
```

## 13. Ejecutar Calidad Silver

Validar plan:

```powershell
python -m src.quality.run_silver_quality_checks --dry-run
```

Ejecutar calidad real:

```powershell
python -m src.quality.run_silver_quality_checks
```

Generar reporte:

```powershell
python -m src.quality.generate_silver_quality_report
```

Salidas locales:

```text
data/quality/silver_quality_results.jsonl
reports/silver_quality_report.html
```

## 14. Construir Gold

Validar planes:

```powershell
python -m src.gold.build_municipal_revenue_marts --dry-run
python -m src.gold.build_predial_compliance_marts --dry-run
python -m src.gold.build_territorial_context_marts --dry-run
```

Ejecutar construcción real:

```powershell
python -m src.gold.build_municipal_revenue_marts --overwrite
python -m src.gold.build_predial_compliance_marts --overwrite
python -m src.gold.build_territorial_context_marts --overwrite
```

Las salidas se escriben en:

```text
data/gold/
```

## 15. Generar y Registrar Tablas Hive

Generar SQL de tablas externas desde los Parquet existentes:

```powershell
docker compose run --rm python-app python -m src.hive.generate_external_tables --overwrite-sql --validate-inputs
```

Aplicar DDLs:

```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -f /app/sql/hive/create_databases.sql
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -f /app/sql/hive/create_bronze_external_tables.sql
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -f /app/sql/hive/create_silver_external_tables.sql
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -f /app/sql/hive/create_gold_external_tables.sql
```

Validar catálogo:

```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW DATABASES;"
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN bronze;"
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN silver;"
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN gold;"
```

Validar consultas ligeras:

```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT COUNT(*) FROM gold.mart_municipal_capacity;"
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT * FROM gold.mart_municipal_revenue_overview LIMIT 5;"
```

## 16. Power BI

La ruta preferente es:

```text
Gold Parquet
-> Hive External Tables
-> HiveServer2
-> ODBC
-> Power BI Desktop en modo Import
```

Parámetros esperados:

| Parámetro | Valor |
| --- | --- |
| Host | `localhost` |
| Puerto | `10000` |
| Base | `gold` |
| Modo recomendado | Import |

Antes de abrir Power BI:

```powershell
docker compose ps
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN gold;"
```

Si la conexión ODBC local falla, existe fallback CSV:

```powershell
python -m src.powerbi.export_gold_fallback --dry-run
python -m src.powerbi.export_gold_fallback
```

Los CSV se generan bajo:

```text
powerbi/exports/
```

No deben versionarse.

Para el modelo semántico y el reporte, revisar:

- `docs/powerbi_model.md`
- `docs/powerbi_hive_connection.md`
- `powerbi/README.md`

## 17. Datos Reales y Archivos No Versionados

El repositorio versiona:

- Código fuente.
- Configuración pública.
- SQL.
- Tests.
- Documentación.
- Archivos `.gitkeep`.

No versiona:

- `.env`
- `.venv`
- `data/landing`
- `data/bronze`
- `data/silver`
- `data/gold`
- `data/quality`
- CSV, ZIP, XLSX, PDF, Parquet y JSONL generados.
- Logs pesados.
- Reportes HTML generados.
- Exports CSV de Power BI.
- Archivos `.pbix` pesados.

Una persona que clone el repositorio debe ejecutar la ingesta/procesamiento local, o colocar los datos esperados en las carpetas correspondientes antes de construir las capas.

## 18. Orden Recomendado Para Usuarios Nuevos

1. Instalar Git, Python, Docker Desktop, Power BI Desktop y driver ODBC Hive.
2. Clonar el repositorio.
3. Crear y activar `.venv`.
4. Instalar `requirements.txt` y `requirements-dev.txt`.
5. Copiar `.env.example` a `.env` si se requiere configuración local.
6. Levantar Docker con `docker compose up -d`.
7. Validar Spark UI y HiveServer2.
8. Ejecutar ingesta o colocar datos locales en `data/landing`.
9. Construir Bronze.
10. Ejecutar calidad Bronze.
11. Construir Silver.
12. Ejecutar calidad Silver.
13. Construir integración Silver.
14. Construir Gold.
15. Generar y aplicar DDLs Hive.
16. Validar `SHOW TABLES IN gold`.
17. Abrir Power BI y conectar por ODBC.
18. Usar fallback CSV solo si ODBC falla.

## 19. Troubleshooting

### Docker Desktop no inicia

Síntoma:

```text
Cannot connect to the Docker daemon
```

Acciones:

- Abrir Docker Desktop.
- Esperar a que el backend Linux esté activo.
- Ejecutar `docker version`.
- Ejecutar `docker compose ps`.

### Puerto 10000 ocupado

Síntoma:

```text
port is already allocated
```

Acciones:

- Revisar contenedores activos con `docker ps`.
- Cerrar otros servicios que usen HiveServer2 o el puerto `10000`.
- Reiniciar Docker Desktop si el puerto quedó retenido.

### HiveServer2 no responde

Síntoma:

```text
Connection refused
```

Acciones:

- Esperar unos segundos después de `docker compose up -d`.
- Revisar `docker compose ps`.
- Revisar logs:

```powershell
docker logs municipal_revenue_hive_server --tail 80
docker logs municipal_revenue_hive_metastore --tail 80
```

### Power BI no conecta por ODBC

Acciones:

- Confirmar que HiveServer2 responde en `localhost:10000`.
- Confirmar que el driver ODBC de Hive sea de 64 bits.
- Crear un DSN de sistema en el Administrador ODBC de Windows.
- Probar conexión en el DSN antes de abrir Power BI.
- Usar modo Import.
- Si persiste el problema, usar el fallback CSV.

### Falta driver ODBC de Hive

Acciones:

- Instalar un driver ODBC compatible con Apache Hive y Windows de 64 bits.
- Reiniciar Power BI Desktop después de instalarlo.
- Revisar que el driver aparezca en el Administrador de Orígenes de Datos ODBC de 64 bits.

### Entorno virtual no activado

Síntoma:

```text
ModuleNotFoundError
```

Acciones:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Dependencias no instaladas

Síntoma:

```text
No module named pyspark
```

Acciones:

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Rutas `data/` inexistentes

Acciones:

- Confirmar que se está en la raíz del repositorio.
- Validar rutas con `src/common/paths.py`.
- Ejecutar ingesta o colocar los archivos esperados localmente.
- No crear rutas absolutas manuales fuera del proyecto.

### Tablas Hive Gold no aparecen

Acciones:

- Confirmar que `data/gold` contiene Parquet.
- Generar SQL Hive con `src.hive.generate_external_tables`.
- Aplicar `create_gold_external_tables.sql`.
- Ejecutar:

```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN gold;"
```

### Error al cargar archivos no generados

Acciones:

- Revisar que la capa previa exista.
- Ejecutar primero el comando `--dry-run` del módulo correspondiente.
- Construir la capa faltante antes de continuar.
- Evitar ejecutar pasos Gold si Silver integrado todavía no existe.

## 20. Detener Servicios

Detener contenedores sin borrar volúmenes:

```powershell
docker compose down
```

Detener contenedores y borrar volúmenes:

```powershell
docker compose down -v
```

Usar `-v` solo si se quiere limpiar completamente el estado local de Hive y otros servicios.

## 21. Criterio de Entorno Listo

El entorno local queda listo cuando:

- `python --version` responde dentro de `.venv`.
- `python -m pytest` no muestra errores bloqueantes.
- `docker compose up -d` levanta servicios.
- Spark Master responde en `http://localhost:8080`.
- Spark Worker responde en `http://localhost:8081`.
- Beeline conecta a HiveServer2.
- `SHOW DATABASES` responde.
- Las capas requeridas existen localmente en `data/`.
- `SHOW TABLES IN gold` lista tablas cuando Gold y DDL Hive ya fueron generados.
- Power BI puede conectarse a Hive por ODBC o usar fallback CSV.
