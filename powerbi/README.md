# Directorio de Visualización Power BI

Este directorio contiene los recursos y evidencias asociadas al diseño e implementación del reporte final del lakehouse local.

## Propósito del Directorio

Concentrar el reporte semántico (.pbix), las capturas de pantalla de validación de conexión y los flujos analíticos diseñados, proporcionando a la vez un mecanismo de contingencia para la carga de datos.

## Estructura de Carpetas y Versionado

* `powerbi/`
  * `exports/`: Directorio reservado para exportaciones CSV de contingencia. Contiene el archivo de control `powerbi/exports/.gitkeep` que debe mantenerse en el repositorio. **Todos los CSVs generados aquí están ignorados por `.gitignore` y NO deben versionarse**.
  * `screenshots/`: Capturas ligeras de pantalla (PNG, JPG) para evidenciar la conexión ODBC exitosa, el modelo en estrella importado y las páginas del dashboard final. **Estas capturas SÍ deben versionarse** como entregable de validación.
  * `README.md`: Este archivo guía.

---

## Nombre de Archivo del Dashboard

El archivo de Power BI Desktop debe guardarse con la siguiente nomenclatura en este directorio:
* `Municipal_Revenue_Analytics.pbix`
*(Nota: Este archivo está ignorado por `.gitignore` para evitar subir archivos binarios pesados al repositorio local).*

---

## Estrategia de Conexión y Fallback

### 1. Conexión Preferente (Hive Server 2 / ODBC)
La forma principal de consumir el modelo analítico es conectar Power BI Desktop al motor local de Apache Hive mediante el driver ODBC de HiveServer2 (`localhost:10000`), cargando las tablas de la base de datos `gold` en modo **Import**.

### 2. Conexión de Contingencia (CSV Fallback)
Si experimenta inestabilidad con el driver ODBC, bloqueos de puertos o lentitud local, puede exportar los marts y dimensiones Gold desde los Parquet locales a formato CSV e importarlos en Power BI.

Para generar los archivos CSV de contingencia, ejecute el script de fallback:

* **Planificar y validar (Dry-run):**
  ```powershell
  .venv\Scripts\python.exe -m src.powerbi.export_gold_fallback --dry-run
  ```
* **Ejecutar exportación (evitando sobreescrituras accidentales):**
  ```powershell
  .venv\Scripts\python.exe -m src.powerbi.export_gold_fallback
  ```
* **Forzar sobreescritura de CSVs previos:**
  ```powershell
  .venv\Scripts\python.exe -m src.powerbi.export_gold_fallback --overwrite
  ```

El script leerá los Parquet directamente desde `data/gold/` y escribirá los siguientes 9 datasets recomendados bajo `powerbi/exports/` en formato CSV UTF-8 sin índice:
* `mart_municipal_revenue_overview.csv`
* `mart_predial_compliance_overview.csv`
* `mart_predial_ranking.csv`
* `mart_municipal_capacity.csv`
* `mart_territorial_context.csv`
* `dim_geography.csv`
* `dim_time.csv`
* `dim_municipality.csv`
* `dim_predial_period.csv`

*(Nota: Por rendimiento y optimización de recursos, las tablas de hechos pesadas como `fact_municipal_income_execution` no se exportan por defecto a CSV. Si se requiere análisis de detalle presupuestal, se debe priorizar el acceso directo mediante Hive/ODBC o la importación del Parquet de la fact mediante Power Query).*

---

## Orden Recomendado para Construir el Dashboard

Para construir el reporte de forma ordenada y eficiente, siga este flujo secuencial:

1. **Validación de la Capa Gold:** Ejecutar `SHOW TABLES IN gold` en Beeline para confirmar que las tablas externas están actualizadas en Hive.
2. **Conexión e Importación:** Configurar el DSN ODBC en Windows y cargar las tablas, o en su defecto, ejecutar el script de fallback e importar los archivos CSV desde `powerbi/exports/`.
3. **Modelado de Relaciones (Star Schema):**
   * Relacionar `dim_geography`, `dim_time`, `dim_municipality` y `dim_predial_period` con los hechos/marts correspondientes mediante relaciones de tipo 1:N unidireccionales.
   * Si es necesario realizar análisis detallado de clasificadores presupuestales, derivar la dimensión `dim_budget_classifier` en Power Query a partir de la fact `fact_municipal_income_execution` y relacionarla convenientemente.
4. **Implementación de Medidas DAX:** Crear una tabla vacía llamada `_Medidas` e implementar los cálculos (Recaudación, PIM, PIA, Efectividad Predial, Capacidades, Rankings, etc.) especificados en [powerbi_model.md](file:///c:/Users/Windows%2011/Desktop/Proyectos/municipal-revenue-bigdata-analytics/docs/powerbi_model.md).
5. **Diseño de Layout Visual:** Diseñar las 8 páginas analíticas estructuradas (6 obligatorias de negocio, 2 complementarias de cobertura y territorio).
6. **Almacenamiento de Evidencias:** Guardar capturas de pantalla de validación en `powerbi/screenshots/` (Beeline, DSN ODBC exitoso, vista de relaciones y páginas principales).

