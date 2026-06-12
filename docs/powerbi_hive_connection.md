# Guía de Conexión Power BI - Apache Hive

## Propósito

Este documento establece el procedimiento técnico para conectar **Power BI Desktop** con la base de datos analítica **Gold** del lakehouse local, la cual se encuentra catalogada en Apache Hive y expuesta mediante HiveServer2.

---

## Parámetros de Conexión a la Base Gold

La conexión se realiza mediante el driver ODBC oficial de Apache Hive instalado en el sistema anfitrión.

| Parámetro | Valor Técnico |
| :--- | :--- |
| **Host / Servidor** | `localhost` |
| **Puerto** | `10000` |
| **Servicio** | HiveServer2 |
| **Base de Datos** | `gold` |
| **Modo de Conexión** | **Import** (Recomendado para optimizar el rendimiento y compresión Vertipaq) |
| **Mecanismo de Autenticación** | `Username` (ej. `hive` o vacío según configuración local de Docker) |

---

## Procedimiento de Conexión y Carga de Datos

### 1. Validación Previa en Beeline
Antes de iniciar Power BI, se debe abrir un terminal y comprobar que el servicio está activo y que las tablas Gold existen en el catálogo:
```powershell
docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN gold;"
```
Confirmar que el resultado arroja las **15 tablas Gold** (marts, dimensiones y hechos).

### 2. Configuración del Origen ODBC en Windows
1. Abrir el **Administrador de Orígenes de Datos ODBC (64 bits)** en Windows.
2. Añadir un DSN de Sistema usando el driver **Cloudera ODBC Driver for Apache Hive** (o el driver oficial de Apache Hive).
3. Configurar el host como `localhost`, puerto `10000`, mecanismo como `User Name` (usuario: `hive`) y base de datos por defecto como `gold`.
4. Hacer clic en **Test** para validar que la conexión es exitosa.

### 3. Carga en Power BI Desktop
1. Abrir Power BI Desktop y seleccionar **Obtener datos -> ODBC**.
2. Seleccionar el DSN creado en el paso anterior.
3. En el navegador de tablas, expandir la base de datos `gold`.
4. Seleccionar los datasets Gold recomendados en la guía de modelado analítico.
5. Hacer clic en **Cargar** (modo Import) para descargar la información al modelo semántico en memoria.

---

## Checklist de Validación Previa de Conexión

Antes de iniciar la conexión en Power BI Desktop, valide los siguientes puntos en orden técnico:

- [ ] **Servicios de Docker Activos:** Ejecutar `docker compose ps` y confirmar que `hive-server` y `hive-metastore` están en estado `Up`.
- [ ] **Tablas Gold en Hive Catalog:** Ejecutar en el terminal:
  ```powershell
  docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN gold;"
  ```
  Confirmar que se listan las 15 tablas de la capa Gold.
- [ ] **Registros en Marts Clave:** Comprobar que los marts analíticos contienen registros realizando consultas simples:
  ```powershell
  docker compose exec hive-server beeline -u "jdbc:hive2://localhost:10000" -e "SELECT COUNT(*) FROM gold.mart_municipal_capacity;"
  ```
  *(Debe devolver exactamente `1874` registros).*
- [ ] **Driver ODBC Instalado:** Asegurar que tiene instalado en Windows el **Cloudera ODBC Driver for Apache Hive (64-bit)** o un driver oficial compatible de Apache Hive.
- [ ] **Puerto 10000 Accesible:** Confirmar que no hay otra aplicación local ocupando el puerto `10000` en la máquina anfitriona de Windows.

---

## Estrategia de Fallback Controlada

Si el driver ODBC local presenta problemas de configuración en el sistema anfitrión o incompatibilidad con la arquitectura del sistema, se debe proceder con el plan de fallback ordenado:

### Opción A: Carga Directa de Parquet en Power BI
1. En Power BI Desktop, seleccionar **Obtener Datos -> Carpeta** o **Parquet** si el conector nativo está instalado.
2. Apuntar cada tabla del modelo a su ruta física local correspondiente en `data/gold/`:
   * `data/gold/municipal_revenue/mart_municipal_revenue_overview`
   * `data/gold/predial_compliance/mart_predial_compliance_overview`
   * *(Repetir para los 9 datasets del modelo analítico).*

### Opción B: Fallback de Exportación CSV (Recomendado ante incompatibilidades de Parquet)
Si Power BI presenta problemas para abrir la estructura interna de los archivos Parquet en el entorno local:
1. Abrir un terminal de PowerShell en la raíz del proyecto.
2. Ejecutar el script de exportación para planificar y validar:
   ```powershell
   .venv\Scripts\python.exe -m src.powerbi.export_gold_fallback --dry-run
   ```
3. Ejecutar la exportación real de contingencia:
   ```powershell
   .venv\Scripts\python.exe -m src.powerbi.export_gold_fallback
   ```
   *(Si ya existían CSVs previos y desea regenerarlos, agregue la opción `--overwrite`).*
4. En Power BI Desktop, seleccionar **Obtener Datos -> Texto o CSV** y cargar los archivos generados en `powerbi/exports/`.

---

## Captura de Evidencias Técnicas Obligatorias

Durante la fase de validación y conexión del reporte, el desarrollador del dashboard debe capturar y guardar las siguientes evidencias en la carpeta `powerbi/screenshots/` (mantener las capturas ligeras para el repositorio Git):

1. `01_beeline_show_tables.png`: Captura de pantalla de la consola mostrando la ejecución exitosa de `SHOW TABLES IN gold;` en Beeline.
2. `02_beeline_count_capacity.png`: Captura mostrando el resultado de la consulta de conteo sobre `gold.mart_municipal_capacity`.
3. `03_odbc_dsn_test_success.png`: Captura del Administrador de DSN ODBC de Windows mostrando el cuadro de diálogo de configuración con el mensaje *"SUCCESS"* al pulsar el botón **Test**.
4. `04_powerbi_data_model_star.png`: Captura de la vista de relaciones en Power BI Desktop, mostrando el modelo en estrella implementado con las dimensiones relacionadas adecuadamente a las tablas de hechos y marts.
5. `05_fallback_contingency_run.png` *(Opcional)*: Captura de pantalla de la ejecución del script `export_gold_fallback.py` en caso de haber tenido que activar la contingencia CSV.

