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

## Estrategia de Fallback Controlada

Si el driver ODBC presenta inestabilidad local, bloqueos de red o errores de controlador en Windows:
1. **Fallback de Datos Parquet:** Power BI permite conectarse directamente a carpetas de datos locales en formato Parquet. Se debe apuntar la conexión al directorio del proyecto: `data/gold/<area_tematica>/<nombre_tabla>`.
2. **Fallback de Datos CSV:** Si la lectura de Parquet no es compatible con el entorno, se consumirá una exportación controlada en formato CSV generada a partir de los Parquet locales.
3. *Nota:* El uso de este fallback es de contingencia de reportabilidad. Las sentencias SQL de creación de tablas en Hive deben seguir validadas y el script DDL funcional.

---

## Captura de Evidencias Técnicas

Durante la conexión se deben guardar las siguientes evidencias en la carpeta `powerbi/screenshots/` (no versionar archivos pesados, solo capturas de validación):
* Captura de consola ejecutando `SHOW TABLES IN gold;`.
* Captura de consola ejecutando `SELECT COUNT(*)` sobre `gold.mart_municipal_capacity`.
* Captura de la ventana de configuración del DSN ODBC en Windows con el test exitoso.
* Captura de la vista de relaciones en Power BI Desktop mostrando el modelo en estrella implementado.
