# Directorio de Visualización Power BI

Este directorio contiene los recursos y evidencias asociadas al diseño e implementación del reporte final del lakehouse local.

## Propósito del Directorio

Concentrar el reporte semántico (.pbix), las capturas de pantalla de validación de conexión, y los flujos analíticos diseñados.

## Estructura de Carpetas

* `powerbi/`
  * `exports/`: Archivos temporales o de backup de datos (CSVs, PDFs). **No deben versionarse**.
  * `screenshots/`: Capturas ligeras de pantalla (PNG, JPG) para evidenciar la conexión ODBC exitosa, el modelo en estrella importado, y las vistas finales de las páginas del dashboard.
  * `README.md`: Este archivo guía.

---

## Nombre de Archivo del Dashboard

El archivo de Power BI Desktop debe guardarse con la siguiente nomenclatura en este directorio:
* `Municipal_Revenue_Analytics.pbix`

---

## Directrices de Construcción del Dashboard

Para construir el reporte de forma ordenada y eficiente, siga este flujo secuencial:

1. **Validación de la Base:** Ejecutar `SHOW TABLES IN gold` en Beeline para confirmar que las tablas externas están actualizadas.
2. **Importación ODBC:** Configurar el DSN ODBC de sistema de Hive Server en Windows y cargar en modo **Import** las tablas de la base de datos `gold`.
3. **Modelado en Estrella:**
   * Relacionar `dim_geography` y `dim_time` / `dim_predial_period` con los hechos/marts correspondientes mediante relaciones 1:N.
   * Derivar la dimensión `dim_budget_classifier` en Power Query desde `fact_municipal_income_execution` si se requiere el análisis atómico de clasificadores presupuestales.
4. **Implementación de DAX:** Crear una tabla de medidas dedicada e implementar las fórmulas de ingresos, predios y capacidades institucionales especificadas en `docs/powerbi_model.md`.
5. **Layout Visual:** Diseñar las 8 páginas analíticas estructuradas (6 ejecutivas, 2 técnicas/geográficas) utilizando los marts planos de ingresos, predios y capacidad.
6. **Validación de Fallback:** Si se experimentan problemas con el driver local, realizar la importación directamente de los Parquet locales en `data/gold/` y documentar la contingencia en `powerbi/screenshots/`.
