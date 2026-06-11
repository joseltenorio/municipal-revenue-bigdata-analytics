# Profiling de datos

## Propósito del documento

Este documento describe la estrategia inicial de profiling para el proyecto **Municipal Revenue Big Data Analytics**.

El profiling permite inspeccionar archivos locales ubicados en Landing antes de diseñar reglas definitivas de Bronze, Silver, calidad de datos y modelo Gold.

En esta etapa, el profiling no reemplaza el discovery. El discovery valida acceso, URLs, formatos y disponibilidad. El profiling valida estructura interna de archivos descargados localmente: columnas, tipos inferidos, nulos, duplicados, valores de muestra y posibles llaves candidatas.

## Alcance del profiling inicial

El profiling inicial busca responder:

- Qué archivos existen localmente en Landing.
- Cuántas filas y columnas tiene cada archivo.
- Qué tipos de datos infiere la lectura local.
- Qué columnas presentan nulos.
- Qué columnas tienen valores únicos en la muestra.
- Qué archivos tienen duplicados exactos.
- Qué valores de muestra aparecen por columna.
- Qué problemas de lectura aparecen por formato o codificación.

## Archivo principal

El script principal de profiling es:

`src/quality/profile_sources.py`

Este script:

- Recorre archivos locales dentro de Landing.
- Soporta archivos `.csv`, `.txt`, `.xlsx`, `.xls`, `.json` y `.parquet`.
- Lee una cantidad máxima controlada de filas por archivo.
- Genera un resumen técnico por archivo y columna.
- Escribe un reporte JSON en `reports/profiling_summary.json`.
- No descarga datos externos.
- No transforma archivos.
- No genera Bronze, Silver ni Gold.

## Comando de ejecución

Ejecución por defecto:

```powershell
python -m src.quality.profile_sources
```

Ejecución indicando directorio y salida:

```powershell
python -m src.quality.profile_sources `
  --input-dir "data/landing" `
  --output "reports/profiling_summary.json" `
  --max-rows 10000
```

## Resultado esperado antes de la ingesta real

Si todavía no existen archivos descargados en Landing, el resultado esperado es un reporte vacío controlado.

Esto no representa un error. Significa que la capacidad de profiling ya está preparada, pero los resultados reales quedan pendientes hasta implementar la ingesta hacia Landing.

Resultado esperado en consola:

```text
Resumen de profiling
Archivos perfilados: 0
No se encontraron archivos locales soportados en Landing.
Esto es esperado antes de implementar la ingesta real.
```

## Reporte generado

El reporte se genera en:

`reports/profiling_summary.json`

El archivo contiene:

- Fecha de generación.
- Cantidad de perfiles generados.
- Lista de archivos perfilados.
- Métricas por archivo.
- Métricas por columna.
- Errores de lectura, si existieran.

El reporte JSON generado localmente no debe tratarse como dato fuente. Puede regenerarse cuando cambien los archivos locales. Si el archivo crece o contiene información derivada de datos reales, no debe versionarse en Git.

## Métricas calculadas

Por archivo:

| Métrica          | Descripción                                 |
| ---------------- | ------------------------------------------- |
| `file_name`      | Nombre del archivo local                    |
| `file_extension` | Extensión del archivo                       |
| `row_count`      | Cantidad de filas leídas                    |
| `column_count`   | Cantidad de columnas                        |
| `duplicate_rows` | Duplicados exactos detectados en la muestra |
| `error`          | Error de lectura, si ocurre                 |

Por columna:

| Métrica          | Descripción                   |
| ---------------- | ----------------------------- |
| `column_name`    | Nombre original de la columna |
| `inferred_dtype` | Tipo inferido por pandas      |
| `non_null_count` | Cantidad de valores no nulos  |
| `null_count`     | Cantidad de valores nulos     |
| `null_rate`      | Proporción de nulos           |
| `unique_count`   | Cantidad de valores únicos    |
| `sample_values`  | Valores de muestra no nulos   |

## Criterios de interpretación

El profiling debe interpretarse con cautela:

- Los tipos inferidos pueden cambiar cuando se procese el archivo completo.
- Un campo único en una muestra no necesariamente es llave definitiva.
- Un campo sin nulos en una muestra puede tener nulos en el archivo completo.
- Los valores de muestra no representan distribución completa.
- Las reglas de calidad definitivas se definen después de observar datos reales.
- Los diccionarios oficiales ayudan a interpretar campos, pero no reemplazan el profiling local.

## Relación con discovery

El discovery del commit anterior identificó recursos directos para las fuentes principales:

- MEF ingresos: CSV por año, recursos recientes diarios o mensuales y diccionario CSV.
- Meta predial: múltiples CSV temáticos y diccionarios.
- RENAMU 2022: ZIP completo y diccionario PDF.

El profiling se ejecutará sobre archivos locales cuando la ingesta hacia Landing descargue recursos controlados.

## Relación con el diccionario de datos

El diccionario de datos mantiene nombres técnicos esperados y significado analítico de campos.

El profiling aporta evidencia empírica para ajustar ese diccionario:

- Nombres reales de columnas.
- Tipos inferidos.
- Niveles de nulos.
- Posibles llaves.
- Valores inesperados.
- Estructuras reales por archivo.

## Estado actual

Estado: capacidad de profiling implementada.

Resultados reales: pendientes de archivos locales en Landing.

La ausencia de archivos locales en esta etapa es esperada porque la ingesta definitiva todavía no se implementa. El proyecto queda preparado para ejecutar profiling real después de descargar recursos hacia Landing en commits posteriores.
