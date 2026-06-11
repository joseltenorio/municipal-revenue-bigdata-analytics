# Municipal Revenue Big Data Analytics

Plataforma local de ingeniería y analítica de datos para analizar presupuesto, ejecución de ingresos e impuesto predial de municipalidades peruanas, utilizando Apache Spark, Apache Hive, arquitectura Medallion, archivos Parquet y Power BI.

## Descripción general

Municipal Revenue Big Data Analytics es un proyecto de Data Engineering y Business Intelligence orientado al procesamiento, integración y análisis de datos públicos relacionados con la gestión de ingresos municipales en el Perú.

El proyecto busca construir una plataforma analítica local tipo lakehouse que permita ingerir datos desde fuentes públicas, conservar archivos originales, transformar datos mediante Apache Spark, exponer datasets mediante Apache Hive y generar información lista para análisis ejecutivo en Power BI.

La solución se enfoca en el análisis del presupuesto y la ejecución de ingresos municipales, el seguimiento de la meta del impuesto predial y el contexto institucional de las municipalidades a partir de fuentes públicas del MEF, SISMERE e INEI.

## Contexto de negocio

Las municipalidades peruanas gestionan ingresos provenientes de distintas fuentes presupuestales, recaudación local e instrumentos de gestión fiscal. Analizar estos datos permite identificar brechas de ejecución, diferencias territoriales, desempeño recaudatorio y cumplimiento de metas asociadas al impuesto predial.

El proyecto está orientado a responder preguntas analíticas como:

- ¿Qué municipalidades presentan mayor o menor ejecución de ingresos?
- ¿Cómo se distribuye la recaudación municipal por departamento, provincia o distrito?
- ¿Qué municipalidades cumplen o incumplen la meta del impuesto predial?
- ¿Existen diferencias de desempeño entre municipalidades provinciales y distritales?
- ¿Qué patrones territoriales se observan en la ejecución de ingresos?
- ¿Qué indicadores pueden apoyar la toma de decisiones presupuestales y de gestión municipal?

## Objetivo general

Diseñar e implementar una plataforma analítica local basada en Apache Spark, Apache Hive y Power BI que permita procesar, validar, integrar y analizar datos públicos de ingresos municipales bajo una arquitectura Medallion.

## Objetivos específicos

- Identificar y documentar fuentes públicas relevantes del MEF, SISMERE e INEI.
- Implementar pipelines de ingesta para archivos CSV, ZIP o APIs públicas según disponibilidad.
- Preservar los archivos originales en una zona Landing.
- Convertir las fuentes originales hacia la capa Bronze en formato Parquet.
- Aplicar profiling y controles de calidad documentados.
- Transformar y estandarizar los datos mediante Apache Spark.
- Exponer las capas analíticas mediante tablas externas en Apache Hive.
- Construir una capa Gold orientada a Power BI.
- Desarrollar un reporte Power BI con seis páginas de análisis para la toma de decisiones.
- Registrar auditoría de ingesta, reintentos, errores y tiempos de ejecución.

## Arquitectura propuesta

El proyecto sigue una arquitectura Medallion implementada de forma local:

```text
Fuentes públicas
  -> Landing
  -> Bronze Parquet
  -> Profiling y Quality Gates
  -> Silver Parquet
  -> Gold Parquet / Marts analíticos
  -> Hive Gold External Tables
  -> Power BI vía Hive ODBC en modo Import
```

## Arquitectura Medallion

### Landing

La zona Landing conserva los archivos originales descargados desde las fuentes públicas.

Esta capa puede contener archivos CSV, ZIP, JSON, XLSX u otros formatos obtenidos directamente desde las fuentes. No se aplican transformaciones de negocio, ya que su propósito es preservar trazabilidad y evidencia del dato original.

### Bronze

La capa Bronze almacena los datos en formato Parquet desde la primera etapa estructurada del pipeline.

Esta capa mantiene la granularidad original de cada fuente, incorpora metadata técnica de ingesta y permite optimizar lectura, almacenamiento y procesamiento con Apache Spark.

### Silver

La capa Silver contiene datos limpios, estandarizados y validados.

En esta etapa se corrigen tipos de datos, se normalizan nombres de columnas, se estandarizan ubigeos, fechas, montos y porcentajes, y se identifican problemas de calidad como nulos críticos, duplicados, registros inconsistentes o claves no integrables.

### Gold

La capa Gold contiene datasets analíticos listos para consumo desde Power BI.

El modelo definitivo será definido después del profiling y la integración de fuentes. Podrá adoptar un modelo estrella, copo de nieve parcial o marts analíticos planos, según la estructura real y la granularidad encontrada en los datos.

## Fuentes de datos

El proyecto considera las siguientes fuentes públicas:

1. Presupuesto y ejecución de ingreso - MEF / SIAF.
2. Seguimiento de la meta del impuesto predial - SISMERE / MEF.
3. Registro Nacional de Municipalidades RENAMU 2022 - INEI.
4. Reporte Power BI de referencia visual para orientar el análisis final.

## Stack tecnológico

- Python
- Apache Spark
- Apache Hive
- Parquet
- Power BI
- Docker
- Docker Compose
- Git
- GitHub

## Control de calidad de datos

El proyecto contempla una etapa formal de profiling y calidad de datos.

Los controles esperados incluyen:

- Completitud de columnas críticas.
- Detección de nulos.
- Detección de duplicados.
- Validación de tipos de datos.
- Validación de rangos numéricos.
- Validación de montos negativos.
- Validación de porcentajes fuera de rango.
- Validación de ubigeos.
- Identificación de registros no integrables entre fuentes.
- Generación de reportes de calidad documentados.

## Auditoría de ingesta y procesamiento

La plataforma registrará información operativa de cada ejecución, incluyendo:

- Identificador único de ejecución.
- Fuente procesada.
- Método de acceso utilizado.
- Fecha y hora de inicio.
- Fecha y hora de fin.
- Duración del proceso.
- Número de intentos.
- Número de reintentos.
- Estado final de la ejecución.
- Código HTTP, si aplica.
- Mensaje de error, si aplica.
- Nombre del archivo descargado.
- Tamaño del archivo.
- Checksum del archivo.
- Cantidad de registros detectados, cuando sea posible.

Esta auditoría permitirá demostrar trazabilidad, resiliencia y control operativo del pipeline.

## Capa de reporting

El reporte Power BI estará orientado a la toma de decisiones sobre ingresos municipales.

Las páginas analíticas planificadas son:

1. Resumen Ejecutivo Municipal.
2. Presupuesto vs Ejecución de Ingresos.
3. Ranking de Municipalidades.
4. Cumplimiento de la Meta del Impuesto Predial.
5. Contexto Municipal RENAMU.
6. Análisis Territorial.

## Estructura del repositorio

```text
config/      Configuración de fuentes, rutas, Spark, Hive, calidad y auditoría.
data/        Zonas locales del lakehouse: Landing, Bronze, Silver, Gold y Quality.
docs/        Documentación técnica, funcional y analítica del proyecto.
src/         Código fuente de ingesta, transformación, calidad y modelado.
sql/         Scripts Hive y consultas analíticas.
powerbi/     Documentación y evidencias del reporte Power BI.
reports/     Reportes generados de profiling, calidad y auditoría.
evidence/    Evidencias de ejecución, tablas, Parquet y dashboard.
tests/       Pruebas automatizadas del proyecto.
```

## Estado del proyecto

El repositorio se encuentra en etapa inicial.

La primera fase define la estructura base, dependencias, convenciones y documentación inicial antes de implementar los pipelines de ingesta, profiling, transformación, Hive y Power BI.

## Consideraciones de versionamiento

Los datos reales no deben subirse al repositorio.

Se versionan únicamente archivos de configuración, documentación, código fuente, scripts SQL, pruebas y carpetas base mediante archivos `.gitkeep`.

No deben subirse:

- Archivos CSV reales.
- Archivos ZIP reales.
- Archivos XLSX reales.
- Archivos Parquet generados.
- Logs pesados.
- Credenciales.
- Archivos `.env` reales.
- Entornos virtuales.
- Outputs temporales.
