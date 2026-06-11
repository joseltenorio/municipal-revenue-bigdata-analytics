# Auditoría de ingesta y política de reintentos

## Propósito del documento

Este documento describe la estrategia de auditoría, reintentos HTTP y fallback utilizada por los procesos de ingesta del proyecto **Municipal Revenue Big Data Analytics**.

El objetivo es asegurar trazabilidad operacional antes de ejecutar descargas completas hacia Landing. La auditoría permite responder cuándo se ejecutó una ingesta, qué fuente se procesó, qué recursos fueron evaluados, si la ejecución fue exitosa o fallida y qué evidencia técnica quedó registrada localmente.

Esta documentación no reemplaza el profiling ni la calidad de datos. La auditoría registra eventos técnicos de ejecución; el profiling analiza estructura, tipos, nulos, duplicados y distribución de valores; la calidad evalúa reglas sobre los datos.

## Alcance

La auditoría de ingesta aplica a los scripts ubicados en:

- `src/ingestion/download_mef_income.py`
- `src/ingestion/download_predial_goal.py`
- `src/ingestion/download_renamu.py`

Estos scripts descargan o validan recursos definidos en:

- `config/sources.yaml`

La lógica común de auditoría y reintentos se centraliza en:

- `src/common/audit.py`
- `src/common/retry.py`

## Ubicación de la auditoría

Los eventos de auditoría se registran localmente en formato JSON Lines dentro de:

`data/quality/ingestion_audit.jsonl`

Cada línea del archivo representa un evento independiente de auditoría.

Este archivo es local y no debe subirse a GitHub.

## Criterio de no versionamiento

No se deben versionar:

- `data/quality/ingestion_audit.jsonl`
- Archivos descargados en `data/landing/`
- Metadata local generada junto a archivos descargados
- CSV, ZIP, XLSX, PDF o Parquet reales
- Logs pesados generados automáticamente
- `.env`
- `.venv`

La auditoría sirve como evidencia local durante ejecución y puede usarse para capturas o resúmenes, pero el archivo generado no forma parte del repositorio.

## Tipos de eventos registrados

Los procesos de ingesta registran eventos informativos y resultados de recursos.

### INGESTION_START

Indica el inicio de una ejecución de ingesta.

Campos principales:

- `timestamp`
- `level`
- `run_id`
- `source_name`
- `event_type`
- `message`
- `metadata`

Ejemplo conceptual:

```json
{
  "timestamp": "2026-06-11T00:00:00+00:00",
  "level": "INFO",
  "run_id": "mef_income_20260611000000_abcd1234",
  "source_name": "mef_income",
  "event_type": "INGESTION_START",
  "message": "Inicio de proceso de ingesta.",
  "metadata": {
    "selected_resources": ["dictionary"],
    "dry_run": true
  }
}
```

### INGESTION_FINISH

Indica el cierre de una ejecución de ingesta, ya sea exitosa o fallida.

En una ejecución exitosa, permite confirmar qué recursos fueron seleccionados y si la ejecución fue `dry-run`.

En una ejecución fallida, permite conservar el tipo de error y el mensaje asociado.

### RESOURCE_RESULT

Indica el resultado de una descarga real de recurso.

Este evento se registra cuando un archivo fue descargado correctamente hacia Landing. En ejecuciones `--dry-run`, normalmente no se genera `RESOURCE_RESULT` porque no existe archivo descargado.

Campos relevantes:

- `run_id`
- `source_name`
- `resource_key`
- `file_name`
- `status`
- `started_at`
- `finished_at`
- `duration_seconds`
- `metadata.source_url`
- `metadata.http_status_code`
- `metadata.content_type`
- `metadata.content_length_bytes`
- `metadata.downloaded_file_size_bytes`
- `metadata.checksum_sha256`
- `metadata.output_path`
- `metadata.metadata_path`

## Identificador de ejecución

Cada ejecución genera un `run_id`.

El `run_id` permite agrupar eventos relacionados con una misma ejecución de un script.

Formato general:

`source_name_YYYYMMDDHHMMSS_suffix`

Ejemplo:

`mef_income_20260611123045_a1b2c3d4`

## Política de reintentos

La política de reintentos se configura en:

`config/retry_policy.yaml`

Los scripts de ingesta no deben implementar reintentos manuales de forma duplicada. La lógica se centraliza en:

`src/common/retry.py`

La política considera:

- Número máximo de intentos.
- Timeout por solicitud HTTP.
- Espera entre intentos.
- Multiplicador de backoff.
- Códigos HTTP reintentables.
- Errores de conexión o timeout.

## Códigos HTTP reintentables

Los códigos HTTP recuperables considerados inicialmente son:

- `408`: Request Timeout.
- `429`: Too Many Requests.
- `500`: Internal Server Error.
- `502`: Bad Gateway.
- `503`: Service Unavailable.
- `504`: Gateway Timeout.

Estos códigos representan fallos temporales o saturación del servicio. No deben tratarse igual que errores permanentes como `404`, donde el recurso probablemente no existe en la URL evaluada.

## Fallback de validación

La validación de disponibilidad usa `HEAD` cuando es posible.

Si el servidor no permite `HEAD` o responde con un estado compatible con fallback, se usa `GET` en modo `stream=True`.

Este criterio evita descargar archivos completos durante validaciones livianas y permite manejar portales que no soportan correctamente solicitudes `HEAD`.

## Relación entre auditoría, metadata y datos

La auditoría central registra eventos de ejecución en:

`data/quality/ingestion_audit.jsonl`

La metadata local por archivo descargado se guarda junto al archivo en Landing, por ejemplo:

`data/landing/mef_income/Ingresos_Diccionario.csv.metadata.json`

La diferencia es:

- La auditoría central permite revisar la ejecución completa.
- La metadata por archivo describe el archivo descargado.
- Landing conserva el archivo original sin transformación.
- Bronze convertirá posteriormente los archivos a Parquet.

## Comandos de validación sin descarga pesada

Validar MEF con el diccionario:

```powershell
python -m src.ingestion.download_mef_income --resource dictionary --dry-run
```

Validar meta predial con una tabla pequeña:

```powershell
python -m src.ingestion.download_predial_goal --resource estadistica --dry-run
```

Validar RENAMU sin descargar:

```powershell
python -m src.ingestion.download_renamu --all-enabled --dry-run
```

Revisar eventos recientes de auditoría:

```powershell
Get-Content data/quality/ingestion_audit.jsonl -Tail 20
```

Verificar que auditoría y datos no estén siendo versionados:

```powershell
git status --short
```

## Comportamiento esperado en dry-run

En modo `--dry-run`, los scripts:

- Validan disponibilidad del recurso.
- Consultan estado HTTP.
- Consultan tipo de contenido.
- Consultan tamaño declarado, si existe.
- Registran inicio y fin de ejecución.
- No descargan archivos reales.
- No generan metadata por archivo descargado.
- No generan Bronze.
- No transforman datos.

## Comportamiento esperado en descarga real

En una descarga real, los scripts:

- Validan disponibilidad.
- Descargan por streaming.
- Guardan el archivo original en Landing.
- Calculan checksum SHA256.
- Generan metadata local del archivo.
- Registran resultado del recurso en auditoría.
- No transforman datos de negocio.
- No generan Parquet.
- No construyen Bronze.

## Manejo de errores

Si una ingesta falla, el proceso debe:

- Registrar un evento de cierre fallido.
- Mostrar el tipo de error.
- Mostrar el mensaje del error.
- Detener la ejecución con código de salida fallido.
- No ocultar errores.
- No generar datos parcialmente interpretados como válidos.

Los errores deben revisarse antes de continuar hacia Bronze.

## Fallback manual

Si una fuente pública cambia, bloquea automatización o requiere descarga manual, la decisión debe quedar documentada.

Un fallback manual aceptable debe registrar:

- Fuente afectada.
- URL original.
- Fecha de descarga manual.
- Archivo obtenido.
- Ubicación en Landing.
- Razón del fallback.
- Evidencia de validación básica.
- Limitaciones conocidas.

El fallback manual no debe usarse para evitar la trazabilidad. Debe ser una excepción documentada.

## Estado actual

Actualmente, los scripts de ingesta de MEF ingresos, meta predial y RENAMU 2022 incorporan:

- Validación de disponibilidad.
- Fallback de `HEAD` a `GET`.
- Reintentos HTTP configurables.
- Descarga por streaming.
- Metadata local por archivo descargado.
- Checksum SHA256.
- Auditoría básica de inicio, fin y resultado de recursos descargados.

La siguiente etapa operativa será ejecutar una descarga local completa y controlada de las fuentes necesarias, revisar la auditoría generada y luego iniciar profiling antes de construir Bronze Parquet.
