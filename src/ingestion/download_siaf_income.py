"""Descarga controlada de la fuente MEF de presupuesto y ejecución de ingresos.

Este script implementa la ingesta inicial hacia Landing para la fuente MEF.
Descarga recursos CSV directos definidos en config/sources.yaml y conserva los
archivos originales sin transformarlos.

No construye Bronze, no limpia datos y no interpreta columnas de negocio.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src.common.audit import audit_info, audit_result, create_run_id
from src.common.config import load_sources_config
from src.common.download import DownloadResult, safe_download_file
from src.common.paths import get_source_landing_path
from src.common.retry import RetryError, build_retry_config, probe_with_fallback


SOURCE_NAME = "siaf_income"
DEFAULT_CHUNK_SIZE = 1024 * 1024


class IngestionError(Exception):
    """Error controlado durante la ingesta MEF."""


@dataclass(frozen=True)
class ResourceMetadata:
    """Metadata básica de un recurso descargado hacia Landing."""

    run_id: str
    source_name: str
    resource_key: str
    file_name: str
    source_url: str
    role: str | None
    year: int | None
    granularity: str | None
    started_at: str
    finished_at: str
    duration_seconds: float
    http_status_code: int
    content_type: str | None
    content_length_bytes: int | None
    downloaded_file_size_bytes: int
    checksum_sha256: str
    final_status: str
    partial_file_used: bool
    partial_file_path: str
    resumed_from_bytes: int
    range_request_used: bool
    server_supports_resume: bool
    download_duration_seconds: float
    average_speed_mbps: float


def utc_now_iso() -> str:
    """Devuelve fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def load_siaf_income_config() -> dict[str, Any]:
    """Carga la configuración de la fuente SIAF income."""

    config = load_sources_config()
    sources = config.get("sources", {})

    if SOURCE_NAME not in sources:
        raise IngestionError(
            f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml."
        )

    source_config = sources[SOURCE_NAME]

    if not source_config.get("enabled", False):
        raise IngestionError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def get_candidate_resources(source_config: dict[str, Any]) -> dict[str, Any]:
    """Obtiene recursos candidatos definidos para SIAF income."""

    resources = source_config.get("candidate_resources", {})

    if not resources:
        raise IngestionError(
            "No existen candidate_resources para siaf_income en config/sources.yaml."
        )

    return resources


def print_available_resources(resources: dict[str, Any]) -> None:
    """Imprime recursos configurados para la fuente MEF."""

    print("=" * 80)
    print("Recursos MEF configurados")

    for resource_key, resource in resources.items():
        print(
            f"- {resource_key}: {resource.get('file_name')} | "
            f"anio={resource.get('year', 'no_aplica')} | "
            f"granularidad={resource.get('granularity', 'no_aplica')} | "
            f"rol={resource.get('role')} | "
            f"prioridad={resource.get('priority')} | "
            f"documentacion={resource.get('use_for_documentation', False)}"
        )


def select_resources(
    resources: dict[str, Any],
    resource_keys: list[str] | None,
    years: list[int] | None,
    granularities: list[str] | None,
    include_documentation: bool,
    all_resources: bool,
) -> dict[str, Any]:
    """Selecciona recursos a descargar según argumentos de ejecución."""

    selected_resources: dict[str, Any] = {}

    if resource_keys:
        for resource_key in resource_keys:
            if resource_key not in resources:
                available = ", ".join(sorted(resources))
                raise IngestionError(
                    f"Recurso no encontrado: '{resource_key}'. "
                    f"Recursos disponibles: {available}."
                )

            selected_resources[resource_key] = resources[resource_key]

        return selected_resources

    for key, resource in resources.items():
        is_documentation = bool(resource.get("use_for_documentation", False))
        resource_year = resource.get("year")
        resource_granularity = resource.get("granularity")

        if all_resources:
            if is_documentation and not include_documentation:
                continue
            selected_resources[key] = resource
            continue

        if years and resource_year in years:
            if is_documentation and not include_documentation:
                continue
            selected_resources[key] = resource
            continue

        if granularities and resource_granularity in granularities:
            if is_documentation and not include_documentation:
                continue
            selected_resources[key] = resource
            continue

        if include_documentation and is_documentation:
            selected_resources[key] = resource

    if not selected_resources:
        available = ", ".join(sorted(resources))
        raise IngestionError(
            "No se seleccionó ningún recurso MEF para descargar. "
            "Usa --resource, --year, --granularity, --all-resources "
            "o --include-documentation. "
            f"Recursos disponibles: {available}."
        )

    return selected_resources


def validate_resource(resource_key: str, resource: dict[str, Any]) -> None:
    """Valida campos mínimos de un recurso candidato."""

    required_fields = ["file_name", "url", "format"]
    missing_fields = [field for field in required_fields if not resource.get(field)]

    if missing_fields:
        raise IngestionError(
            f"El recurso '{resource_key}' no tiene campos requeridos: {missing_fields}."
        )

    if resource["format"] != "csv":
        raise IngestionError(
            f"El recurso '{resource_key}' tiene formato '{resource['format']}'. "
            "Este script de SIAF income solo descarga recursos CSV."
        )


def probe_resource(url: str, timeout_seconds: int) -> requests.Response:
    """Valida disponibilidad del recurso usando HEAD con fallback y reintentos."""

    retry_config = build_retry_config(timeout_seconds=timeout_seconds)
    return probe_with_fallback(url=url, retry_config=retry_config)


def calculate_sha256(file_path: Path) -> str:
    """Calcula checksum SHA256 de un archivo local."""

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(DEFAULT_CHUNK_SIZE), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def parse_content_length(response: requests.Response) -> int | None:
    """Obtiene content-length como entero si está disponible."""

    content_length = response.headers.get("content-length")

    if content_length and content_length.isdigit():
        return int(content_length)

    return None


def download_resource(
    resource_key: str,
    resource: dict[str, Any],
    output_dir: Path,
    timeout_seconds: int,
    overwrite: bool,
    dry_run: bool,
) -> ResourceMetadata | None:
    """Descarga un recurso CSV hacia Landing."""

    validate_resource(resource_key=resource_key, resource=resource)

    run_id = uuid.uuid4().hex[:12]
    started_at = utc_now_iso()
    started_dt = datetime.now(timezone.utc)

    file_name = resource["file_name"]
    url = resource["url"]
    output_path = output_dir / file_name
    metadata_path = output_dir / f"{file_name}.metadata.json"

    print("=" * 80)
    print(f"Fuente: {SOURCE_NAME}")
    print(f"Recurso: {resource_key}")
    print(f"Archivo: {file_name}")
    print(f"URL: {url}")

    response = probe_resource(url=url, timeout_seconds=timeout_seconds)

    content_type = response.headers.get("content-type")
    content_length_bytes = parse_content_length(response)

    print(f"Estado HTTP: {response.status_code}")
    print(f"Tipo de contenido: {content_type}")
    print(f"Tamaño declarado: {content_length_bytes}")

    if dry_run:
        print("Dry run activo. No se descargó el archivo.")
        response.close()
        return None

    output_dir.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        response.close()
        raise IngestionError(
            f"El archivo ya existe en Landing: {output_path}. "
            "Usa --overwrite si deseas reemplazarlo."
        )

    retry_config = build_retry_config(timeout_seconds=timeout_seconds)

    download_result: DownloadResult = safe_download_file(
        url=url,
        destination_path=output_path,
        retry_config=retry_config,
        chunk_size=DEFAULT_CHUNK_SIZE,
        overwrite=overwrite,
        show_progress=True,
    )

    finished_at = utc_now_iso()
    finished_dt = datetime.now(timezone.utc)
    duration_seconds = round((finished_dt - started_dt).total_seconds(), 3)

    file_size = output_path.stat().st_size
    checksum = calculate_sha256(output_path)

    metadata = ResourceMetadata(
        run_id=run_id,
        source_name=SOURCE_NAME,
        resource_key=resource_key,
        file_name=file_name,
        source_url=url,
        role=resource.get("role"),
        year=resource.get("year"),
        granularity=resource.get("granularity"),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        http_status_code=response.status_code,
        content_type=content_type,
        content_length_bytes=content_length_bytes,
        downloaded_file_size_bytes=file_size,
        checksum_sha256=checksum,
        final_status="SUCCESS",
        partial_file_used=download_result.partial_file_used,
        partial_file_path=str(download_result.partial_path),
        resumed_from_bytes=download_result.resumed_from_bytes,
        range_request_used=download_result.range_request_used,
        server_supports_resume=download_result.server_supports_resume,
        download_duration_seconds=download_result.duration_seconds,
        average_speed_mbps=download_result.average_speed_mbps,
    )

    metadata_path.write_text(
        json.dumps(asdict(metadata), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    response.close()

    print(f"Archivo guardado: {output_path}")
    print(f"Metadata guardada: {metadata_path}")
    print(f"Tamaño descargado: {file_size:,} bytes")
    print(f"Checksum SHA256: {checksum}")
    print(f"Duración descarga: {download_result.duration_seconds} s")
    print(f"Velocidad promedio: {download_result.average_speed_mbps} MB/s")

    audit_result(
        run_id=run_id,
        source_name=SOURCE_NAME,
        resource_key=resource_key,
        file_name=file_name,
        status="SUCCESS",
        message="Recurso descargado correctamente.",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        metadata={
            "source_url": url,
            "access_method": resource.get("format"),
            "http_status_code": response.status_code,
            "content_type": content_type,
            "content_length_bytes": content_length_bytes,
            "downloaded_file_size_bytes": file_size,
            "checksum_sha256": checksum,
            "output_path": str(output_path),
            "metadata_path": str(metadata_path),
            "partial_file_used": download_result.partial_file_used,
            "partial_file_path": str(download_result.partial_path),
            "resumed_from_bytes": download_result.resumed_from_bytes,
            "range_request_used": download_result.range_request_used,
            "server_supports_resume": download_result.server_supports_resume,
            "download_duration_seconds": download_result.duration_seconds,
            "average_speed_mbps": download_result.average_speed_mbps,
            "max_attempts": retry_config.max_attempts,
        },
    )

    return metadata


def parse_args() -> argparse.Namespace:
    """Define argumentos de ejecución."""

    parser = argparse.ArgumentParser(
        description="Descarga recursos MEF de ingresos hacia Landing."
    )
    parser.add_argument(
        "--resource",
        action="append",
        dest="resources",
        help=(
            "Clave del recurso definido en config/sources.yaml. "
            "Puede repetirse. Ejemplo: --resource annual_2024 --resource dictionary."
        ),
    )
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        dest="years",
        help=(
            "Año de recursos MEF a descargar. Puede repetirse. "
            "Ejemplo: --year 2023 --year 2024."
        ),
    )
    parser.add_argument(
        "--granularity",
        action="append",
        choices=["annual", "monthly", "daily"],
        dest="granularities",
        help=(
            "Granularidad a descargar. Puede repetirse. "
            "Ejemplo: --granularity annual."
        ),
    )
    parser.add_argument(
        "--include-documentation",
        action="store_true",
        help="Incluye recursos marcados como documentación, por ejemplo diccionarios.",
    )
    parser.add_argument(
        "--all-resources",
        action="store_true",
        help=(
            "Descarga todos los recursos MEF configurados. "
            "Usar con cuidado porque puede descargar archivos grandes."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout en segundos para solicitudes HTTP.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe archivos existentes en Landing.",
    )
    parser.add_argument(
        "--list-resources",
        action="store_true",
        help="Lista los recursos MEF configurados y termina sin descargar.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida disponibilidad de recursos sin descargar archivos.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada del script."""

    args = parse_args()
    run_id = create_run_id(SOURCE_NAME)

    try:
        source_config = load_siaf_income_config()
        resources = get_candidate_resources(source_config)

        if args.list_resources:
            print_available_resources(resources)
            return

        selected_resources = select_resources(
            resources=resources,
            resource_keys=args.resources,
            years=args.years,
            granularities=args.granularities,
            include_documentation=args.include_documentation,
            all_resources=args.all_resources,
        )

        landing_subdir = source_config.get("landing_subdir", SOURCE_NAME)
        output_dir = get_source_landing_path(landing_subdir)

        print("=" * 80)
        print("Iniciando ingesta SIAF income hacia Landing")
        print(f"Directorio Landing: {output_dir}")
        print(f"Recursos seleccionados: {', '.join(selected_resources)}")

        audit_info(
            run_id=run_id,
            source_name=SOURCE_NAME,
            event_type="INGESTION_START",
            message="Inicio de proceso de ingesta.",
            metadata={
                "selected_resources": list(selected_resources),
                "dry_run": args.dry_run,
            },
        )

        for resource_key, resource in selected_resources.items():
            download_resource(
                resource_key=resource_key,
                resource=resource,
                output_dir=output_dir,
                timeout_seconds=args.timeout,
                overwrite=args.overwrite,
                dry_run=args.dry_run,
            )

        audit_info(
            run_id=run_id,
            source_name=SOURCE_NAME,
            event_type="INGESTION_FINISH",
            message="Proceso de ingesta finalizado.",
            metadata={
                "selected_resources": list(selected_resources),
                "dry_run": args.dry_run,
            },
        )

        print("=" * 80)
        print("Proceso SIAF income finalizado")

    except (requests.RequestException, RetryError, IngestionError, OSError, ValueError) as exc:
        audit_info(
            run_id=run_id,
            source_name=SOURCE_NAME,
            event_type="INGESTION_FINISH",
            message=f"Proceso de ingesta fallido: {type(exc).__name__}: {exc}",
            metadata={
                "dry_run": getattr(args, "dry_run", None),
                "error": str(exc),
            },
        )

        print("=" * 80)
        print("Proceso SIAF income fallido")
        print(f"Error: {type(exc).__name__}: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
