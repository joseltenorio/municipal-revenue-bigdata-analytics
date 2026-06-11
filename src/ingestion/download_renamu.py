"""Descarga y extracción controlada de la fuente RENAMU 2022.

Este script implementa la ingesta inicial hacia Landing para RENAMU 2022.
Descarga recursos definidos en config/sources.yaml, conserva los archivos
originales y permite extraer el ZIP completo de forma controlada.

No construye Bronze, no limpia datos, no interpreta columnas y no selecciona
variables analíticas.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src.common.config import load_sources_config
from src.common.paths import get_source_landing_path


SOURCE_NAME = "renamu"
DEFAULT_CHUNK_SIZE = 1024 * 1024
SUPPORTED_FORMATS = {"zip", "pdf", "xlsx"}


class IngestionError(Exception):
    """Error controlado durante la ingesta RENAMU."""


@dataclass(frozen=True)
class ResourceMetadata:
    """Metadata básica de un recurso descargado hacia Landing."""

    run_id: str
    source_name: str
    resource_key: str
    file_name: str
    source_url: str
    started_at: str
    finished_at: str
    duration_seconds: float
    http_status_code: int
    content_type: str | None
    content_length_bytes: int | None
    downloaded_file_size_bytes: int
    checksum_sha256: str
    final_status: str


@dataclass(frozen=True)
class ExtractionMetadata:
    """Metadata básica de extracción de un ZIP."""

    run_id: str
    source_name: str
    zip_file_name: str
    extracted_at: str
    extraction_dir: str
    extracted_files_count: int
    extracted_files: list[str]


def utc_now_iso() -> str:
    """Devuelve fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def format_bytes(value: int | None) -> str:
    """Formatea bytes para impresión en consola."""

    if value is None:
        return "no_disponible"

    return f"{value:,} bytes"


def load_renamu_config() -> dict[str, Any]:
    """Carga la configuración de la fuente RENAMU."""

    config = load_sources_config()
    sources = config.get("sources", {})

    if SOURCE_NAME not in sources:
        raise IngestionError(f"No existe la fuente '{SOURCE_NAME}' en config/sources.yaml.")

    source_config = sources[SOURCE_NAME]

    if not source_config.get("enabled", False):
        raise IngestionError(f"La fuente '{SOURCE_NAME}' no está habilitada.")

    return source_config


def get_candidate_resources(source_config: dict[str, Any]) -> dict[str, Any]:
    """Obtiene recursos candidatos definidos para RENAMU."""

    resources = source_config.get("candidate_resources", {})

    if not resources:
        raise IngestionError(
            "No existen candidate_resources para renamu en config/sources.yaml."
        )

    return resources


def select_resources(
    resources: dict[str, Any],
    resource_key: str | None,
    include_documentation: bool,
    all_enabled: bool,
) -> dict[str, Any]:
    """Selecciona recursos a descargar según argumentos de ejecución."""

    if resource_key:
        if resource_key not in resources:
            available = ", ".join(sorted(resources))
            raise IngestionError(
                f"Recurso no encontrado: '{resource_key}'. "
                f"Recursos disponibles: {available}."
            )
        return {resource_key: resources[resource_key]}

    selected_resources: dict[str, Any] = {}

    for key, resource in resources.items():
        use_for_ingestion = bool(resource.get("use_for_ingestion", False))
        use_for_documentation = bool(resource.get("use_for_documentation", False))

        if all_enabled and (use_for_ingestion or use_for_documentation):
            selected_resources[key] = resource
            continue

        if include_documentation and use_for_documentation:
            selected_resources[key] = resource
            continue

        if use_for_ingestion:
            selected_resources[key] = resource

    if not selected_resources:
        available = ", ".join(sorted(resources))
        raise IngestionError(
            "No se seleccionó ningún recurso RENAMU para descargar. "
            "Usa --resource, --include-documentation o --all-enabled. "
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

    resource_format = str(resource["format"]).lower()

    if resource_format not in SUPPORTED_FORMATS:
        supported = ", ".join(sorted(SUPPORTED_FORMATS))
        raise IngestionError(
            f"El recurso '{resource_key}' tiene formato '{resource_format}'. "
            f"Formatos soportados: {supported}."
        )


def probe_resource(url: str, timeout_seconds: int) -> requests.Response:
    """Valida disponibilidad del recurso usando HEAD con fallback a GET."""

    response = requests.head(url, timeout=timeout_seconds, allow_redirects=True)

    if response.status_code in {403, 405}:
        response = requests.get(
            url,
            timeout=timeout_seconds,
            allow_redirects=True,
            stream=True,
        )

    response.raise_for_status()
    return response


def calculate_sha256(file_path: Path) -> str:
    """Calcula checksum SHA256 de un archivo local."""

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(DEFAULT_CHUNK_SIZE), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def download_resource(
    resource_key: str,
    resource: dict[str, Any],
    output_dir: Path,
    timeout_seconds: int,
    overwrite: bool,
    dry_run: bool,
) -> Path | None:
    """Descarga un recurso RENAMU hacia Landing."""

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
    content_length = response.headers.get("content-length")
    content_length_bytes = (
        int(content_length)
        if content_length and content_length.isdigit()
        else None
    )

    print(f"Estado HTTP: {response.status_code}")
    print(f"Tipo de contenido: {content_type}")
    print(f"Tamaño declarado: {format_bytes(content_length_bytes)}")

    if dry_run:
        print("Dry run activo. No se descargó el archivo.")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not overwrite:
        raise IngestionError(
            f"El archivo ya existe en Landing: {output_path}. "
            "Usa --overwrite si deseas reemplazarlo."
        )

    downloaded_bytes = 0

    with requests.get(url, timeout=timeout_seconds, stream=True) as download_response:
        download_response.raise_for_status()

        with output_path.open("wb") as file:
            for chunk in download_response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                if chunk:
                    file.write(chunk)
                    downloaded_bytes += len(chunk)

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
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        http_status_code=response.status_code,
        content_type=content_type,
        content_length_bytes=content_length_bytes,
        downloaded_file_size_bytes=file_size,
        checksum_sha256=checksum,
        final_status="SUCCESS",
    )

    metadata_path.write_text(
        json.dumps(asdict(metadata), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Descargado aproximadamente: {format_bytes(downloaded_bytes)}")
    print(f"Archivo guardado: {output_path}")
    print(f"Metadata guardada: {metadata_path}")
    print(f"Tamaño descargado: {format_bytes(file_size)}")
    print(f"Checksum SHA256: {checksum}")

    return output_path


def validate_zip_member(zip_member: zipfile.ZipInfo, extraction_dir: Path) -> Path:
    """Valida que un archivo ZIP no intente escribir fuera del directorio destino."""

    destination_path = extraction_dir / zip_member.filename
    resolved_destination = destination_path.resolve()
    resolved_extraction_dir = extraction_dir.resolve()

    if not str(resolved_destination).startswith(str(resolved_extraction_dir)):
        raise IngestionError(
            f"Entrada insegura dentro del ZIP: {zip_member.filename}"
        )

    return destination_path


def extract_zip_file(
    zip_path: Path,
    extraction_dir: Path,
    overwrite: bool,
) -> ExtractionMetadata:
    """Extrae un archivo ZIP RENAMU de forma controlada."""

    if not zip_path.exists():
        raise IngestionError(f"No existe el ZIP a extraer: {zip_path}")

    if extraction_dir.exists() and any(extraction_dir.iterdir()) and not overwrite:
        raise IngestionError(
            f"El directorio de extracción ya contiene archivos: {extraction_dir}. "
            "Usa --overwrite si deseas reemplazar o completar la extracción."
        )

    extraction_dir.mkdir(parents=True, exist_ok=True)

    extracted_files: list[str] = []
    run_id = uuid.uuid4().hex[:12]

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        for member in zip_file.infolist():
            validate_zip_member(zip_member=member, extraction_dir=extraction_dir)
            zip_file.extract(member, extraction_dir)

            if not member.is_dir():
                extracted_files.append(member.filename)

    metadata = ExtractionMetadata(
        run_id=run_id,
        source_name=SOURCE_NAME,
        zip_file_name=zip_path.name,
        extracted_at=utc_now_iso(),
        extraction_dir=str(extraction_dir),
        extracted_files_count=len(extracted_files),
        extracted_files=extracted_files,
    )

    metadata_path = extraction_dir / f"{zip_path.name}.extraction.metadata.json"
    metadata_path.write_text(
        json.dumps(asdict(metadata), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("=" * 80)
    print("Extracción RENAMU finalizada")
    print(f"ZIP: {zip_path}")
    print(f"Directorio de extracción: {extraction_dir}")
    print(f"Archivos extraídos: {len(extracted_files)}")
    print(f"Metadata de extracción: {metadata_path}")

    return metadata


def print_available_resources(resources: dict[str, Any]) -> None:
    """Imprime recursos configurados para la fuente RENAMU."""

    print("=" * 80)
    print("Recursos RENAMU configurados")

    for resource_key, resource in resources.items():
        print(
            f"- {resource_key}: {resource.get('file_name')} | "
            f"formato={resource.get('format')} | "
            f"rol={resource.get('role')} | "
            f"prioridad={resource.get('priority')} | "
            f"ingesta={resource.get('use_for_ingestion', False)} | "
            f"documentacion={resource.get('use_for_documentation', False)}"
        )


def parse_args() -> argparse.Namespace:
    """Define argumentos de ejecución."""

    parser = argparse.ArgumentParser(
        description="Descarga y extrae recursos RENAMU 2022 hacia Landing."
    )
    parser.add_argument(
        "--resource",
        help="Clave del recurso definido en config/sources.yaml. Ejemplo: full_zip.",
    )
    parser.add_argument(
        "--include-documentation",
        action="store_true",
        help="Incluye recursos marcados como documentación, por ejemplo diccionario PDF.",
    )
    parser.add_argument(
        "--all-enabled",
        action="store_true",
        help="Descarga recursos de ingesta y documentación habilitados en la fuente.",
    )
    parser.add_argument(
        "--list-resources",
        action="store_true",
        help="Lista los recursos RENAMU configurados y termina sin descargar.",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Extrae archivos ZIP descargados o existentes en Landing.",
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
        help="Sobrescribe archivos existentes o permite reemplazar extracción.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida disponibilidad de recursos sin descargar ni extraer.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada del script."""

    args = parse_args()

    try:
        source_config = load_renamu_config()
        resources = get_candidate_resources(source_config)

        if args.list_resources:
            print_available_resources(resources)
            return

        selected_resources = select_resources(
            resources=resources,
            resource_key=args.resource,
            include_documentation=args.include_documentation,
            all_enabled=args.all_enabled,
        )

        landing_subdir = source_config.get("landing_subdir", SOURCE_NAME)
        output_dir = get_source_landing_path(landing_subdir)
        extraction_dir = output_dir / "extracted"

        print("=" * 80)
        print("Iniciando ingesta RENAMU hacia Landing")
        print(f"Directorio Landing: {output_dir}")
        print(f"Directorio de extracción: {extraction_dir}")
        print(f"Recursos seleccionados: {', '.join(selected_resources)}")

        downloaded_paths: list[Path] = []

        for resource_key, resource in selected_resources.items():
            downloaded_path = download_resource(
                resource_key=resource_key,
                resource=resource,
                output_dir=output_dir,
                timeout_seconds=args.timeout,
                overwrite=args.overwrite,
                dry_run=args.dry_run,
            )

            if downloaded_path:
                downloaded_paths.append(downloaded_path)

        if args.extract and not args.dry_run:
            zip_paths = [
                path for path in downloaded_paths
                if path.suffix.lower() == ".zip"
            ]

            if not zip_paths:
                existing_zip = output_dir / "2022.zip"
                if existing_zip.exists():
                    zip_paths = [existing_zip]

            if not zip_paths:
                raise IngestionError(
                    "No se encontró ningún ZIP para extraer. "
                    "Descarga full_zip o verifica que 2022.zip exista en Landing."
                )

            for zip_path in zip_paths:
                extract_zip_file(
                    zip_path=zip_path,
                    extraction_dir=extraction_dir,
                    overwrite=args.overwrite,
                )

        print("=" * 80)
        print("Proceso RENAMU finalizado")

    except (
        requests.RequestException,
        zipfile.BadZipFile,
        IngestionError,
        OSError,
        ValueError,
    ) as exc:
        print("=" * 80)
        print("Proceso RENAMU fallido")
        print(f"Error: {type(exc).__name__}: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()