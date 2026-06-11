"""Utilidades de auditoría de ingesta.

Este módulo centraliza la escritura de registros de auditoría técnica para las
descargas hacia Landing. La auditoría se guarda localmente en data/quality/ y no
debe versionarse en Git.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import get_config_value, load_audit_config
from src.common.paths import PROJECT_ROOT


@dataclass(frozen=True)
class IngestionAuditRecord:
    """Registro estándar de auditoría de ingesta."""

    run_id: str
    source_name: str
    source_url: str
    access_method: str
    started_at: str
    finished_at: str
    duration_seconds: float
    attempt_number: int
    max_attempts: int
    retry_count: int
    http_status_code: int | None
    error_type: str | None
    error_message: str | None
    downloaded_file_name: str | None
    downloaded_file_size_bytes: int | None
    checksum_sha256: str | None
    records_detected: int | None
    final_status: str


def utc_now_iso() -> str:
    """Devuelve fecha y hora actual en UTC."""

    return datetime.now(timezone.utc).isoformat()


def resolve_audit_path() -> Path:
    """Resuelve la ruta local del archivo de auditoría."""

    config = load_audit_config()
    relative_path = get_config_value(
        config,
        "audit.storage.relative_path",
        "data/quality/ingestion_audit.jsonl",
    )

    return PROJECT_ROOT / relative_path


def append_audit_record(record: IngestionAuditRecord) -> Path:
    """Agrega un registro de auditoría al archivo JSONL local."""

    output_path = resolve_audit_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")

    return output_path


def read_audit_records(audit_path: Path | None = None) -> list[dict[str, Any]]:
    """Lee registros de auditoría JSONL.

    Esta función se usa principalmente para pruebas o revisión local.
    """

    resolved_path = audit_path or resolve_audit_path()

    if not resolved_path.exists():
        return []

    records: list[dict[str, Any]] = []

    with resolved_path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped_line = line.strip()
            if stripped_line:
                records.append(json.loads(stripped_line))

    return records


def create_run_id(source_name: str) -> str:
    """Crea un identificador único de ejecución para una fuente."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    return f"{source_name}_{timestamp}_{suffix}"


def write_audit_event(event: dict[str, Any]) -> Path:
    """Escribe un evento de auditoría genérico en formato JSON Lines."""

    output_path = resolve_audit_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False) + "\n")

    return output_path


def audit_info(
    *,
    run_id: str,
    source_name: str,
    event_type: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Registra un evento informativo de auditoría."""

    event = {
        "timestamp": utc_now_iso(),
        "level": "INFO",
        "run_id": run_id,
        "source_name": source_name,
        "event_type": event_type,
        "message": message,
        "metadata": metadata or {},
    }

    return write_audit_event(event)


def audit_result(
    *,
    run_id: str,
    source_name: str,
    resource_key: str,
    file_name: str,
    status: str,
    message: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    metadata: dict[str, Any] | None = None,
) -> Path:
    """Registra el resultado de procesamiento de un recurso."""

    event = {
        "timestamp": utc_now_iso(),
        "level": "INFO" if status == "SUCCESS" else "ERROR",
        "run_id": run_id,
        "source_name": source_name,
        "event_type": "RESOURCE_RESULT",
        "resource_key": resource_key,
        "file_name": file_name,
        "status": status,
        "message": message,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "metadata": metadata or {},
    }

    return write_audit_event(event)


def build_audit_record(
    *,
    run_id: str,
    source_name: str,
    source_url: str,
    access_method: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    attempt_number: int,
    max_attempts: int,
    retry_count: int,
    http_status_code: int | None,
    error_type: str | None,
    error_message: str | None,
    downloaded_file_name: str | None,
    downloaded_file_size_bytes: int | None,
    checksum_sha256: str | None,
    records_detected: int | None,
    final_status: str,
) -> IngestionAuditRecord:
    """Construye un registro de auditoría con nombres de campos estándar."""

    return IngestionAuditRecord(
        run_id=run_id,
        source_name=source_name,
        source_url=source_url,
        access_method=access_method,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        attempt_number=attempt_number,
        max_attempts=max_attempts,
        retry_count=retry_count,
        http_status_code=http_status_code,
        error_type=error_type,
        error_message=error_message,
        downloaded_file_name=downloaded_file_name,
        downloaded_file_size_bytes=downloaded_file_size_bytes,
        checksum_sha256=checksum_sha256,
        records_detected=records_detected,
        final_status=final_status,
    )