"""Descarga segura de archivos para procesos de ingesta.

Este módulo centraliza la escritura de archivos descargados hacia Landing.

Características:
- Escribe primero en un archivo temporal `.part`.
- Renombra al archivo final solo cuando la descarga termina correctamente.
- Permite reanudar descargas si existe `.part` y el servidor soporta Range.
- Muestra progreso en consola sin agregar dependencias externas.
- Devuelve métricas útiles para metadata y auditoría.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.common.retry import RetryPolicy, request_with_retries


@dataclass(frozen=True)
class DownloadResult:
    """Resultado técnico de una descarga segura."""

    destination_path: Path
    partial_path: Path
    downloaded_bytes: int
    final_file_size_bytes: int
    resumed_from_bytes: int
    partial_file_used: bool
    range_request_used: bool
    server_supports_resume: bool
    started_at: str
    finished_at: str
    duration_seconds: float
    average_speed_mbps: float


def utc_now_iso() -> str:
    """Devuelve fecha y hora actual en UTC con formato ISO."""

    return datetime.now(timezone.utc).isoformat()


def format_bytes(value: int | None) -> str:
    """Formatea bytes para impresión legible en consola."""

    if value is None:
        return "no_disponible"

    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)

    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:,.2f} {unit}"
        size /= 1024

    return f"{value:,} B"


def print_progress(downloaded: int, total_size: int | None) -> None:
    """Imprime progreso de descarga en una sola línea."""

    if total_size and total_size > 0:
        percent = downloaded * 100 / total_size
        print(
            f"\rAvance: {format_bytes(downloaded)} / {format_bytes(total_size)} "
            f"({percent:6.2f}%)",
            end="",
            flush=True,
        )
    else:
        print(
            f"\rDescargado: {format_bytes(downloaded)}",
            end="",
            flush=True,
        )


def safe_download_file(
    *,
    url: str,
    destination_path: Path,
    retry_config: RetryPolicy,
    chunk_size: int,
    overwrite: bool = False,
    show_progress: bool = True,
) -> DownloadResult:
    """Descarga un archivo usando `.part` y renombrado final seguro.

    Si existe un archivo `.part`, intenta reanudar la descarga usando el header
    HTTP Range. Si el servidor ignora Range y responde 200, reinicia la descarga
    desde cero para evitar mezclar contenido.
    """

    started_at = utc_now_iso()
    started_dt = datetime.now(timezone.utc)

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = destination_path.with_suffix(destination_path.suffix + ".part")

    if destination_path.exists() and not overwrite:
        raise FileExistsError(
            f"El archivo final ya existe: {destination_path}. "
            "Usa --overwrite si deseas reemplazarlo."
        )

    if overwrite:
        if destination_path.exists():
            destination_path.unlink()
        if partial_path.exists():
            partial_path.unlink()

    resumed_from_bytes = partial_path.stat().st_size if partial_path.exists() else 0
    partial_file_used = partial_path.exists()
    range_request_used = resumed_from_bytes > 0
    server_supports_resume = False

    headers: dict[str, str] = {}
    file_mode = "wb"

    if resumed_from_bytes > 0:
        headers["Range"] = f"bytes={resumed_from_bytes}-"
        file_mode = "ab"
        print(
            f"Reanudando descarga desde {format_bytes(resumed_from_bytes)} "
            f"usando archivo temporal: {partial_path}"
        )
    else:
        print(f"Iniciando descarga hacia archivo temporal: {partial_path}")

    with request_with_retries(
        method="GET",
        url=url,
        retry_config=retry_config,
        stream=True,
        headers=headers or None,
    ) as response:
        response.raise_for_status()

        if response.status_code == 206:
            server_supports_resume = True
        elif resumed_from_bytes > 0 and response.status_code == 200:
            print(
                "\nEl servidor no respondió con 206 Partial Content. "
                "Se reiniciará la descarga desde cero."
            )
            partial_path.unlink(missing_ok=True)
            resumed_from_bytes = 0
            range_request_used = False
            partial_file_used = False
            file_mode = "wb"

        content_length = response.headers.get("content-length")
        response_content_length = (
            int(content_length)
            if content_length and content_length.isdigit()
            else None
        )

        if response_content_length is not None and response.status_code == 206:
            total_size = resumed_from_bytes + response_content_length
        else:
            total_size = response_content_length

        downloaded_total = resumed_from_bytes

        with partial_path.open(file_mode) as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue

                file.write(chunk)
                downloaded_total += len(chunk)

                if show_progress:
                    print_progress(downloaded=downloaded_total, total_size=total_size)

    if show_progress:
        print()

    partial_path.replace(destination_path)

    finished_at = utc_now_iso()
    finished_dt = datetime.now(timezone.utc)
    duration_seconds = round((finished_dt - started_dt).total_seconds(), 3)

    final_size = destination_path.stat().st_size
    average_speed_mbps = (
        round((final_size / (1024 * 1024)) / duration_seconds, 3)
        if duration_seconds > 0
        else 0.0
    )

    return DownloadResult(
        destination_path=destination_path,
        partial_path=partial_path,
        downloaded_bytes=max(0, final_size - resumed_from_bytes),
        final_file_size_bytes=final_size,
        resumed_from_bytes=resumed_from_bytes,
        partial_file_used=partial_file_used,
        range_request_used=range_request_used,
        server_supports_resume=server_supports_resume,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        average_speed_mbps=average_speed_mbps,
    )
