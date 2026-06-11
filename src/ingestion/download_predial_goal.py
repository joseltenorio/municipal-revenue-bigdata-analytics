"""Discovery de la fuente de seguimiento de meta del impuesto predial.

Este script valida disponibilidad inicial de URLs candidatas relacionadas con
la fuente predial. No descarga datos finales ni escribe archivos en Landing.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import requests


DEFAULT_URLS = [
    "https://datosabiertos.mef.gob.pe/dataset/seguimiento-de-la-meta-del-impuesto-predial",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_estadistica.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_preguntas.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_formulario.csv",
    "https://fs.datosabiertos.mef.gob.pe/datastorefiles/rentas_respuestas_diccionario.csv",
]


@dataclass(frozen=True)
class DiscoveryResult:
    """Resultado técnico de una prueba de acceso a una URL."""

    source_name: str
    url: str
    checked_at: str
    status_code: int | None
    content_type: str | None
    content_length: str | None
    final_url: str | None
    error: str | None


def probe_url(source_name: str, url: str, timeout_seconds: int) -> DiscoveryResult:
    """Ejecuta una prueba liviana de acceso usando HEAD y fallback a GET."""

    checked_at = datetime.now(timezone.utc).isoformat()

    try:
        response = requests.head(url, timeout=timeout_seconds, allow_redirects=True)

        if response.status_code in {403, 405}:
            response = requests.get(
                url,
                timeout=timeout_seconds,
                allow_redirects=True,
                stream=True,
            )

        return DiscoveryResult(
            source_name=source_name,
            url=url,
            checked_at=checked_at,
            status_code=response.status_code,
            content_type=response.headers.get("content-type"),
            content_length=response.headers.get("content-length"),
            final_url=response.url,
            error=None,
        )

    except requests.RequestException as exc:
        return DiscoveryResult(
            source_name=source_name,
            url=url,
            checked_at=checked_at,
            status_code=None,
            content_type=None,
            content_length=None,
            final_url=None,
            error=f"{type(exc).__name__}: {exc}",
        )


def print_results(results: Iterable[DiscoveryResult]) -> None:
    """Imprime resultados de discovery en consola."""

    for result in results:
        print("=" * 80)
        print(f"Fuente: {result.source_name}")
        print(f"URL evaluada: {result.url}")
        print(f"Fecha de revisión: {result.checked_at}")
        print(f"Estado HTTP: {result.status_code}")
        print(f"Tipo de contenido: {result.content_type}")
        print(f"Tamaño declarado: {result.content_length}")
        print(f"URL final: {result.final_url}")
        print(f"Error: {result.error}")


def parse_args() -> argparse.Namespace:
    """Define argumentos de ejecución para discovery."""

    parser = argparse.ArgumentParser(
        description="Ejecuta discovery liviano sobre la fuente de meta predial."
    )
    parser.add_argument(
        "--url",
        action="append",
        help="URL específica a evaluar. Puede repetirse varias veces.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout en segundos para cada solicitud.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada del script."""

    args = parse_args()
    urls = args.url if args.url else DEFAULT_URLS

    results = [
        probe_url(
            source_name="predial_goal",
            url=url,
            timeout_seconds=args.timeout,
        )
        for url in urls
    ]

    print_results(results)


if __name__ == "__main__":
    main()