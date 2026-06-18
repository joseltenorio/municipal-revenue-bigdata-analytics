"""Ejecuta la ingesta completa hacia Landing.

Este runner ejecuta, en orden, los scripts de ingesta de las tres fuentes
principales del proyecto:

1. SIAF ingresos.
2. SISMEPRE.
3. RENAMU 2022.

No construye Bronze, no ejecuta Spark y no transforma datos de negocio.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class IngestionStep:
    """Paso de ingesta ejecutable."""

    name: str
    module: str
    args: list[str]


def build_ingestion_steps(
    *,
    dry_run: bool,
    overwrite: bool,
    timeout: int,
    include_mef: bool,
    include_predial: bool,
    include_renamu: bool,
) -> list[IngestionStep]:
    """Construye la lista de pasos de ingesta a ejecutar."""

    common_args = ["--timeout", str(timeout)]

    if dry_run:
        common_args.append("--dry-run")

    if overwrite:
        common_args.append("--overwrite")

    steps: list[IngestionStep] = []

    if include_mef:
        steps.append(
            IngestionStep(
                name="SIAF ingresos",
                module="src.ingestion.download_siaf_income",
                args=[
                    "--all-resources",
                    "--include-documentation",
                    *common_args,
                ],
            )
        )

    if include_predial:
        steps.append(
            IngestionStep(
                name="SISMEPRE",
                module="src.ingestion.download_sismepre",
                args=[
                    "--all-enabled",
                    *common_args,
                ],
            )
        )

    if include_renamu:
        renamu_args = [
            "--all-enabled",
            *common_args,
        ]

        if not dry_run:
            renamu_args.append("--extract")

        steps.append(
            IngestionStep(
                name="RENAMU 2022",
                module="src.ingestion.download_renamu",
                args=renamu_args,
            )
        )

    return steps


def run_step(step_number: int, total_steps: int, step: IngestionStep) -> None:
    """Ejecuta un paso de ingesta y detiene el runner si falla."""

    command = [
        sys.executable,
        "-m",
        step.module,
        *step.args,
    ]

    print("\n" + "=" * 80)
    print(f"Paso {step_number}/{total_steps}: {step.name}")
    print(f"Módulo: {step.module}")
    print(f"Comando: {' '.join(command)}")
    print("=" * 80)

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Falló el paso {step_number}/{total_steps}: {step.name}. "
            f"Código de salida: {result.returncode}"
        )

    print(f"Paso {step_number}/{total_steps} finalizado correctamente: {step.name}")


def parse_args() -> argparse.Namespace:
    """Define argumentos del runner de ingesta."""

    parser = argparse.ArgumentParser(
        description="Ejecuta la ingesta completa hacia Landing."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida disponibilidad sin descargar archivos.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescribe archivos existentes y elimina temporales .part previos.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout en segundos para solicitudes HTTP de los scripts hijos.",
    )
    parser.add_argument(
        "--skip-mef",
        action="store_true",
        help="Omite la ingesta de SIAF ingresos.",
    )
    parser.add_argument(
        "--skip-predial",
        action="store_true",
        help="Omite la ingesta de SISMEPRE.",
    )
    parser.add_argument(
        "--skip-renamu",
        action="store_true",
        help="Omite la ingesta de RENAMU 2022.",
    )
    return parser.parse_args()


def main() -> None:
    """Ejecuta la ingesta completa."""

    args = parse_args()

    steps = build_ingestion_steps(
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        timeout=args.timeout,
        include_mef=not args.skip_mef,
        include_predial=not args.skip_predial,
        include_renamu=not args.skip_renamu,
    )

    if not steps:
        raise SystemExit("No hay pasos de ingesta para ejecutar.")

    print("=" * 80)
    print("Iniciando runner de ingesta hacia Landing")
    print(f"Directorio del proyecto: {PROJECT_ROOT}")
    print(f"Modo dry-run: {args.dry_run}")
    print(f"Overwrite: {args.overwrite}")
    print(f"Pasos seleccionados: {', '.join(step.name for step in steps)}")

    try:
        for index, step in enumerate(steps, start=1):
            run_step(step_number=index, total_steps=len(steps), step=step)
    except RuntimeError as exc:
        print("\n" + "=" * 80)
        print("Runner de ingesta fallido")
        print(f"Error: {exc}")
        raise SystemExit(1) from exc

    print("\n" + "=" * 80)
    print("Runner de ingesta finalizado correctamente")
    print("=" * 80)


if __name__ == "__main__":
    main()
