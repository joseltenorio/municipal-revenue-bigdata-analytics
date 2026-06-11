"""Profiling inicial de archivos locales en Landing.

Este modulo no descarga fuentes externas. Su objetivo es inspeccionar archivos
que ya existan localmente en la capa Landing y generar un resumen tecnico
basico para orientar decisiones posteriores de Bronze, Silver y calidad.

Si Landing no contiene archivos soportados, el script genera un reporte vacio
controlado. Esto es esperado antes de implementar la ingesta real.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from src.common.paths import LANDING_DIR, REPORTS_DIR


SUPPORTED_EXTENSIONS = {".csv", ".txt", ".xlsx", ".xls", ".json", ".parquet"}


@dataclass(frozen=True)
class ColumnProfile:
    """Resumen tecnico de una columna."""

    column_name: str
    inferred_dtype: str
    non_null_count: int
    null_count: int
    null_rate: float
    unique_count: int
    sample_values: list[str]


@dataclass(frozen=True)
class FileProfile:
    """Resumen tecnico de un archivo perfilado."""

    file_path: str
    file_name: str
    file_extension: str
    profiled_at: str
    row_count: int
    column_count: int
    duplicate_rows: int
    columns: list[ColumnProfile]
    candidate_key_notes: list[str]
    error: str | None


def find_supported_files(input_dir: Path) -> list[Path]:
    """Busca archivos soportados dentro de un directorio."""

    if not input_dir.exists():
        return []

    return sorted(
        file_path
        for file_path in input_dir.rglob("*")
        if file_path.is_file()
        and file_path.suffix.lower() in SUPPORTED_EXTENSIONS
        and not file_path.name.startswith(".")
    )


def read_file(file_path: Path, max_rows: int) -> pd.DataFrame:
    """Lee una muestra controlada de un archivo local soportado."""

    extension = file_path.suffix.lower()

    if extension in {".csv", ".txt"}:
        return pd.read_csv(file_path, nrows=max_rows, low_memory=False)

    if extension in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, nrows=max_rows)

    if extension == ".json":
        return pd.read_json(file_path).head(max_rows)

    if extension == ".parquet":
        return pd.read_parquet(file_path).head(max_rows)

    raise ValueError(f"Extension no soportada: {extension}")


def normalize_sample_values(values: Iterable[Any], max_values: int = 5) -> list[str]:
    """Normaliza valores de muestra para serializarlos como texto."""

    sample_values: list[str] = []

    for value in values:
        if pd.isna(value):
            continue

        text_value = str(value)

        if text_value not in sample_values:
            sample_values.append(text_value)

        if len(sample_values) >= max_values:
            break

    return sample_values


def profile_dataframe(file_path: Path, dataframe: pd.DataFrame) -> FileProfile:
    """Construye el perfil tecnico de un DataFrame."""

    profiled_at = datetime.now(timezone.utc).isoformat()
    row_count = int(len(dataframe))
    column_count = int(len(dataframe.columns))
    duplicate_rows = int(dataframe.duplicated().sum()) if row_count > 0 else 0

    columns: list[ColumnProfile] = []
    candidate_key_notes: list[str] = []

    for column_name in dataframe.columns:
        series = dataframe[column_name]

        non_null_count = int(series.notna().sum())
        null_count = int(series.isna().sum())
        null_rate = round(null_count / row_count, 6) if row_count else 0.0
        unique_count = int(series.nunique(dropna=True))
        sample_values = normalize_sample_values(series.dropna().head(50).tolist())

        columns.append(
            ColumnProfile(
                column_name=str(column_name),
                inferred_dtype=str(series.dtype),
                non_null_count=non_null_count,
                null_count=null_count,
                null_rate=null_rate,
                unique_count=unique_count,
                sample_values=sample_values,
            )
        )

        if row_count > 0 and null_count == 0 and unique_count == row_count:
            candidate_key_notes.append(
                f"La columna '{column_name}' no tiene nulos y es unica en la muestra."
            )

    return FileProfile(
        file_path=str(file_path),
        file_name=file_path.name,
        file_extension=file_path.suffix.lower(),
        profiled_at=profiled_at,
        row_count=row_count,
        column_count=column_count,
        duplicate_rows=duplicate_rows,
        columns=columns,
        candidate_key_notes=candidate_key_notes,
        error=None,
    )


def profile_file(file_path: Path, max_rows: int) -> FileProfile:
    """Perfila un archivo y captura errores de lectura."""

    profiled_at = datetime.now(timezone.utc).isoformat()

    try:
        dataframe = read_file(file_path=file_path, max_rows=max_rows)
        return profile_dataframe(file_path=file_path, dataframe=dataframe)

    except Exception as exc:  # noqa: BLE001
        return FileProfile(
            file_path=str(file_path),
            file_name=file_path.name,
            file_extension=file_path.suffix.lower(),
            profiled_at=profiled_at,
            row_count=0,
            column_count=0,
            duplicate_rows=0,
            columns=[],
            candidate_key_notes=[],
            error=f"{type(exc).__name__}: {exc}",
        )


def write_json_report(profiles: list[FileProfile], output_path: Path) -> None:
    """Escribe el reporte de profiling en formato JSON."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile_count": len(profiles),
        "profiles": [asdict(profile) for profile in profiles],
    }

    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def print_summary(profiles: list[FileProfile]) -> None:
    """Imprime un resumen en consola."""

    print("=" * 80)
    print("Resumen de profiling")
    print(f"Archivos perfilados: {len(profiles)}")

    if not profiles:
        print("No se encontraron archivos locales soportados en Landing.")
        print("Esto es esperado antes de implementar la ingesta real.")
        return

    for profile in profiles:
        print("-" * 80)
        print(f"Archivo: {profile.file_name}")
        print(f"Filas analizadas: {profile.row_count}")
        print(f"Columnas: {profile.column_count}")
        print(f"Duplicados exactos: {profile.duplicate_rows}")
        print(f"Error: {profile.error}")


def parse_args() -> argparse.Namespace:
    """Define argumentos de ejecucion."""

    parser = argparse.ArgumentParser(
        description="Ejecuta profiling inicial sobre archivos locales en Landing."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=LANDING_DIR,
        help="Directorio local a perfilar. Por defecto usa data/landing.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPORTS_DIR / "profiling_summary.json",
        help="Ruta del reporte JSON de salida.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=10_000,
        help="Cantidad maxima de filas a leer por archivo.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada del script."""

    args = parse_args()

    input_dir = args.input_dir
    output_path = args.output
    max_rows = args.max_rows

    files = find_supported_files(input_dir)

    profiles = [
        profile_file(file_path=file_path, max_rows=max_rows)
        for file_path in files
    ]

    write_json_report(profiles=profiles, output_path=output_path)
    print_summary(profiles=profiles)

    print("=" * 80)
    print(f"Reporte generado: {output_path}")


if __name__ == "__main__":
    main()