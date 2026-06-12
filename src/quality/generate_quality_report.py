"""Generación de reporte HTML para resultados de calidad Bronze."""

from __future__ import annotations

import argparse
import html
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from src.common.paths import PROJECT_ROOT, QUALITY_DIR, REPORTS_DIR


DEFAULT_INPUT_PATH = QUALITY_DIR / "bronze_quality_results.jsonl"
DEFAULT_OUTPUT_PATH = REPORTS_DIR / "data_quality_report.html"


class QualityReportError(Exception):
    """Error controlado durante la generación del reporte de calidad."""


def resolve_project_path(path_value: str | Path) -> Path:
    """Resuelve una ruta relativa al proyecto."""

    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def read_quality_results(input_path: Path) -> list[dict[str, Any]]:
    """Lee resultados de calidad desde un archivo JSON Lines."""

    if not input_path.exists():
        raise QualityReportError(f"No existe el archivo de resultados: {input_path}")

    results: list[dict[str, Any]] = []

    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            stripped_line = line.strip()
            if not stripped_line:
                continue
            try:
                results.append(json.loads(stripped_line))
            except json.JSONDecodeError as exc:
                raise QualityReportError(
                    f"JSON inválido en línea {line_number}: {exc}"
                ) from exc

    return results


def summarize_by_field(results: list[dict[str, Any]], field_name: str) -> Counter[str]:
    """Resume resultados por un campo de texto."""

    return Counter(str(result.get(field_name, "no_disponible")) for result in results)


def summarize_by_source_and_status(
    results: list[dict[str, Any]],
) -> dict[str, Counter[str]]:
    """Resume resultados por fuente y estado."""

    summary: dict[str, Counter[str]] = defaultdict(Counter)

    for result in results:
        source_name = str(result.get("source_name", "no_disponible"))
        status = str(result.get("status", "no_disponible"))
        summary[source_name][status] += 1

    return dict(summary)


def render_counter_table(title: str, counter: Counter[str]) -> str:
    """Renderiza una tabla HTML simple para un contador."""

    rows = "\n".join(
        "<tr><td>{}</td><td>{}</td></tr>".format(
            html.escape(key),
            value,
        )
        for key, value in sorted(counter.items())
    )
    return f"""
    <h2>{html.escape(title)}</h2>
    <table>
      <thead><tr><th>Valor</th><th>Cantidad</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def render_results_table(results: list[dict[str, Any]]) -> str:
    """Renderiza el detalle de reglas evaluadas."""

    rows = []
    for result in results:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(result.get('source_name', '')))}</td>"
            f"<td>{html.escape(str(result.get('resource_key', '')))}</td>"
            f"<td>{html.escape(str(result.get('rule_id', '')))}</td>"
            f"<td>{html.escape(str(result.get('status', '')))}</td>"
            f"<td>{html.escape(str(result.get('evaluated', '')))}</td>"
            f"<td>{html.escape(str(result.get('message', '')))}</td>"
            "</tr>"
        )

    return f"""
    <h2>Detalle de reglas</h2>
    <table>
      <thead>
        <tr>
          <th>Fuente</th>
          <th>Recurso</th>
          <th>Regla</th>
          <th>Estado</th>
          <th>Evaluada</th>
          <th>Mensaje</th>
        </tr>
      </thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    """


def render_html_report(results: list[dict[str, Any]]) -> str:
    """Genera el HTML completo del reporte de calidad."""

    status_summary = summarize_by_field(results, "status")
    source_summary = summarize_by_field(results, "source_name")
    source_status_summary = summarize_by_source_and_status(results)

    source_status_rows = []
    for source_name, counter in sorted(source_status_summary.items()):
        source_status_rows.append(
            "<tr>"
            f"<td>{html.escape(source_name)}</td>"
            f"<td>{counter.get('PASS', 0)}</td>"
            f"<td>{counter.get('WARNING', 0)}</td>"
            f"<td>{counter.get('FAIL', 0)}</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Reporte de calidad Bronze</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 32px; color: #1f2933; }}
    h1, h2 {{ color: #102a43; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
    th, td {{ border: 1px solid #bcccdc; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f0f4f8; }}
    .summary {{ margin-bottom: 24px; }}
  </style>
</head>
<body>
  <h1>Reporte de calidad Bronze</h1>
  <p class="summary">Resultados evaluados: {len(results)}</p>
  {render_counter_table("Resumen por estado", status_summary)}
  {render_counter_table("Resumen por fuente", source_summary)}
  <h2>Resumen por fuente y estado</h2>
  <table>
    <thead><tr><th>Fuente</th><th>PASS</th><th>WARNING</th><th>FAIL</th></tr></thead>
    <tbody>{''.join(source_status_rows)}</tbody>
  </table>
  {render_results_table(results)}
</body>
</html>
"""


def write_html_report(results: list[dict[str, Any]], output_path: Path) -> Path:
    """Escribe el reporte HTML en disco."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html_report(results), encoding="utf-8")
    return output_path


def generate_quality_report(input_path: Path, output_path: Path) -> Path:
    """Lee resultados JSONL y genera el reporte HTML."""

    results = read_quality_results(input_path)
    return write_html_report(results=results, output_path=output_path)


def parse_args() -> argparse.Namespace:
    """Procesa argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description="Genera un reporte HTML desde resultados JSONL de calidad."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Archivo JSONL de entrada.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Archivo HTML de salida.",
    )
    return parser.parse_args()


def main() -> None:
    """Punto de entrada CLI."""

    args = parse_args()
    input_path = resolve_project_path(args.input)
    output_path = resolve_project_path(args.output)
    generated_path = generate_quality_report(
        input_path=input_path,
        output_path=output_path,
    )
    print(f"Reporte generado: {generated_path}")


if __name__ == "__main__":
    main()
