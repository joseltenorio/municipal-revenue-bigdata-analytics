"""Generación de reporte HTML para resultados de calidad Silver."""

from __future__ import annotations

import argparse
import html
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.paths import PROJECT_ROOT, QUALITY_DIR, REPORTS_DIR


DEFAULT_INPUT_PATH = QUALITY_DIR / "silver_quality_results.jsonl"
DEFAULT_OUTPUT_PATH = REPORTS_DIR / "silver_quality_report.html"


class SilverQualityReportError(Exception):
    """Error controlado durante la generación del reporte Silver."""


def resolve_project_path(path_value: str | Path) -> Path:
    """Resuelve una ruta absoluta o relativa al proyecto."""

    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def read_silver_quality_results(input_path: Path) -> list[dict[str, Any]]:
    """Lee resultados Silver desde JSON Lines."""

    if not input_path.exists():
        raise SilverQualityReportError(
            f"No existe el archivo de resultados Silver: {input_path}"
        )

    results: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            stripped_line = line.strip()
            if not stripped_line:
                continue
            try:
                results.append(json.loads(stripped_line))
            except json.JSONDecodeError as exc:
                raise SilverQualityReportError(
                    f"JSON inválido en línea {line_number}: {exc}"
                ) from exc

    return results


def summarize_by_field(results: list[dict[str, Any]], field_name: str) -> Counter[str]:
    """Resume resultados por campo."""

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
    """Renderiza una tabla HTML para un contador."""

    rows = "\n".join(
        "<tr><td>{}</td><td>{}</td></tr>".format(html.escape(key), value)
        for key, value in sorted(counter.items())
    )
    return f"""
    <h2>{html.escape(title)}</h2>
    <table>
      <thead><tr><th>Valor</th><th>Cantidad</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    """


def render_source_status_table(results: list[dict[str, Any]]) -> str:
    """Renderiza resumen por fuente y estado."""

    summary = summarize_by_source_and_status(results)
    rows = []
    for source_name, counter in sorted(summary.items()):
        rows.append(
            "<tr>"
            f"<td>{html.escape(source_name)}</td>"
            f"<td>{counter.get('PASS', 0)}</td>"
            f"<td>{counter.get('WARNING', 0)}</td>"
            f"<td>{counter.get('FAIL', 0)}</td>"
            "</tr>"
        )

    return f"""
    <h2>Resumen por fuente y estado</h2>
    <table>
      <thead><tr><th>Fuente</th><th>PASS</th><th>WARNING</th><th>FAIL</th></tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    """


def render_problem_resources(results: list[dict[str, Any]]) -> str:
    """Renderiza recursos con FAIL y principales WARNING."""

    problem_results = [
        result
        for result in results
        if str(result.get("status")) in {"FAIL", "WARNING"}
    ][:80]
    rows = []
    for result in problem_results:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(result.get('status', '')))}</td>"
            f"<td>{html.escape(str(result.get('source_name', '')))}</td>"
            f"<td>{html.escape(str(result.get('resource_key', '')))}</td>"
            f"<td>{html.escape(str(result.get('rule_name', '')))}</td>"
            f"<td>{html.escape(str(result.get('message', '')))}</td>"
            "</tr>"
        )

    return f"""
    <h2>Recursos con alertas</h2>
    <table>
      <thead>
        <tr><th>Estado</th><th>Fuente</th><th>Recurso</th><th>Regla</th><th>Mensaje</th></tr>
      </thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    """


def render_rule_listing(results: list[dict[str, Any]]) -> str:
    """Renderiza listado resumido de reglas evaluadas."""

    rows = []
    for result in results[:300]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(result.get('source_name', '')))}</td>"
            f"<td>{html.escape(str(result.get('resource_key', '')))}</td>"
            f"<td>{html.escape(str(result.get('rule_name', '')))}</td>"
            f"<td>{html.escape(str(result.get('status', '')))}</td>"
            f"<td>{html.escape(str(result.get('evaluated', '')))}</td>"
            f"<td>{html.escape(str(result.get('message', '')))}</td>"
            "</tr>"
        )

    return f"""
    <h2>Listado de reglas</h2>
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
    """Genera HTML completo del reporte Silver."""

    generated_at = datetime.now(timezone.utc).isoformat()
    status_summary = summarize_by_field(results, "status")
    source_summary = summarize_by_field(results, "source_name")
    rule_summary = summarize_by_field(results, "rule_name")

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Reporte de calidad Silver</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 32px; color: #1f2933; }}
    h1, h2 {{ color: #102a43; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
    th, td {{ border: 1px solid #bcccdc; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f0f4f8; }}
    .summary {{ margin-bottom: 24px; }}
    .meta {{ color: #52606d; }}
  </style>
</head>
<body>
  <h1>Reporte de calidad Silver</h1>
  <p class="summary">Resultados evaluados: {len(results)}</p>
  <p class="meta">Generado en UTC: {html.escape(generated_at)}</p>
  {render_counter_table("Resumen por estado", status_summary)}
  {render_counter_table("Resumen por fuente", source_summary)}
  {render_counter_table("Resumen por regla", rule_summary)}
  {render_source_status_table(results)}
  {render_problem_resources(results)}
  {render_rule_listing(results)}
</body>
</html>
"""


def write_html_report(results: list[dict[str, Any]], output_path: Path) -> Path:
    """Escribe el reporte HTML Silver."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html_report(results), encoding="utf-8")
    return output_path


def generate_silver_quality_report(input_path: Path, output_path: Path) -> Path:
    """Lee JSONL Silver y genera HTML."""

    results = read_silver_quality_results(input_path)
    return write_html_report(results=results, output_path=output_path)


def parse_args() -> argparse.Namespace:
    """Procesa argumentos CLI."""

    parser = argparse.ArgumentParser(
        description="Genera un reporte HTML desde resultados JSONL de calidad Silver."
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
    generated_path = generate_silver_quality_report(
        input_path=input_path,
        output_path=output_path,
    )
    print(f"Reporte Silver generado: {generated_path}")


if __name__ == "__main__":
    main()
