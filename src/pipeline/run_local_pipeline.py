"""Runner local para refresco completo y reproducible del pipeline."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import pyarrow.dataset as ds

from src.common.paths import GOLD_DIR, PROJECT_ROOT, SQL_DIR


STAGE_SEQUENCE = ["bronze", "silver", "integration", "gold", "hive", "validate"]
DEFAULT_HIVE_URL = "jdbc:hive2://localhost:10000"
PROBLEMATIC_MATCH_STATUSES = {
    "missing_map",
    "unmatched",
    "invalid_ubigeo",
    "ambiguous_sec_ejec",
}
PRIMARY_GOLD_DATASETS = [
    "dim_municipality",
    "fact_siaf_income",
    "mart_municipal_revenue_overview",
]


class PipelineRunnerError(RuntimeError):
    """Error controlado del runner local."""


@dataclass(frozen=True)
class PipelineConfig:
    """Configuracion de ejecucion del runner."""

    stage: str
    from_stage: str | None
    overwrite: bool
    dry_run: bool
    include_bronze: bool
    skip_hive: bool
    skip_validate: bool
    fail_fast: bool
    hive_url: str
    verbose: bool


@dataclass(frozen=True)
class PipelineStep:
    """Paso atomico del pipeline."""

    stage: str
    name: str
    kind: str
    module: str | None = None
    command: list[str] | None = None
    sql_file: Path | None = None
    action: Callable[[PipelineConfig], str] | None = None


@dataclass
class StepResult:
    """Resultado ejecutado o planificado de un paso."""

    stage: str
    name: str
    status: str
    duration_seconds: float
    command_display: str
    error_summary: str | None = None
    details: str | None = None


@dataclass
class PipelineRunSummary:
    """Resumen final del pipeline."""

    results: list[StepResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0


def format_command(command: list[str]) -> str:
    """Formatea un comando para salida legible."""

    return " ".join(command)


def internal_module_command(module: str, config: PipelineConfig) -> list[str]:
    """Construye el comando de un modulo Python interno."""

    command = [sys.executable, "-m", module]
    if config.overwrite:
        command.append("--overwrite")
    return command


def hive_generate_sql_command(config: PipelineConfig) -> list[str]:
    """Construye el comando para generar DDL Hive."""

    command = [
        sys.executable,
        "-m",
        "src.hive.generate_external_tables",
        "--overwrite-sql",
        "--validate-inputs",
    ]
    return command


def build_stage_steps(stage: str, config: PipelineConfig) -> list[PipelineStep]:
    """Devuelve los pasos configurados para una etapa."""

    if stage == "bronze":
        return [
            PipelineStep(stage=stage, name="build_bronze_siaf_income", kind="module", module="src.bronze.build_bronze_siaf_income"),
            PipelineStep(stage=stage, name="build_bronze_sismepre", kind="module", module="src.bronze.build_bronze_sismepre"),
            PipelineStep(stage=stage, name="build_bronze_renamu", kind="module", module="src.bronze.build_bronze_renamu"),
            PipelineStep(stage=stage, name="build_bronze_municipal_classification", kind="module", module="src.bronze.build_bronze_municipal_classification"),
        ]
    if stage == "silver":
        return [
            PipelineStep(stage=stage, name="transform_siaf_income", kind="module", module="src.silver.transform_siaf_income"),
            PipelineStep(stage=stage, name="transform_sismepre", kind="module", module="src.silver.transform_sismepre"),
            PipelineStep(stage=stage, name="transform_renamu", kind="module", module="src.silver.transform_renamu"),
            PipelineStep(stage=stage, name="transform_municipal_classification", kind="module", module="src.silver.transform_municipal_classification"),
        ]
    if stage == "integration":
        return [
            PipelineStep(stage=stage, name="integrate_municipal_sources", kind="module", module="src.silver.integrate_municipal_sources"),
        ]
    if stage == "gold":
        return [
            PipelineStep(stage=stage, name="build_municipal_dimensions", kind="module", module="src.gold.build_municipal_dimensions"),
            PipelineStep(stage=stage, name="build_revenue_predial_facts", kind="module", module="src.gold.build_revenue_predial_facts"),
            PipelineStep(stage=stage, name="build_powerbi_analytic_marts", kind="module", module="src.gold.build_powerbi_analytic_marts"),
            PipelineStep(stage=stage, name="build_audit_quality_marts", kind="module", module="src.gold.build_audit_quality_marts"),
            PipelineStep(stage=stage, name="build_dashboard_export_marts", kind="module", module="src.powerbi.build_dashboard_export_marts"),
        ]
    if stage == "hive":
        return [
            PipelineStep(stage=stage, name="generate_external_tables", kind="command", command=hive_generate_sql_command(config)),
            PipelineStep(stage=stage, name="apply_create_databases", kind="beeline_file", sql_file=SQL_DIR / "hive" / "create_databases.sql"),
            PipelineStep(stage=stage, name="apply_create_silver_external_tables", kind="beeline_file", sql_file=SQL_DIR / "hive" / "create_silver_external_tables.sql"),
            PipelineStep(stage=stage, name="apply_create_gold_external_tables", kind="beeline_file", sql_file=SQL_DIR / "hive" / "create_gold_external_tables.sql"),
        ]
    if stage == "validate":
        return [
            PipelineStep(stage=stage, name="validate_local_gold_outputs", kind="action", action=run_local_validations),
            PipelineStep(stage=stage, name="validate_hive_catalog", kind="action", action=run_hive_validations),
        ]
    raise PipelineRunnerError(f"Etapa no soportada: {stage}")


def resolve_stage_sequence(config: PipelineConfig) -> list[str]:
    """Resuelve el orden efectivo de etapas."""

    if config.from_stage:
        start_index = STAGE_SEQUENCE.index(config.from_stage)
        selected = STAGE_SEQUENCE[start_index:]
    elif config.stage == "all":
        selected = STAGE_SEQUENCE.copy()
    else:
        selected = [config.stage]

    if "bronze" in selected and not config.include_bronze and config.stage == "all" and not config.from_stage:
        selected.remove("bronze")
    if config.skip_hive and "hive" in selected:
        selected.remove("hive")
    if config.skip_validate and "validate" in selected:
        selected.remove("validate")
    return selected


def build_execution_plan(config: PipelineConfig) -> list[PipelineStep]:
    """Arma el plan de ejecucion completo."""

    steps: list[PipelineStep] = []
    for stage in resolve_stage_sequence(config):
        stage_steps = build_stage_steps(stage, config)
        if stage == "validate" and config.skip_hive:
            stage_steps = [
                step for step in stage_steps if step.name != "validate_hive_catalog"
            ]
        steps.extend(stage_steps)
    return steps


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parsea argumentos CLI del runner."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "integration", "gold", "hive", "validate", "all"],
        default="all",
        help="Etapa objetivo del pipeline.",
    )
    parser.add_argument(
        "--from-stage",
        choices=["bronze", "silver", "integration", "gold", "hive", "validate"],
        default=None,
        help="Ejecuta desde una etapa hasta el final.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Propaga --overwrite a modulos internos.")
    parser.add_argument("--dry-run", action="store_true", help="Muestra el plan sin ejecutar pasos.")
    parser.add_argument("--include-bronze", action="store_true", help="Incluye Bronze cuando --stage all.")
    parser.add_argument("--skip-hive", action="store_true", help="Omite la etapa Hive y la validacion Hive.")
    parser.add_argument("--skip-validate", action="store_true", help="Omite la etapa validate.")
    failure_group = parser.add_mutually_exclusive_group()
    failure_group.add_argument("--fail-fast", dest="fail_fast", action="store_true", help="Detiene el pipeline al primer fallo.")
    failure_group.add_argument("--continue-on-error", dest="fail_fast", action="store_false", help="Continua aunque fallen pasos, pero retorna exit code no cero si hubo fallos.")
    parser.set_defaults(fail_fast=True)
    parser.add_argument("--hive-url", default=DEFAULT_HIVE_URL, help="URL JDBC de HiveServer2.")
    parser.add_argument("--verbose", action="store_true", help="Imprime stdout/stderr de pasos exitosos.")
    return parser.parse_args(argv)


def namespace_to_config(args: argparse.Namespace) -> PipelineConfig:
    """Convierte argparse.Namespace a configuracion inmutable."""

    return PipelineConfig(
        stage=args.stage,
        from_stage=args.from_stage,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        include_bronze=args.include_bronze,
        skip_hive=args.skip_hive,
        skip_validate=args.skip_validate,
        fail_fast=args.fail_fast,
        hive_url=args.hive_url,
        verbose=args.verbose,
    )


def run_subprocess(command: list[str], *, verbose: bool) -> str:
    """Ejecuta un comando local y retorna salida consolidada."""

    completed = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    output = "\n".join(
        piece for piece in [completed.stdout.strip(), completed.stderr.strip()] if piece
    ).strip()
    if completed.returncode != 0:
        raise PipelineRunnerError(
            output or f"El comando fallo con codigo {completed.returncode}: {format_command(command)}"
        )
    if verbose and output:
        print(output)
    return output


def ensure_beeline_available() -> str:
    """Valida disponibilidad de beeline en el entorno actual."""

    beeline_path = shutil.which("beeline")
    if not beeline_path:
        raise PipelineRunnerError(
            "No se encontro `beeline` en el entorno actual. Ejecuta la etapa Hive/validate desde un entorno con beeline o usa el fallback manual con `docker compose exec hive-server ...`."
        )
    return beeline_path


def build_beeline_file_command(sql_file: Path, hive_url: str) -> list[str]:
    """Construye comando beeline para aplicar un archivo SQL."""

    beeline_path = ensure_beeline_available()
    return [beeline_path, "-u", hive_url, "-f", str(sql_file)]


def build_beeline_query_command(query: str, hive_url: str) -> list[str]:
    """Construye comando beeline para ejecutar una consulta."""

    beeline_path = ensure_beeline_available()
    return [
        beeline_path,
        "-u",
        hive_url,
        "--silent=true",
        "--showHeader=false",
        "--outputformat=tsv2",
        "-e",
        query,
    ]


def candidate_hive_urls(hive_url: str) -> list[str]:
    """Genera URLs candidatas para host local y red Docker."""

    candidates = [hive_url]
    if "localhost:10000" in hive_url:
        candidates.append(hive_url.replace("localhost:10000", "hive-server:10000"))
    if "127.0.0.1:10000" in hive_url:
        candidates.append(hive_url.replace("127.0.0.1:10000", "hive-server:10000"))

    unique_candidates: list[str] = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


def run_beeline_file(sql_file: Path, config: PipelineConfig) -> str:
    """Aplica un archivo SQL usando la primera URL Hive disponible."""

    errors: list[str] = []
    for hive_url in candidate_hive_urls(config.hive_url):
        command = build_beeline_file_command(sql_file, hive_url)
        try:
            return run_subprocess(command, verbose=config.verbose)
        except PipelineRunnerError as exc:
            errors.append(f"{hive_url}: {exc}")
    raise PipelineRunnerError(
        "No fue posible aplicar DDL Hive con ninguna URL candidata. "
        + " | ".join(errors)
    )


def run_beeline_query(query: str, config: PipelineConfig) -> str:
    """Ejecuta una consulta Hive usando la primera URL disponible."""

    errors: list[str] = []
    for hive_url in candidate_hive_urls(config.hive_url):
        command = build_beeline_query_command(query, hive_url)
        try:
            return run_subprocess(command, verbose=config.verbose)
        except PipelineRunnerError as exc:
            errors.append(f"{hive_url}: {exc}")
    raise PipelineRunnerError(
        "No fue posible ejecutar validaciones Hive con ninguna URL candidata. "
        + " | ".join(errors)
    )


def dataset_count_rows(path: Path) -> int:
    """Cuenta filas de un dataset Parquet sin materializar todo el contenido."""

    return int(ds.dataset(str(path), format="parquet").count_rows())


def summarize_match_status(path: Path) -> tuple[list[dict[str, object]], int]:
    """Resume match_status y detecta no unicidad sec_ejec -> municipality_key."""

    table = ds.dataset(str(path), format="parquet").to_table(
        columns=[
            "has_municipality_match",
            "match_status",
            "sec_ejec",
            "municipality_key",
        ]
    )
    dataframe = table.to_pandas()
    grouped = (
        dataframe.groupby(["has_municipality_match", "match_status"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    summary = grouped.to_dict(orient="records")
    sec_mapping = (
        dataframe.groupby("sec_ejec", dropna=False)["municipality_key"]
        .nunique(dropna=True)
        .reset_index(name="municipality_key_count")
    )
    duplicated_count = int((sec_mapping["municipality_key_count"] > 1).sum())
    has_bad_status = dataframe["match_status"].isin(PROBLEMATIC_MATCH_STATUSES).any()
    has_false_match = (~dataframe["has_municipality_match"].fillna(False)).any()
    has_null_muni = dataframe["municipality_key"].isna().any()
    if has_bad_status or has_false_match or has_null_muni:
        raise PipelineRunnerError(
            "fact_siaf_income contiene match_status problemáticos, has_municipality_match=false o municipality_key nulo."
        )
    return summary, duplicated_count


def run_local_validations(config: PipelineConfig) -> str:
    """Ejecuta validaciones locales sobre Parquet Gold."""

    counts: dict[str, int] = {}
    for dataset_name in PRIMARY_GOLD_DATASETS:
        dataset_path = GOLD_DIR / dataset_name
        if not dataset_path.exists():
            raise PipelineRunnerError(f"No existe dataset Gold requerido: {dataset_path}")
        counts[dataset_name] = dataset_count_rows(dataset_path)

    fact_path = GOLD_DIR / "fact_siaf_income"
    summary, duplicated_count = summarize_match_status(fact_path)
    if duplicated_count != 0:
        raise PipelineRunnerError(
            f"Se detectaron {duplicated_count} sec_ejec con mas de un municipality_key en gold.fact_siaf_income."
        )

    lines = ["Conteos Gold principales:"]
    for dataset_name, row_count in counts.items():
        lines.append(f"- {dataset_name}: {row_count}")
    lines.append("Distribucion fact_siaf_income por has_municipality_match/match_status:")
    for item in summary:
        lines.append(
            f"- has_municipality_match={item['has_municipality_match']} | match_status={item['match_status']} | count={item['count']}"
        )
    lines.append(f"- sec_ejec con mas de un municipality_key: {duplicated_count}")
    return "\n".join(lines)


def extract_last_data_line(output: str) -> str:
    """Extrae la ultima linea util de una salida beeline."""

    lines = [line.strip() for line in output.splitlines() if line.strip()]
    filtered = [
        line
        for line in lines
        if not line.startswith("Connecting to:")
        and not line.startswith("Connected to:")
        and not line.startswith("Beeline version")
        and not line.startswith("Transaction isolation")
        and not line.startswith("No current connection")
        and line not in {"OK", "No rows affected (0.0 seconds)"}
    ]
    if not filtered:
        return ""
    return filtered[-1]


def run_hive_validations(config: PipelineConfig) -> str:
    """Ejecuta validaciones Hive via beeline."""

    queries = [
        ("show_tables_gold", "SHOW TABLES IN gold;"),
        ("count_dim_municipality", "SELECT COUNT(*) FROM gold.dim_municipality;"),
        ("count_fact_siaf_income", "SELECT COUNT(*) FROM gold.fact_siaf_income;"),
        ("count_mart_municipal_revenue_overview", "SELECT COUNT(*) FROM gold.mart_municipal_revenue_overview;"),
        (
            "match_status_summary",
            "SELECT has_municipality_match, match_status, COUNT(*) FROM gold.fact_siaf_income GROUP BY has_municipality_match, match_status;",
        ),
        (
            "sec_ejec_uniqueness",
            "SELECT COUNT(*) FROM ( SELECT sec_ejec FROM gold.fact_siaf_income GROUP BY sec_ejec HAVING COUNT(DISTINCT municipality_key) > 1 ) t;",
        ),
    ]

    outputs: list[str] = []
    for label, query in queries:
        output = run_beeline_query(query, config)
        outputs.append(f"[{label}]\n{output}".strip())
        if label == "sec_ejec_uniqueness":
            last_line = extract_last_data_line(output)
            normalized = last_line.replace("|", "\t").strip()
            tokens = [token.strip() for token in normalized.split("\t") if token.strip()]
            if not tokens or tokens[-1] != "0":
                raise PipelineRunnerError(
                    "La validacion Hive detecto sec_ejec con mas de un municipality_key."
                )
    return "\n\n".join(outputs)


def execute_step(step: PipelineStep, config: PipelineConfig) -> StepResult:
    """Ejecuta un paso o lo reporta en dry-run."""

    if step.kind == "module":
        assert step.module is not None
        command = internal_module_command(step.module, config)
        command_display = format_command(command)
    elif step.kind == "command":
        assert step.command is not None
        command = step.command
        command_display = format_command(command)
    elif step.kind == "beeline_file":
        assert step.sql_file is not None
        command = build_beeline_file_command(
            step.sql_file,
            candidate_hive_urls(config.hive_url)[0],
        )
        command_display = format_command(command)
    else:
        command = None
        command_display = step.action.__name__ if step.action else step.name

    if config.dry_run:
        print(f"[DRY-RUN] {step.name}: {command_display}")
        return StepResult(
            stage=step.stage,
            name=step.name,
            status="SKIPPED",
            duration_seconds=0.0,
            command_display=command_display,
            details="dry-run",
        )

    started = time.perf_counter()
    try:
        if step.kind in {"module", "command", "beeline_file"}:
            if step.kind == "beeline_file":
                assert step.sql_file is not None
                details = run_beeline_file(step.sql_file, config)
            else:
                assert command is not None
                details = run_subprocess(command, verbose=config.verbose)
        else:
            assert step.action is not None
            details = step.action(config)
            if config.verbose and details:
                print(details)
        duration = time.perf_counter() - started
        return StepResult(
            stage=step.stage,
            name=step.name,
            status="SUCCESS",
            duration_seconds=duration,
            command_display=command_display,
            details=details,
        )
    except Exception as exc:
        duration = time.perf_counter() - started
        return StepResult(
            stage=step.stage,
            name=step.name,
            status="FAILED",
            duration_seconds=duration,
            command_display=command_display,
            error_summary=str(exc).splitlines()[0],
        )


def print_step_result(result: StepResult) -> None:
    """Imprime resultado compacto por paso."""

    line = (
        f"[{result.status}] stage={result.stage} step={result.name} "
        f"duration={result.duration_seconds:.2f}s"
    )
    print(line)
    print(f"  command={result.command_display}")
    if result.error_summary:
        print(f"  error={result.error_summary}")


def print_summary(summary: PipelineRunSummary) -> None:
    """Imprime resumen final del pipeline."""

    total = len(summary.results)
    success = sum(result.status == "SUCCESS" for result in summary.results)
    failed = sum(result.status == "FAILED" for result in summary.results)
    skipped = sum(result.status == "SKIPPED" for result in summary.results)
    print("=" * 80)
    print("Resumen pipeline local")
    print(f"total_steps={total}")
    print(f"success={success}")
    print(f"failed={failed}")
    print(f"skipped={skipped}")
    print(f"total_duration_seconds={summary.total_duration_seconds:.2f}")


def run_pipeline(config: PipelineConfig) -> PipelineRunSummary:
    """Ejecuta el pipeline completo segun configuracion."""

    plan = build_execution_plan(config)
    summary = PipelineRunSummary()
    started = time.perf_counter()

    for step in plan:
        result = execute_step(step, config)
        summary.results.append(result)
        print_step_result(result)
        if result.status == "FAILED" and config.fail_fast:
            break

    summary.total_duration_seconds = time.perf_counter() - started
    print_summary(summary)

    has_failures = any(result.status == "FAILED" for result in summary.results)
    if has_failures:
        raise SystemExit(1)
    return summary


def main(argv: list[str] | None = None) -> None:
    """Punto de entrada CLI."""

    args = parse_args(argv)
    config = namespace_to_config(args)
    run_pipeline(config)


if __name__ == "__main__":
    main()
