"""Pruebas unitarias para el runner local del pipeline."""

from __future__ import annotations

import pytest

from src.pipeline.run_local_pipeline import (
    PipelineConfig,
    StepResult,
    build_execution_plan,
    internal_module_command,
    run_pipeline,
)


def make_config(**overrides) -> PipelineConfig:
    """Construye configuracion base para pruebas."""

    base = dict(
        stage="all",
        from_stage=None,
        overwrite=False,
        dry_run=False,
        include_bronze=False,
        skip_hive=False,
        skip_validate=False,
        fail_fast=True,
        hive_url="jdbc:hive2://localhost:10000",
        verbose=False,
    )
    base.update(overrides)
    return PipelineConfig(**base)


def step_names(config: PipelineConfig) -> list[str]:
    """Lista nombres de pasos del plan."""

    return [step.name for step in build_execution_plan(config)]


def test_stage_all_sin_include_bronze_arranca_desde_silver() -> None:
    names = step_names(make_config(stage="all"))
    assert "build_bronze_siaf_income" not in names
    assert names[0] == "transform_siaf_income"


def test_stage_all_con_include_bronze_incluye_bronze() -> None:
    names = step_names(make_config(stage="all", include_bronze=True))
    assert names[:4] == [
        "build_bronze_siaf_income",
        "build_bronze_sismepre",
        "build_bronze_renamu",
        "build_bronze_municipal_classification",
    ]


def test_stage_gold_ejecuta_solo_gold() -> None:
    names = step_names(make_config(stage="gold"))
    assert names == [
        "build_municipal_dimensions",
        "build_revenue_predial_facts",
        "build_powerbi_analytic_marts",
        "build_audit_quality_marts",
        "build_dashboard_export_marts",
    ]


def test_from_stage_integration_arma_restante_pipeline() -> None:
    names = step_names(make_config(from_stage="integration"))
    assert names[0] == "integrate_municipal_sources"
    assert "build_municipal_dimensions" in names
    assert "generate_external_tables" in names
    assert "validate_local_gold_outputs" in names


def test_skip_hive_excluye_hive_y_validacion_hive() -> None:
    names = step_names(make_config(stage="all", skip_hive=True))
    assert "generate_external_tables" not in names
    assert "apply_create_databases" not in names
    assert "validate_hive_catalog" not in names


def test_skip_validate_excluye_validate() -> None:
    names = step_names(make_config(stage="all", skip_validate=True))
    assert "validate_local_gold_outputs" not in names
    assert "validate_hive_catalog" not in names


def test_internal_module_command_agrega_overwrite() -> None:
    command = internal_module_command(
        "src.gold.build_municipal_dimensions",
        make_config(overwrite=True),
    )
    assert command[-1] == "--overwrite"


def test_dry_run_no_ejecuta_pasos(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[str] = []

    def fake_execute_step(step, config):
        executed.append(step.name)
        return StepResult(
            stage=step.stage,
            name=step.name,
            status="SKIPPED",
            duration_seconds=0.0,
            command_display=step.name,
            details="dry-run",
        )

    monkeypatch.setattr("src.pipeline.run_local_pipeline.execute_step", fake_execute_step)
    summary = run_pipeline(make_config(stage="gold", dry_run=True))
    assert executed == [
        "build_municipal_dimensions",
        "build_revenue_predial_facts",
        "build_powerbi_analytic_marts",
        "build_audit_quality_marts",
        "build_dashboard_export_marts",
    ]
    assert all(result.status == "SKIPPED" for result in summary.results)


def test_fail_fast_detiene_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[str] = []

    def fake_execute_step(step, config):
        executed.append(step.name)
        status = "FAILED" if step.name == "transform_sismepre" else "SUCCESS"
        return StepResult(
            stage=step.stage,
            name=step.name,
            status=status,
            duration_seconds=0.0,
            command_display=step.name,
            error_summary="boom" if status == "FAILED" else None,
        )

    monkeypatch.setattr("src.pipeline.run_local_pipeline.execute_step", fake_execute_step)
    with pytest.raises(SystemExit):
        run_pipeline(make_config(stage="silver", fail_fast=True))
    assert executed == ["transform_siaf_income", "transform_sismepre"]


def test_continue_on_error_continua_y_reporta(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[str] = []

    def fake_execute_step(step, config):
        executed.append(step.name)
        status = "FAILED" if step.name == "transform_sismepre" else "SUCCESS"
        return StepResult(
            stage=step.stage,
            name=step.name,
            status=status,
            duration_seconds=0.0,
            command_display=step.name,
            error_summary="boom" if status == "FAILED" else None,
        )

    monkeypatch.setattr("src.pipeline.run_local_pipeline.execute_step", fake_execute_step)
    with pytest.raises(SystemExit):
        run_pipeline(make_config(stage="silver", fail_fast=False))
    assert executed == [
        "transform_siaf_income",
        "transform_sismepre",
        "transform_renamu",
        "transform_municipal_classification",
    ]


def test_no_ejecuta_bronze_por_defecto_en_all() -> None:
    names = step_names(make_config(stage="all", include_bronze=False))
    assert all(not name.startswith("build_bronze_") for name in names)


def test_validate_hive_catalog_se_omite_con_skip_hive_desde_validate() -> None:
    names = step_names(make_config(stage="validate", skip_hive=True))
    assert names == ["validate_local_gold_outputs"]
