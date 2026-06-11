"""Pruebas de política de reintentos y auditoría de ingesta."""

from pathlib import Path

from src.common.audit import (
    IngestionAuditRecord,
    append_audit_record,
    read_audit_records,
)
from src.common.retry import (
    RetryPolicy,
    calculate_wait_seconds,
    is_retryable_http_status,
    load_retry_policy,
)


def test_load_retry_policy_for_mef_income() -> None:
    """Valida que la política MEF pueda cargarse desde YAML."""

    policy = load_retry_policy("mef_income")

    assert policy.max_attempts >= 1
    assert policy.timeout_seconds > 0
    assert 500 in policy.retryable_http_status_codes


def test_retryable_http_status() -> None:
    """Valida clasificación de códigos HTTP reintentables."""

    policy = RetryPolicy(
        max_attempts=3,
        timeout_seconds=60,
        backoff_seconds=5,
        backoff_multiplier=2,
        retryable_http_status_codes={429, 500, 502, 503, 504},
        retryable_errors={"TimeoutError", "ConnectionError", "HTTPError"},
    )

    assert is_retryable_http_status(429, policy)
    assert is_retryable_http_status(503, policy)
    assert not is_retryable_http_status(404, policy)
    assert not is_retryable_http_status(200, policy)


def test_calculate_wait_seconds() -> None:
    """Valida cálculo de backoff exponencial simple."""

    policy = RetryPolicy(
        max_attempts=3,
        timeout_seconds=60,
        backoff_seconds=5,
        backoff_multiplier=2,
        retryable_http_status_codes={500},
        retryable_errors=set(),
    )

    assert calculate_wait_seconds(policy, 1) == 5
    assert calculate_wait_seconds(policy, 2) == 10
    assert calculate_wait_seconds(policy, 3) == 20


def test_append_and_read_audit_record(tmp_path: Path, monkeypatch) -> None:
    """Valida escritura y lectura local de auditoría JSONL."""

    audit_path = tmp_path / "ingestion_audit.jsonl"

    monkeypatch.setattr(
        "src.common.audit.resolve_audit_path",
        lambda: audit_path,
    )

    record = IngestionAuditRecord(
        run_id="test-run",
        source_name="mef_income",
        source_url="https://example.com/file.csv",
        access_method="csv",
        started_at="2026-01-01T00:00:00+00:00",
        finished_at="2026-01-01T00:00:01+00:00",
        duration_seconds=1.0,
        attempt_number=1,
        max_attempts=3,
        retry_count=0,
        http_status_code=200,
        error_type=None,
        error_message=None,
        downloaded_file_name="file.csv",
        downloaded_file_size_bytes=100,
        checksum_sha256="abc",
        records_detected=None,
        final_status="SUCCESS",
    )

    output_path = append_audit_record(record)
    records = read_audit_records(output_path)

    assert output_path == audit_path
    assert len(records) == 1
    assert records[0]["run_id"] == "test-run"
    assert records[0]["final_status"] == "SUCCESS"