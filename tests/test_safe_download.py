"""Pruebas para descarga segura con archivo .part."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.common.download import safe_download_file
from src.common.retry import RetryPolicy


class FakeResponse:
    """Respuesta HTTP falsa para probar iter_content."""

    def __init__(
        self,
        *,
        status_code: int,
        chunks: list[bytes],
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._chunks = chunks
        self.headers = headers or {}

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:  # noqa: ANN001
        self.close()

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int):  # noqa: ANN001
        for chunk in self._chunks:
            yield chunk

    def close(self) -> None:
        return None


def build_policy() -> RetryPolicy:
    """Construye política de reintentos mínima para pruebas."""

    return RetryPolicy(
        max_attempts=1,
        timeout_seconds=10,
        backoff_seconds=0,
        backoff_multiplier=1,
        retryable_http_status_codes={408, 429, 500, 502, 503, 504},
        retryable_errors=set(),
    )


def test_safe_download_file_writes_part_then_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Debe escribir .part y luego renombrar al archivo final."""

    def fake_request_with_retries(**kwargs):  # noqa: ANN003
        return FakeResponse(
            status_code=200,
            chunks=[b"abc", b"def"],
            headers={"content-length": "6"},
        )

    monkeypatch.setattr(
        "src.common.download.request_with_retries",
        fake_request_with_retries,
    )

    destination = tmp_path / "file.csv"

    result = safe_download_file(
        url="https://example.test/file.csv",
        destination_path=destination,
        retry_config=build_policy(),
        chunk_size=3,
        overwrite=False,
        show_progress=False,
    )

    assert destination.exists()
    assert destination.read_bytes() == b"abcdef"
    assert not (tmp_path / "file.csv.part").exists()
    assert result.final_file_size_bytes == 6
    assert result.resumed_from_bytes == 0
    assert result.range_request_used is False


def test_safe_download_file_resumes_from_part_when_server_returns_206(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Debe reanudar desde .part cuando el servidor responde 206."""

    partial = tmp_path / "file.csv.part"
    partial.write_bytes(b"abc")

    captured_headers: list[dict[str, str] | None] = []

    def fake_request_with_retries(**kwargs):  # noqa: ANN003
        captured_headers.append(kwargs.get("headers"))
        return FakeResponse(
            status_code=206,
            chunks=[b"def"],
            headers={"content-length": "3"},
        )

    monkeypatch.setattr(
        "src.common.download.request_with_retries",
        fake_request_with_retries,
    )

    destination = tmp_path / "file.csv"

    result = safe_download_file(
        url="https://example.test/file.csv",
        destination_path=destination,
        retry_config=build_policy(),
        chunk_size=3,
        overwrite=False,
        show_progress=False,
    )

    assert destination.exists()
    assert destination.read_bytes() == b"abcdef"
    assert result.resumed_from_bytes == 3
    assert result.range_request_used is True
    assert result.server_supports_resume is True
    assert captured_headers[0] == {"Range": "bytes=3-"}


def test_safe_download_file_restarts_when_server_ignores_range(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Debe reiniciar si existe .part pero el servidor responde 200."""

    partial = tmp_path / "file.csv.part"
    partial.write_bytes(b"old")

    def fake_request_with_retries(**kwargs):  # noqa: ANN003
        return FakeResponse(
            status_code=200,
            chunks=[b"new"],
            headers={"content-length": "3"},
        )

    monkeypatch.setattr(
        "src.common.download.request_with_retries",
        fake_request_with_retries,
    )

    destination = tmp_path / "file.csv"

    result = safe_download_file(
        url="https://example.test/file.csv",
        destination_path=destination,
        retry_config=build_policy(),
        chunk_size=3,
        overwrite=False,
        show_progress=False,
    )

    assert destination.exists()
    assert destination.read_bytes() == b"new"
    assert result.resumed_from_bytes == 0
    assert result.range_request_used is False
    assert result.server_supports_resume is False
