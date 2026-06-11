"""Cliente HTTP con reintentos y fallback para procesos de ingesta.

Este módulo centraliza la lógica de solicitudes HTTP resilientes para las
descargas hacia Landing.

Objetivos:
- Evitar duplicar lógica de requests en cada script de ingesta.
- Aplicar reintentos sobre errores temporales.
- Aplicar fallback de HEAD a GET cuando el servidor no permite HEAD.
- Mantener una interfaz simple para los scripts de descarga.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from src.common.config import load_retry_policy_config


@dataclass(frozen=True)
class RetryPolicy:
    """Política de reintentos HTTP."""

    max_attempts: int
    timeout_seconds: int
    backoff_seconds: int
    backoff_multiplier: int
    retryable_http_status_codes: set[int]
    retryable_errors: set[str]


@dataclass(frozen=True)
class RetryAttempt:
    """Detalle técnico de un intento HTTP."""

    attempt_number: int
    started_at: str
    finished_at: str
    duration_seconds: float
    http_status_code: int | None
    error_type: str | None
    error_message: str | None


class RetryError(Exception):
    """Error emitido cuando una solicitud falla después de los reintentos."""

    def __init__(self, message: str, attempts: list[RetryAttempt]) -> None:
        super().__init__(message)
        self.attempts = attempts


def utc_now_iso() -> str:
    """Devuelve fecha y hora actual en UTC."""

    return datetime.now(timezone.utc).isoformat()


def load_retry_policy_config_safe() -> dict[str, Any]:
    """Carga config/retry_policy.yaml con fallback seguro."""

    try:
        return load_retry_policy_config()
    except FileNotFoundError:
        return {}


def load_retry_policy(source_name: str = "default") -> RetryPolicy:
    """Carga la política de reintentos para una fuente.

    Si no existe configuración específica para la fuente, usa la configuración
    default. Si tampoco existe YAML, usa valores seguros por defecto.
    """

    config = load_retry_policy_config_safe()

    retry_config = config.get("retry_policy", {})
    default_config = retry_config.get("default", {})
    source_config = retry_config.get("sources", {}).get(source_name, {})

    merged_config = {
        **default_config,
        **source_config,
    }

    retryable_status_codes = merged_config.get(
        "retryable_http_status_codes",
        [408, 429, 500, 502, 503, 504],
    )

    retryable_errors = merged_config.get(
        "retryable_errors",
        [
            "Timeout",
            "ConnectTimeout",
            "ReadTimeout",
            "ConnectionError",
            "HTTPError",
        ],
    )

    return RetryPolicy(
        max_attempts=int(merged_config.get("max_attempts", 3)),
        timeout_seconds=int(merged_config.get("timeout_seconds", 60)),
        backoff_seconds=int(merged_config.get("backoff_seconds", 5)),
        backoff_multiplier=int(merged_config.get("backoff_multiplier", 2)),
        retryable_http_status_codes={
            int(status) for status in retryable_status_codes
        },
        retryable_errors={str(error_name) for error_name in retryable_errors},
    )


def build_retry_config(
    *,
    source_name: str = "default",
    timeout_seconds: int | None = None,
) -> RetryPolicy:
    """Construye configuración de reintentos para scripts de ingesta.

    Esta es la función que usan los scripts:

    retry_config = build_retry_config(timeout_seconds=timeout_seconds)
    """

    policy = load_retry_policy(source_name=source_name)

    if timeout_seconds is None:
        return policy

    return RetryPolicy(
        max_attempts=policy.max_attempts,
        timeout_seconds=timeout_seconds,
        backoff_seconds=policy.backoff_seconds,
        backoff_multiplier=policy.backoff_multiplier,
        retryable_http_status_codes=policy.retryable_http_status_codes,
        retryable_errors=policy.retryable_errors,
    )


def is_retryable_http_status(
    status_code: int | None,
    policy: RetryPolicy,
) -> bool:
    """Indica si un código HTTP debe reintentarse."""

    if status_code is None:
        return False

    return status_code in policy.retryable_http_status_codes


def is_retryable_exception(
    error: BaseException,
    policy: RetryPolicy,
) -> bool:
    """Indica si una excepción de requests debe reintentarse."""

    error_type = type(error).__name__

    if error_type in policy.retryable_errors:
        return True

    return isinstance(
        error,
        (
            requests.Timeout,
            requests.ConnectTimeout,
            requests.ReadTimeout,
            requests.ConnectionError,
        ),
    )


def calculate_wait_seconds(
    policy: RetryPolicy,
    attempt_number: int,
) -> int:
    """Calcula espera entre intentos usando backoff exponencial simple."""

    exponent = max(0, attempt_number - 1)
    return int(policy.backoff_seconds * (policy.backoff_multiplier**exponent))


def request_with_retries(
    *,
    method: str,
    url: str,
    retry_config: RetryPolicy,
    stream: bool = False,
    **kwargs: Any,
) -> requests.Response:
    """Ejecuta una solicitud HTTP con reintentos.

    Retorna un objeto requests.Response para poder usarlo así:

    with request_with_retries(...) as response:
        ...
    """

    attempts: list[RetryAttempt] = []

    for attempt_number in range(1, retry_config.max_attempts + 1):
        started_at = utc_now_iso()
        started_dt = datetime.now(timezone.utc)

        try:
            response = requests.request(
                method=method,
                url=url,
                timeout=retry_config.timeout_seconds,
                allow_redirects=True,
                stream=stream,
                **kwargs,
            )

            finished_at = utc_now_iso()
            finished_dt = datetime.now(timezone.utc)
            duration_seconds = round((finished_dt - started_dt).total_seconds(), 3)

            attempts.append(
                RetryAttempt(
                    attempt_number=attempt_number,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration_seconds,
                    http_status_code=response.status_code,
                    error_type=None,
                    error_message=None,
                )
            )

            if is_retryable_http_status(
                status_code=response.status_code,
                policy=retry_config,
            ):
                if attempt_number < retry_config.max_attempts:
                    response.close()
                    time.sleep(
                        calculate_wait_seconds(
                            policy=retry_config,
                            attempt_number=attempt_number,
                        )
                    )
                    continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            finished_at = utc_now_iso()
            finished_dt = datetime.now(timezone.utc)
            duration_seconds = round((finished_dt - started_dt).total_seconds(), 3)

            status_code = exc.response.status_code if exc.response is not None else None

            attempts.append(
                RetryAttempt(
                    attempt_number=attempt_number,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration_seconds,
                    http_status_code=status_code,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            )

            should_retry = (
                is_retryable_http_status(
                    status_code=status_code,
                    policy=retry_config,
                )
                or is_retryable_exception(
                    error=exc,
                    policy=retry_config,
                )
            )

            if attempt_number < retry_config.max_attempts and should_retry:
                time.sleep(
                    calculate_wait_seconds(
                        policy=retry_config,
                        attempt_number=attempt_number,
                    )
                )
                continue

            raise RetryError(
                f"La solicitud falló después de {attempt_number} intento(s): {url}",
                attempts=attempts,
            ) from exc

    raise RetryError(
        f"La solicitud falló sin respuesta exitosa: {url}",
        attempts=attempts,
    )


def probe_with_fallback(
    url: str,
    retry_config: RetryPolicy,
) -> requests.Response:
    """Valida disponibilidad usando HEAD y fallback a GET.

    Primero intenta HEAD. Si el servidor no permite HEAD o responde 403/405,
    usa GET en modo stream como alternativa.
    """

    response = request_with_retries(
        method="HEAD",
        url=url,
        retry_config=retry_config,
        stream=True,
    )

    if response.status_code in {403, 405}:
        response.close()
        response = request_with_retries(
            method="GET",
            url=url,
            retry_config=retry_config,
            stream=True,
        )

    return response