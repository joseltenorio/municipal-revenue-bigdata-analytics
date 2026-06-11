"""Configuración común de logging para scripts del proyecto."""

from __future__ import annotations

import logging
import os
from logging import Logger


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def get_log_level(default: str = "INFO") -> int:
    """Obtiene el nivel de logging desde LOG_LEVEL o usa un valor por defecto."""
    
    level_name = os.getenv("LOG_LEVEL", default).upper()
    return getattr(logging, level_name, logging.INFO)


def configure_logging(level: int | None = None) -> None:
    """Configura logging básico para ejecución local."""

    logging.basicConfig(
        level=level if level is not None else get_log_level(),
        format=DEFAULT_LOG_FORMAT,
    )


def get_logger(name: str) -> Logger:
    """Devuelve un logger configurado para un módulo específico."""

    configure_logging()
    return logging.getLogger(name)