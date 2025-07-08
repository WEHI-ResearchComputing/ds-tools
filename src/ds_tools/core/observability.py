"""Observability setup for ds-tools."""

import logging
import sys
from typing import Any

import structlog
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from .config import settings


def setup_tracing() -> None:
    """Set up OpenTelemetry tracing."""
    if not settings.otel_enabled:
        return

    resource = Resource.create({"service.name": settings.otel_service_name})
    provider = TracerProvider(resource=resource)

    # For now, just export to console - can be configured for OTLP later
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)


def setup_logging() -> None:
    """Set up structured logging with structlog."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.log_level.upper()),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    """Get a logger instance."""
    return structlog.get_logger(name)


# Initialize on import
setup_logging()
setup_tracing()
