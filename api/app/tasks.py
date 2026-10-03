"""Registry of accepted task types and their payload schemas.

Clients can only submit types listed here; there is no way to pass a command line.
The C++ handlers (cpp/src/handlers.cpp) re-validate the same limits (defence in depth).
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


SAFE_NAME = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"  # a bare file name: no slashes, no '..'


class ImageResizePayload(_Strict):
    source: str = Field(pattern=SAFE_NAME, description="file name inside the worker's image directory")
    width: int = Field(ge=1, le=4096)
    height: int = Field(ge=1, le=4096)
    keep_aspect: bool = True
    format: Literal["png", "jpg"] = "png"

    def model_post_init(self, _ctx) -> None:
        if ".." in self.source:
            raise ValueError("source must not contain '..'")


class GenerateReportPayload(_Strict):
    report_type: Literal["job_summary"] = "job_summary"
    days: int = Field(default=7, ge=1, le=365)
    format: Literal["csv", "json"] = "csv"


class FlakyApiPayload(_Strict):
    """Simulated third-party API call that fails some of the time."""
    endpoint: str = Field(pattern=r"^[A-Za-z0-9/_.-]{1,100}$")
    failure_rate: float = Field(default=0.5, ge=0.0, le=1.0)
    latency_ms: int = Field(default=100, ge=0, le=5000)
    fail_first_n_attempts: int = Field(default=0, ge=0, le=10)  # deterministic failures for demos/tests
    permanent: bool = False  # failures are "HTTP 400" (never retried) instead of "HTTP 503"


class ClassifyDocumentPayload(_Strict):
    """Handled by the optional PyTorch worker (ai_worker/), not the C++ scheduler."""
    text: str = Field(min_length=1, max_length=20_000)


TASK_TYPES: dict[str, type[BaseModel]] = {
    "image_resize": ImageResizePayload,
    "generate_report": GenerateReportPayload,
    "flaky_api": FlakyApiPayload,
    "classify_document": ClassifyDocumentPayload,
}
