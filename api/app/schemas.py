from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

Status = Literal["queued", "running", "retrying", "succeeded", "dead", "cancelled"]


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    task_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=0, le=9, description="0 (lowest) .. 9 (most urgent)")
    run_at: AwareDatetime | None = Field(default=None, description="earliest start time (timezone required)")
    delay_seconds: int | None = Field(default=None, ge=0, description="alternative to run_at")
    max_retries: int = Field(default=3, ge=0)
    webhook_url: str | None = None
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200, pattern=r"^[\x21-\x7e]+$")


class Attempt(BaseModel):
    attempt: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str
    error: str | None
    duration_ms: int | None


class Job(BaseModel):
    id: uuid.UUID
    task_type: str
    payload: dict[str, Any]
    priority: int
    status: Status
    run_at: datetime
    attempts: int
    max_retries: int
    worker_id: str | None
    lease_expires_at: datetime | None
    idempotency_key: str | None
    result: Any | None
    last_error: str | None
    webhook_url: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobDetail(Job):
    history: list[Attempt]


class JobList(BaseModel):
    items: list[Job]
    limit: int
    offset: int
    next_offset: int | None


class LogLine(BaseModel):
    id: int
    ts: datetime
    attempt: int | None
    level: str
    message: str


class DeadLetter(BaseModel):
    job_id: uuid.UUID
    task_type: str
    moved_at: datetime
    reason: str
    attempts: int
    last_error: str | None
    payload: dict[str, Any]
