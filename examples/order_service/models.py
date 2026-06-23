"""Event schemas. Validation failures raise pydantic.ValidationError, which is a
subclass of ValueError → classified PERMANENT by rabbitkit → straight to DLQ."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class OrderCreated(BaseModel):
    order_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    amount_cents: int = Field(ge=0)
    currency: str = Field(pattern="^[A-Z]{3}$")
    created_at: datetime
    event_version: int = 1
