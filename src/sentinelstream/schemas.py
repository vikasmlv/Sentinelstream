from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class Event(BaseModel):
    """Transaction event on the `transactions` topic. Invalid payload => poison => DLQ."""

    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)

    event_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    ts: float = Field(ge=0)  # event time, seconds
    amount: float = Field(ge=0)
    product_cd: str = ""
    device_id: str | None = None
    geo_id: str | None = None  # IEEE-CIS has no IP; addr1 is used as a coarse location proxy
    label: int | None = None  # simulator-only ground truth, NEVER used as a feature
