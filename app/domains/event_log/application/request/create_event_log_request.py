from datetime import datetime
from typing import Any

from pydantic import BaseModel


class CreateEventLogRequest(BaseModel):
    id: str
    anonymous_id: str
    session_id: str
    event_type: str
    event_data: dict[str, Any] | None = None
    page_url: str | None = None
    device_type: str
    timestamp: datetime | None = None
