from dataclasses import dataclass
from datetime import datetime


@dataclass
class EventLog:
    id: str
    anonymous_id: str
    session_id: str
    event_type: str
    event_data: dict | None
    page_url: str | None
    device_type: str
    timestamp: datetime | None = None
    created_at: datetime | None = None
