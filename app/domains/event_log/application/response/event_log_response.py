from pydantic import BaseModel


class EventLogResponse(BaseModel):
    id: str
    status: str = "ok"
