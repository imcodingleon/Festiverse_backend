from typing import Any

from pydantic import BaseModel


class DashboardResponse(BaseModel):
    view_name: str
    rows: list[dict[str, Any]]
    total: int
