import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from app.domains.event_log.adapter.outbound.persistence.dashboard_query_adapter import DashboardQueryAdapter
from app.domains.event_log.application.response.dashboard_responses import DashboardResponse
from app.domains.event_log.application.usecase.get_dashboard_usecase import (
    GetDashboardUseCase,
    P4_QUERY_NAMES,
    VALID_VIEW_NAMES,
)
from app.infrastructure.database.session import async_session_factory

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

ALL_VIEW_NAMES = sorted(VALID_VIEW_NAMES | P4_QUERY_NAMES)


async def _get_dashboard_usecase():
    async with async_session_factory() as session:
        yield GetDashboardUseCase(DashboardQueryAdapter(session))


@router.get("/views", response_model=list[str])
async def list_views() -> list[str]:
    return ALL_VIEW_NAMES


@router.get("/{view_name}", response_model=DashboardResponse)
async def get_dashboard(
    view_name: str,
    date_from: date | None = Query(None, description="조회 시작일 (YYYY-MM-DD)"),
    date_to: date | None = Query(None, description="조회 종료일 (YYYY-MM-DD)"),
    usecase: GetDashboardUseCase = Depends(_get_dashboard_usecase),
) -> DashboardResponse:
    if view_name not in VALID_VIEW_NAMES and view_name not in P4_QUERY_NAMES:
        raise HTTPException(status_code=404, detail=f"Unknown view: {view_name}")

    try:
        rows = await usecase.execute(view_name, date_from, date_to)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return DashboardResponse(view_name=view_name, rows=rows, total=len(rows))
