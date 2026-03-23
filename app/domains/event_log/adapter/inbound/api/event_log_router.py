import logging

from fastapi import APIRouter, Depends

from app.domains.event_log.adapter.outbound.persistence.event_log_repository import EventLogRepository
from app.domains.event_log.application.request.create_event_log_request import CreateEventLogRequest
from app.domains.event_log.application.response.event_log_response import EventLogResponse
from app.domains.event_log.application.usecase.create_event_log_usecase import CreateEventLogUseCase
from app.infrastructure.database.session import async_session_factory

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["events"])


async def _get_create_event_log_usecase():
    async with async_session_factory() as session:
        yield CreateEventLogUseCase(EventLogRepository(session))


@router.post("/events", response_model=EventLogResponse, status_code=201)
async def create_event(
    request: CreateEventLogRequest,
    usecase: CreateEventLogUseCase = Depends(_get_create_event_log_usecase),
) -> EventLogResponse:
    event_id = await usecase.execute(request)
    return EventLogResponse(id=event_id)
