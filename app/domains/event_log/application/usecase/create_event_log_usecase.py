from app.domains.event_log.application.port.event_log_repository_port import EventLogRepositoryPort
from app.domains.event_log.application.request.create_event_log_request import CreateEventLogRequest
from app.domains.event_log.domain.entity.event_log import EventLog


class CreateEventLogUseCase:

    def __init__(self, repository: EventLogRepositoryPort) -> None:
        self._repository = repository

    async def execute(self, request: CreateEventLogRequest) -> str:
        event_log = EventLog(
            id=request.id,
            anonymous_id=request.anonymous_id,
            session_id=request.session_id,
            event_type=request.event_type,
            event_data=request.event_data,
            page_url=request.page_url,
            device_type=request.device_type,
            timestamp=request.timestamp,
        )
        await self._repository.save(event_log)
        return event_log.id
