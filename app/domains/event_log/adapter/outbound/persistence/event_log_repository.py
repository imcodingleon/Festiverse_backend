from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.event_log.application.port.event_log_repository_port import EventLogRepositoryPort
from app.domains.event_log.domain.entity.event_log import EventLog
from app.domains.event_log.infrastructure.mapper.event_log_mapper import EventLogMapper


class EventLogRepository(EventLogRepositoryPort):

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, event_log: EventLog) -> None:
        model = EventLogMapper.to_model(event_log)
        self._session.add(model)
        await self._session.commit()
