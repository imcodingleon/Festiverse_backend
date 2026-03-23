from abc import ABC, abstractmethod

from app.domains.event_log.domain.entity.event_log import EventLog


class EventLogRepositoryPort(ABC):

    @abstractmethod
    async def save(self, event_log: EventLog) -> None:
        ...
