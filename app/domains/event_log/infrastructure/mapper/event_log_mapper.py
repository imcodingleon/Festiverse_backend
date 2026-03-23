from app.domains.event_log.domain.entity.event_log import EventLog
from app.domains.event_log.infrastructure.orm.event_log_model import EventLogModel


class EventLogMapper:

    @staticmethod
    def to_entity(model: EventLogModel) -> EventLog:
        return EventLog(
            id=model.id,
            anonymous_id=model.anonymous_id,
            session_id=model.session_id,
            event_type=model.event_type,
            event_data=model.event_data,
            page_url=model.page_url,
            device_type=model.device_type,
            timestamp=model.timestamp,
            created_at=model.created_at,
        )

    @staticmethod
    def to_model(entity: EventLog) -> EventLogModel:
        return EventLogModel(
            id=entity.id,
            anonymous_id=entity.anonymous_id,
            session_id=entity.session_id,
            event_type=entity.event_type,
            event_data=entity.event_data,
            page_url=entity.page_url,
            device_type=entity.device_type,
            timestamp=entity.timestamp,
        )
