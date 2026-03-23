from sqlalchemy import Column, Index, String, JSON, TIMESTAMP, func

from app.infrastructure.database.base import Base


class EventLogModel(Base):
    __tablename__ = "event_logs"

    id = Column(String(36), primary_key=True)
    anonymous_id = Column(String(36), nullable=False)
    session_id = Column(String(36), nullable=False)
    event_type = Column(String(50), nullable=False)
    event_data = Column(JSON, nullable=True)
    page_url = Column(String(500), nullable=True)
    device_type = Column(String(10), nullable=False)
    timestamp = Column(TIMESTAMP, nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())

    __table_args__ = (
        Index("idx_event_type", "event_type"),
        Index("idx_session_id", "session_id"),
        Index("idx_anonymous_id", "anonymous_id"),
        Index("idx_created_at", "created_at"),
        Index("idx_event_session", "event_type", "session_id"),
    )
