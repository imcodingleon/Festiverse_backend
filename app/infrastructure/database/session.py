from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.config.settings import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False, pool_recycle=3600)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session


async def init_db() -> None:
    from app.infrastructure.database.base import Base
    import app.domains.performance.infrastructure.orm.performance_model  # noqa: F401
    import app.domains.performance.infrastructure.orm.venue_model  # noqa: F401
    import app.domains.ticket.infrastructure.orm.ticket_info_model  # noqa: F401
    import app.domains.event_log.infrastructure.orm.event_log_model  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from app.domains.event_log.infrastructure.views.view_manager import create_dashboard_views
    await create_dashboard_views(engine)
