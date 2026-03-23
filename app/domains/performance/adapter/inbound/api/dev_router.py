import logging

from fastapi import APIRouter
from pydantic import BaseModel

from app.domains.performance.adapter.outbound.external.kopis_api_adapter import KopisApiAdapter
from app.domains.performance.adapter.outbound.persistence.performance_repository import PerformanceRepository
from app.domains.performance.application.usecase.sync_performances_usecase import SyncPerformancesUseCase
from app.infrastructure.config.settings import settings
from app.infrastructure.database.session import async_session_factory
from app.infrastructure.external.http_client import get_http_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dev", tags=["dev"])


class SyncResponse(BaseModel):
    synced_count: int


@router.post("/sync", response_model=SyncResponse)
async def sync_performances() -> SyncResponse:
    """KOPIS 배치 동기화를 수동으로 1회 실행한다 (개발용)."""
    client = await get_http_client()
    kopis_api = KopisApiAdapter(client, settings.KOPIS_BASE_URL, settings.KOPIS_API_KEY)
    async with async_session_factory() as session:
        repo = PerformanceRepository(session)
        usecase = SyncPerformancesUseCase(repo, kopis_api)
        count = await usecase.execute()
    logger.info("수동 동기화 완료: %d건", count)
    return SyncResponse(synced_count=count)
