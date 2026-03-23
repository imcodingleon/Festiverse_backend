import logging

from fastapi import APIRouter
from pydantic import BaseModel

from sqlalchemy import text

from app.domains.performance.adapter.outbound.external.kopis_api_adapter import KopisApiAdapter
from app.domains.performance.adapter.outbound.persistence.performance_repository import PerformanceRepository
from app.domains.performance.application.usecase.seed_notion_details_usecase import SeedNotionDetailsUseCase
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


@router.post("/seed-notion", response_model=SyncResponse)
async def seed_notion_details() -> SyncResponse:
    """노션 페스티벌 상세 정보를 NOTION_ 레코드에 시드한다 (1회성)."""
    async with async_session_factory() as session:
        repo = PerformanceRepository(session)
        usecase = SeedNotionDetailsUseCase(repo)
        count = await usecase.execute()
    logger.info("노션 시드 완료: %d건", count)
    return SyncResponse(synced_count=count)


# PF_ 레코드 장르 수정 (KOPIS '대중음악'을 실제 장르로 교정)
GENRE_FIXES: dict[str, str] = {
    "PF287381": "락/인디",          # 사운드 플래닛 페스티벌
    "PF285675": "EDM",              # S2O Korea
    "PF286584": "락/인디",          # DMZ 피스트레인
    "PF285568": "락/인디",          # 뷰티풀 민트 라이프
    "PF286798": "재즈",             # 제18회 서울재즈페스티벌
    "PF282654": "락/인디",          # 더 글로우
    "PF284703": "EDM",              # 워터밤 서울
    "PF285771": "락/인디",          # 서울히어로락페스티벌
}


@router.post("/fix-genres", response_model=SyncResponse)
async def fix_genres() -> SyncResponse:
    """PF_ 레코드의 장르를 실제 장르로 교정 (1회성)."""
    count = 0
    async with async_session_factory() as session:
        for mt20id, genre in GENRE_FIXES.items():
            await session.execute(
                text("UPDATE performances SET genrenm = :genre WHERE mt20id = :id"),
                {"genre": genre, "id": mt20id},
            )
            count += 1
        await session.commit()
    logger.info("장르 교정 완료: %d건", count)
    return SyncResponse(synced_count=count)


# 25개 페스티벌에 해당하는 DB ID 목록
KEEP_IDS = [
    # NOTION_ 레코드 (16개)
    "NOTION_001",  # 서울 파크 뮤직 페스티벌
    "NOTION_002",  # 아시안 팝 페스티벌 2026
    "NOTION_004",  # PEAK FESTIVAL 2026
    "NOTION_005",  # HIPHOPPLAYA FESTIVAL 2026
    "NOTION_006",  # 2026 WORLD DJ FESTIVAL
    "NOTION_009",  # 부산국제록페스티벌 2026
    "NOTION_010",  # 인천펜타포트락페스티벌 2026
    "NOTION_012",  # 2026 EDC KOREA
    "NOTION_014",  # RAPBEAT 2026
    "NOTION_015",  # 그린캠프 페스티벌
    "NOTION_016",  # KT&G 상상실현 페스티벌
    "NOTION_017",  # 2026 통영프린지 페스티벌
    "NOTION_018",  # LOUD BRIDGE FESTIVAL 2026
    "NOTION_019",  # 2026 체리블라썸뮤직페스티벌
    "NOTION_020",  # 2026 LOVESOME(러브썸)
    "NOTION_021",  # 워터밤 부산 2026
    "NOTION_022",  # THE AIR HOUSE
    # KOPIS PF_ 레코드 (8개) — NOTION_ 없는 페스티벌
    "PF282654",    # 더 글로우 (THE GLOW)
    "PF284703",    # 워터밤 [서울]
    "PF285568",    # 뷰티풀 민트 라이프
    "PF285675",    # S2O Korea
    "PF285771",    # 서울히어로락페스티벌
    "PF286584",    # DMZ 피스트레인 뮤직 페스티벌
    "PF286798",    # 제18회 서울재즈페스티벌
    "PF287381",    # 사운드 플래닛 페스티벌
]


class CleanupResponse(BaseModel):
    kept: int
    deleted: int


@router.post("/cleanup-festivals", response_model=CleanupResponse)
async def cleanup_festivals() -> CleanupResponse:
    """25개 페스티벌만 남기고 나머지 삭제 (1회성)."""
    placeholders = ", ".join([f":id{i}" for i in range(len(KEEP_IDS))])
    params = {f"id{i}": kid for i, kid in enumerate(KEEP_IDS)}

    async with async_session_factory() as session:
        result = await session.execute(
            text(f"DELETE FROM performances WHERE mt20id NOT IN ({placeholders})"),
            params,
        )
        deleted = result.rowcount
        await session.commit()

    logger.info("페스티벌 정리 완료: %d건 삭제, %d건 유지", deleted, len(KEEP_IDS))
    return CleanupResponse(kept=len(KEEP_IDS), deleted=deleted)
