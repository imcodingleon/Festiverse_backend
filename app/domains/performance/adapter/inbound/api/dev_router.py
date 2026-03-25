import logging
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter
from pydantic import BaseModel

from sqlalchemy import text

from app.domains.event_log.infrastructure.orm.event_log_model import EventLogModel
from app.domains.performance.adapter.outbound.external.kopis_api_adapter import KopisApiAdapter
from app.domains.performance.adapter.outbound.persistence.performance_repository import PerformanceRepository
from app.domains.performance.application.usecase.seed_notion_details_usecase import SeedNotionDetailsUseCase
from app.domains.performance.application.usecase.sync_performances_usecase import SyncPerformancesUseCase
from app.domains.ticket.adapter.outbound.external.parsers.interpark_parser import InterparkParser
from app.domains.ticket.adapter.outbound.external.parsers.melon_parser import MelonParser
from app.domains.ticket.adapter.outbound.external.parsers.ticketlink_parser import TicketlinkParser
from app.domains.ticket.adapter.outbound.external.ticket_crawl_adapter import TicketCrawlAdapter
from app.domains.ticket.adapter.outbound.persistence.performance_link_query import PerformanceLinkQuery
from app.domains.ticket.adapter.outbound.persistence.ticket_repository import TicketRepository
from app.domains.ticket.application.usecase.sync_tickets_usecase import SyncTicketsUseCase
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


@router.post("/sync-tickets", response_model=SyncResponse)
async def sync_tickets() -> SyncResponse:
    """티켓 크롤링 Phase 1(relates 기반)만 1회 실행한다 (개발용)."""
    client = await get_http_client()
    parsers = [MelonParser(), TicketlinkParser(), InterparkParser()]
    crawl_adapter = TicketCrawlAdapter(client, parsers)

    async with async_session_factory() as session:
        ticket_repo = TicketRepository(session)
        link_query = PerformanceLinkQuery(session)
        usecase = SyncTicketsUseCase(
            ticket_repo, crawl_adapter, link_query,
            crawl_delay=settings.CRAWL_DELAY_SECONDS,
        )
        count = await usecase.execute()
    logger.info("티켓 크롤링 완료: %d건", count)
    return SyncResponse(synced_count=count)


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


# ---------------------------------------------------------------------------
# 이벤트 로그 시드 (대시보드 데모용)
# ---------------------------------------------------------------------------

# 시드에 사용할 페스티벌 (ID, 이름)
_SEED_FESTIVALS = [
    ("PF286798", "서울재즈페스티벌"),
    ("PF284703", "워터밤 서울"),
    ("PF285568", "뷰티풀 민트 라이프"),
    ("PF285675", "S2O Korea"),
    ("PF287381", "사운드 플래닛 페스티벌"),
    ("PF286584", "DMZ 피스트레인"),
    ("PF282654", "더 글로우"),
    ("PF285771", "서울히어로락페스티벌"),
    ("NOTION_001", "서울파크뮤직페스티벌"),
    ("NOTION_005", "HIPHOPPLAYA FESTIVAL"),
]

_SEED_DATE = datetime(2026, 3, 16, tzinfo=timezone.utc)

_FILTER_REGIONS = ["서울", "경기", "부산", "인천", "대전"]
_SECTIONS = ["hero", "basic_info", "lineup", "ticket_price", "ticket_booking", "blog_review"]
_TICKET_PROVIDERS = ["melon", "interpark", "ticketlink"]


def _ev(
    event_type: str,
    sid: str,
    aid: str,
    ts: datetime,
    event_data: dict | None = None,
    page_url: str = "/",
    device_type: str = "desktop",
) -> EventLogModel:
    return EventLogModel(
        id=str(uuid.uuid4()),
        anonymous_id=aid,
        session_id=sid,
        event_type=event_type,
        event_data=event_data or {},
        page_url=page_url,
        device_type=device_type,
        created_at=ts,
    )


def _build_seed_events() -> list[EventLogModel]:
    """40명의 사용자 시나리오 생성 (5개 패턴)."""
    models: list[EventLogModel] = []
    user_idx = 0

    def next_user(base_hour: int, base_min: int):
        nonlocal user_idx
        user_idx += 1
        sid = f"sess-seed-{user_idx:03d}"
        aid = f"anon-seed-{user_idx:03d}"
        extra_hours, base_min = divmod(base_min, 60)
        base_hour = min(base_hour + extra_hours, 23)
        ts = _SEED_DATE.replace(hour=base_hour, minute=base_min, second=0)
        device = "mobile" if user_idx % 5 != 0 else "desktop"
        fest_idx = user_idx % len(_SEED_FESTIVALS)
        fid, fname = _SEED_FESTIVALS[fest_idx]
        region = _FILTER_REGIONS[user_idx % len(_FILTER_REGIONS)]
        provider = _TICKET_PROVIDERS[user_idx % len(_TICKET_PROVIDERS)]
        return sid, aid, ts, device, fid, fname, region, provider

    # --- 패턴 A: 풀 퍼널 완주 (8명) ---
    for i in range(8):
        sid, aid, ts, dev, fid, fname, region, provider = next_user(9 + i, (i * 7) % 60)
        t = ts
        models.append(_ev("app_session_started", sid, aid, t, {
            "is_return_user": i > 3, "days_since_last_visit": 3 if i > 3 else None, "referrer": None,
        }, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_entered", sid, aid, t, {}, device_type=dev))
        t += timedelta(seconds=2)
        tft = 2000 + i * 500
        models.append(_ev("filter_option_toggled", sid, aid, t, {
            "filter_type": "region", "filter_value": region,
            "is_selected": True, "time_since_page_entered_ms": tft,
        }, device_type=dev))
        t += timedelta(seconds=3)
        tfa = tft + 2000 + i * 300
        models.append(_ev("filter_apply_button_clicked", sid, aid, t, {
            "applied_filters": {"region": [region], "genre": []},
            "filter_count": 1, "time_since_page_entered_ms": tfa,
        }, device_type=dev))
        t += timedelta(seconds=3)
        ttd = tfa + 3000 + i * 400
        models.append(_ev("festival_item_clicked", sid, aid, t, {
            "festival_id": fid, "festival_name": fname, "list_position": i % 5,
            "active_filters": {"region": [region], "genre": [], "selected_date": None, "keyword": ""},
            "is_filtered_session": True, "time_since_page_entered_ms": ttd,
        }, device_type=dev))
        t += timedelta(seconds=1)
        detail_url = f"/performance/{fid}"
        models.append(_ev("detail_page_entered", sid, aid, t, {
            "festival_id": fid, "festival_name": fname,
        }, page_url=detail_url, device_type=dev))
        for si, sec in enumerate(_SECTIONS[:4]):
            t += timedelta(seconds=2)
            models.append(_ev("section_viewed", sid, aid, t, {
                "festival_id": fid, "section_name": sec,
                "section_index": si, "time_since_page_entered_ms": 2000 * (si + 1),
                "is_section_rendered": True,
            }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=2)
        models.append(_ev("blog_review_clicked", sid, aid, t, {
            "festival_id": fid, "review_index": i % 3,
            "review_title": f"{fname} 후기", "review_url": f"https://blog.naver.com/seed{user_idx}",
            "time_since_page_entered_ms": 15000 + i * 1000,
        }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=3)
        models.append(_ev("ticket_button_clicked", sid, aid, t, {
            "festival_id": fid, "festival_name": fname, "ticket_provider": provider,
            "review_clicked_in_session": True, "review_click_count_in_session": 1,
            "sections_viewed_in_session": _SECTIONS[:4],
            "sections_viewed_count_in_session": 4, "time_since_page_entered_ms": 20000 + i * 1000,
        }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=2)
        models.append(_ev("detail_page_exited", sid, aid, t, {
            "festival_id": fid, "time_on_page_ms": 25000 + i * 2000,
            "last_section_viewed": "ticket_price",
            "sections_viewed_list": _SECTIONS[:4], "sections_viewed_count": 4,
        }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_exited", sid, aid, t, {
            "time_on_page_ms": 35000 + i * 3000,
        }, device_type=dev))

    # --- 패턴 B: 블로그만 보고 이탈 (8명) ---
    for i in range(8):
        sid, aid, ts, dev, fid, fname, region, provider = next_user(11 + i % 4, 10 + i * 6)
        t = ts
        models.append(_ev("app_session_started", sid, aid, t, {
            "is_return_user": False, "days_since_last_visit": None, "referrer": None,
        }, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_entered", sid, aid, t, {}, device_type=dev))
        t += timedelta(seconds=2)
        tft = 3000 + i * 400
        models.append(_ev("filter_option_toggled", sid, aid, t, {
            "filter_type": "region", "filter_value": region,
            "is_selected": True, "time_since_page_entered_ms": tft,
        }, device_type=dev))
        t += timedelta(seconds=2)
        tfa = tft + 2500
        models.append(_ev("filter_apply_button_clicked", sid, aid, t, {
            "applied_filters": {"region": [region], "genre": []},
            "filter_count": 1, "time_since_page_entered_ms": tfa,
        }, device_type=dev))
        t += timedelta(seconds=3)
        ttd = tfa + 4000
        models.append(_ev("festival_item_clicked", sid, aid, t, {
            "festival_id": fid, "festival_name": fname, "list_position": i % 5,
            "active_filters": {"region": [region], "genre": [], "selected_date": None, "keyword": ""},
            "is_filtered_session": True, "time_since_page_entered_ms": ttd,
        }, device_type=dev))
        t += timedelta(seconds=1)
        detail_url = f"/performance/{fid}"
        models.append(_ev("detail_page_entered", sid, aid, t, {
            "festival_id": fid, "festival_name": fname,
        }, page_url=detail_url, device_type=dev))
        for si, sec in enumerate(_SECTIONS[:3]):
            t += timedelta(seconds=2)
            models.append(_ev("section_viewed", sid, aid, t, {
                "festival_id": fid, "section_name": sec,
                "section_index": si, "time_since_page_entered_ms": 2000 * (si + 1),
                "is_section_rendered": True,
            }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=2)
        models.append(_ev("blog_review_clicked", sid, aid, t, {
            "festival_id": fid, "review_index": 0,
            "review_title": f"{fname} 리뷰", "review_url": f"https://blog.naver.com/seed{user_idx}",
            "time_since_page_entered_ms": 12000 + i * 800,
        }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=3)
        models.append(_ev("detail_page_exited", sid, aid, t, {
            "festival_id": fid, "time_on_page_ms": 18000 + i * 1500,
            "last_section_viewed": "lineup",
            "sections_viewed_list": _SECTIONS[:3], "sections_viewed_count": 3,
        }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_exited", sid, aid, t, {
            "time_on_page_ms": 28000 + i * 2000,
        }, device_type=dev))

    # --- 패턴 C: 상세만 보고 이탈 (10명) ---
    for i in range(10):
        sid, aid, ts, dev, fid, fname, region, provider = next_user(13 + i % 5, 5 + i * 5)
        t = ts
        models.append(_ev("app_session_started", sid, aid, t, {
            "is_return_user": False, "days_since_last_visit": None, "referrer": None,
        }, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_entered", sid, aid, t, {}, device_type=dev))
        t += timedelta(seconds=4)
        ttd = 6000 + i * 600
        models.append(_ev("festival_item_clicked", sid, aid, t, {
            "festival_id": fid, "festival_name": fname, "list_position": i % 8,
            "active_filters": {"region": [], "genre": [], "selected_date": None, "keyword": ""},
            "is_filtered_session": False, "time_since_page_entered_ms": ttd,
        }, device_type=dev))
        t += timedelta(seconds=1)
        detail_url = f"/performance/{fid}"
        models.append(_ev("detail_page_entered", sid, aid, t, {
            "festival_id": fid, "festival_name": fname,
        }, page_url=detail_url, device_type=dev))
        for si, sec in enumerate(_SECTIONS[:2]):
            t += timedelta(seconds=2)
            models.append(_ev("section_viewed", sid, aid, t, {
                "festival_id": fid, "section_name": sec,
                "section_index": si, "time_since_page_entered_ms": 2000 * (si + 1),
                "is_section_rendered": True,
            }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=3)
        models.append(_ev("detail_page_exited", sid, aid, t, {
            "festival_id": fid, "time_on_page_ms": 8000 + i * 1000,
            "last_section_viewed": "basic_info",
            "sections_viewed_list": _SECTIONS[:2], "sections_viewed_count": 2,
        }, page_url=detail_url, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_exited", sid, aid, t, {
            "time_on_page_ms": 15000 + i * 1500,
        }, device_type=dev))

    # --- 패턴 D: 필터만 사용하고 이탈 (8명) ---
    for i in range(8):
        sid, aid, ts, dev, fid, fname, region, provider = next_user(18 + i % 3, 15 + i * 7)
        t = ts
        models.append(_ev("app_session_started", sid, aid, t, {
            "is_return_user": False, "days_since_last_visit": None, "referrer": None,
        }, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_entered", sid, aid, t, {}, device_type=dev))
        t += timedelta(seconds=3)
        tft = 3000 + i * 500
        models.append(_ev("filter_option_toggled", sid, aid, t, {
            "filter_type": "region", "filter_value": region,
            "is_selected": True, "time_since_page_entered_ms": tft,
        }, device_type=dev))
        t += timedelta(seconds=3)
        tfa = tft + 3000
        models.append(_ev("filter_apply_button_clicked", sid, aid, t, {
            "applied_filters": {"region": [region], "genre": []},
            "filter_count": 1, "time_since_page_entered_ms": tfa,
        }, device_type=dev))
        t += timedelta(seconds=5)
        models.append(_ev("search_page_exited", sid, aid, t, {
            "time_on_page_ms": 12000 + i * 1000,
        }, device_type=dev))

    # --- 패턴 E: 즉시 이탈 (6명) ---
    for i in range(6):
        sid, aid, ts, dev, fid, fname, region, provider = next_user(21 + i % 2, 30 + i * 5)
        t = ts
        models.append(_ev("app_session_started", sid, aid, t, {
            "is_return_user": False, "days_since_last_visit": None, "referrer": None,
        }, device_type=dev))
        t += timedelta(seconds=1)
        models.append(_ev("search_page_entered", sid, aid, t, {}, device_type=dev))
        t += timedelta(seconds=4)
        models.append(_ev("search_page_exited", sid, aid, t, {
            "time_on_page_ms": 4000 + i * 500,
        }, device_type=dev))

    return models


@router.post("/seed-event-logs", response_model=SyncResponse)
async def seed_event_logs() -> SyncResponse:
    """3월 16일 기준 가상 이벤트 로그를 삽입한다 (1회성, 중복 실행 시 기존 데이터 교체)."""
    models = _build_seed_events()

    async with async_session_factory() as session:
        # 기존 3월 16일 시드 데이터 삭제 (중복 방지)
        await session.execute(
            text("DELETE FROM event_logs WHERE DATE(created_at) = '2026-03-16'"
                 " AND session_id LIKE 'sess-seed-%'"),
        )

        for m in models:
            session.add(m)
        await session.commit()

    logger.info("이벤트 로그 시드 완료: %d건", len(models))
    return SyncResponse(synced_count=len(models))
