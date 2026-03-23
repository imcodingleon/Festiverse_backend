import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.domains.performance.adapter.inbound.api.performance_router import router as performance_router
from app.domains.performance.adapter.outbound.external.kopis_api_adapter import KopisApiAdapter
from app.domains.performance.adapter.outbound.persistence.performance_repository import PerformanceRepository
from app.domains.performance.application.usecase.seed_festivals_usecase import SeedFestivalsUseCase
from app.domains.performance.application.usecase.sync_performances_usecase import SyncPerformancesUseCase
from app.domains.blog.adapter.inbound.api.blog_router import router as blog_router
from app.domains.event_log.adapter.inbound.api.dashboard_router import router as dashboard_router
from app.domains.event_log.adapter.inbound.api.event_log_router import router as event_log_router
from app.domains.ticket.adapter.inbound.api.ticket_router import router as ticket_router
from app.domains.ticket.adapter.outbound.external.parsers.interpark_parser import InterparkParser
from app.domains.ticket.adapter.outbound.external.parsers.melon_parser import MelonParser
from app.domains.ticket.adapter.outbound.external.parsers.ticketlink_parser import TicketlinkParser
from app.domains.ticket.adapter.outbound.external.searchers.interpark_searcher import InterparkSearcher
from app.domains.ticket.adapter.outbound.external.searchers.melon_searcher import MelonSearcher
from app.domains.ticket.adapter.outbound.external.searchers.ticketlink_searcher import TicketlinkSearcher
from app.domains.ticket.adapter.outbound.external.ticket_crawl_adapter import TicketCrawlAdapter
from app.domains.ticket.adapter.outbound.external.ticket_search_adapter import TicketSearchAdapter
from app.domains.ticket.adapter.outbound.persistence.performance_link_query import PerformanceLinkQuery
from app.domains.ticket.adapter.outbound.persistence.ticket_repository import TicketRepository
from app.domains.ticket.application.usecase.sync_tickets_usecase import SyncTicketsUseCase
from app.domains.ticket.domain.service.performance_matcher import PerformanceMatcher
from app.domains.performance.adapter.inbound.api.dev_router import router as dev_router
from app.infrastructure.config.settings import settings
from app.infrastructure.database.session import async_session_factory, init_db
from app.infrastructure.external.http_client import close_http_client, get_http_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def run_batch_sync() -> None:
    """배치 수집 작업 실행."""
    client = await get_http_client()
    kopis_api = KopisApiAdapter(client, settings.KOPIS_BASE_URL, settings.KOPIS_API_KEY)
    async with async_session_factory() as session:
        perf_repo = PerformanceRepository(session)
        usecase = SyncPerformancesUseCase(perf_repo, kopis_api)
        count = await usecase.execute()
        logger.info("배치 수집 완료: %d건", count)


# 노션 페스티벌 리스트 기반 KOPIS 검색 키워드
FESTIVAL_SEARCH_KEYWORDS = [
    "그린캠프",
    "서울파크뮤직",
    "사운드플래닛",
    "아시안팝페스티벌",
    "통영프린지",
    "상상실현",
    "피크페스티벌",
    "힙합플레이야",
    "월드디제이",
    "DMZ피스트레인",
    "라우드브릿지",
    "울산서머페스티벌",
    "부산국제록페스티벌",
    "서울재즈페스티벌",
    "체리블라썸",
    "인천펜타포트",
    "전주얼티밋",
    "서울히어로",
    "워터밤",
    "S2O",
    "EDC",
    "러브썸",
    "뷰티풀민트",
    "랩비트",
]


async def run_seed_festivals() -> None:
    """노션 페스티벌 리스트 기반 시드 작업 실행 (upsert 방식)."""
    client = await get_http_client()
    kopis_api = KopisApiAdapter(client, settings.KOPIS_BASE_URL, settings.KOPIS_API_KEY)
    async with async_session_factory() as session:
        perf_repo = PerformanceRepository(session)
        usecase = SeedFestivalsUseCase(perf_repo, kopis_api)
        count = await usecase.execute(FESTIVAL_SEARCH_KEYWORDS)
        logger.info("시드 완료: %d건", count)


async def run_ticket_sync() -> None:
    """티켓 크롤링 배치 작업 실행."""
    client = await get_http_client()
    parsers = [MelonParser(), TicketlinkParser(), InterparkParser()]
    crawl_adapter = TicketCrawlAdapter(client, parsers)

    # 검색 기반 크롤링 (Phase 2) 컴포넌트
    searchers = [
        MelonSearcher(client),
        InterparkSearcher(client),
        TicketlinkSearcher(client),
    ]
    search_adapter = TicketSearchAdapter(searchers)
    matcher = PerformanceMatcher()

    async with async_session_factory() as session:
        ticket_repo = TicketRepository(session)
        link_query = PerformanceLinkQuery(session)
        usecase = SyncTicketsUseCase(
            ticket_repo, crawl_adapter, link_query,
            crawl_delay=settings.CRAWL_DELAY_SECONDS,
            ticket_search=search_adapter,
            matcher=matcher,
            search_batch_limit=settings.SEARCH_BATCH_LIMIT,
            search_delay=settings.SEARCH_DELAY_SECONDS,
        )
        count = await usecase.execute()
        logger.info("티켓 크롤링 완료: %d건", count)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("서버 시작: DB 초기화")
    await init_db()

    # 시드는 수동 실행 (run_seed_festivals) — 서버 시작 시 자동 실행하지 않음
    # 초기 배치 수집도 스케줄러에 맡김

    scheduler.add_job(run_batch_sync, "cron", hour=settings.BATCH_HOUR, id="batch_sync")
    scheduler.add_job(run_ticket_sync, "cron", hour=settings.TICKET_BATCH_HOUR, id="ticket_sync")
    scheduler.start()
    logger.info("스케줄러 시작 (배치: %02d:00, 티켓: %02d:00)", settings.BATCH_HOUR, settings.TICKET_BATCH_HOUR)

    yield

    scheduler.shutdown(wait=False)
    await close_http_client()
    logger.info("서버 종료")


app = FastAPI(title="Festiverse API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(performance_router)
app.include_router(ticket_router)
app.include_router(blog_router)
app.include_router(event_log_router)
app.include_router(dashboard_router)
app.include_router(dev_router)
