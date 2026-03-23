"""
event_log 통합 테스트용 conftest.
실제 MySQL(Docker)에 테스트용 테이블·View를 생성하고 테스트 후 정리한다.
"""
import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.domains.event_log.infrastructure.orm.event_log_model import EventLogModel
from app.domains.event_log.infrastructure.views.view_manager import create_dashboard_views
from app.infrastructure.config.settings import settings
from app.infrastructure.database.base import Base


SAMPLE_SESSION_ID = "sess-test-001"
SAMPLE_ANON_ID = "anon-test-001"
SAMPLE_DATE = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
DB_URL = settings.TEST_DATABASE_URL or settings.DATABASE_URL


def new_engine():
    return create_async_engine(DB_URL, echo=False, pool_pre_ping=True)


async def _do_setup():
    engine = new_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        await create_dashboard_views(engine)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            for ev in _build_sample_scenario():
                sess.add(EventLogModel(**ev))

            past = datetime.now(timezone.utc) - timedelta(days=18)
            p4_ev = _make_event(
                "ticket_button_clicked",
                {"festival_id": "PF999002", "festival_name": "P4 Test",
                 "ticket_provider": "melon",
                 "review_clicked_in_session": False,
                 "review_click_count_in_session": 0,
                 "sections_viewed_in_session": [],
                 "sections_viewed_count_in_session": 0,
                 "time_since_page_entered_ms": 5000},
                session_id="sess-p4-test",
                anonymous_id="anon-p4-test",
                page_url="/performance/PF999002",
                created_at=past,
            )
            sess.add(EventLogModel(**p4_ev))
            await sess.commit()
    finally:
        await engine.dispose()


async def _do_teardown():
    engine = new_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    finally:
        await engine.dispose()


def _make_event(
    event_type: str,
    event_data: dict | None = None,
    session_id: str = "sess-aaa",
    anonymous_id: str = "anon-aaa",
    page_url: str = "/",
    device_type: str = "desktop",
    created_at: datetime | None = None,
) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "anonymous_id": anonymous_id,
        "session_id": session_id,
        "event_type": event_type,
        "event_data": event_data,
        "page_url": page_url,
        "device_type": device_type,
        "created_at": created_at or datetime.now(timezone.utc),
    }


def _build_sample_scenario() -> list[dict]:
    sid = SAMPLE_SESSION_ID
    aid = SAMPLE_ANON_ID
    base = SAMPLE_DATE
    events = []
    t = 0

    def ev(etype, edata=None, page="/"):
        nonlocal t
        t += 1
        return _make_event(
            etype, edata, session_id=sid, anonymous_id=aid,
            page_url=page, created_at=base + timedelta(seconds=t),
        )

    events.append(ev("app_session_started", {
        "is_return_user": False, "days_since_last_visit": None, "referrer": None,
    }))
    events.append(ev("search_page_entered", {}))
    events.append(ev("filter_option_toggled", {
        "filter_type": "region", "filter_value": "서울",
        "is_selected": True, "time_since_page_entered_ms": 3000,
    }))
    events.append(ev("filter_apply_button_clicked", {
        "applied_filters": {"region": ["서울"], "genre": []},
        "filter_count": 1, "time_since_page_entered_ms": 8000,
    }))
    events.append(ev("festival_item_clicked", {
        "festival_id": "PF999001", "festival_name": "테스트페스티벌",
        "list_position": 0,
        "active_filters": {"region": ["서울"], "genre": [], "selected_date": None, "keyword": ""},
        "is_filtered_session": True, "time_since_page_entered_ms": 12000,
    }))
    events.append(ev("detail_page_entered", {
        "festival_id": "PF999001", "festival_name": "테스트페스티벌",
    }, page="/performance/PF999001"))

    for i, sec in enumerate(["hero", "basic_info", "lineup", "ticket_price"]):
        events.append(ev("section_viewed", {
            "festival_id": "PF999001", "section_name": sec,
            "section_index": i, "time_since_page_entered_ms": 2000 * (i + 1),
            "is_section_rendered": True,
        }, page="/performance/PF999001"))

    events.append(ev("blog_review_clicked", {
        "festival_id": "PF999001", "review_index": 1,
        "review_title": "테스트 리뷰", "review_url": "https://blog.naver.com/test",
        "time_since_page_entered_ms": 15000,
    }, page="/performance/PF999001"))
    events.append(ev("ticket_button_clicked", {
        "festival_id": "PF999001", "festival_name": "테스트페스티벌",
        "ticket_provider": "melon",
        "review_clicked_in_session": True, "review_click_count_in_session": 1,
        "sections_viewed_in_session": ["hero", "basic_info", "lineup", "ticket_price"],
        "sections_viewed_count_in_session": 4, "time_since_page_entered_ms": 20000,
    }, page="/performance/PF999001"))
    events.append(ev("detail_page_exited", {
        "festival_id": "PF999001", "time_on_page_ms": 25000,
        "last_section_viewed": "ticket_price",
        "sections_viewed_list": ["hero", "basic_info", "lineup", "ticket_price"],
        "sections_viewed_count": 4,
    }, page="/performance/PF999001"))
    events.append(ev("search_page_exited", {"time_on_page_ms": 30000}))

    return events


@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown(request):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(_do_setup())

    def fin():
        loop2 = asyncio.new_event_loop()
        loop2.run_until_complete(_do_teardown())
        loop2.close()

    request.addfinalizer(fin)
    loop.close()
