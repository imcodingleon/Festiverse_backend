"""
E2E 테스트 — POST /api/events → GET /api/dashboard/{view} 파이프라인 검증.
프론트엔드 사용자 여정을 HTTP API로 시뮬레이션하고, 대시보드 지표 정합성을 확인한다.

MySQL Docker 컨테이너 실행 필수: docker-compose up -d mysql
"""
import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.domains.event_log.infrastructure.views.view_manager import create_dashboard_views
from app.domains.event_log.tests.conftest import DB_URL
from app.infrastructure.database.base import Base

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# 이벤트 페이로드 빌더
# ---------------------------------------------------------------------------


def _payload(
    event_type: str,
    session_id: str,
    anonymous_id: str,
    event_data: dict | None = None,
    page_url: str = "/",
    device_type: str = "desktop",
) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "anonymous_id": anonymous_id,
        "session_id": session_id,
        "event_type": event_type,
        "event_data": event_data or {},
        "page_url": page_url,
        "device_type": device_type,
    }


def _build_user_a_events() -> list[dict]:
    """User A: 풀 퍼널 완주 (필터→상세→리뷰→티켓)."""
    sid, aid = "sess-e2e-a", "anon-e2e-a"
    detail_page = "/performance/PF-E2E-001"
    events = [
        _payload("app_session_started", sid, aid, {
            "is_return_user": False, "days_since_last_visit": None, "referrer": None,
        }),
        _payload("search_page_entered", sid, aid, {}),
        _payload("filter_option_toggled", sid, aid, {
            "filter_type": "region", "filter_value": "서울",
            "is_selected": True, "time_since_page_entered_ms": 3000,
        }),
        _payload("filter_apply_button_clicked", sid, aid, {
            "applied_filters": {"region": ["서울"], "genre": []},
            "filter_count": 1, "time_since_page_entered_ms": 5000,
        }),
        _payload("festival_item_clicked", sid, aid, {
            "festival_id": "PF-E2E-001", "festival_name": "E2E테스트페스티벌",
            "list_position": 0,
            "active_filters": {"region": ["서울"], "genre": [], "selected_date": None, "keyword": ""},
            "is_filtered_session": True, "time_since_page_entered_ms": 10000,
        }),
        _payload("detail_page_entered", sid, aid, {
            "festival_id": "PF-E2E-001", "festival_name": "E2E테스트페스티벌",
        }, page_url=detail_page),
    ]
    for i, sec in enumerate(["hero", "basic_info", "lineup", "ticket_price"]):
        events.append(_payload("section_viewed", sid, aid, {
            "festival_id": "PF-E2E-001", "section_name": sec,
            "section_index": i, "time_since_page_entered_ms": 2000 * (i + 1),
            "is_section_rendered": True,
        }, page_url=detail_page))

    events.append(_payload("blog_review_clicked", sid, aid, {
        "festival_id": "PF-E2E-001", "review_index": 0,
        "review_title": "E2E 리뷰", "review_url": "https://blog.naver.com/e2e",
        "time_since_page_entered_ms": 15000,
    }, page_url=detail_page))
    events.append(_payload("ticket_button_clicked", sid, aid, {
        "festival_id": "PF-E2E-001", "festival_name": "E2E테스트페스티벌",
        "ticket_provider": "melon",
        "review_clicked_in_session": True, "review_click_count_in_session": 1,
        "sections_viewed_in_session": ["hero", "basic_info", "lineup", "ticket_price"],
        "sections_viewed_count_in_session": 4, "time_since_page_entered_ms": 20000,
    }, page_url=detail_page))
    events.append(_payload("detail_page_exited", sid, aid, {
        "festival_id": "PF-E2E-001", "time_on_page_ms": 25000,
        "last_section_viewed": "ticket_price",
        "sections_viewed_list": ["hero", "basic_info", "lineup", "ticket_price"],
        "sections_viewed_count": 4,
    }, page_url=detail_page))
    events.append(_payload("search_page_exited", sid, aid, {"time_on_page_ms": 35000}))
    return events


def _build_user_b_events() -> list[dict]:
    """User B: 상세 진입 후 이탈 (필터 미사용, 티켓 미클릭)."""
    sid, aid = "sess-e2e-b", "anon-e2e-b"
    detail_page = "/performance/PF-E2E-002"
    events = [
        _payload("app_session_started", sid, aid, {
            "is_return_user": False, "days_since_last_visit": None, "referrer": None,
        }),
        _payload("search_page_entered", sid, aid, {}),
        _payload("festival_item_clicked", sid, aid, {
            "festival_id": "PF-E2E-002", "festival_name": "E2E이탈페스티벌",
            "list_position": 2,
            "active_filters": {"region": [], "genre": [], "selected_date": None, "keyword": ""},
            "is_filtered_session": False, "time_since_page_entered_ms": 8000,
        }),
        _payload("detail_page_entered", sid, aid, {
            "festival_id": "PF-E2E-002", "festival_name": "E2E이탈페스티벌",
        }, page_url=detail_page),
    ]
    for i, sec in enumerate(["hero", "basic_info"]):
        events.append(_payload("section_viewed", sid, aid, {
            "festival_id": "PF-E2E-002", "section_name": sec,
            "section_index": i, "time_since_page_entered_ms": 2000 * (i + 1),
            "is_section_rendered": True,
        }, page_url=detail_page))

    events.append(_payload("detail_page_exited", sid, aid, {
        "festival_id": "PF-E2E-002", "time_on_page_ms": 5000,
        "last_section_viewed": "basic_info",
        "sections_viewed_list": ["hero", "basic_info"],
        "sections_viewed_count": 2,
    }, page_url=detail_page))
    events.append(_payload("search_page_exited", sid, aid, {"time_on_page_ms": 20000}))
    return events


# ---------------------------------------------------------------------------
# P4 전용: 과거 ticket_button_clicked + 재방문 이벤트
# ---------------------------------------------------------------------------

P4_ANON_ID = "anon-e2e-p4"
P4_SESSION_OLD = "sess-e2e-p4-old"
P4_SESSION_NEW = "sess-e2e-p4-new"


def _build_p4_events() -> list[dict]:
    """P4 검증용 이벤트: 과거 티켓 클릭 + 재방문 세션."""
    return [
        _payload("ticket_button_clicked", P4_SESSION_OLD, P4_ANON_ID, {
            "festival_id": "PF-P4-001", "festival_name": "P4테스트",
            "ticket_provider": "interpark",
            "review_clicked_in_session": False, "review_click_count_in_session": 0,
            "sections_viewed_in_session": [], "sections_viewed_count_in_session": 0,
            "time_since_page_entered_ms": 5000,
        }, page_url="/performance/PF-P4-001"),
        _payload("detail_page_entered", P4_SESSION_OLD, P4_ANON_ID, {
            "festival_id": "PF-P4-001", "festival_name": "P4테스트",
        }, page_url="/performance/PF-P4-001"),
        # 재방문 이벤트 (anchor 이후 — created_at을 나중에 조정)
        _payload("app_session_started", P4_SESSION_NEW, P4_ANON_ID, {
            "is_return_user": True, "days_since_last_visit": 3, "referrer": None,
        }),
        _payload("search_page_entered", P4_SESSION_NEW, P4_ANON_ID, {}),
    ]


# ---------------------------------------------------------------------------
# NullPool 기반 엔진 팩토리 (이벤트 루프 교차 문제 방지)
# ---------------------------------------------------------------------------

def _new_engine():
    return create_async_engine(DB_URL, echo=False, poolclass=NullPool)


def _new_factory(engine=None):
    eng = engine or _new_engine()
    return async_sessionmaker(eng, expire_on_commit=False), eng


def _patch_session_factories(factory):
    """라우터 모듈의 async_session_factory를 테스트용으로 교체."""
    import app.infrastructure.database.session as session_mod
    import app.domains.event_log.adapter.inbound.api.event_log_router as event_router_mod
    import app.domains.event_log.adapter.inbound.api.dashboard_router as dash_router_mod

    session_mod.async_session_factory = factory
    event_router_mod.async_session_factory = factory
    dash_router_mod.async_session_factory = factory


_original_factories = {}


# ---------------------------------------------------------------------------
# Module-scope fixture: DB 세팅 + 이벤트 전송
# ---------------------------------------------------------------------------

async def _do_e2e_setup():
    engine = _new_engine()

    # 테이블 + View 생성
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await create_dashboard_views(engine)

    # 원본 저장 + 테스트 팩토리로 교체
    import app.infrastructure.database.session as session_mod
    import app.domains.event_log.adapter.inbound.api.event_log_router as event_router_mod
    import app.domains.event_log.adapter.inbound.api.dashboard_router as dash_router_mod

    _original_factories["session"] = session_mod.async_session_factory
    _original_factories["event_router"] = event_router_mod.async_session_factory
    _original_factories["dash_router"] = dash_router_mod.async_session_factory
    _original_factories["engine"] = session_mod.engine

    test_factory = async_sessionmaker(engine, expire_on_commit=False)
    _patch_session_factories(test_factory)
    session_mod.engine = engine

    # 이벤트 POST
    from app.main import app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        all_events = _build_user_a_events() + _build_user_b_events() + _build_p4_events()
        for ev in all_events:
            resp = await client.post("/api/events", json=ev)
            assert resp.status_code == 201, f"POST 실패 [{ev['event_type']}]: {resp.text}"

    # P4: created_at 백데이팅 (old 세션 → 18일 전, new 세션 → 15일 전)
    past_old = datetime.now(timezone.utc) - timedelta(days=18)
    past_new = datetime.now(timezone.utc) - timedelta(days=15)
    async with test_factory() as sess:
        await sess.execute(
            text("UPDATE event_logs SET created_at = :ts WHERE session_id = :sid"),
            {"ts": past_old, "sid": P4_SESSION_OLD},
        )
        await sess.execute(
            text("UPDATE event_logs SET created_at = :ts WHERE session_id = :sid"),
            {"ts": past_new, "sid": P4_SESSION_NEW},
        )
        await sess.commit()

    await engine.dispose()


async def _do_e2e_teardown():
    import app.infrastructure.database.session as session_mod
    import app.domains.event_log.adapter.inbound.api.event_log_router as event_router_mod
    import app.domains.event_log.adapter.inbound.api.dashboard_router as dash_router_mod

    session_mod.async_session_factory = _original_factories["session"]
    session_mod.engine = _original_factories["engine"]
    event_router_mod.async_session_factory = _original_factories["event_router"]
    dash_router_mod.async_session_factory = _original_factories["dash_router"]

    engine = _new_engine()
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
    finally:
        await engine.dispose()


@pytest.fixture(scope="module", autouse=True)
def e2e_setup_teardown(request):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(_do_e2e_setup())
    loop.close()

    def fin():
        loop2 = asyncio.new_event_loop()
        loop2.run_until_complete(_do_e2e_teardown())
        loop2.close()

    request.addfinalizer(fin)


# ---------------------------------------------------------------------------
# 헬퍼: HTTP API로 대시보드 조회
# ---------------------------------------------------------------------------

async def _get_dashboard(
    view_name: str,
    date_from: str | None = TODAY,
    date_to: str | None = TODAY,
) -> dict:
    # 매 호출마다 fresh NullPool 엔진 → 이벤트 루프 안전
    factory, engine = _new_factory()
    _patch_session_factories(factory)
    try:
        from app.main import app
        params = {}
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get(f"/api/dashboard/{view_name}", params=params)
            assert resp.status_code == 200, f"GET {view_name} 실패: {resp.text}"
            return resp.json()
    finally:
        await engine.dispose()


async def _query_db(sql_str: str, params: dict | None = None) -> list[dict]:
    """테스트에서 직접 DB를 조회하는 헬퍼."""
    engine = _new_engine()
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            result = await sess.execute(text(sql_str), params or {})
            columns = list(result.keys())
            return [dict(zip(columns, r)) for r in result.fetchall()]
    finally:
        await engine.dispose()


# ============================================================
# 검증 1 — 이벤트 수집 정합성
# ============================================================

class TestE2EEventIngestion:

    @pytest.mark.asyncio
    async def test_all_events_stored(self):
        """User A(14건) + User B(8건) + P4(4건) = 26건 저장 확인."""
        rows = await _query_db("SELECT COUNT(*) AS cnt FROM event_logs")
        assert rows[0]["cnt"] == 26, f"Expected 26 events, got {rows[0]['cnt']}"

    @pytest.mark.asyncio
    async def test_today_events_count(self):
        """오늘 날짜 이벤트만 22건 (P4 백데이팅 4건 제외)."""
        rows = await _query_db(
            "SELECT COUNT(*) AS cnt FROM event_logs WHERE DATE(created_at) = :d",
            {"d": TODAY},
        )
        assert rows[0]["cnt"] == 22, f"Expected 22 today events, got {rows[0]['cnt']}"


# ============================================================
# 검증 2 — P1 View 15종 (HTTP API)
# ============================================================

class TestE2EP1Dashboard:

    @pytest.mark.asyncio
    async def test_v_p1_pv(self):
        data = await _get_dashboard("v_p1_pv")
        assert data["total"] >= 1
        assert data["rows"][0]["pv"] == 2

    @pytest.mark.asyncio
    async def test_v_p1_fsr(self):
        data = await _get_dashboard("v_p1_fsr")
        assert float(data["rows"][0]["fsr"]) == 0.5

    @pytest.mark.asyncio
    async def test_v_p1_far(self):
        data = await _get_dashboard("v_p1_far")
        assert float(data["rows"][0]["far"]) == 0.5

    @pytest.mark.asyncio
    async def test_v_p1_dcr(self):
        data = await _get_dashboard("v_p1_dcr")
        assert float(data["rows"][0]["dcr"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p1_tft(self):
        data = await _get_dashboard("v_p1_tft")
        assert int(data["rows"][0]["avg_tft_ms"]) == 3000

    @pytest.mark.asyncio
    async def test_v_p1_tfa(self):
        data = await _get_dashboard("v_p1_tfa")
        assert int(data["rows"][0]["avg_tfa_ms"]) == 5000

    @pytest.mark.asyncio
    async def test_v_p1_ttd(self):
        data = await _get_dashboard("v_p1_ttd")
        assert int(data["rows"][0]["avg_ttd_ms"]) == 9000

    @pytest.mark.asyncio
    async def test_v_p1_time_on_page(self):
        data = await _get_dashboard("v_p1_time_on_page")
        assert int(data["rows"][0]["avg_time_on_page_ms"]) == 27500

    @pytest.mark.asyncio
    async def test_v_p1_fuc(self):
        data = await _get_dashboard("v_p1_fuc")
        assert float(data["rows"][0]["avg_fuc"]) >= 1.0

    @pytest.mark.asyncio
    async def test_v_p1_rer(self):
        data = await _get_dashboard("v_p1_rer")
        assert float(data["rows"][0]["rer"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p1_afa(self):
        data = await _get_dashboard("v_p1_afa")
        assert float(data["rows"][0]["avg_afa"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p1_sur(self):
        data = await _get_dashboard("v_p1_sur")
        assert float(data["rows"][0]["sur"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p1_scr(self):
        data = await _get_dashboard("v_p1_scr")
        assert float(data["rows"][0]["scr"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p1_time_on_page_seg(self):
        data = await _get_dashboard("v_p1_time_on_page_seg")
        segments = {r["segment"] for r in data["rows"]}
        assert "Filtered" in segments, f"Filtered 세그먼트 없음: {data['rows']}"
        assert "Non Filtered" in segments, f"Non Filtered 세그먼트 없음: {data['rows']}"

        filtered = [r for r in data["rows"] if r["segment"] == "Filtered"]
        assert int(filtered[0]["avg_time_on_page_ms"]) == 35000

        non_filtered = [r for r in data["rows"] if r["segment"] == "Non Filtered"]
        assert int(non_filtered[0]["avg_time_on_page_ms"]) == 20000

    @pytest.mark.asyncio
    async def test_v_p1_ttd_seg(self):
        data = await _get_dashboard("v_p1_ttd_seg")
        segments = {r["segment"] for r in data["rows"]}
        assert "Filtered" in segments, f"Filtered 세그먼트 없음: {data['rows']}"
        assert "Non Filtered" in segments, f"Non Filtered 세그먼트 없음: {data['rows']}"

        filtered = [r for r in data["rows"] if r["segment"] == "Filtered"]
        assert int(filtered[0]["avg_ttd_ms"]) == 10000

        non_filtered = [r for r in data["rows"] if r["segment"] == "Non Filtered"]
        assert int(non_filtered[0]["avg_ttd_ms"]) == 8000


# ============================================================
# 검증 3 — P2 View 6종 (HTTP API)
# ============================================================

class TestE2EP2Dashboard:

    @pytest.mark.asyncio
    async def test_v_p2_section_reach(self):
        data = await _get_dashboard("v_p2_section_reach")
        reached = {r["section_name"]: float(r["reach_rate"]) for r in data["rows"] if r["section_name"]}
        # LEFT JOIN + GROUP BY section_name 구조: 각 섹션 그룹 내 reach_rate = 1.0
        for sec in ["hero", "basic_info", "lineup", "ticket_price"]:
            assert reached.get(sec, 0) == 1.0, f"{sec} reach_rate != 1.0, got {reached}"

    @pytest.mark.asyncio
    async def test_v_p2_blog_click(self):
        data = await _get_dashboard("v_p2_blog_click")
        assert float(data["rows"][0]["blog_click_rate"]) == 0.5

    @pytest.mark.asyncio
    async def test_v_p2_immediate_bounce(self):
        data = await _get_dashboard("v_p2_immediate_bounce")
        assert float(data["rows"][0]["immediate_bounce_rate"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p2_review_position(self):
        data = await _get_dashboard("v_p2_review_position")
        assert len(data["rows"]) >= 1
        total_share = sum(float(r["click_share"]) for r in data["rows"])
        assert abs(total_share - 1.0) < 0.01

    @pytest.mark.asyncio
    async def test_v_p2_blog_return(self):
        data = await _get_dashboard("v_p2_blog_return")
        assert float(data["rows"][0]["return_rate"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p2_share(self):
        data = await _get_dashboard("v_p2_share")
        assert float(data["rows"][0]["share_rate"]) == 0.0


# ============================================================
# 검증 4 — P3 View 5종 (HTTP API)
# ============================================================

class TestE2EP3Dashboard:

    @pytest.mark.asyncio
    async def test_v_p3_conversion(self):
        data = await _get_dashboard("v_p3_conversion")
        assert float(data["rows"][0]["p3_rate"]) == 0.5

    @pytest.mark.asyncio
    async def test_v_p3_review_to_ticket(self):
        data = await _get_dashboard("v_p3_review_to_ticket")
        assert float(data["rows"][0]["review_to_ticket_rate"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p3_no_review_ticket(self):
        data = await _get_dashboard("v_p3_no_review_ticket")
        if data["rows"]:
            val = data["rows"][0].get("no_review_ticket_rate")
            assert val is None or float(val) == 0.0

    @pytest.mark.asyncio
    async def test_v_p3_review_count_conv(self):
        data = await _get_dashboard("v_p3_review_count_conv")
        # review_count=1인 세션 (User A) → 전환율 1.0
        matched = [r for r in data["rows"] if int(r["review_count"]) == 1]
        assert len(matched) >= 1
        assert float(matched[0]["conversion_rate"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p3_section_x_ticket(self):
        data = await _get_dashboard("v_p3_section_x_ticket")
        reached = {r["section_name"]: r for r in data["rows"]}
        for sec in ["hero", "basic_info", "lineup", "ticket_price"]:
            assert sec in reached, f"{sec} not in v_p3_section_x_ticket"
            assert float(reached[sec]["reached_ticket_rate"]) > 0


# ============================================================
# 검증 5 — P4 쿼리 4종 (HTTP API + created_at 백데이팅)
# ============================================================

class TestE2EP4Dashboard:

    @pytest.mark.asyncio
    async def test_v_p4_intent_users(self):
        data = await _get_dashboard("v_p4_intent_users", date_from=None, date_to=TODAY)
        assert isinstance(data["rows"], list)
        anon_ids = [r["anonymous_id"] for r in data["rows"]]
        assert P4_ANON_ID in anon_ids, f"P4 intent user {P4_ANON_ID} not found in {anon_ids}"

    @pytest.mark.asyncio
    async def test_v_p4_reuse_broad(self):
        data = await _get_dashboard("v_p4_reuse_broad", date_from=None, date_to=TODAY)
        assert len(data["rows"]) >= 1
        row = data["rows"][0]
        assert int(row["intent_users"]) >= 1
        assert int(row["reuse_users_broad"]) >= 1

    @pytest.mark.asyncio
    async def test_v_p4_reuse_strict(self):
        data = await _get_dashboard("v_p4_reuse_strict", date_from=None, date_to=TODAY)
        assert len(data["rows"]) >= 1
        row = data["rows"][0]
        assert int(row["intent_users"]) >= 1
        assert int(row["reuse_users_strict"]) >= 1

    @pytest.mark.asyncio
    async def test_v_p4_conversion(self):
        data = await _get_dashboard("v_p4_conversion", date_from=None, date_to=TODAY)
        assert len(data["rows"]) >= 1
        row = data["rows"][0]
        assert "p4_broad_rate" in row
        assert "p4_strict_rate" in row
        assert float(row["p4_broad_rate"]) > 0
        assert float(row["p4_strict_rate"]) > 0
