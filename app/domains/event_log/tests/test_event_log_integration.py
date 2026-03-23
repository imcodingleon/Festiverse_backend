"""
event_log 통합 테스트 — POST /api/events API + 30종 SQL View 검증.
MySQL Docker 컨테이너 실행 필수.
"""
import uuid
from datetime import date

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.domains.event_log.tests.conftest import (
    SAMPLE_DATE,
    SAMPLE_SESSION_ID,
    new_engine,
)

REPORT_DATE = SAMPLE_DATE.strftime("%Y-%m-%d")


async def _query_view(view_name: str) -> list[dict]:
    engine = new_engine()
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            result = await sess.execute(
                text(f"SELECT * FROM {view_name} WHERE report_date = :rd"),
                {"rd": REPORT_DATE},
            )
            columns = list(result.keys())
            return [dict(zip(columns, r)) for r in result.fetchall()]
    finally:
        await engine.dispose()


async def _query_view_no_filter(view_name: str) -> list[dict]:
    """report_date 컬럼이 없는 View 또는 전체 조회용."""
    engine = new_engine()
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as sess:
            result = await sess.execute(text(f"SELECT * FROM {view_name}"))
            columns = list(result.keys())
            return [dict(zip(columns, r)) for r in result.fetchall()]
    finally:
        await engine.dispose()


# ============================================================
# 검증 1 — POST /api/events API
# ============================================================

class TestPostEventsAPI:

    @pytest.mark.asyncio
    async def test_valid_payload_returns_201(self):
        from app.main import app
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            payload = {
                "id": str(uuid.uuid4()),
                "anonymous_id": "anon-api-test",
                "session_id": "sess-api-test",
                "event_type": "search_page_entered",
                "event_data": {},
                "page_url": "/",
                "device_type": "desktop",
            }
            resp = await client.post("/api/events", json=payload)
            assert resp.status_code == 201
            body = resp.json()
            assert body["id"] == payload["id"]
            assert body["status"] == "ok"

    @pytest.mark.asyncio
    async def test_event_data_json_stored_and_queryable(self):
        """event_data JSON이 정상 저장되어 ->>'$.key'로 조회 가능한지 확인."""
        engine = new_engine()
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                row = await sess.execute(
                    text("""
                        SELECT event_data->>'$.filter_type' AS ft
                        FROM event_logs
                        WHERE session_id = :sid AND event_type = 'filter_option_toggled'
                    """),
                    {"sid": SAMPLE_SESSION_ID},
                )
                result = row.fetchone()
                assert result is not None
                assert result[0] == "region"
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_missing_required_field_returns_422(self):
        from app.main import app
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            payload = {"id": str(uuid.uuid4()), "event_type": "search_page_entered"}
            resp = await client.post("/api/events", json=payload)
            assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_created_at_not_required_in_payload(self):
        """FE는 created_at을 보내지 않는다 — DTO에 필수 필드가 아님을 확인.
        Request DTO에 created_at이 없는지 코드 수준에서 검증."""
        from app.domains.event_log.application.request.create_event_log_request import CreateEventLogRequest
        import pydantic

        fields = CreateEventLogRequest.model_fields
        assert "created_at" not in fields, "created_at must NOT be in CreateEventLogRequest"

        req = CreateEventLogRequest(
            id=str(uuid.uuid4()),
            anonymous_id="anon-no-ts",
            session_id="sess-no-ts",
            event_type="app_session_started",
            event_data={"is_return_user": False},
            page_url="/",
            device_type="desktop",
        )
        assert req.id is not None


# ============================================================
# 검증 2 — 샘플 시나리오 데이터 확인
# ============================================================

class TestSampleScenario:

    @pytest.mark.asyncio
    async def test_sample_data_inserted(self):
        engine = new_engine()
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                row = await sess.execute(
                    text("SELECT COUNT(*) FROM event_logs WHERE session_id = :sid"),
                    {"sid": SAMPLE_SESSION_ID},
                )
                count = row.scalar()
                assert count >= 14, f"Expected >=14 events, got {count}"
        finally:
            await engine.dispose()


# ============================================================
# 검증 3 — P1 View 15종
# ============================================================

class TestP1Views:

    @pytest.mark.asyncio
    async def test_v_p1_pv(self):
        rows = await _query_view("v_p1_pv")
        assert len(rows) >= 1
        assert rows[0]["pv"] == 1

    @pytest.mark.asyncio
    async def test_v_p1_fsr(self):
        rows = await _query_view("v_p1_fsr")
        assert len(rows) >= 1
        assert float(rows[0]["fsr"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p1_far(self):
        rows = await _query_view("v_p1_far")
        assert len(rows) >= 1
        assert float(rows[0]["far"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p1_dcr(self):
        rows = await _query_view("v_p1_dcr")
        assert len(rows) >= 1
        assert float(rows[0]["dcr"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p1_tft(self):
        rows = await _query_view("v_p1_tft")
        assert len(rows) >= 1
        assert int(rows[0]["avg_tft_ms"]) == 3000

    @pytest.mark.asyncio
    async def test_v_p1_tfa(self):
        rows = await _query_view("v_p1_tfa")
        assert len(rows) >= 1
        assert int(rows[0]["avg_tfa_ms"]) == 8000

    @pytest.mark.asyncio
    async def test_v_p1_ttd(self):
        rows = await _query_view("v_p1_ttd")
        assert len(rows) >= 1
        assert int(rows[0]["avg_ttd_ms"]) == 12000

    @pytest.mark.asyncio
    async def test_v_p1_time_on_page(self):
        rows = await _query_view("v_p1_time_on_page")
        assert len(rows) >= 1
        val = int(rows[0]["avg_time_on_page_ms"])
        assert val == 30000, f"Expected 30000, got {val}"

    @pytest.mark.asyncio
    async def test_v_p1_fuc(self):
        rows = await _query_view("v_p1_fuc")
        assert len(rows) >= 1
        assert float(rows[0]["avg_fuc"]) >= 1.0

    @pytest.mark.asyncio
    async def test_v_p1_rer(self):
        rows = await _query_view("v_p1_rer")
        assert len(rows) >= 1
        assert float(rows[0]["rer"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p1_afa(self):
        rows = await _query_view("v_p1_afa")
        assert len(rows) >= 1
        assert float(rows[0]["avg_afa"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p1_sur(self):
        rows = await _query_view("v_p1_sur")
        assert len(rows) >= 1
        assert float(rows[0]["sur"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p1_scr(self):
        rows = await _query_view("v_p1_scr")
        assert len(rows) >= 1
        assert float(rows[0]["scr"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p1_time_on_page_seg(self):
        rows = await _query_view("v_p1_time_on_page_seg")
        filtered = [r for r in rows if r["segment"] == "Filtered"]
        assert len(filtered) >= 1, f"No Filtered segment found in {rows}"
        assert int(filtered[0]["avg_time_on_page_ms"]) == 30000

    @pytest.mark.asyncio
    async def test_v_p1_ttd_seg(self):
        rows = await _query_view("v_p1_ttd_seg")
        filtered = [r for r in rows if r["segment"] == "Filtered"]
        assert len(filtered) >= 1, f"No Filtered segment found in {rows}"
        assert int(filtered[0]["avg_ttd_ms"]) == 12000


# ============================================================
# 검증 4 — P2 View 6종
# ============================================================

class TestP2Views:

    @pytest.mark.asyncio
    async def test_v_p2_section_reach(self):
        rows = await _query_view("v_p2_section_reach")
        reached = {r["section_name"]: float(r["reach_rate"]) for r in rows if r["section_name"]}
        for sec in ["hero", "basic_info", "lineup", "ticket_price"]:
            assert reached.get(sec, 0) == 1.0, f"{sec} reach_rate != 1.0, got {reached}"

    @pytest.mark.asyncio
    async def test_v_p2_blog_click(self):
        rows = await _query_view("v_p2_blog_click")
        assert len(rows) >= 1
        assert float(rows[0]["blog_click_rate"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p2_immediate_bounce(self):
        rows = await _query_view("v_p2_immediate_bounce")
        assert len(rows) >= 1
        assert float(rows[0]["immediate_bounce_rate"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p2_review_position(self):
        rows = await _query_view("v_p2_review_position")
        assert len(rows) >= 1
        total_share = sum(float(r["click_share"]) for r in rows)
        assert abs(total_share - 1.0) < 0.01

    @pytest.mark.asyncio
    async def test_v_p2_blog_return(self):
        rows = await _query_view("v_p2_blog_return")
        assert len(rows) >= 1
        assert float(rows[0]["return_rate"]) == 0.0

    @pytest.mark.asyncio
    async def test_v_p2_share(self):
        rows = await _query_view("v_p2_share")
        assert len(rows) >= 1
        assert float(rows[0]["share_rate"]) == 0.0


# ============================================================
# 검증 5 — P3 View 5종
# ============================================================

class TestP3Views:

    @pytest.mark.asyncio
    async def test_v_p3_conversion(self):
        rows = await _query_view("v_p3_conversion")
        assert len(rows) >= 1
        assert float(rows[0]["p3_rate"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p3_review_to_ticket(self):
        rows = await _query_view("v_p3_review_to_ticket")
        assert len(rows) >= 1
        assert float(rows[0]["review_to_ticket_rate"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p3_no_review_ticket(self):
        rows = await _query_view("v_p3_no_review_ticket")
        if rows:
            val = rows[0].get("no_review_ticket_rate")
            assert val is None or float(val) == 0.0

    @pytest.mark.asyncio
    async def test_v_p3_review_count_conv(self):
        rows = await _query_view("v_p3_review_count_conv")
        matched = [r for r in rows if int(r["review_count"]) == 1]
        assert len(matched) >= 1
        assert float(matched[0]["conversion_rate"]) == 1.0

    @pytest.mark.asyncio
    async def test_v_p3_section_x_ticket(self):
        rows = await _query_view("v_p3_section_x_ticket")
        reached = {r["section_name"]: r for r in rows}
        for sec in ["hero", "basic_info", "lineup", "ticket_price"]:
            assert sec in reached, f"{sec} not in v_p3_section_x_ticket"
            assert float(reached[sec]["reached_ticket_rate"]) > 0


# ============================================================
# 검증 6 — P4 파라미터화 쿼리 4종
# ============================================================

class TestP4Queries:

    @pytest.mark.asyncio
    async def test_v_p4_intent_users(self):
        from app.domains.event_log.adapter.outbound.persistence.dashboard_query_adapter import DashboardQueryAdapter
        engine = new_engine()
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                adapter = DashboardQueryAdapter(sess)
                rows = await adapter.query_p4_intent_users(date.today())
                assert isinstance(rows, list)
                if rows:
                    assert "anonymous_id" in rows[0]
                    assert "anchor_time" in rows[0]
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_v_p4_reuse_broad(self):
        from app.domains.event_log.adapter.outbound.persistence.dashboard_query_adapter import DashboardQueryAdapter
        engine = new_engine()
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                adapter = DashboardQueryAdapter(sess)
                result = await adapter.query_p4_reuse_broad(date.today())
                assert "report_date" in result
                assert "reuse_users_broad" in result
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_v_p4_reuse_strict(self):
        from app.domains.event_log.adapter.outbound.persistence.dashboard_query_adapter import DashboardQueryAdapter
        engine = new_engine()
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                adapter = DashboardQueryAdapter(sess)
                result = await adapter.query_p4_reuse_strict(date.today())
                assert "report_date" in result
                assert "reuse_users_strict" in result
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_v_p4_conversion(self):
        from app.domains.event_log.adapter.outbound.persistence.dashboard_query_adapter import DashboardQueryAdapter
        engine = new_engine()
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as sess:
                adapter = DashboardQueryAdapter(sess)
                result = await adapter.query_p4_conversion(date.today())
                assert "p4_broad_rate" in result
                assert "p4_strict_rate" in result
        finally:
            await engine.dispose()
