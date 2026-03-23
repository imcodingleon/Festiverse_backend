from datetime import date

from app.domains.event_log.application.port.dashboard_query_port import DashboardQueryPort

VALID_VIEW_NAMES = frozenset([
    "v_p1_pv", "v_p1_fsr", "v_p1_far", "v_p1_dcr",
    "v_p1_tft", "v_p1_tfa", "v_p1_ttd", "v_p1_time_on_page",
    "v_p1_fuc", "v_p1_rer", "v_p1_afa", "v_p1_sur", "v_p1_scr",
    "v_p1_time_on_page_seg", "v_p1_ttd_seg",
    "v_p2_section_reach", "v_p2_blog_click", "v_p2_immediate_bounce",
    "v_p2_review_position", "v_p2_blog_return", "v_p2_share",
    "v_p3_conversion", "v_p3_review_to_ticket", "v_p3_no_review_ticket",
    "v_p3_review_count_conv", "v_p3_section_x_ticket",
])

P4_QUERY_NAMES = frozenset([
    "v_p4_intent_users", "v_p4_reuse_broad",
    "v_p4_reuse_strict", "v_p4_conversion",
])


class GetDashboardUseCase:

    def __init__(self, query_port: DashboardQueryPort) -> None:
        self._query_port = query_port

    async def execute(
        self,
        view_name: str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict]:
        if view_name in VALID_VIEW_NAMES:
            return await self._query_port.query_view(view_name, date_from, date_to)

        if view_name in P4_QUERY_NAMES:
            report_date = date_to or date.today()
            return await self._execute_p4(view_name, report_date)

        raise ValueError(f"Unknown view: {view_name}")

    async def _execute_p4(self, view_name: str, report_date: date) -> list[dict]:
        if view_name == "v_p4_intent_users":
            return await self._query_port.query_p4_intent_users(report_date)
        if view_name == "v_p4_reuse_broad":
            result = await self._query_port.query_p4_reuse_broad(report_date)
            return [result]
        if view_name == "v_p4_reuse_strict":
            result = await self._query_port.query_p4_reuse_strict(report_date)
            return [result]
        if view_name == "v_p4_conversion":
            result = await self._query_port.query_p4_conversion(report_date)
            return [result]
        raise ValueError(f"Unknown P4 query: {view_name}")
