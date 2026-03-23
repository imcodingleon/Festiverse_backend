from abc import ABC, abstractmethod
from datetime import date


class DashboardQueryPort(ABC):
    """대시보드 SQL View / 파라미터화 쿼리 조회 포트."""

    @abstractmethod
    async def query_view(
        self,
        view_name: str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict]:
        """P1~P3 SQL View를 조회하여 dict 리스트로 반환."""
        ...

    @abstractmethod
    async def query_p4_intent_users(
        self,
        report_date: date,
    ) -> list[dict]:
        """P4 IntentUsers: report_date 기준 D-21~D-14 윈도우 내 ticket_button_clicked 사용자."""
        ...

    @abstractmethod
    async def query_p4_reuse_broad(
        self,
        report_date: date,
    ) -> dict:
        """P4 ReuseUsers (broad): IntentUsers 중 anchor_time+14일 내 app_session_started 존재."""
        ...

    @abstractmethod
    async def query_p4_reuse_strict(
        self,
        report_date: date,
    ) -> dict:
        """P4 ReuseUsers (strict): IntentUsers 중 anchor_time+14일 내 search/detail_page_entered 존재."""
        ...

    @abstractmethod
    async def query_p4_conversion(
        self,
        report_date: date,
    ) -> dict:
        """P4 전환율: broad/strict 비율."""
        ...
