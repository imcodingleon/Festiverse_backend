from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.event_log.application.port.dashboard_query_port import DashboardQueryPort
from app.domains.event_log.application.usecase.get_dashboard_usecase import VALID_VIEW_NAMES


def _rows_to_dicts(result) -> list[dict]:
    columns = list(result.keys())
    return [dict(zip(columns, row)) for row in result.fetchall()]


class DashboardQueryAdapter(DashboardQueryPort):

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def query_view(
        self,
        view_name: str,
        date_from: date | None = None,
        date_to: date | None = None,
    ) -> list[dict]:
        if view_name not in VALID_VIEW_NAMES:
            raise ValueError(f"Invalid view name: {view_name}")

        where_clauses = []
        params: dict = {}

        if date_from:
            where_clauses.append("report_date >= :date_from")
            params["date_from"] = date_from.isoformat()
        if date_to:
            where_clauses.append("report_date <= :date_to")
            params["date_to"] = date_to.isoformat()

        where_sql = ""
        if where_clauses:
            where_sql = " WHERE " + " AND ".join(where_clauses)

        sql = f"SELECT * FROM {view_name}{where_sql} ORDER BY report_date"
        result = await self._session.execute(text(sql), params)
        return _rows_to_dicts(result)

    # ------------------------------------------------------------------
    # P4 파라미터화 쿼리 — View 대신 동적 날짜 기반 raw SQL 사용
    # 집계 규칙: Intent 윈도우 = report_date-21 ~ report_date-14 (7일간)
    #            Reuse 판정 = 각 user의 anchor_time ~ anchor_time + 14일
    # ------------------------------------------------------------------

    async def query_p4_intent_users(self, report_date: date) -> list[dict]:
        window_start = report_date - timedelta(days=21)
        window_end = report_date - timedelta(days=14)

        sql = text("""
            SELECT
                anonymous_id,
                MAX(created_at) AS anchor_time
            FROM event_logs
            WHERE event_type = 'ticket_button_clicked'
              AND DATE(created_at) BETWEEN :window_start AND :window_end
            GROUP BY anonymous_id
        """)
        result = await self._session.execute(sql, {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        })
        rows = _rows_to_dicts(result)
        return [{"report_date": report_date.isoformat(), **r} for r in rows]

    async def query_p4_reuse_broad(self, report_date: date) -> dict:
        window_start = report_date - timedelta(days=21)
        window_end = report_date - timedelta(days=14)

        sql = text("""
            WITH intent AS (
                SELECT
                    anonymous_id,
                    MAX(created_at) AS anchor_time
                FROM event_logs
                WHERE event_type = 'ticket_button_clicked'
                  AND DATE(created_at) BETWEEN :window_start AND :window_end
                GROUP BY anonymous_id
            )
            SELECT
                COUNT(DISTINCT i.anonymous_id) AS intent_users,
                COUNT(DISTINCT CASE
                    WHEN e.id IS NOT NULL THEN i.anonymous_id
                END) AS reuse_users_broad
            FROM intent i
            LEFT JOIN event_logs e
              ON e.anonymous_id = i.anonymous_id
             AND e.event_type = 'app_session_started'
             AND e.created_at > i.anchor_time
             AND e.created_at <= DATE_ADD(i.anchor_time, INTERVAL 14 DAY)
        """)
        result = await self._session.execute(sql, {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        })
        rows = _rows_to_dicts(result)
        row = rows[0] if rows else {"intent_users": 0, "reuse_users_broad": 0}
        return {"report_date": report_date.isoformat(), **row}

    async def query_p4_reuse_strict(self, report_date: date) -> dict:
        window_start = report_date - timedelta(days=21)
        window_end = report_date - timedelta(days=14)

        sql = text("""
            WITH intent AS (
                SELECT
                    anonymous_id,
                    MAX(created_at) AS anchor_time
                FROM event_logs
                WHERE event_type = 'ticket_button_clicked'
                  AND DATE(created_at) BETWEEN :window_start AND :window_end
                GROUP BY anonymous_id
            )
            SELECT
                COUNT(DISTINCT i.anonymous_id) AS intent_users,
                COUNT(DISTINCT CASE
                    WHEN e.id IS NOT NULL THEN i.anonymous_id
                END) AS reuse_users_strict
            FROM intent i
            LEFT JOIN event_logs e
              ON e.anonymous_id = i.anonymous_id
             AND e.event_type IN ('search_page_entered', 'detail_page_entered')
             AND e.created_at > i.anchor_time
             AND e.created_at <= DATE_ADD(i.anchor_time, INTERVAL 14 DAY)
        """)
        result = await self._session.execute(sql, {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        })
        rows = _rows_to_dicts(result)
        row = rows[0] if rows else {"intent_users": 0, "reuse_users_strict": 0}
        return {"report_date": report_date.isoformat(), **row}

    async def query_p4_conversion(self, report_date: date) -> dict:
        window_start = report_date - timedelta(days=21)
        window_end = report_date - timedelta(days=14)

        sql = text("""
            WITH intent AS (
                SELECT
                    anonymous_id,
                    MAX(created_at) AS anchor_time
                FROM event_logs
                WHERE event_type = 'ticket_button_clicked'
                  AND DATE(created_at) BETWEEN :window_start AND :window_end
                GROUP BY anonymous_id
            ),
            reuse AS (
                SELECT
                    COUNT(DISTINCT i.anonymous_id) AS intent_users,
                    COUNT(DISTINCT CASE
                        WHEN eb.id IS NOT NULL THEN i.anonymous_id
                    END) AS reuse_broad,
                    COUNT(DISTINCT CASE
                        WHEN es.id IS NOT NULL THEN i.anonymous_id
                    END) AS reuse_strict
                FROM intent i
                LEFT JOIN event_logs eb
                  ON eb.anonymous_id = i.anonymous_id
                 AND eb.event_type = 'app_session_started'
                 AND eb.created_at > i.anchor_time
                 AND eb.created_at <= DATE_ADD(i.anchor_time, INTERVAL 14 DAY)
                LEFT JOIN event_logs es
                  ON es.anonymous_id = i.anonymous_id
                 AND es.event_type IN ('search_page_entered', 'detail_page_entered')
                 AND es.created_at > i.anchor_time
                 AND es.created_at <= DATE_ADD(i.anchor_time, INTERVAL 14 DAY)
            )
            SELECT
                intent_users,
                reuse_broad,
                reuse_strict,
                CASE WHEN intent_users > 0
                     THEN ROUND(reuse_broad / intent_users, 4)
                     ELSE 0 END AS p4_broad_rate,
                CASE WHEN intent_users > 0
                     THEN ROUND(reuse_strict / intent_users, 4)
                     ELSE 0 END AS p4_strict_rate
            FROM reuse
        """)
        result = await self._session.execute(sql, {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        })
        rows = _rows_to_dicts(result)
        row = rows[0] if rows else {
            "intent_users": 0, "reuse_broad": 0, "reuse_strict": 0,
            "p4_broad_rate": 0, "p4_strict_rate": 0,
        }
        return {"report_date": report_date.isoformat(), **row}
