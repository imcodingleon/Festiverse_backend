import logging
import re
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

SQL_FILE = Path(__file__).parent / "create_views.sql"

_CREATE_VIEW_RE = re.compile(
    r"(CREATE\s+OR\s+REPLACE\s+VIEW\s+.+?;)",
    re.DOTALL | re.IGNORECASE,
)


async def create_dashboard_views(engine: AsyncEngine) -> None:
    """create_views.sql에 정의된 모든 SQL View를 생성(또는 교체)한다."""
    sql_content = SQL_FILE.read_text(encoding="utf-8")

    statements = _CREATE_VIEW_RE.findall(sql_content)

    created = 0
    async with engine.begin() as conn:
        for stmt in statements:
            stmt_clean = stmt.rstrip(";").strip()
            if not stmt_clean:
                continue
            try:
                await conn.execute(text(stmt_clean))
                created += 1
            except Exception as exc:
                logger.warning("View 생성 실패: %s — %s", stmt_clean[:60], exc)

    logger.info("대시보드 View %d개 생성 완료", created)
