from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domains.performance.application.port.performance_repository_port import PerformanceRepositoryPort
from app.domains.performance.domain.entity.performance import Performance
from app.domains.performance.infrastructure.mapper.performance_mapper import PerformanceMapper
from app.domains.performance.infrastructure.orm.performance_model import PerformanceModel


class PerformanceRepository(PerformanceRepositoryPort):

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_all(
        self,
        stdate: str | None = None,
        eddate: str | None = None,
        genre: str | None = None,
        region: str | None = None,
        keyword: str | None = None,
        state: str | None = None,
        festival: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> list[Performance]:
        stmt = select(PerformanceModel)

        if stdate:
            stmt = stmt.where(PerformanceModel.prfpdto >= stdate)
        if eddate:
            stmt = stmt.where(PerformanceModel.prfpdfrom <= eddate)
        if genre:
            stmt = stmt.where(PerformanceModel.genrenm == genre)
        if region:
            stmt = stmt.where(PerformanceModel.area.contains(region))
        if keyword:
            stmt = stmt.where(PerformanceModel.prfnm.contains(keyword))
        if state:
            stmt = stmt.where(PerformanceModel.prfstate == state)
        if festival:
            stmt = stmt.where(PerformanceModel.festival == festival)

        stmt = stmt.order_by(PerformanceModel.prfpdfrom.desc())
        stmt = stmt.offset((page - 1) * size).limit(size)

        result = await self._session.execute(stmt)
        models = result.scalars().all()
        return [PerformanceMapper.to_entity(m) for m in models]

    async def find_by_id(self, mt20id: str) -> Performance | None:
        model = await self._session.get(PerformanceModel, mt20id)
        if model is None:
            return None
        return PerformanceMapper.to_entity(model)

    async def save(self, performance: Performance) -> None:
        model = PerformanceMapper.to_model(performance)
        await self._session.merge(model)
        await self._session.commit()

    async def save_many(self, performances: list[Performance]) -> None:
        for p in performances:
            existing = await self._session.get(PerformanceModel, p.mt20id)
            if existing and existing.mt20id.startswith("NOTION_"):
                continue
            model = PerformanceMapper.to_model(p)
            await self._session.merge(model)
        await self._session.commit()
