from datetime import datetime, timedelta

from app.domains.performance.application.port.kopis_api_port import KopisApiPort
from app.domains.performance.application.port.performance_repository_port import PerformanceRepositoryPort
from app.domains.performance.application.port.venue_repository_port import VenueRepositoryPort
from app.domains.performance.application.response.performance_detail_response import (
    DiscountInfoResponse,
    PerformanceDetailResponse,
    RelateResponse,
)
from app.domains.performance.application.response.venue_response import VenueResponse
from app.domains.ticket.application.port.ticket_repository_port import TicketRepositoryPort

CACHE_TTL = timedelta(hours=24)


class GetPerformanceDetailUseCase:

    def __init__(
        self,
        performance_repo: PerformanceRepositoryPort,
        venue_repo: VenueRepositoryPort,
        kopis_api: KopisApiPort,
        ticket_repo: TicketRepositoryPort | None = None,
    ) -> None:
        self._performance_repo = performance_repo
        self._venue_repo = venue_repo
        self._kopis_api = kopis_api
        self._ticket_repo = ticket_repo

    async def execute(self, mt20id: str) -> PerformanceDetailResponse | None:
        performance = await self._performance_repo.find_by_id(mt20id)

        # DB 캐시 miss이거나 24시간 경과시 KOPIS API 호출
        # NOTION_ 접두사 레코드는 KOPIS에 없으므로 API 호출 스킵
        now = datetime.utcnow()
        need_fetch = False
        is_notion = mt20id.startswith("NOTION_")
        if not is_notion:
            if performance is None or performance.updated_at is None:
                need_fetch = True
            else:
                updated = performance.updated_at.replace(tzinfo=None)
                if (now - updated) > CACHE_TTL:
                    need_fetch = True

        if need_fetch:
            fetched = await self._kopis_api.fetch_performance_detail(mt20id)
            if fetched is None:
                if performance is None:
                    return None
            else:
                fetched.updated_at = now
                await self._performance_repo.save(fetched)
                performance = fetched

        # 공연장 정보 조회
        venue_response: VenueResponse | None = None
        if performance.mt10id:
            venue = await self._venue_repo.find_by_id(performance.mt10id)
            if venue is None:
                venue = await self._kopis_api.fetch_venue_detail(performance.mt10id)
                if venue is not None:
                    await self._venue_repo.save(venue)

            if venue is not None:
                venue_response = VenueResponse(
                    mt10id=venue.mt10id,
                    fcltynm=venue.fcltynm,
                    seatscale=venue.seatscale,
                    telno=venue.telno,
                    relateurl=venue.relateurl,
                    adres=venue.adres,
                    la=venue.la,
                    lo=venue.lo,
                    parkinglot=venue.parkinglot,
                    restaurant=venue.restaurant,
                    cafe=venue.cafe,
                    store=venue.store,
                    disability=venue.disability,
                )

        # 할인 정보 수집
        discounts: list[DiscountInfoResponse] = []
        if self._ticket_repo:
            tickets = await self._ticket_repo.find_by_mt20id(performance.mt20id)
            for ticket in tickets:
                for p in ticket.prices:
                    if p.get("discounted", False):
                        discounts.append(DiscountInfoResponse(
                            seat_type=p.get("seat_type", ""),
                            price=p.get("price", 0),
                            vendor_name=ticket.vendor_name,
                        ))

        return PerformanceDetailResponse(
            mt20id=performance.mt20id,
            prfnm=performance.prfnm,
            prfpdfrom=performance.prfpdfrom,
            prfpdto=performance.prfpdto,
            fcltynm=performance.fcltynm,
            prfcast=performance.prfcast,
            prfcrew=performance.prfcrew,
            prfruntime=performance.prfruntime,
            prfage=performance.prfage,
            pcseguidance=performance.pcseguidance,
            poster=performance.poster,
            genrenm=performance.genrenm,
            prfstate=performance.prfstate,
            openrun=performance.openrun,
            styurls=performance.styurls,
            relates=[RelateResponse(name=r["name"], url=r["url"]) for r in performance.relates],
            dtguidance=performance.dtguidance,
            area=performance.area,
            sty=performance.sty,
            venue=venue_response,
            discounts=discounts,
        )
