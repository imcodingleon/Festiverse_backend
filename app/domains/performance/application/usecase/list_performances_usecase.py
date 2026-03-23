from app.domains.performance.application.port.performance_repository_port import PerformanceRepositoryPort
from app.domains.performance.application.request.list_performances_request import ListPerformancesRequest
from app.domains.performance.application.response.performance_summary_response import PerformanceSummaryResponse


class ListPerformancesUseCase:

    def __init__(self, performance_repo: PerformanceRepositoryPort) -> None:
        self._performance_repo = performance_repo

    async def execute(self, request: ListPerformancesRequest) -> list[PerformanceSummaryResponse]:
        performances = await self._performance_repo.find_all(
            stdate=request.stdate,
            eddate=request.eddate,
            genre=request.genre,
            region=request.region,
            keyword=request.keyword,
            state=request.state,
            page=request.page,
            size=request.size,
        )
        return [
            PerformanceSummaryResponse(
                mt20id=p.mt20id,
                prfnm=p.prfnm,
                prfpdfrom=p.prfpdfrom,
                prfpdto=p.prfpdto,
                fcltynm=p.fcltynm,
                poster=p.poster,
                genrenm=p.genrenm,
                prfstate=p.prfstate,
                area=p.area,
                dtguidance=p.dtguidance,
            )
            for p in performances
        ]
