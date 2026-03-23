from app.domains.performance.application.port.performance_repository_port import PerformanceRepositoryPort
from app.domains.performance.application.request.list_festivals_request import ListFestivalsRequest
from app.domains.performance.application.response.festival_summary_response import FestivalSummaryResponse


class ListFestivalsUseCase:

    def __init__(self, performance_repo: PerformanceRepositoryPort) -> None:
        self._performance_repo = performance_repo

    async def execute(self, request: ListFestivalsRequest) -> list[FestivalSummaryResponse]:
        performances = await self._performance_repo.find_all(
            stdate=request.stdate,
            eddate=request.eddate,
            genre=request.genre,
            keyword=request.keyword,
            festival="Y",
            page=request.page,
            size=request.size,
        )
        return [
            FestivalSummaryResponse(
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
                festival=p.festival,
            )
            for p in performances
        ]
