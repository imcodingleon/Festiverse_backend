from pydantic import BaseModel


class PerformanceSummaryResponse(BaseModel):
    mt20id: str
    prfnm: str
    prfpdfrom: str
    prfpdto: str
    fcltynm: str
    poster: str
    genrenm: str
    prfstate: str
    area: str
    dtguidance: str = ""

    model_config = {"from_attributes": True}
