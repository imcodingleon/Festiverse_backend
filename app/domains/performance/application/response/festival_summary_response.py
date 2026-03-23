from pydantic import BaseModel


class FestivalSummaryResponse(BaseModel):
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
    festival: str

    model_config = {"from_attributes": True}
