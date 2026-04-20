from pydantic import BaseModel


class MeasurementConfig(BaseModel):
    integration_time_s: float
    binwidth_ps: int = 500
    channel1: int = 1
    channel2: int = 2
    dark_count: int = 0
