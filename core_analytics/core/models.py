from typing import Dict
from pydantic import BaseModel, Field, ConfigDict
from azure.monitor.query import LogsQueryResult


class ProcessData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    user_count_results: Dict[str, LogsQueryResult] = Field(default_factory=dict)
    stroke_count_results: Dict[str, LogsQueryResult] = Field(default_factory=dict)
    unknown_results: Dict[str, LogsQueryResult] = Field(default_factory=dict)