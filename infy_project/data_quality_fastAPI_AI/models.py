from pydantic import BaseModel
from typing import List, Optional


class DQReport(BaseModel):
    column: str
    status: str
    count: int
    samples: Optional[str]
