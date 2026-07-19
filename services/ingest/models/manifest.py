from datetime import datetime
from typing import List

from pydantic import BaseModel, field_validator


class ManifestFile(BaseModel):
    hash: str
    size: int
    url: str
    basename: str
    format: str


class Manifest(BaseModel):
    status: str
    dt: datetime
    files: List[ManifestFile]

    @field_validator("dt", mode="before")
    @classmethod
    def parse_dt(cls, v):
        return datetime.strptime(v, "%Y%m%d%H%M%S")
