from pathlib import Path
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import yaml

from pipeline.config.config import load as load_cfg

HERE        = Path(__file__).resolve().parent
PIPELINE    = HERE.parent
CONFIG_PATH = PIPELINE / "config" / "config.yaml"


class Column(BaseModel):
    name: str
    type: str
    pk: bool = False
    nullable: bool = True

    fk: Optional[Dict[str, Any]] = None
    expected_values: Optional[List[str]] = None
    range: Optional[Dict[str, float]] = None
    pii: Optional[bool] = None
    format: Optional[str] = None


class FileEntry(BaseModel):
    file_name: str
    columns: List[Column]

    class Config:
        extra = "allow"


class MetadataSettings(BaseModel):
    batch: List[FileEntry]
    stream: List[FileEntry]


def load() -> MetadataSettings:
    cfg = load_cfg(str(CONFIG_PATH))
    metadata_path = Path(cfg.paths.metadata_file).resolve()

    with open(metadata_path) as f:
        raw = yaml.safe_load(f)

    return MetadataSettings(**raw)


metadata_settings = load()