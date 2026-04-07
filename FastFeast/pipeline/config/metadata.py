<<<<<<< HEAD
from pathlib import Path
from pydantic import BaseModel
import yaml

from pipeline.config.config import load as load_cfg

HERE       = Path(__file__).resolve().parent
PIPELINE   = HERE.parent
CONFIG_PATH = PIPELINE / "config" / "config.yaml"

class FileEntry(BaseModel):
    file_name: str
    class Config:
        extra = "allow"  

class MetadataSettings(BaseModel):
    batch:  list[FileEntry]
    stream: list[FileEntry]

def load() -> MetadataSettings:
    cfg      = load_cfg(str(CONFIG_PATH))
    metadata_path = Path(cfg.paths.metadata_file).resolve()
    with open(metadata_path) as f:
        raw = yaml.safeload(f)
    return MetadataSettings(**raw)

metadata_settings = load()
=======
from dataclasses import dataclass
import yaml
from typing import List
import os
from pathlib import Path
from dacite import from_dict
from typing import Any, Dict, Optional



@dataclass
class Column:
    name: str
    type: str
    pk: bool = False
    nullable: bool = True

    # optional extra metadata
    fk: Optional[Dict[str, Any]] = None
    expected_values: Optional[List[str]] = None
    range: Optional[Dict[str, float]] = None
    pii: Optional[bool] = None
    format: Optional[str] = None

@dataclass
class FileMeta:
    file_name: str
    columns: List[Column]

@dataclass
class Settings:
    batch: List[FileMeta]
    stream: List[FileMeta]


def load(path: str) -> Settings:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    return from_dict(
        data_class=Settings,
        data=data,
    )
>>>>>>> origin/dev
