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