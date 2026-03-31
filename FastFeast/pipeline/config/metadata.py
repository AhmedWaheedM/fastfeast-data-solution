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

def load(path: str) -> Settings:
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    return from_dict(
        data_class=Settings,
        data=data,
    )


#Call load function to return data from yaml file
#import this directly in .py script
yaml_path = Path(os.getenv("FILES_METADATA_YAML", Path(__file__).parent / "files_metadata.yaml"))
metadata_settings = load(yaml_path)


# ------------------------------------------------------
# Lazy Config Loader
# ------------------------------------------------------
_config = None

def get_metadata():
    global _config
    if _config is None:
        _config = load(yaml_path)
    return _config