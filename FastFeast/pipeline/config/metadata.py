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