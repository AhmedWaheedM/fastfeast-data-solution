import hashlib
import time
from FastFeast.pipeline.config.config import config_settings


def get_file_hash(file_path):
    sha = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha.update(chunk)
    return sha.hexdigest()


def wait_for_file(file_path, timeout_sec=config_settings.pipeline.time_wait):
    path = Path(file_path)
    start = time.time()
    while not path.exists():
        if time.time() - start > timeout_sec:
            return False
        time.sleep(1)
    return True