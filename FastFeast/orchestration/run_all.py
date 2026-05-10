import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from FastFeast.orchestration.parallel_process import process_dimension_phase
from FastFeast.support.logger import pipeline as log


def run_stream_once(run_date: str) -> int:
    cmd = [
        sys.executable,
        "-m",
        "FastFeast.pipeline.ingestion.Micro_batch_File_Watcher",
        "--once",
        "--date",
        run_date,
    ]
    log.info("PHASE 2 START  stream facts", date=run_date)
    return subprocess.run(cmd, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="FastFeast strict two-phase orchestrator")
    parser.add_argument("--date", default=date.today().isoformat(), help="Processing date in YYYY-MM-DD format")
    args = parser.parse_args()

    run_date = args.date

    log.info("PHASE 1 START  dimensions", date=run_date)
    dimensions_ok = process_dimension_phase(date_str=run_date)
    if not dimensions_ok:
        log.error("PHASE 1 FAILED  aborting facts phase", date=run_date)
        return 1

    log.info("PHASE BARRIER reached  dimensions complete, starting facts", date=run_date)
    stream_rc = run_stream_once(run_date)

    if stream_rc != 0:
        log.error("PHASE 2 FAILED  stream processor returned non-zero", return_code=stream_rc, date=run_date)
        return stream_rc

    log.info("ORCHESTRATION SUCCESS  dimensions then facts", date=run_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())