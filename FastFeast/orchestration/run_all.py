import subprocess
import sys

from FastFeast.support.logger import pipeline as log


def run_stream():
    return subprocess.Popen([sys.executable, "-m", "FastFeast.pipeline.ingestion.Micro_batch_File_Watcher"])


def run_batch():
    return subprocess.Popen([sys.executable, "-m", "FastFeast.orchestration.parallel_process"])


def main():
    log.info("Starting BOTH pipelines as separate processes")

    stream_proc = run_stream()
    batch_proc = run_batch()

    stream_proc.wait()
    batch_proc.wait()

    log.info("Both pipelines finished")


if __name__ == "__main__":
    main()