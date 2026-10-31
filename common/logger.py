"""Logger for the collectors.

Rule: INFO is used ONLY for "started" / "stopped" (and "buffer: uploaded N file(s)"
when buffered data reaches Swift), everything else is ERROR.
So a log file is a list of start/stop times plus the errors in between.
Output: stdout (docker compose logs / kubectl logs) and, unless LOG_TO_FILE=false,
/logs/<name>.log. Timestamps in UTC.
"""
import logging
import os
import time

from common import config


def get_logger(name: str) -> logging.Logger:
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%SZ")
    fmt.converter = time.gmtime  # UTC, same as the data file names

    handlers = [logging.StreamHandler()]
    # In Kubernetes, logs are read with `kubectl logs`, so LOG_TO_FILE=false there.
    if os.getenv("LOG_TO_FILE", "true") == "true":
        config.LOG_DIR.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(config.LOG_DIR / f"{name}.log", encoding="utf-8"))

    for handler in handlers:
        handler.setFormatter(fmt)
        log.addHandler(handler)
    return log
