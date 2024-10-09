import logging
from structlog import getLogger

def get_runner_logger(name: str) -> logging.Logger:
    return getLogger(f"beam_dramatiq_runner.{name}")
