from typing import Callable

import dramatiq
from dramatiq import set_broker, set_encoder
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from dramatiq.brokers.redis import RedisBroker

from beam_dramatiq_runner.encoder import DillEncoder
from .logger import get_runner_logger

from .types import E, K

broker = RedisBroker(url="redis://localhost:6379/0")
set_broker(broker)
set_encoder(DillEncoder())

backend = RedisBackend(client=broker.client)
broker.add_middleware(Results(backend=backend, store_results=True, result_ttl=10000))


logger = get_runner_logger(__name__)


@dramatiq.actor
def dramatiq_map(func: Callable[..., E], chunk: list[E], *args, **kwargs) -> list[E]:
    logger.info("map", func=func, chunk=chunk)
    if chunk is None:
        return []

    if hasattr(func, 'process'):
        return list(func.process(chunk))

    return list(map(func, chunk))


@dramatiq.actor
def dramatiq_filter(
    predicate: Callable[..., E], chunk: list[E], *args, **kwargs
) -> list[E]:
    if chunk is None:
        return []

    return list(filter(predicate, chunk))


@dramatiq.actor
def dramatiq_groupby(
    grouper: Callable[..., E], chunk: list[E], *args, **kwargs
) -> dict[K, E]:
    if chunk is None:
        return {}

    result = {}
    for item in chunk:
        key = grouper(item)
        if key not in result:
            result[key] = []
        result[key].append(item)

    return result
