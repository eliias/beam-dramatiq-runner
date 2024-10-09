import dill as pickle
from dramatiq import Encoder
from dramatiq.encoder import MessageData

from beam_dramatiq_runner.logger import get_runner_logger

logger = get_runner_logger(__name__)


class DillEncoder(Encoder):
    """Pickles messages with dill.

    Warning:
      This encoder is not secure against maliciously-constructed data.
      Use it at your own risk.
    """

    def encode(self, data: MessageData) -> bytes:
        return pickle.dumps(data)

    def decode(self, data: bytes) -> MessageData:
        return pickle.loads(data)
