import apache_beam as beam
from dramatiq import set_broker, set_encoder
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from dramatiq.brokers.redis import RedisBroker

from beam_dramatiq_runner import DramatiqPipelineRunner
from beam_dramatiq_runner.encoder import DillEncoder
from beam_dramatiq_runner.logger import get_runner_logger

broker = RedisBroker(url="redis://localhost:6379/0")
set_broker(broker)
set_encoder(DillEncoder())

backend = RedisBackend(client=broker.client)
broker.add_middleware(Results(backend=backend, store_results=True, result_ttl=10000))


logger = get_runner_logger(__name__)


def run():
    options = beam.pipeline.PipelineOptions()
    with beam.Pipeline(options=options, runner=DramatiqPipelineRunner()) as pipeline:
        # test_collection = pipeline
        #     | "Create items" >> beam.Create([1, 2, 3])

        def extract_age(element):
            return element["Age"]

        age_sum = (
            pipeline
            # | "Read CSV" >> beam.io.ReadFromText("data/sample_humans.csv")
            # | "Parse CSV" >> beam.ParDo(ParseCSV())
            | "Create items" >> beam.Create([{"Age": 21}, {"Age": 39}])
            | "Extract Age" >> beam.Map(extract_age)
            | "Sum Ages" >> beam.CombineGlobally(sum)
        )

        age_sum | "Print Result" >> beam.Map(print)


if __name__ == "__main__":
    run()
