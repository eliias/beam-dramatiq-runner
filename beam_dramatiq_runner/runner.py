from dataclasses import field, dataclass

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.runners.runner import PipelineResult, PipelineState
from apache_beam.pipeline import Pipeline, PipelineVisitor, AppliedPTransform
from apache_beam.pvalue import PBegin, PValue

from .logger import get_runner_logger
from .overrides import dramatiq_overrides
from .transforms import TRANSLATIONS, NoOp, DramatiqCollection

logger = get_runner_logger(__name__)


class DramatiqConsumerTrackingPipelineVisitor(PipelineVisitor):
    """Visitor for extracting value-consumer relations from the graph.

    Tracks the AppliedPTransforms that consume each PValue in the Pipeline. This
    is used to schedule consuming PTransforms to consume input after the upstream
    transform has produced and committed output.
    """

    def __init__(self):
        self.value_to_consumers: dict[PValue, set[AppliedPTransform]] = {}
        self.root_transforms: set[AppliedPTransform] = set()
        self.step_names: dict[AppliedPTransform, str] = {}

        self._num_transforms = 0
        self._views = set()

    @property
    def views(self):
        """Returns a list of side intputs extracted from the graph.

        Returns:
          A list of pvalue.AsSideInput.
        """
        return list(self._views)

    def visit_transform(self, applied_ptransform: AppliedPTransform) -> None:
        inputs = list(applied_ptransform.inputs)
        if inputs:
            for input_value in inputs:
                if isinstance(input_value, PBegin):
                    self.root_transforms.add(applied_ptransform)
                if input_value not in self.value_to_consumers:
                    self.value_to_consumers[input_value] = set()
                self.value_to_consumers[input_value].add(applied_ptransform)
        else:
            self.root_transforms.add(applied_ptransform)
        self.step_names[applied_ptransform] = "s%d" % self._num_transforms
        self._num_transforms += 1

        for side_input in applied_ptransform.side_inputs:
            self._views.add(side_input)


class DramatiqPipelineRunner(BundleBasedDirectRunner):
    @staticmethod
    def is_fnapi_compatible():
        return False

    @staticmethod
    def to_dramatiq_visitor():
        @dataclass
        class DramatiqVisitor(PipelineVisitor):
            collections: dict[AppliedPTransform, DramatiqCollection] = field(
                default_factory=dict
            )

            def visit_transform(self, transform_node) -> None:
                op_class = TRANSLATIONS.get(transform_node.transform.__class__, NoOp)
                op = op_class(transform_node)

                inputs = list(transform_node.inputs)
                if inputs:
                    collection_inputs = []
                    for input_value in inputs:
                        if isinstance(input_value, PBegin):
                            collection_inputs.append(None)

                        prev_op = input_value.producer
                        if prev_op in self.collections:
                            next_input = self.collections[prev_op]
                            collection_inputs.append(next_input)

                    if len(collection_inputs) == 1:
                        self.collections[transform_node] = op.apply(
                            collection_inputs[0]
                        )
                    else:
                        self.collections[transform_node] = op.apply(collection_inputs)

                else:
                    self.collections[transform_node] = op.apply(None)

        return DramatiqVisitor()

    def run_pipeline(self, pipeline: Pipeline, options: PipelineOptions):
        logger.info("Running pipeline with beam-dramatiq-runner.")

        pipeline.replace_all(dramatiq_overrides())

        dramatiq_visitor = self.to_dramatiq_visitor()
        pipeline.visit(dramatiq_visitor)

        results = dramatiq_visitor.collections.values()
        return DramatiqPipelineResult(results)


class DramatiqPipelineResult(PipelineResult):
    """A DramatiqPipelineResult provides access to info about a pipeline."""

    def __init__(self, results: list[DramatiqCollection]):
        super().__init__(PipelineState.RUNNING)
        self.results = results

    def __del__(self):
        if self._state == PipelineState.RUNNING:
            logger.warning(
                "The DramatiqPipelineResult is being garbage-collected while the "
                "DramatiqRunner is still running the corresponding pipeline. This may "
                "lead to incomplete execution of the pipeline if the main thread "
                "exits before pipeline completion. Consider using "
                "result.wait_until_finish() to wait for completion of pipeline "
                "execution."
            )

    def wait_until_finish(self, duration=None):
        if not PipelineState.is_terminal(self.state):
            if duration:
                raise NotImplementedError(
                    "DramatiqRunner does not support duration argument."
                )
            try:
                self._state = PipelineState.DONE
            except:  # noqa
                self._state = PipelineState.FAILED
                raise
        return self._state

    def aggregated_values(self, aggregator_or_name):
        return {}

    def metrics(self):
        raise NotImplementedError("collecting metrics will come later!")

    def cancel(self):
        """Shuts down pipeline workers.

        For testing use only. Does not properly wait for pipeline workers to shut
        down.
        """
        self._state = PipelineState.CANCELLING
        self._state = PipelineState.CANCELLED
