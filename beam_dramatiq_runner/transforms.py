from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union, cast, Callable, Sequence

import apache_beam
from apache_beam.pipeline import AppliedPTransform

from .overrides import _Create, _Flatten, _GroupByKeyOnly
from .tasks import dramatiq_map, dramatiq_filter, dramatiq_groupby
from .types import E


class DramatiqCollection:
    def __init__(self, items: list = None):
        self.items = items

    def map(self, func: Callable[..., E], *args, **kwargs):
        """Apply a function to all elements in the collection in parallel."""
        message = dramatiq_map.send(func, self.items, *args, **kwargs)
        return DramatiqCollection(message.get_result(block=True))

    def filter(self, predicate: Callable[..., E], *args, **kwargs):
        """Filter elements in the collection based on a predicate in parallel."""
        message = dramatiq_filter.send(predicate, self.items, *args, **kwargs)
        return DramatiqCollection(message.get_result(block=True))

    def flatten(self):
        items = []
        for item in self.items:
            if isinstance(item, DramatiqCollection):
                items.extend(item.flatten().items)
            else:
                items.extend(item)

        return DramatiqCollection([])

    def groupby(self, grouper: Callable[..., E], *args, **kwargs):
        message = dramatiq_groupby.send(grouper, self.items, *args, **kwargs)
        return DramatiqCollection(message.get_result(block=True))


Input = Union[DramatiqCollection, Sequence[DramatiqCollection], None]


@dataclass
class DramatiqOp(ABC):
    applied: AppliedPTransform

    @property
    def transform(self):
        return self.applied.transform

    @abstractmethod
    def apply(self, input: Input) -> DramatiqCollection:
        pass


class NoOp(DramatiqOp):
    def apply(self, input: Input) -> DramatiqCollection:
        if not input:
            return DramatiqCollection([])

        return input


class Create(DramatiqOp):
    def apply(self, input: Input) -> DramatiqCollection:
        assert input is None, "Create expects no input!"
        original_transform = cast(_Create, self.transform)
        return DramatiqCollection(list(original_transform.values))


class ParDo(DramatiqOp):
    def apply(self, input: DramatiqCollection) -> DramatiqCollection:
        transform = cast(apache_beam.ParDo, self.transform)
        if input is None:
            return DramatiqCollection()
        return input.map(transform.fn.process, *transform.args, **transform.kwargs)


class Map(DramatiqOp):
    def apply(self, input: DramatiqCollection) -> DramatiqCollection:
        transform = cast(apache_beam.Map, self.transform)
        return input.map(
            transform.fn.process, *transform.args, **transform.kwargs
        ).flatten()


class GroupByKey(DramatiqOp):
    def apply(self, input: DramatiqCollection) -> DramatiqCollection:
        def key(item):
            return item[0]

        def value(item):
            k, v = item
            return k, [elm[1] for elm in v]

        return input.groupby(key).map(value)


class Flatten(DramatiqOp):
    def apply(self, input: Input) -> DramatiqCollection:
        assert type(input) is list, "Must take a sequence of DramatiqCollections!"
        collection = DramatiqCollection([item.flatten() for item in input.items])
        return collection.flatten()


TRANSLATIONS = {
    _Create: Create,
    apache_beam.ParDo: ParDo,
    apache_beam.Map: Map,
    _GroupByKeyOnly: GroupByKey,
    _Flatten: Flatten,
}
