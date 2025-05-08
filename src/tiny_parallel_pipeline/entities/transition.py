import asyncio
from abc import ABC, abstractmethod
import multiprocessing


from tiny_parallel_pipeline import ResourceStatus, Resource


class TransitionCalculation(ABC):
    def __init__(self, name: str | None = None, allow_multiprocess_pool: bool = False):
        self._name = name
        self._allow_multiprocess_pool = allow_multiprocess_pool
        self._in_resources: list[Resource] | None = []
        self._out_resources: list[Resource] | None = []

        self._compiled = False

    def set_name(self, name: str) -> 'Transition':
        self._name = name
        return self

    @property
    def name(self):
        return self._name

    @property
    def allow_multiprocess_pool(self):
        return self._allow_multiprocess_pool

    def set_in_resources(self, *in_resources: list[Resource]) -> 'Transition':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        self._in_resources = list(in_resources)
        for r in self._in_resources:
            assert isinstance(r, Resource)
        return self

    def extend_in_resources(self, *in_resources: list[Resource]) -> 'Transition':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        self._in_resources.extend(in_resources)
        for r in self._in_resources:
            assert isinstance(r, Resource)
        return self

    def set_out_resources(self, *out_resources: list[Resource]) -> 'Transition':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        self._out_resources = list(out_resources)
        for r in self._out_resources:
            assert isinstance(r, Resource)
        return self

    def extend_out_resources(self, *out_resources: list[Resource]) -> 'Transition':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        self._out_resources.extend(out_resources)
        for r in self._in_resources:
            assert isinstance(r, Resource)
        return self

    def compile(self) -> 'TransitionCalculation':
        self._compiled = True
        return self

    def __eq__(self, other):
        return isinstance(other, TransitionCalculation) and id(self) == id(other)

    def __hash__(self):
        return id(self)

    async def execute(self) -> tuple[bool, str]:
        for r in self._in_resources:
            assert r.status == ResourceStatus.READY
            assert r.data is not None, str(r)
        for r in self._out_resources:
            r.update_status(ResourceStatus.IN_PROGRESS)

        is_ok, err_msg = await self._execute_impl(self._in_resources, self._out_resources)

#
        for r in self._out_resources:
            r.update_status(ResourceStatus.READY)
#

        return is_ok, err_msg

    @abstractmethod
    async def _execute_impl(self) -> tuple[bool, str]:
        return self._execute_impl()

    def post_execute_populate_out_resource_data(self, async_out_resources: list[Resource]) -> None:
        for mr, ar in zip(self._out_resources, async_out_resources):
            mr.populate_data(ar.data)
            mr.update_status(ar.status)

    def __repr__(self) -> str:
        ress2repr = lambda ress: repr(ress) if ress else repr([repr(r) for r in ress])
        in_repr = ress2repr(self._in_resources)
        out_repr = ress2repr(self._out_resources)
        return f'<{self.__class__.__name__} {self._name} : {in_repr} -> {out_repr}>'

    def __str__(self) -> str:
        return repr(self)
