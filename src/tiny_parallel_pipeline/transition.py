"""Transitions: the steps that turn input resources into output resources."""


from abc import ABC, abstractmethod
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Self


from tiny_parallel_pipeline import ResourceStatus, Resource


# Annotation only: lazily evaluated, never resolved at runtime, so ResourcesDir stays unimported.
type ResourceOrDir = Resource | ResourcesDir
type ResourceValue = ResourceOrDir | list[ResourceValue] | dict[str, ResourceValue]
type NamedResources = dict[str, ResourceValue]


class TransitionCalculation(ABC):
    """Base class for one pipeline step. A subclass does the work in `_execute_impl`."""
    def __init__(self, name: str | None = None, allow_multiprocess_pool: bool = False, retries_count: int = 1):
        self._name = name
        self._allow_multiprocess_pool = allow_multiprocess_pool
        self._retries_count = retries_count
        self._in_resources: NamedResources = dict()
        self._out_resources: NamedResources = dict()
        self._termination_requested = False

        self._compiled = False

    def in_resources_flat(self) -> Iterator[Resource]:
        return _flatten(self._in_resources)

    def out_resources_flat(self) -> Iterator[Resource]:
        return _flatten(self._out_resources)

    def terminate(self) -> None:
        """Ask _execute_impl to bail out before it starts any further work."""
        self._termination_requested = True

    def set_name(self, name: str) -> Self:
        self._name = name
        return self

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def allow_multiprocess_pool(self) -> bool:
        return self._allow_multiprocess_pool

    def compile(self) -> Self:
        self._compiled = True
        return self

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TransitionCalculation) and id(self) == id(other)

    def __hash__(self) -> int:
        return id(self)

    async def execute(self) -> tuple[bool, str | None]:
        """Checks the inputs are ready, runs `_execute_impl`, then marks the outputs ready."""
        for r in self.in_resources_flat():
            assert r.status == ResourceStatus.READY
            assert r.data is not None, str(r)
        for r in self.out_resources_flat():
            r.update_status(ResourceStatus.IN_PROGRESS)

        for attempt in range(self._retries_count):
            is_ok, err_msg = await self._execute_impl(SimpleNamespace(**self._in_resources),
                                                      SimpleNamespace(**self._out_resources))
            if is_ok or self._termination_requested:
                break
            retries_left = self._retries_count - attempt - 1
            if retries_left > 0:
                print(f'[WARN] {self._name}: {err_msg}\n'
                      f'[WARN] {self._name}: will retry {retries_left} more times')

        if is_ok:
            for r in self.out_resources_flat():
                r.update_status(ResourceStatus.READY)
        else:
            for r in self.out_resources_flat():
                r.update_status(ResourceStatus.FAILED, err_msg)

        return is_ok, err_msg

    def post_execute_populate_out_resource_data(self, async_out_resources: NamedResources) -> None:
        for name, ar in async_out_resources.items():
            mr = self._out_resources[name]
            pairs = (zip(mr, ar) if isinstance(ar, list)
                     else ((mr[k], v) for k, v in ar.items()) if isinstance(ar, dict)
                     else ((mr, ar),))
            for m, a in pairs:
                m.populate_data(a.data)
                m.update_status(a.status, a.failed_reason)

    def __repr__(self) -> str:
        many = lambda r: f'[{len(r)} x {next(iter(r.values() if isinstance(r, dict) else r))!r}]'
        one = lambda r: many(r) if isinstance(r, (list, dict)) and r else repr(r)
        ress2repr = lambda rs: '{' + ', '.join(f'{n}: {one(r)}' for n, r in rs.items()) + '}'
        in_repr = ress2repr(self._in_resources)
        out_repr = ress2repr(self._out_resources)
        return f'<{self.__class__.__name__} {self._name} : {in_repr} -> {out_repr}>'

    def __str__(self) -> str:
        return repr(self)

    def _set_in_resources(self, **in_name_2_resource: ResourceValue) -> Self:
        """For subclasses only: names the inputs `_execute_impl` reads as attributes."""
        return self._update_resources(self._in_resources, in_name_2_resource)

    def _set_out_resources(self, **out_name_2_resource: ResourceValue) -> Self:
        """For subclasses only: names the outputs `_execute_impl` fills as attributes."""
        return self._update_resources(self._out_resources, out_name_2_resource)

    def _update_resources(self, resources: NamedResources, name_2_resource: NamedResources) -> Self:
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        # A name may hold many, so a fan-in does not have to be flattened into one key per
        # resource just to be declared.
        for r in _flatten(name_2_resource):
            assert isinstance(r, Resource)
        resources.update(name_2_resource)
        return self

    @abstractmethod
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        raise NotImplementedError('transition.py TransitionCalculation @abstractmethod _execute_impl')


def _flatten(resources: NamedResources) -> Iterator[Resource]:
    """A resource, a Dir or dict of them, or a list of either; the graph wants them flat."""
    for value in resources.values():
        yield from _leaves(value)


def _leaves(value: ResourceValue) -> Iterator[Resource]:
    if isinstance(value, list):
        for item in value:
            yield from _leaves(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _leaves(item)
    elif hasattr(value, 'walk'):   # a Dir, duck-typed: a runtime import would cycle
        yield from (leaf for _, leaf in value.walk())
    else:
        yield value
