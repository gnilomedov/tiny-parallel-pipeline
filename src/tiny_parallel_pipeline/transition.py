"""Transitions: the steps that turn input resources into output resources."""


from abc import ABC, abstractmethod
from types import SimpleNamespace


from tiny_parallel_pipeline import ResourceStatus, Resource


class TransitionCalculation(ABC):
    """Base class for one pipeline step. A subclass does the work in `_execute_impl`."""
    def __init__(self, name: str | None = None, allow_multiprocess_pool: bool = False,
                 retries_count: int = 1):
        self._name = name
        self._allow_multiprocess_pool = allow_multiprocess_pool
        self._retries_count = retries_count
        self._in_resources: dict[str, Resource] = dict()
        self._out_resources: dict[str, Resource] = dict()
        self._termination_requested = False

        self._compiled = False

    def terminate(self) -> None:
        """Ask _execute_impl to bail out before it starts any further work."""
        self._termination_requested = True

    def set_name(self, name: str) -> 'TransitionCalculation':
        self._name = name
        return self

    @property
    def name(self):
        return self._name

    @property
    def allow_multiprocess_pool(self):
        return self._allow_multiprocess_pool

    def compile(self) -> 'TransitionCalculation':
        self._compiled = True
        return self

    def __eq__(self, other):
        return isinstance(other, TransitionCalculation) and id(self) == id(other)

    def __hash__(self):
        return id(self)

    async def execute(self) -> tuple[bool, str]:
        """Checks the inputs are ready, runs `_execute_impl`, then marks the outputs ready."""
        for r in self._in_resources.values():
            assert r.status == ResourceStatus.READY
            assert r.data is not None, str(r)
        for r in self._out_resources.values():
            r.update_status(ResourceStatus.IN_PROGRESS)

        for attempt in range(self._retries_count):
            is_ok, err_msg = await self._execute_impl(
                SimpleNamespace(**self._in_resources), SimpleNamespace(**self._out_resources))
            if is_ok or self._termination_requested:
                break
            retries_left = self._retries_count - attempt - 1
            if retries_left > 0:
                print(f'[WARN] {self._name}: {err_msg}\n'
                      f'[WARN] {self._name}: will retry {retries_left} more times')

        if is_ok:
            for r in self._out_resources.values():
                r.update_status(ResourceStatus.READY)
        else:
            for r in self._out_resources.values():
                r.update_status(ResourceStatus.FAILED, err_msg)

        return is_ok, err_msg

    def post_execute_populate_out_resource_data(self, async_out_resources: dict[str, Resource]) -> None:
        for name, ar in async_out_resources.items():
            mr = self._out_resources[name]
            mr.populate_data(ar.data)
            mr.update_status(ar.status, ar.failed_reason)

    def __repr__(self) -> str:
        ress2repr = lambda rs: '{' + ', '.join(f'{n}: {r!r}' for n, r in rs.items()) + '}'
        in_repr = ress2repr(self._in_resources)
        out_repr = ress2repr(self._out_resources)
        return f'<{self.__class__.__name__} {self._name} : {in_repr} -> {out_repr}>'

    def __str__(self) -> str:
        return repr(self)

    def _set_in_resources(self, **in_name_2_resource: Resource) -> 'TransitionCalculation':
        """For subclasses only: names the inputs `_execute_impl` reads as attributes."""
        return self._update_resources(self._in_resources, in_name_2_resource)

    def _set_out_resources(self, **out_name_2_resource: Resource) -> 'TransitionCalculation':
        """For subclasses only: names the outputs `_execute_impl` fills as attributes."""
        return self._update_resources(self._out_resources, out_name_2_resource)

    def _update_resources(self, resources: dict[str, Resource],
                          name_2_resource: dict[str, Resource]) -> 'TransitionCalculation':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        for r in name_2_resource.values():
            assert isinstance(r, Resource)
        resources.update(name_2_resource)
        return self

    @abstractmethod
    async def _execute_impl(self, in_resources, out_resources) -> tuple[bool, str]:
        raise "NOT IMPLEMENTED: transition.py TransitionCalculation @abstractmethod _execute_impl"
