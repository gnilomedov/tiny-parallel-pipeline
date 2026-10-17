"""Transitions: the steps that turn input resources into output resources."""


import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from types import SimpleNamespace
from typing import Concatenate, ParamSpec, Self, dataclass_transform, override


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

    def compile(self) -> Self:
        self._compiled = True
        return self

    def set_allow_multiprocess_pool(self, allow_multiprocess_pool: bool) -> Self:
        self._allow_multiprocess_pool = allow_multiprocess_pool
        return self

    def set_retries_count(self, retries_count: int) -> Self:
        self._retries_count = retries_count
        return self

    def check_lazy_available(self) -> None:
        """Names the resources for `_check_lazy_available_impl`, as `execute` does for its own."""
        self._check_lazy_available_impl(SimpleNamespace(**self._in_resources),
                                        SimpleNamespace(**self._out_resources))

    async def execute(self) -> tuple[bool, str | None]:
        """Checks the inputs are ready, runs `_execute_impl`, then marks the outputs ready."""
        for r in self.in_resources_flat():
            assert r.status == ResourceStatus.READY
            assert r.data is not None, str(r)
        for r in self.out_resources_flat():
            if r.status == ResourceStatus.EMPTY:  # LAZY_AVAILABLE stays: _execute_impl reads it as its lazy marker
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

    def terminate(self) -> None:
        """Ask _execute_impl to bail out before it starts any further work."""
        self._termination_requested = True

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def allow_multiprocess_pool(self) -> bool:
        return self._allow_multiprocess_pool

    def post_execute_populate_out_resource_data(self, async_out_resources: NamedResources) -> None:
        for name, ar in async_out_resources.items():
            mr = self._out_resources[name]
            pairs = (zip(mr, ar, strict=True) if isinstance(ar, list)
                     else ((mr[k], v) for k, v in ar.items()) if isinstance(ar, dict)
                     else ((mr, ar),))
            for m, a in pairs:
                m.populate_data(a.data)
                m.update_status(a.status, a.failed_reason)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, TransitionCalculation) and id(self) == id(other)

    def __hash__(self) -> int:
        return id(self)

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

    def _check_lazy_available_impl(self, in_resources: SimpleNamespace,
                                   out_resources: SimpleNamespace) -> None:
        """Override to mark what a cache holds LAZY_AVAILABLE and drop the inputs it replaces."""

    @abstractmethod
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        raise NotImplementedError('transition.py TransitionCalculation @abstractmethod _execute_impl')


class MilestoneTransition(TransitionCalculation):
    """Marker: runs for its side effect, so it needs no out resource to be pulled into the graph.

    Only a leaf: nothing can wait for it. `force_schedule` targets its inputs, so they never GC.

    Naming recommendation: subclass names end with 'Milestone'.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.force_schedule: bool = False  # Otherwise scheduled IFF all its in resources are scheduled anyway.


_ExecuteFnParams = ParamSpec('_ExecuteFnParams')  # the decorated fn's parameters = the construction keywords


def stateless_transition(*, allow_multiprocess_pool: bool = False, retries_count: int = 1,
                         lazy_check_fn: Callable[..., bool] | None = None
                         ) -> Callable[[Callable[_ExecuteFnParams, object]],
                                       Callable[Concatenate[str, _ExecuteFnParams], TransitionCalculation]]:
    """Decorator, always called (`@tpp.stateless_transition()`): fn -> a transition class whose wiring is its signature.

    Parameters named `in_x` / `out_x` wire resource `x` and receive the Resource itself; any other is config. The class
    is built as `<fn name>(name, **the same keywords)`: a missing, extra or misspelled keyword fails at construction,
    and the ParamSpec lets a type checker flag it earlier. Defined at module level, the class pickles by name.

    `lazy_check_fn(<some of fn's keywords>) -> bool`: True marks every output LAZY_AVAILABLE and drops the inputs it
    does not name; fn then gets those as None and takes its cached path on `out_x.status == LAZY_AVAILABLE`.

    E.g.:
    @tpp.stateless_transition(retries_count=3)
    async def run_step(some_config: str, in_token_foo: TokenFooResource,
                       out_token_bar: TokenBarResource) -> tuple[bool, str | None]:
        out_token_bar.populate_data(f'{in_token_foo.data} {some_config}')
        return True, None

    E.g. to init; the decorator's flags are defaults, a fluent setter overrides one for this instance only:
    step = run_step('step-me', some_config='config', in_token_foo=res.token_foo, out_token_bar=res.token_bar)
    patient_step = run_step('step-patient', some_config='config', in_token_foo=res.token_foo,
                            out_token_bar=res.token_bar).set_retries_count(10)
    """
    def decorate(fn: Callable) -> type[TransitionCalculation]:
        signature = inspect.signature(fn)
        keyword_kinds = (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        assert all(p.kind in keyword_kinds for p in signature.parameters.values()), (
            f'{fn.__qualname__}: keyword parameters only, no *args / **kwargs')
        assert 'name' not in signature.parameters, f'{fn.__qualname__}: `name` is the transition name'
        in_names = [n for n in signature.parameters if n.startswith('in_')]
        lazy_names = list(inspect.signature(lazy_check_fn).parameters) if lazy_check_fn else []
        assert set(lazy_names) <= set(signature.parameters), (
            f'{fn.__qualname__}: lazy_check_fn takes {lazy_names}, beyond its keywords {list(signature.parameters)}')

        class _StatelessTransition(TransitionCalculation):
            def __init__(self, name: str, **values):
                signature.bind(**values)  # a missing, extra or misspelled keyword fails here, not at execute time
                super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool, retries_count=retries_count)
                self._set_in_resources(**{n.removeprefix('in_'): v for n, v in values.items() if n.startswith('in_')})
                self._set_out_resources(**{n.removeprefix('out_'): v for n, v in values.items()
                                           if n.startswith('out_')})
                self._config = {n: v for n, v in values.items() if not n.startswith(('in_', 'out_'))}

            @override
            def _check_lazy_available_impl(self, in_resources: SimpleNamespace, out_resources: SimpleNamespace) -> None:
                if lazy_check_fn is None:
                    return
                keywords = self._keywords(in_resources, out_resources)
                if lazy_check_fn(**{n: keywords[n] for n in lazy_names}):
                    for r in self.out_resources_flat():
                        r.update_status(ResourceStatus.LAZY_AVAILABLE)
                    for unread in [n for n in self._in_resources if f'in_{n}' not in lazy_names]:
                        del self._in_resources[unread]

            @override
            async def _execute_impl(self, in_resources: SimpleNamespace,
                                    out_resources: SimpleNamespace) -> tuple[bool, str | None]:
                result = fn(**self._keywords(in_resources, out_resources))
                return await result if inspect.isawaitable(result) else result

            def _keywords(self, in_resources: SimpleNamespace, out_resources: SimpleNamespace) -> dict:
                # fn's keywords: config, then each in_ / out_ Resource; an input dropped by the lazy check is None.
                return {**self._config, **{n: getattr(in_resources, n.removeprefix('in_'), None) for n in in_names},
                        **{f'out_{n}': r for n, r in vars(out_resources).items()}}

        _StatelessTransition.__name__ = fn.__name__
        _StatelessTransition.__qualname__ = fn.__qualname__
        _StatelessTransition.__module__ = fn.__module__
        _StatelessTransition.__doc__ = fn.__doc__
        return _StatelessTransition

    return decorate


@dataclass_transform(kw_only_default=True)
class TypedTransition(TransitionCalculation):
    """Builds `__init__(name, *, ...)` from the subclass's annotations, as a dataclass does.

    `in_x` / `out_x` wire resource `x`; any other annotation `y` is config, kept as `self.y`. An optional
    `__post_init__` derives the rest; class keywords set `allow_multiprocess_pool` and `retries_count`:

    E.g.:
    class BakeTransition(tpp.TypedTransition, retries_count=3):
        archive: CorpusArchive
        in_token: InTockenResource
        out_tocken: OutTockenResource

        def __post_init__(self):
            self._state_field = "state"

        @override
        async def _execute_impl(self, in_resources, out_resources):
            return True, None

    E.g. to init:
    bake = BakeTransition('bake-me', archive=archive,
                          in_token=res.token_foo,
                          out_tocken=res.tocken_bar)
    """

    def __init_subclass__(cls, allow_multiprocess_pool: bool = False, retries_count: int = 1, **kwargs):
        super().__init_subclass__(**kwargs)
        assert '__init__' not in cls.__dict__, f'{cls.__name__}: no __init__ of its own, derive in __post_init__'
        names = [n for klazz in reversed(cls.__mro__) if issubclass(klazz, TypedTransition)
                 for n in inspect.get_annotations(klazz)]

        def __init__(self, name: str, **values):
            assert sorted(values) == sorted(names), f'{cls.__name__} takes {sorted(names)}, got {sorted(values)}'
            TransitionCalculation.__init__(self, name, allow_multiprocess_pool=allow_multiprocess_pool,
                                           retries_count=retries_count)
            self._set_in_resources(**{n.removeprefix('in_'): v for n, v in values.items() if n.startswith('in_')})
            self._set_out_resources(**{n.removeprefix('out_'): v for n, v in values.items() if n.startswith('out_')})
            for n, v in values.items():
                if not n.startswith(('in_', 'out_')):
                    setattr(self, n, v)
            if hasattr(self, '__post_init__'):
                self.__post_init__()

        cls.__init__ = __init__


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
    elif hasattr(value, 'walk_path_value'):   # a Dir, duck-typed: a runtime import would cycle
        yield from (leaf for _, leaf in value.walk_path_value())
    else:
        yield value
