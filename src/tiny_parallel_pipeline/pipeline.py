"""Grouping: resources and transitions as named trees, and the Pipeline holding them."""


from collections.abc import Iterator


from tiny_parallel_pipeline import Resource, TransitionCalculation


class Dir:
    """Attributes are all `leaf_class` instances, nested `Dir`s, or lists/dicts of either."""

    def walk_path_value(self, prefix: str = '') -> Iterator[tuple[str, object]]:
        for path, value in vars(self).items():
            yield from self._walk_value(f'{prefix}{path}', value)

    def walk_values(self) -> list[object]:
        return [value for _, value in self.walk_path_value()]

    def assert_contract(self, leaf_class: type) -> None:
        for path, value in self.walk_path_value():
            assert isinstance(value, leaf_class), (
                f'{type(self).__name__}.{path} is {type(value).__name__}, '
                f'want {leaf_class.__name__} or Dir')

    def _walk_value(self, path: str, value: object) -> Iterator[tuple[str, object]]:
        if isinstance(value, Dir):
            yield from value.walk_path_value(f'{path}.')
        elif isinstance(value, list):
            for i, item in enumerate(value):
                yield from self._walk_value(f'{path}[{i}]', item)
        elif isinstance(value, dict):
            for key, item in value.items():
                yield from self._walk_value(f'{path}[{key!r}]', item)
        else:
            yield path, value


class ResourcesDir(Dir):
    """A tree of resources. Nesting gives them dotted names like `video.info`."""
    pass


class TransitionsDir(Dir):
    """A tree of transitions. `walk_values()` flattens it for the Scheduler."""
    pass


class Pipeline:
    """One resources tree plus one transitions tree, ready to be scheduled."""
    def __init__(self, name: str, resources: ResourcesDir, transitions: TransitionsDir):
        resources.assert_contract(Resource)
        transitions.assert_contract(TransitionCalculation)
        self.name = name
        self.resources = resources
        self.transitions = transitions

    def __or__(self, downstream: 'Pipeline') -> '_PipelineChain':
        """E.g.: all_pipes = pipe_a | pipe_b | pipe_c"""
        return _PipelineChain(self, downstream)


class _PipelineChain:
    """Stages collected by `|`, kept flat until `as_pipeline()` freezes them into one."""

    def __init__(self, *pipelines: Pipeline):
        self._pipelines = pipelines

    def __or__(self, downstream: Pipeline) -> '_PipelineChain':
        return _PipelineChain(*self._pipelines, downstream)

    def as_pipeline(self) -> Pipeline:
        resources, transitions = ResourcesDir(), TransitionsDir()
        for pipeline in self._pipelines:
            if hasattr(resources, pipeline.name):
                raise ValueError(f'two stages named {pipeline.name}')
            setattr(resources, pipeline.name, pipeline.resources)
            setattr(transitions, pipeline.name, pipeline.transitions)
        return Pipeline('_'.join(p.name for p in self._pipelines), resources, transitions)
