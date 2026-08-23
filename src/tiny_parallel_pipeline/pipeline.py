"""Grouping: resources and transitions as named trees, and the Pipeline holding them."""


from collections.abc import Iterator


from tiny_parallel_pipeline import (
    Resource, TransitionCalculation, Scheduler)


class Dir:
    """Attributes are all `leaf_class` instances, nested `Dir`s, or lists of either."""

    def walk(self, prefix: str = '') -> Iterator[tuple[str, object]]:
        for name, value in vars(self).items():
            yield from self._walk_value(f'{prefix}{name}', value)

    def assert_contract(self, leaf_class: type) -> None:
        for path, value in self.walk():
            assert isinstance(value, leaf_class), (
                f'{type(self).__name__}.{path} is {type(value).__name__}, '
                f'want {leaf_class.__name__} or Dir')

    def _walk_value(self, path: str, value: object) -> Iterator[tuple[str, object]]:
        if isinstance(value, Dir):
            yield from value.walk(f'{path}.')
        elif isinstance(value, list):
            for i, item in enumerate(value):
                yield from self._walk_value(f'{path}[{i}]', item)
        else:
            yield path, value


class ResourcesDir(Dir):
    """A tree of resources. Nesting gives them dotted names like `video.info`."""
    pass


class TransitionsDir(Dir):
    """A tree of transitions. `all()` flattens it for the Scheduler."""
    def all(self) -> list[TransitionCalculation]:
        return [t for _, t in self.walk()]


class Pipeline:
    """One resources tree plus one transitions tree, ready to be scheduled."""
    def __init__(self, resources: ResourcesDir, transitions: TransitionsDir):
        resources.assert_contract(Resource)
        transitions.assert_contract(TransitionCalculation)
        self.resources = resources
        self.transitions = transitions

    def scheduler(self, *targets: Resource) -> Scheduler:
        """Compiles the transitions; with `targets` only the steps they need get scheduled."""
        scheduler = Scheduler().add_transitions(*[t.compile() for t in self.transitions.all()])
        if targets:
            scheduler.add_resources(*targets)
        else:
            scheduler.pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg
        return scheduler
