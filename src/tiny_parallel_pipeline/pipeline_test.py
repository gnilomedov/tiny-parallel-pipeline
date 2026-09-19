import pytest


import tiny_parallel_pipeline as tpp


from tiny_parallel_pipeline.resource_test import DummyResource
from tiny_parallel_pipeline.transition_test import DummyTransitionCalculation


#
# Test-specific subclasses
#

class LeafDir(tpp.ResourcesDir):
    def __init__(self):
        self.a = DummyResource('A')
        self.b = DummyResource('B')

class ListDir(tpp.ResourcesDir):
    def __init__(self):
        self.items = [DummyResource('L0'), DummyResource('L1')]
        self.dirs = [LeafDir(), LeafDir()]

class DictDir(tpp.ResourcesDir):
    def __init__(self):
        self.by_key = {'AAPL': DummyResource('D0'), 'MSFT': DummyResource('D1')}
        self.dirs = {'x': LeafDir()}

class DummyDir(tpp.ResourcesDir):
    def __init__(self):
        self.x = DummyResource('X')

class NestedDir(tpp.ResourcesDir):
    def __init__(self):
        self.seed = (DummyResource('SEED')
            .populate_data('d')
            .update_status(tpp.ResourceStatus.READY))
        self.leaf = LeafDir()


#
# Fixtures
#

@pytest.fixture
def resources():
    return NestedDir()

@pytest.fixture
def transitions(resources):
    class Transitions(tpp.TransitionsDir):
        def __init__(self):
            self.first = DummyTransitionCalculation(
                'T1', {'seed': resources.seed}, {'a': resources.leaf.a})
            self.second = DummyTransitionCalculation(
                'T2', {'a': resources.leaf.a}, {'b': resources.leaf.b})
    return Transitions()


#
# Tests
#

class TestDir:
    def test_walk_path_value(self, resources):
        assert [p for p, _ in LeafDir().walk_path_value()] == ['a', 'b']
        assert [p for p, _ in resources.walk_path_value()] == ['seed', 'leaf.a', 'leaf.b']
        assert [p for p, _ in resources.walk_path_value('top.')] == [
            'top.seed', 'top.leaf.a', 'top.leaf.b']
        assert dict(resources.walk_path_value())['leaf.a'] is resources.leaf.a
        assert [p for p, _ in ListDir().walk_path_value()] == [
            'items[0]', 'items[1]', 'dirs[0].a', 'dirs[0].b', 'dirs[1].a', 'dirs[1].b']

    def test_walk_path_value_of_dicts(self):
        d = DictDir()
        assert [p for p, _ in d.walk_path_value()] == [
            "by_key['AAPL']", "by_key['MSFT']", "dirs['x'].a", "dirs['x'].b"]
        assert dict(d.walk_path_value())["by_key['MSFT']"] is d.by_key['MSFT']

        class MixedDir(tpp.ResourcesDir):
            def __init__(self):
                self.mix = {'k': [LeafDir()]}
        assert [p for p, _ in MixedDir().walk_path_value()] == ["mix['k'][0].a", "mix['k'][0].b"]

    def test_walk_values_drops_the_paths(self, resources):
        assert resources.walk_values() == [resources.seed, resources.leaf.a, resources.leaf.b]

    def test_assert_contract_ok(self, resources):
        class OuterDir(tpp.ResourcesDir):
            def __init__(self):
                self.inner = LeafDir()
        resources.assert_contract(tpp.Resource)
        OuterDir().assert_contract(tpp.Resource)
        ListDir().assert_contract(tpp.Resource)
        DictDir().assert_contract(tpp.Resource)

    def test_assert_contract_rejects_leaf(self):
        class BadDir(tpp.ResourcesDir):
            def __init__(self):
                self.oops = 'not a resource'
        class OuterDir(tpp.ResourcesDir):
            def __init__(self):
                self.inner = BadDir()
        with pytest.raises(AssertionError, match='BadDir.oops is str, want Resource or Dir'):
            BadDir().assert_contract(tpp.Resource)
        with pytest.raises(AssertionError,
                           match=r'OuterDir\.inner\.oops is str, want Resource or Dir'):
            OuterDir().assert_contract(tpp.Resource)

        class BadListDir(tpp.ResourcesDir):
            def __init__(self):
                self.oops = [DummyResource('OK'), 'not a resource']
        with pytest.raises(AssertionError,
                           match=r'BadListDir\.oops\[1\] is str, want Resource or Dir'):
            BadListDir().assert_contract(tpp.Resource)

        class BadDictDir(tpp.ResourcesDir):
            def __init__(self):
                self.oops = {'ok': DummyResource('OK'), 'bad': 'not a resource'}
        with pytest.raises(AssertionError,
                           match=r"BadDictDir\.oops\['bad'\] is str, want Resource or Dir"):
            BadDictDir().assert_contract(tpp.Resource)

    def test_walk_values_in_declaration_order(self, transitions):
        assert [t.name for t in transitions.walk_values()] == ['T1', 'T2']

    def test_walk_values_flattens_lists(self, resources):
        class Transitions(tpp.TransitionsDir):
            def __init__(self):
                self.many = [
                    DummyTransitionCalculation('T1', {}, {'a': resources.leaf.a}),
                    DummyTransitionCalculation('T2', {}, {'b': resources.leaf.b})]
        assert [t.name for t in Transitions().walk_values()] == ['T1', 'T2']

    def test_walk_values_flattens_dicts(self, resources):
        class Transitions(tpp.TransitionsDir):
            def __init__(self):
                self.many = {
                    'a': DummyTransitionCalculation('T1', {}, {'a': resources.leaf.a}),
                    'b': DummyTransitionCalculation('T2', {}, {'b': resources.leaf.b})}
        assert [t.name for t in Transitions().walk_values()] == ['T1', 'T2']


class TestPipeline:
    def test_init_asserts_contracts(self, resources, transitions, monkeypatch):
        seen = []
        monkeypatch.setattr(tpp.Dir, 'assert_contract',
                            lambda self, leaf: seen.append((type(self).__name__, leaf)))
        tpp.Pipeline('one', resources, transitions)
        assert seen == [('NestedDir', tpp.Resource), ('Transitions', tpp.TransitionCalculation)]

    def test_init_rejects_broken_contracts(self, resources, transitions):
        class BadResourcesDir(tpp.ResourcesDir):
            def __init__(self):
                self.oops = 'not a resource'
        class BadTransitionsDir(tpp.TransitionsDir):
            def __init__(self):
                self.oops = 'not a transition'
        with pytest.raises(AssertionError,
                           match='BadResourcesDir.oops is str, want Resource or Dir'):
            tpp.Pipeline('one', BadResourcesDir(), transitions)
        with pytest.raises(AssertionError,
                           match='BadTransitionsDir.oops is str, want TransitionCalculation'):
            tpp.Pipeline('one', resources, BadTransitionsDir())

    def test_compile_scheduler(self, resources, transitions):
        executor = tpp.Executor(tpp.Pipeline('one', resources, transitions)).compile_scheduler()
        assert all(t._compiled for t in transitions.walk_values())
        assert len(executor._scheduler._want_transitions) == 2
        assert [t.name for t in executor._scheduler.get_ready_to_execute_transitions()] == ['T1']

    def test_compile_scheduler_asserts_compile_failure(self, resources):
        class Transitions(tpp.TransitionsDir):
            def __init__(self):
                self.only = DummyTransitionCalculation(  # leaf.b has no producer
                    'T1', {'b': resources.leaf.b}, {'a': resources.leaf.a})
        with pytest.raises(AssertionError, match='No transition to calculate'):
            tpp.Executor(tpp.Pipeline('one', resources, Transitions())).compile_scheduler()


def test_pipeline_chain(resources, transitions):
    first = tpp.Pipeline('first', resources, transitions)
    second = tpp.Pipeline('second', DummyDir(), tpp.TransitionsDir())

    merged = (first | second).as_pipeline()

    assert list(vars(merged.resources)) == ['first', 'second']
    assert [path for path, _ in merged.resources.walk_path_value()] == [
        'first.seed', 'first.leaf.a', 'first.leaf.b', 'second.x']
    assert [t.name for t in merged.transitions.walk_values()] == ['T1', 'T2']

    # One name is one stage, and stages connect by sharing an object, never by sharing an id.
    with pytest.raises(ValueError, match='two stages named first'):
        (first | tpp.Pipeline('first', DummyDir(), tpp.TransitionsDir())).as_pipeline()
