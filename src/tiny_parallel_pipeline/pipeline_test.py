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
                'T1', in_res=[resources.seed], out_res=[resources.leaf.a])
            self.second = DummyTransitionCalculation(
                'T2', in_res=[resources.leaf.a], out_res=[resources.leaf.b])
    return Transitions()


#
# Tests
#

class TestDir:
    def test_walk(self, resources):
        assert [p for p, _ in LeafDir().walk()] == ['a', 'b']
        assert [p for p, _ in resources.walk()] == ['seed', 'leaf.a', 'leaf.b']
        assert [p for p, _ in resources.walk('top.')] == [
            'top.seed', 'top.leaf.a', 'top.leaf.b']
        assert dict(resources.walk())['leaf.a'] is resources.leaf.a
        assert [p for p, _ in ListDir().walk()] == [
            'items[0]', 'items[1]', 'dirs[0].a', 'dirs[0].b', 'dirs[1].a', 'dirs[1].b']

    def test_assert_contract_ok(self, resources):
        class OuterDir(tpp.ResourcesDir):
            def __init__(self):
                self.inner = LeafDir()
        resources.assert_contract(tpp.Resource)
        OuterDir().assert_contract(tpp.Resource)
        ListDir().assert_contract(tpp.Resource)

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

    def test_all_in_declaration_order(self, transitions):
        assert [t.name for t in transitions.all()] == ['T1', 'T2']

    def test_all_flattens_lists(self, resources):
        class Transitions(tpp.TransitionsDir):
            def __init__(self):
                self.many = [
                    DummyTransitionCalculation('T1', out_res=[resources.leaf.a]),
                    DummyTransitionCalculation('T2', out_res=[resources.leaf.b])]
        assert [t.name for t in Transitions().all()] == ['T1', 'T2']


class TestPipeline:
    def test_init_asserts_contracts(self, resources, transitions, monkeypatch):
        seen = []
        monkeypatch.setattr(tpp.Dir, 'assert_contract',
                            lambda self, leaf: seen.append((type(self).__name__, leaf)))
        tpp.Pipeline(resources, transitions)
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
            tpp.Pipeline(BadResourcesDir(), transitions)
        with pytest.raises(AssertionError,
                           match='BadTransitionsDir.oops is str, want TransitionCalculation'):
            tpp.Pipeline(resources, BadTransitionsDir())

    def test_scheduler(self, resources, transitions):
        scheduler = tpp.Pipeline(resources, transitions).scheduler()
        assert all(t._compiled for t in transitions.all())
        assert scheduler.remaining_resources_count() == 2
        assert [t.name for t in scheduler.get_ready_to_execute_transitions()] == ['T1']

    def test_scheduler_asserts_compile_failure(self, resources):
        class Transitions(tpp.TransitionsDir):
            def __init__(self):
                self.only = DummyTransitionCalculation(  # leaf.b has no producer
                    'T1', in_res=[resources.leaf.b], out_res=[resources.leaf.a])
        with pytest.raises(AssertionError, match='No transition to calculate'):
            tpp.Pipeline(resources, Transitions()).scheduler()
