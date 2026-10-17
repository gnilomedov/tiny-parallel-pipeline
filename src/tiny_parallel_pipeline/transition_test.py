import asyncio
import os
import pickle
from typing import override

import pytest


import tiny_parallel_pipeline as tpp


from tiny_parallel_pipeline.resource_test import DummyResource


#
# Test-specific subclass
#

class DummyTransitionCalculation(tpp.TransitionCalculation):
    def __init__(self, name,
                 in_name_2_resource: dict = None, out_name_2_resource: dict = None,
                 simulate_async_sleep_period=0.0,
                 data_add_pid=False,
                 allow_multiprocess_pool=False):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._set_in_resources(**(in_name_2_resource or {}))
        self._set_out_resources(**(out_name_2_resource or {}))
        self._simulate_async_sleep_period = simulate_async_sleep_period
        self._data_add_pid = data_add_pid

    @override
    async def _execute_impl(self, in_resources, out_resources):
        if self._simulate_async_sleep_period > 0.0:
            await asyncio.sleep(self._simulate_async_sleep_period)
        in_info = '|'.join([r.id.in_class_id for r in vars(in_resources).values()])
        for r in vars(out_resources).values():
            data = f'by {self._name} {in_info}'
            if self._data_add_pid:
                data = (data, os.getpid())
            r.populate_data(data)
        return True, None


class TwoStepTransitionCalculation(DummyTransitionCalculation):
    """Names its inputs in two calls, pinning that `_set_in_resources` adds, not replaces."""
    def __init__(self, name, first, second, only):
        super().__init__(name, dict(first=first), dict(only=only))
        self._set_in_resources(second=second)


class FlakyTransitionCalculation(tpp.TransitionCalculation):
    """Fails its first `fail_times` attempts, so the retry loop in `execute` can be watched."""
    def __init__(self, name, out, fail_times, retries_count):
        super().__init__(name, retries_count=retries_count)
        self._set_out_resources(only=out)
        self._fail_times = fail_times
        self.attempts = 0

    @override
    async def _execute_impl(self, in_resources, out_resources):
        self.attempts += 1
        if self.attempts <= self._fail_times:
            return False, f'boom {self.attempts}'
        out_resources.only.populate_data('ok')
        return True, None


class DirOfResources(tpp.ResourcesDir):
    """A Dir built inline, so `_leaves` can be shown reaching into one."""
    def __init__(self, **resources):
        for name, resource in resources.items():
            setattr(self, name, resource)


class FanInTransitionCalculation(tpp.TransitionCalculation):
    """Takes and fills whole lists of resources under one name each."""
    def __init__(self, name, ins: list, outs: list):
        super().__init__(name)
        self._set_in_resources(many=ins, one=DummyResource('SOLO')
                               .populate_data('d').update_status(tpp.ResourceStatus.READY))
        self._set_out_resources(many=outs)

    @override
    async def _execute_impl(self, in_resources, out_resources):
        self.seen = (in_resources.many, in_resources.one)
        for i, r in enumerate(out_resources.many):
            r.populate_data(f'out-{i}')
        return True, None


#
# Tests
#

class TestDummyTransitionCalculation:
    def test_repr(self):
        assert str(DummyTransitionCalculation(name='Dummy-A')) == (
            '<DummyTransitionCalculation Dummy-A : {} -> {}>')
        t = TwoStepTransitionCalculation(
            'Dummy-A', DummyResource('IN=0'), DummyResource('IN=1'),
            DummyResource('OUT=0')).compile()
        assert str(t) == (
            '<TwoStepTransitionCalculation Dummy-A : '
            '{first: <DummyResource id=DummyResource:IN=0 status=EMPTY data=empty>,'
            ' second: <DummyResource id=DummyResource:IN=1 status=EMPTY data=empty>} -> '
            '{only: <DummyResource id=DummyResource:OUT=0 status=EMPTY data=empty>}>')

    def test_hashable(self):
        s = DummyTransitionCalculation(name='Dummy-A')
        t = DummyTransitionCalculation(name='Dummy-B')
        assert s == s and s != t
        transition2name = {s: s.name, t: t.name}
        assert (transition2name[s], transition2name[t]) == ('Dummy-A', 'Dummy-B')

    def test_execute(self):
        r1 = DummyResource('IN=0').populate_data('d=111').update_status(tpp.ResourceStatus.READY)
        r2 = DummyResource('IN=1').populate_data('d=222').update_status(tpp.ResourceStatus.READY)
        r3 = DummyResource('OUT=0')
        t = DummyTransitionCalculation(
            'Dummy-A', dict(in0=r1, in1=r2), dict(out0=r3)).compile()
        assert str(t) == (
            '<DummyTransitionCalculation Dummy-A : '
            '{in0: <DummyResource id=DummyResource:IN=0 status=READY data=set>,'
            ' in1: <DummyResource id=DummyResource:IN=1 status=READY data=set>} -> '
            '{out0: <DummyResource id=DummyResource:OUT=0 status=EMPTY data=empty>}>')

        assert asyncio.run(t.execute()) == (True, None)

        assert str(t) == (
            '<DummyTransitionCalculation Dummy-A : '
            '{in0: <DummyResource id=DummyResource:IN=0 status=READY data=set>,'
            ' in1: <DummyResource id=DummyResource:IN=1 status=READY data=set>} -> '
            '{out0: <DummyResource id=DummyResource:OUT=0 status=READY data=set>}>')
        assert r3.data == 'by Dummy-A IN=0|IN=1'


class TestRetries:
    def test_a_later_attempt_succeeding_makes_the_whole_execute_succeed(self):
        r = DummyResource('OUT')
        t = FlakyTransitionCalculation('Flaky', r, fail_times=2, retries_count=3).compile()

        assert asyncio.run(t.execute()) == (True, None)
        assert t.attempts == 3
        assert r.status == tpp.ResourceStatus.READY

    def test_running_out_of_attempts_fails_and_keeps_the_last_reason(self):
        r = DummyResource('OUT')
        t = FlakyTransitionCalculation('Flaky', r, fail_times=9, retries_count=2).compile()

        assert asyncio.run(t.execute()) == (False, 'boom 2')
        assert t.attempts == 2
        assert (r.status, r.failed_reason) == (tpp.ResourceStatus.FAILED, 'boom 2')

    def test_terminate_spends_no_further_attempts(self):
        t = FlakyTransitionCalculation(
            'Flaky', DummyResource('OUT'), fail_times=9, retries_count=5).compile()
        t.terminate()

        assert asyncio.run(t.execute()) == (False, 'boom 1')
        assert t.attempts == 1


class TestListValuedResources:
    @staticmethod
    def _ready(name):
        return DummyResource(name).populate_data(f'd={name}').update_status(
            tpp.ResourceStatus.READY)

    def test_flatten(self):
        a, b, c = DummyResource('A'), DummyResource('B'), DummyResource('C')

        assert list(tpp.transition._flatten(
            {'one': a, 'many': [b], 'keyed': {'c': c}})) == [a, b, c]
        assert list(tpp.transition._flatten({})) == []

    def test_the_graph_sees_a_list_flat_while_execute_impl_sees_it_whole(self):
        ins = [self._ready('IN=0'), self._ready('IN=1')]
        outs = [DummyResource('OUT=0'), DummyResource('OUT=1')]
        t = FanInTransitionCalculation('Fan', ins, outs).compile()

        assert asyncio.run(t.execute()) == (True, None)

        assert list(t.in_resources_flat()) == [*ins, t._in_resources['one']]
        assert t.seen[0] == ins                                   # not flattened for the impl
        assert [r.status for r in outs] == [tpp.ResourceStatus.READY] * 2

    def test_pool_results_are_copied_back_member_by_member(self):
        outs = [DummyResource('OUT=0'), DummyResource('OUT=1')]
        t = FanInTransitionCalculation('Fan', [self._ready('IN=0')], outs).compile()
        theirs = [DummyResource(f'OUT={i}').populate_data(f'from-worker-{i}').update_status(
            tpp.ResourceStatus.READY) for i in range(2)]

        t.post_execute_populate_out_resource_data({'many': theirs})

        assert [r.data for r in outs] == ['from-worker-0', 'from-worker-1']

    def test_leaves(self):
        a, b, c, d = (DummyResource(k) for k in 'ABCD')
        one_dir = DirOfResources(x=b, y=c)
        leaves = lambda v: list(tpp.transition._leaves(v))

        assert leaves(a) == [a]
        assert leaves(one_dir) == [b, c]
        assert leaves([a, d]) == [a, d]
        assert leaves({'p': a, 'q': d}) == [a, d]
        # Each container must recurse, or the inner one reaches the graph in place of its leaves.
        assert leaves([a, [d]]) == [a, d]
        assert leaves([one_dir, a]) == [b, c, a]
        assert leaves({'p': [a], 'q': {'r': d}}) == [a, d]
        assert leaves({'p': one_dir}) == [b, c]
        assert leaves({'p': [one_dir, {'q': [a]}]}) == [b, c, a]
        assert leaves([]) == [] and leaves({}) == []

    def test_pool_results_are_copied_back_by_key(self):
        outs = {'p': DummyResource('OUT=P'), 'q': DummyResource('OUT=Q')}
        t = DummyTransitionCalculation('Dict', {}, dict(many=outs))
        theirs = {k: DummyResource(f'OUT={k}').populate_data(f'from-worker-{k}').update_status(
            tpp.ResourceStatus.READY) for k in ('p', 'q')}

        t.post_execute_populate_out_resource_data({'many': theirs})

        # Paired by key, not by position: a dict has no order to rely on.
        assert [outs[k].data for k in ('p', 'q')] == ['from-worker-p', 'from-worker-q']

    def test_repr_summarises_a_container_rather_than_listing_it(self):
        a, b = DummyResource('A'), DummyResource('B')

        as_list = repr(DummyTransitionCalculation('L', dict(many=[a, b]), {}))
        as_dict = repr(DummyTransitionCalculation('D', dict(many={'a': a, 'b': b}), {}))

        assert 'many: [2 x <DummyResource id=DummyResource:A' in as_list
        assert 'many: [2 x <DummyResource id=DummyResource:A' in as_dict


def test_set_retries_count_and_allow_multiprocess_pool():
    calls = []

    @tpp.stateless_transition()
    def flaky():
        calls.append(1)
        return (True, None) if len(calls) > 1 else (False, 'flaky')

    t = flaky('flaky').set_retries_count(2).set_allow_multiprocess_pool(True)

    # this instance only: the decorator's defaults stay for the next one; the retry really happens
    assert (t.allow_multiprocess_pool, t._retries_count) == (True, 2)
    assert (flaky('other').allow_multiprocess_pool, flaky('other')._retries_count) == (False, 1)
    assert asyncio.run(t.compile().execute()) == (True, None) and len(calls) == 2


@tpp.stateless_transition(allow_multiprocess_pool=True, retries_count=2)
async def append_suffix(suffix: str, in_src: DummyResource, out_dst: DummyResource) -> tuple[bool, str | None]:
    """Module level, so instances pickle."""
    out_dst.populate_data(in_src.data + suffix)
    return True, None


def test_stateless_transition():
    @tpp.stateless_transition()
    def fail():
        return False, 'boom'

    src = DummyResource('IN').populate_data('d').update_status(tpp.ResourceStatus.READY)
    dst = DummyResource('OUT')
    t = append_suffix('append', suffix='!', in_src=src, out_dst=dst).compile()

    # a class of the function's name; in_ / out_ prefixes dropped; decorator keywords reach the instance
    assert isinstance(append_suffix, type) and append_suffix.__name__ == 'append_suffix'
    assert append_suffix.__doc__ == 'Module level, so instances pickle.'
    assert [r.id.in_class_id for r in t.in_resources_flat()] == ['IN']
    assert [r.id.in_class_id for r in t.out_resources_flat()] == ['OUT']
    assert (t.allow_multiprocess_pool, t._retries_count) == (True, 2)
    assert asyncio.run(t.execute()) == (True, None)  # async fn awaited; config and the Resources themselves passed
    assert dst.data == 'd!'
    assert asyncio.run(fail('fail').compile().execute()) == (False, 'boom')  # no flags, plain fn
    assert (fail('fail').allow_multiprocess_pool, fail('fail')._retries_count) == (False, 1)
    # pickles by name, as the pool needs
    assert type(pickle.loads(pickle.dumps(t))) is append_suffix
    # misconfigured: fails at construction, not at execute time
    for wrong in ({'sufix': '!', 'in_src': src, 'out_dst': dst},  # misspelled
                  {'suffix': '!', 'in_src': src},  # missing
                  {'suffix': '!', 'in_src': src, 'out_dst': dst, 'in_more': src}):  # extra
        with pytest.raises(TypeError):
            append_suffix('append', **wrong)
    # malformed fn: fails when decorated
    with pytest.raises(AssertionError, match='keyword parameters only'):
        tpp.stateless_transition()(lambda **values: (True, None))
    with pytest.raises(AssertionError, match='transition name'):
        tpp.stateless_transition()(lambda name: (True, None))


def test_typed_transition():
    class CopyTransition(tpp.TypedTransition):
        suffix: str
        in_src: DummyResource
        in_more: dict
        out_dst: DummyResource

        @override
        async def _execute_impl(self, in_resources, out_resources):
            out_resources.dst.populate_data(in_resources.src.data + self.suffix)
            return True, None

    src = DummyResource('IN').populate_data('d').update_status(tpp.ResourceStatus.READY)
    more = {'k': DummyResource('MORE').populate_data('m').update_status(tpp.ResourceStatus.READY)}
    dst = DummyResource('OUT')
    t = CopyTransition('copy', suffix='!', in_src=src, in_more=more, out_dst=dst).compile()

    # in_ / out_ prefixes dropped in the wiring; a keyed dict flattens; config kept under its own name
    assert [r.id.in_class_id for r in t.in_resources_flat()] == ['IN', 'MORE']
    assert [r.id.in_class_id for r in t.out_resources_flat()] == ['OUT']
    assert t.suffix == '!' and t.name == 'copy'
    assert (t.allow_multiprocess_pool, t._retries_count) == (False, 1)  # base defaults
    assert asyncio.run(t.execute()) == (True, None)
    assert dst.data == 'd!'
    # missing or extra keyword
    with pytest.raises(AssertionError, match='takes'):
        CopyTransition('copy', suffix='!', in_src=src, in_more=more)
    with pytest.raises(AssertionError, match='takes'):
        CopyTransition('copy', suffix='!', in_src=src, in_more=more, out_dst=dst, extra=1)


def test_typed_transition_inheritance():
    class BaseCopy(tpp.TypedTransition, allow_multiprocess_pool=True, retries_count=3):
        in_src: DummyResource

        @override
        async def _execute_impl(self, in_resources, out_resources):
            return True, None

    class TaggedCopy(BaseCopy):
        tag: str
        out_dst: DummyResource

        def __post_init__(self):
            self._label = f'{self.tag}:{self.name}'

    class PlainCopy(BaseCopy):
        out_dst: DummyResource

    t = TaggedCopy('copy', tag='x', in_src=DummyResource('IN'), out_dst=DummyResource('OUT'))

    # parent annotations count; __post_init__ sees the fields; siblings keep their own names
    assert [r.id.in_class_id for r in t.in_resources_flat()] == ['IN']
    assert t._label == 'x:copy'
    with pytest.raises(AssertionError, match=r"PlainCopy takes \['in_src', 'out_dst'\]"):
        PlainCopy('copy', tag='x', in_src=DummyResource('IN'), out_dst=DummyResource('OUT'))
    # class keywords reach the parent's instances only: a subclass starts from the defaults again
    base = BaseCopy('base', in_src=DummyResource('IN'))
    assert (base.allow_multiprocess_pool, base._retries_count) == (True, 3)
    assert (t.allow_multiprocess_pool, t._retries_count) == (False, 1)
    # an own __init__ is refused when the class is made
    with pytest.raises(AssertionError, match='no __init__ of its own'):
        class OwnInit(tpp.TypedTransition):
            def __init__(self):
                pass


def test_lazy_available_kept_into_execute_impl():
    class CachedTransition(tpp.TypedTransition):
        out_dst: DummyResource

        @override
        def _check_lazy_available_impl(self, in_resources, out_resources):
            out_resources.dst.update_status(tpp.ResourceStatus.LAZY_AVAILABLE)

        @override
        async def _execute_impl(self, in_resources, out_resources):
            out_resources.dst.populate_data(
                'cached' if out_resources.dst.status == tpp.ResourceStatus.LAZY_AVAILABLE else 'computed')
            return True, None

    lazy = CachedTransition('lazy', out_dst=DummyResource('LAZY')).compile()
    cold = CachedTransition('cold', out_dst=DummyResource('COLD')).compile()
    lazy.check_lazy_available()

    # the marker reaches _execute_impl; READY after the run either way
    assert asyncio.run(lazy.execute()) == asyncio.run(cold.execute()) == (True, None)
    assert [(r.data, r.status) for t in (lazy, cold) for r in t.out_resources_flat()] == [
        ('cached', tpp.ResourceStatus.READY), ('computed', tpp.ResourceStatus.READY)]


def _in_cache(cache: dict, in_key: DummyResource) -> bool:
    return in_key.data in cache


@tpp.stateless_transition(lazy_check_fn=_in_cache)
def look_up(cache: dict, in_key: DummyResource, in_heavy: DummyResource, out_value: DummyResource) -> tuple[bool, None]:
    """Cached path reads only the key; the heavy input is None there."""
    if out_value.status == tpp.ResourceStatus.LAZY_AVAILABLE:
        out_value.populate_data(f'cached {cache[in_key.data]} heavy={in_heavy}')
    else:
        out_value.populate_data(f'computed from {in_heavy.data}')
    return True, None


def test_stateless_transition_lazy_check_fn():
    ready = lambda name, data: DummyResource(name).populate_data(data).update_status(tpp.ResourceStatus.READY)
    make = lambda key: look_up('look-up', cache={'k': 'v'}, in_key=ready('KEY', key), in_heavy=ready('HEAVY', 'h'),
                               out_value=DummyResource('VALUE')).compile()
    cached = make('k')
    cold = make('other')

    # cached: output marked, only the input the lazy fn names kept, fn takes the cached path with heavy=None
    cached.check_lazy_available()
    assert [r.id.in_class_id for r in cached.in_resources_flat()] == ['KEY']
    assert [r.status for r in cached.out_resources_flat()] == [tpp.ResourceStatus.LAZY_AVAILABLE]
    assert asyncio.run(cached.execute()) == (True, None)
    assert [r.data for r in cached.out_resources_flat()] == ['cached v heavy=None']
    # not cached: nothing marked or dropped, computed
    cold.check_lazy_available()
    assert [r.id.in_class_id for r in cold.in_resources_flat()] == ['KEY', 'HEAVY']
    assert asyncio.run(cold.execute()) == (True, None)
    assert [r.data for r in cold.out_resources_flat()] == ['computed from h']
    # a lazy fn naming a keyword fn does not have: fails when decorated
    with pytest.raises(AssertionError, match='beyond its keywords'):
        tpp.stateless_transition(lazy_check_fn=lambda in_other: True)(lambda in_key: (True, None))
