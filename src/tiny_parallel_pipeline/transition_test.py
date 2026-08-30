import asyncio
import os
from typing import override


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
        super().__init__(name, {'first': first}, {'only': only})
        self._set_in_resources(second=second)


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
            'Dummy-A', {'in0': r1, 'in1': r2}, {'out0': r3}).compile()
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
