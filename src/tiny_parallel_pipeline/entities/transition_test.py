import asyncio
import os
import pytest
from sortedcontainers import SortedSet
from typing import override


import tiny_parallel_pipeline as tpp


from tiny_parallel_pipeline.entities.resource_test import DummyResource


# --- Test-specific subclass ---


class DummyTransitionCalculation(tpp.TransitionCalculation):
    def __init__(self, name, in_res=None, out_res=None,
                 simulate_async_sleep_period=0.0,
                 data_add_pid=False,
                 allow_multiprocess_pool=False):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        in_res = in_res or [DummyResource('in')]
        self.set_in_resources(*in_res)
        out_res = out_res or [DummyResource('out')]
        self.set_out_resources(*out_res)
        self._simulate_async_sleep_period = simulate_async_sleep_period
        self._data_add_pid = data_add_pid

    @override
    async def _execute_impl(self, in_resources, out_resources):
        if self._simulate_async_sleep_period > 0.0:
            await asyncio.sleep(self._simulate_async_sleep_period)
        in_info = '|'.join([r.id.in_class_id for r in in_resources])
        for r in out_resources:
            data = f'by {self._name} {in_info}'
            if self._data_add_pid:
                data = (data, os.getpid())
            r.populate_data(data)
        return True, None


# --- Tests ---

class TestDummyTransitionCalculation:
    def test_init(self):
        t = DummyTransitionCalculation(name='Dummy-A')
        assert str(t) == (
            '<DummyTransitionCalculation Dummy-A : '
            '[<DummyResource id=DummyResource:in status=EMPTY data=empty>] -> '
            '[<DummyResource id=DummyResource:out status=EMPTY data=empty>]>')

    def test_fluent(self):
        t = (DummyTransitionCalculation(name='Dummy-A')
                .set_in_resources(DummyResource('IN=0'), DummyResource('IN=1'))
                .set_out_resources(DummyResource('OUT=0'))
                .compile())
        assert str(t) == (
            '<DummyTransitionCalculation Dummy-A : '
            '[<DummyResource id=DummyResource:IN=0 status=EMPTY data=empty>,'
            ' <DummyResource id=DummyResource:IN=1 status=EMPTY data=empty>] -> '
            '[<DummyResource id=DummyResource:OUT=0 status=EMPTY data=empty>]>')

    def test_hashable(self):
        s = (DummyTransitionCalculation(name='Dummy-A')
                .set_in_resources(DummyResource('IN=0'))
                .extend_in_resources(DummyResource('IN=1'))
                .set_out_resources(DummyResource('OUT=2'))
                .compile())
        t = (DummyTransitionCalculation(name='Dummy-B')
                .set_in_resources(DummyResource('IN=3'))
                .set_out_resources(DummyResource('OUT=4'))
                .compile())
        assert s == s
        assert s != t

        transition2name = dict()
        transition2name[s] = s.name
        transition2name[t] = t.name

        assert transition2name[s] == 'Dummy-A'
        assert transition2name[t] == 'Dummy-B'

    # @pytest.mark.asyncio
    def test_execute(self):
        r1 = (DummyResource('IN=0')
            .populate_data('d=111')
            .update_status(tpp.ResourceStatus.READY))
        r2 = (DummyResource('IN=1')
            .populate_data('d=222')
            .update_status(tpp.ResourceStatus.READY))
        r3 = DummyResource('OUT=0')
        t = (DummyTransitionCalculation(name='Dummy-A')
                .set_in_resources(r1, r2)
                .set_out_resources(r3)
                .compile())

        assert str(t) == (
            '<DummyTransitionCalculation Dummy-A : '
            '[<DummyResource id=DummyResource:IN=0 status=READY data=set>,'
            ' <DummyResource id=DummyResource:IN=1 status=READY data=set>] -> '
            '[<DummyResource id=DummyResource:OUT=0 status=EMPTY data=empty>]>')

        asyncio.run(t.execute())

        assert str(t) == (
            '<DummyTransitionCalculation Dummy-A : '
            '[<DummyResource id=DummyResource:IN=0 status=READY data=set>,'
            ' <DummyResource id=DummyResource:IN=1 status=READY data=set>] -> '
            '[<DummyResource id=DummyResource:OUT=0 status=READY data=set>]>')
        assert r3.data == 'by Dummy-A IN=0|IN=1'
