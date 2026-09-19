import asyncio
import multiprocessing
import os
import pytest
from typing import override
from unittest import mock


import tiny_parallel_pipeline as tpp

from tiny_parallel_pipeline.execute import _pull_all_target_resources_from_transitions

from tiny_parallel_pipeline.resource_test import DummyResource
from tiny_parallel_pipeline.transition_test import (
    DummyTransitionCalculation, FlakyTransitionCalculation)


#
# Test-specific subclass
#

class LazyTransitionCalculation(DummyTransitionCalculation):
    """Reports its outputs already cached, so the graph can drop whatever only fed it."""
    @override
    def _check_lazy_available_impl(self, in_resources, out_resources):
        out_resources.out.update_status(tpp.ResourceStatus.LAZY_AVAILABLE)
        self._in_resources.clear()


class DummyMilestone(tpp.MilestoneTransition):
    """Reports what it read and produces nothing, which is the whole point of a milestone."""
    def __init__(self, name, in_name_2_resource: dict = None):
        super().__init__(name)
        self._set_in_resources(**(in_name_2_resource or {}))
        self.seen = None

    @override
    async def _execute_impl(self, in_resources, out_resources):
        self.seen = sorted(r.data for r in vars(in_resources).values())
        return True, None


class MakerMilestone(DummyMilestone):
    """Hands a resource on to another transition, which is what a milestone may not do."""
    def __init__(self, name, in_name_2_resource: dict = None, out_name_2_resource: dict = None):
        super().__init__(name, in_name_2_resource)
        self._set_out_resources(**(out_name_2_resource or {}))


class ForcedMilestone(DummyMilestone):
    """Wanted whatever the targets need, which is what drags its own inputs into the graph."""
    def __init__(self, name, in_name_2_resource: dict = None):
        super().__init__(name, in_name_2_resource)
        self.force_schedule = True


#
# Scheduler tests
#

class TestScheduler:
    def test_compile_ok_ready(self):
        r1 = DummyResource('A').update_status(tpp.ResourceStatus.READY)
        r2 = DummyResource('B')
        t = DummyTransitionCalculation('T1', {'a': r1}, {'b': r2})

        scheduler = tpp.Scheduler().add_target_resource(r1, r2).add_transitions(t)

        assert r1.id in scheduler._id_2_target_resource
        assert r2.id in scheduler._id_2_target_resource
        assert t in scheduler._transition_2_status
        assert scheduler.compile() == (True, None)

    def test_nothing_can_be_added_after_compile(self):
        scheduler = tpp.Scheduler().add_transitions(
            DummyTransitionCalculation('T1', {}, {'a': DummyResource('A')}))
        assert scheduler.compile() == (True, None)

        with pytest.raises(ValueError, match='Frozen after compiled.'):
            scheduler.add_target_resource(DummyResource('B'))

    def test_compile_ok_input_less(self):
        r1 = DummyResource('A')
        r2 = DummyResource('B')
        t = DummyTransitionCalculation('T1', {}, {'a': r1, 'b': r2})

        scheduler = _scheduler_wanting_everything(t)

        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg

    def test_compile_unreachable(self):
        r1 = DummyResource('A')
        r2 = DummyResource('B')
        scheduler = tpp.Scheduler().add_target_resource(r1, r2).add_transitions(
            DummyTransitionCalculation('T1', {'a': r1}, {'b': r2}))

        is_ok, err_msg = scheduler.compile()
        assert not is_ok
        assert err_msg.startswith('No transition to calculate ') and 'DummyResource:A' in err_msg

    def test_compile_loop(self):
        r1 = DummyResource('A')
        r2 = DummyResource('B')
        t1 = DummyTransitionCalculation('T1', {'a': r1}, {'b': r2})
        t2 = DummyTransitionCalculation('T2', {'b': r2}, {'a': r1})

        scheduler = _scheduler_wanting_everything(t1, t2)

        is_ok, err_msg = scheduler.compile()
        assert not is_ok

        def who(repr_line):  # the name out of a Resource or a TransitionCalculation repr
            head = repr_line.split(' ')[1]
            return head.split(':')[-1] if head.startswith('id=') else head

        # The stack walks the cycle back to where it started, naming each step on the way.
        lines = err_msg.split('\n')
        assert lines[0] == 'Dependency loop'
        assert [who(line) for line in lines[1:]] == ['A', 'T2', 'B', 'T1', 'A']

    def test_compile_rejects_duplicate_wiring(self):
        dup = DummyResource('A')
        t = DummyTransitionCalculation('T1', {'x': dup, 'y': dup}, {'b': DummyResource('B')})
        scheduler = _scheduler_wanting_everything(t)
        is_ok, err_msg = scheduler.compile()
        assert not is_ok and 'multiple times in' in err_msg

        seed = DummyResource('A').update_status(tpp.ResourceStatus.READY)
        out = DummyResource('B')
        scheduler = _scheduler_wanting_everything(
                DummyTransitionCalculation('T1', {'a': seed}, {'b': out}),
                DummyTransitionCalculation('T2', {'a': seed}, {'b': out}))
        is_ok, err_msg = scheduler.compile()
        # Both claimants named, so the message points at the clash and not at the whole map.
        assert not is_ok and 'out of multiple transitions' in err_msg
        assert 'T1' in err_msg and 'T2' in err_msg

    def test_ready_to_execute_transitions(self):
        r1 = DummyResource('A').update_status(tpp.ResourceStatus.READY)
        r2 = DummyResource('B')
        r3 = DummyResource('C')

        t12 = DummyTransitionCalculation('T12', {'a': r1}, {'b': r2})
        t23 = DummyTransitionCalculation('T23', {}, {'c': r3})
        scheduler = _scheduler_wanting_everything(t12, t23)
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg
        assert sorted([t.name for t in scheduler.get_ready_to_execute_transitions()]) == ['T12', 'T23']

        scheduler.mark_transitions_in_progress(t12)
        assert [t.name for t in scheduler.get_ready_to_execute_transitions()] == ['T23']

    def test_on_transition_succeed(self):
        r1 = DummyResource('A').update_status(tpp.ResourceStatus.READY).populate_data('data-A')
        r2 = DummyResource('B')
        r3 = DummyResource('C')

        scheduler = _scheduler_wanting_everything(
                DummyTransitionCalculation('T12', {'a': r1}, {'b': r2}),
                DummyTransitionCalculation('T23', {'b': r2}, {'c': r3}))
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg
        assert len(scheduler._want_transitions) == 2

        transition_bucket = scheduler.get_ready_to_execute_transitions()
        assert [t.name for t in transition_bucket] == ['T12']
        scheduler.mark_transitions_in_progress(*transition_bucket)
        assert len(scheduler.get_ready_to_execute_transitions()) == 0
        for t in transition_bucket:
            asyncio.run(t.execute())
        scheduler.on_transition_succeed(*transition_bucket)
        assert len(scheduler._want_transitions) == 1

        transition_bucket = scheduler.get_ready_to_execute_transitions()
        assert [t.name for t in transition_bucket] == ['T23']
        scheduler.mark_transitions_in_progress(*transition_bucket)
        assert len(scheduler.get_ready_to_execute_transitions()) == 0
        for t in transition_bucket:
            asyncio.run(t.execute())
        scheduler.on_transition_succeed(*transition_bucket)
        assert len(scheduler.get_ready_to_execute_transitions()) == 0
        assert len(scheduler._want_transitions) == 0


def test_check_lazy_available():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    mid, out = DummyResource('MID'), DummyResource('OUT')
    feeder = DummyTransitionCalculation('T-feeder', {'seed': seed}, {'mid': mid})
    feeder._execute_impl = mock.AsyncMock()
    executor = _executor(feeder, LazyTransitionCalculation('T-cached', {'mid': mid}, {'out': out}), targets=[out])

    assert asyncio.run(executor.run()) == (True, None)

    # T-cached still runs -- it is what loads the cache -- but it no longer needs `mid`.
    feeder._execute_impl.assert_not_called()
    assert (out.data, out.status) == ('by T-cached ', tpp.ResourceStatus.READY)


def test_an_input_is_collected_once_every_reader_has_run():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    mid, left, right = (DummyResource(k) for k in ('MID', 'LEFT', 'RIGHT'))
    executor = _executor(
        DummyTransitionCalculation('T-mid', {'seed': seed}, {'mid': mid}),
        DummyTransitionCalculation('T-left', {'mid': mid}, {'left': left}),
        DummyTransitionCalculation('T-right', {'mid': mid, 'left': left}, {'right': right}),
        targets=[left, right])

    assert asyncio.run(executor.run()) == (True, None)

    # MID had two readers in two waves: it survived the first and went after the second.
    assert mid.status == tpp.ResourceStatus.GARBAGE_COLLECTED
    assert mid.data == 'GC-ed after T-right'
    # LEFT is read by T-right too, but the caller asked for it, so it is theirs to keep.
    assert (left.status, left.data) == (tpp.ResourceStatus.READY, 'by T-left MID')
    # Nothing reads RIGHT, and SEED was READY before the graph was built, so neither is tracked.
    assert (right.status, right.data) == (tpp.ResourceStatus.READY, 'by T-right MID|LEFT')
    assert (seed.status, seed.data) == (tpp.ResourceStatus.READY, 'd')


def test_what_keeps_a_resource_from_being_collected():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    kept = DummyResource('KEPT', garbage_collection_allowed=False)
    shared, out, unread = (DummyResource(k) for k in ('SHARED', 'OUT', 'UNREAD'))
    executor = _executor(
        DummyTransitionCalculation('T-kept', {'seed': seed}, {'kept': kept}),
        DummyTransitionCalculation('T-shared', {'seed': seed}, {'shared': shared}),
        DummyTransitionCalculation('T-out', {'kept': kept, 'shared': shared}, {'out': out}),
        DummyTransitionCalculation('T-unread', {'shared': shared}, {'unread': unread}),
        targets=[out])

    assert asyncio.run(executor.run()) == (True, None)

    # Read by T-out and done with, but it said no.
    assert (kept.status, kept.data) == (tpp.ResourceStatus.READY, 'by T-kept SEED')
    # T-unread is not reached from the target, so its claim on SHARED does not hold it alive.
    assert shared.status == tpp.ResourceStatus.GARBAGE_COLLECTED
    assert (unread.status, unread.data) == (tpp.ResourceStatus.EMPTY, None)


def test_a_milestone_runs_on_inputs_that_are_wanted_anyway():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    mid, out = DummyResource('MID'), DummyResource('OUT')
    reader = DummyMilestone('M-reader', {'seed': seed, 'mid': mid})
    empty = DummyMilestone('M-empty')
    final = DummyMilestone('M-final', {'out': out})
    executor = _executor(
        DummyTransitionCalculation('T-mid', {'seed': seed}, {'mid': mid}),
        DummyTransitionCalculation('T-out', {'mid': mid}, {'out': out}),
        reader, empty, final, targets=[out])

    assert asyncio.run(executor.run()) == (True, None)

    # No out resource to be reached through, yet both ran: MID and SEED were made either way.
    assert reader.seen == ['by T-mid SEED', 'd']
    # Reading nothing, it has nothing to wait for, so it is wanted from the start.
    assert empty.seen == []
    # Reading the last target, it comes after every resource anyone asked for is ready.
    assert final.seen == ['by T-out MID']
    # The milestone counts as a reader, so MID outlived it rather than going with T-out.
    assert mid.status == tpp.ResourceStatus.GARBAGE_COLLECTED


def test_a_milestone_stays_out_when_an_input_is_not_wanted():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    aside, out = DummyResource('ASIDE'), DummyResource('OUT')
    milestone = DummyMilestone('M-aside', {'aside': aside})
    executor = _executor(
        DummyTransitionCalculation('T-out', {'seed': seed}, {'out': out}),
        DummyTransitionCalculation('T-aside', {'seed': seed}, {'aside': aside}),
        milestone, targets=[out])

    assert asyncio.run(executor.run()) == (True, None)

    # ASIDE is off the path to OUT: a milestone asks for no work, so its reader never runs.
    assert milestone.seen is None
    assert (aside.status, aside.data) == (tpp.ResourceStatus.EMPTY, None)


def test_a_milestone_may_not_feed_another_one():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    passed_on, out = DummyResource('PASSED_ON'), DummyResource('OUT')
    scheduler = tpp.Scheduler().add_transitions(
        DummyTransitionCalculation('T-out', {'seed': seed}, {'out': out}),
        DummyMilestone('M-reader', {'passed_on': passed_on}),
        MakerMilestone('M-maker', {'seed': seed}, {'passed_on': passed_on})
    ).add_target_resource(out)

    # Whether the reader is wanted would otherwise depend on the order the two were added.
    with pytest.raises(ValueError, match='not supposed to make'):
        scheduler.compile()


def test_a_forced_milestone_pulls_in_the_branch_it_reads():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    aside, out = DummyResource('ASIDE'), DummyResource('OUT')
    milestone = ForcedMilestone('M-aside', {'aside': aside})
    executor = _executor(
        DummyTransitionCalculation('T-out', {'seed': seed}, {'out': out}),
        DummyTransitionCalculation('T-aside', {'seed': seed}, {'aside': aside}),
        milestone, targets=[out])

    assert asyncio.run(executor.run()) == (True, None)

    # Same off-path ASIDE as above, but forcing the reader made the graph produce it.
    assert milestone.seen == ['by T-aside SEED']
    # Forcing made ASIDE a target of its own, which is what spares it from the collector.
    assert (aside.status, aside.data) == (tpp.ResourceStatus.READY, 'by T-aside SEED')


def test_a_forced_milestone_fails_compile_on_an_input_nobody_makes():
    seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
    orphan, out = DummyResource('ORPHAN'), DummyResource('OUT')
    scheduler = tpp.Scheduler().add_transitions(
        DummyTransitionCalculation('T-out', {'seed': seed}, {'out': out}),
        ForcedMilestone('M-orphan', {'orphan': orphan})
    ).add_target_resource(out)

    # Unforced the branch would just be left out; forcing asks for a resource that has no maker.
    is_ok, err_msg = scheduler.compile()
    assert not is_ok
    assert err_msg.startswith('No transition to calculate ') and 'DummyResource:ORPHAN' in err_msg


#
# Executor tests
#

class TestExecutor:
    def test_executor_runs_all_transitions(self):
        r1 = DummyResource('A').populate_data('d1').update_status(tpp.ResourceStatus.READY)
        r2 = DummyResource('B').populate_data('d2').update_status(tpp.ResourceStatus.READY)
        r3 = DummyResource('C')
        r4 = DummyResource('D')
        r5 = DummyResource('E')

        t1 = DummyTransitionCalculation('T13', {'a': r1}, {'c': r3})
        t2 = DummyTransitionCalculation('T24', {'b': r2}, {'d': r4})
        t3 = DummyTransitionCalculation('T145', {'a': r1, 'd': r4}, {'e': r5})

        asyncio.run(_executor(t1, t2, t3).run())

        # Wanting every resource is what keeps them all: none is collected.
        assert r3.status == tpp.ResourceStatus.READY
        assert r4.status == tpp.ResourceStatus.READY
        assert r5.status == tpp.ResourceStatus.READY

        assert r3.data == 'by T13 A'
        assert r4.data == 'by T24 B'
        assert r5.data == 'by T145 A|D'


    def test_run_stops_at_the_first_failure_and_terminates_what_is_pending(self):
        seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
        bad_out, slow_out = DummyResource('BAD'), DummyResource('SLOW')
        bad = FlakyTransitionCalculation('Bad', bad_out, fail_times=9, retries_count=1)
        slow = DummyTransitionCalculation('Slow', {'seed': seed}, {'slow': slow_out},
                                          simulate_async_sleep_period=0.05)

        assert asyncio.run(_executor(bad, slow).run()) == (False, 'Bad failed: boom 1')
        assert bad_out.status == tpp.ResourceStatus.FAILED
        # Nothing still in flight is left running once the run is doomed.
        assert slow._termination_requested

    def test_executor_runs_race_condition(self):
        resources = [DummyResource(f'r{i}', garbage_collection_allowed=False)
                     for i in range(10)]
        resources[0].populate_data(f'd0').update_status(tpp.ResourceStatus.READY)
        transitions = [DummyTransitionCalculation(
            f'T1', {'seed': resources[0]}, {'made': resources[1]},
            data_add_pid=True, allow_multiprocess_pool=True)]
        transitions += [
            DummyTransitionCalculation(
                    f'T{i}', {'seed': resources[0], 'made': resources[1]},
                    {'made': resources[i]},
                    data_add_pid=True, allow_multiprocess_pool=True)
                for i in range(2, len(resources))
        ]

        pool = multiprocessing.Pool(3)
        asyncio.run(_executor(*transitions, pool=pool).run())

        assert set(r.status for r in resources) == {tpp.ResourceStatus.READY}

        main_pid = os.getpid()
        transition_pids = set(r.data[1] for r in resources)
        assert main_pid not in transition_pids, f'{repr(main_pid)} {repr(transition_pids)}'
        assert len(transition_pids) > 1, f'{repr(main_pid)} {repr(transition_pids)}'


def _scheduler_wanting_everything(*transitions) -> tpp.Scheduler:
    """A scheduler with no target named, so every resource the transitions touch is wanted."""
    return tpp.Scheduler().add_transitions(*transitions).add_target_resource(
        *_pull_all_target_resources_from_transitions(transitions))


def _executor(*transitions, targets=(), pool=None) -> tpp.Executor:
    """These tests wire transitions, not trees, so the pipeline around them is throwaway."""
    class Transitions(tpp.TransitionsDir):
        def __init__(self):
            self.of_the_test = list(transitions)
    return tpp.Executor(tpp.Pipeline('test', tpp.ResourcesDir(), Transitions()),
                        pool).compile_scheduler(*targets)
