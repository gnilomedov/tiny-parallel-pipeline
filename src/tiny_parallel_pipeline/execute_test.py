import asyncio
import multiprocessing
import os


import tiny_parallel_pipeline as tpp

from tiny_parallel_pipeline.resource_test import DummyResource
from tiny_parallel_pipeline.transition_test import (
    DummyTransitionCalculation, FlakyTransitionCalculation)


#
# Scheduler tests
#

class TestScheduler:
    def test_compile_ok_ready(self):
        r1 = DummyResource('A').update_status(tpp.ResourceStatus.READY)
        r2 = DummyResource('B')
        t = DummyTransitionCalculation('T1', {'a': r1}, {'b': r2})

        scheduler = tpp.Scheduler().add_resources(r1, r2).add_transitions(t)

        assert r1.id in scheduler._id2resource
        assert r2.id in scheduler._id2resource
        assert t in scheduler._transition2status

        is_ok, err_msg = scheduler.compile()
        assert is_ok
        assert err_msg is None

    def test_compile_ok_input_less(self):
        r1 = DummyResource('A')
        r2 = DummyResource('B')
        t = DummyTransitionCalculation('T1', {}, {'a': r1, 'b': r2})

        scheduler = tpp.Scheduler().add_transitions(t).pull_all_resources_from_transitions()

        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg

    def test_compile_unreachable(self):
        r1 = DummyResource('A')
        r2 = DummyResource('B')
        t = DummyTransitionCalculation('T1', {'a': r1}, {'b': r2})

        scheduler = tpp.Scheduler().add_resources(r1, r2).add_transitions(t)

        assert r1.id in scheduler._id2resource
        assert r2.id in scheduler._id2resource
        assert t in scheduler._transition2status

        is_ok, err_msg = scheduler.compile()
        assert not is_ok
        assert err_msg == 'No transition to calculate <DummyResource id=DummyResource:A status=EMPTY data=empty>.'

    def test_compile_loop(self):
        r1 = DummyResource('A')
        r2 = DummyResource('B')
        t1 = DummyTransitionCalculation('T1', {'a': r1}, {'b': r2})
        t2 = DummyTransitionCalculation('T2', {'b': r2}, {'a': r1})

        scheduler = tpp.Scheduler().add_transitions(t1, t2).pull_all_resources_from_transitions()

        is_ok, err_msg = scheduler.compile()
        assert not is_ok
        assert err_msg.split('\n') == [
                'Dependency loop',
                '<DummyResource id=DummyResource:A status=EMPTY data=empty>',
                '<DummyTransitionCalculation T2 : {b: <DummyResource id=DummyResource:B '
                    'status=EMPTY data=empty>} -> {a: <DummyResource id=DummyResource:A '
                    'status=EMPTY data=empty>}>',
                '<DummyResource id=DummyResource:B status=EMPTY data=empty>',
                '<DummyTransitionCalculation T1 : {a: <DummyResource id=DummyResource:A '
                    'status=EMPTY data=empty>} -> {b: <DummyResource id=DummyResource:B '
                    'status=EMPTY data=empty>}>',
                '<DummyResource id=DummyResource:A status=EMPTY data=empty>',
            ]

    def test_compile_rejects_duplicate_wiring(self):
        dup = DummyResource('A')
        t = DummyTransitionCalculation('T1', {'x': dup, 'y': dup}, {'b': DummyResource('B')})
        scheduler = tpp.Scheduler().add_transitions(t).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert not is_ok and 'multiple times in' in err_msg

        seed = DummyResource('A').update_status(tpp.ResourceStatus.READY)
        out = DummyResource('B')
        scheduler = tpp.Scheduler().add_transitions(
                DummyTransitionCalculation('T1', {'a': seed}, {'b': out}),
                DummyTransitionCalculation('T2', {'a': seed}, {'b': out})
            ).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert not is_ok and 'out of multiple transitions' in err_msg

    def test_ready_to_execute_transitions(self):
        r1 = DummyResource('A').update_status(tpp.ResourceStatus.READY)
        r2 = DummyResource('B')
        r3 = DummyResource('C')

        scheduler = tpp.Scheduler().add_transitions(
                DummyTransitionCalculation('T12', {'a': r1}, {'b': r2}),
                DummyTransitionCalculation('T23', {'b': r2}, {'c': r3})
            ).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg
        assert [t.name for t in scheduler.get_ready_to_execute_transitions()] == ['T12']

        t12 = DummyTransitionCalculation('T12', {'a': r1}, {'b': r2})
        t23 = DummyTransitionCalculation('T23', {}, {'c': r3})
        scheduler = tpp.Scheduler().add_transitions(t12, t23).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg
        assert sorted([t.name for t in scheduler.get_ready_to_execute_transitions()]) == ['T12', 'T23']

        scheduler.mark_transitions_in_progress(t12)
        assert [t.name for t in scheduler.get_ready_to_execute_transitions()] == ['T23']

    def test_on_transition_succeed(self):
        r1 = DummyResource('A').update_status(tpp.ResourceStatus.READY).populate_data('data-A')
        r2 = DummyResource('B')
        r3 = DummyResource('C')

        scheduler = tpp.Scheduler().add_transitions(
                DummyTransitionCalculation('T12', {'a': r1}, {'b': r2}),
                DummyTransitionCalculation('T23', {'b': r2}, {'c': r3})
            ).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg
        assert scheduler.remaining_resources_count() == 2

        transition_bucket = scheduler.get_ready_to_execute_transitions()
        assert [t.name for t in transition_bucket] == ['T12']
        scheduler.mark_transitions_in_progress(*transition_bucket)
        assert len(scheduler.get_ready_to_execute_transitions()) == 0
        for t in transition_bucket:
            asyncio.run(t.execute())
        scheduler.on_transition_succeed(*transition_bucket)
        assert scheduler.remaining_resources_count() == 1

        transition_bucket = scheduler.get_ready_to_execute_transitions()
        assert [t.name for t in transition_bucket] == ['T23']
        scheduler.mark_transitions_in_progress(*transition_bucket)
        assert len(scheduler.get_ready_to_execute_transitions()) == 0
        for t in transition_bucket:
            asyncio.run(t.execute())
        scheduler.on_transition_succeed(*transition_bucket)
        assert len(scheduler.get_ready_to_execute_transitions()) == 0
        assert scheduler.remaining_resources_count() == 0


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

        t1 = DummyTransitionCalculation('T13', {'a': r1}, {'c': r3}, data_add_pid=True)
        t2 = DummyTransitionCalculation('T24', {'b': r2}, {'d': r4}, data_add_pid=True)
        t3 = DummyTransitionCalculation('T145', {'a': r1, 'd': r4}, {'e': r5},
                                        data_add_pid=True)

        scheduler = tpp.Scheduler().add_transitions(t1, t2, t3).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg

        executor = tpp.Executor(scheduler)
        asyncio.run(executor.run())

        assert r3.status == tpp.ResourceStatus.READY
        assert r4.status == tpp.ResourceStatus.READY
        assert r5.status == tpp.ResourceStatus.READY

        assert r3.data[0] == 'by T13 A'
        assert r4.data[0] == 'by T24 B'
        assert r5.data[0] == 'by T145 A|D'


    def test_run_stops_at_the_first_failure_and_terminates_what_is_pending(self):
        seed = DummyResource('SEED').populate_data('d').update_status(tpp.ResourceStatus.READY)
        bad_out, slow_out = DummyResource('BAD'), DummyResource('SLOW')
        bad = FlakyTransitionCalculation('Bad', bad_out, fail_times=9, retries_count=1)
        slow = DummyTransitionCalculation('Slow', {'seed': seed}, {'slow': slow_out},
                                          simulate_async_sleep_period=0.05)

        scheduler = tpp.Scheduler().add_transitions(
            bad, slow).pull_all_resources_from_transitions()
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg

        assert asyncio.run(tpp.Executor(scheduler).run()) == (False, 'Bad failed: boom 1')
        assert bad_out.status == tpp.ResourceStatus.FAILED
        # Nothing still in flight is left running once the run is doomed.
        assert slow._termination_requested

    def test_executor_runs_race_condition(self):
        resources = [DummyResource(f'r{i}') for i in range(10)]
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

        scheduler = (tpp.Scheduler()
            .add_transitions(*transitions)
            .pull_all_resources_from_transitions())
        is_ok, err_msg = scheduler.compile()
        assert is_ok, err_msg

        num_pool_workers = 3
        pool = multiprocessing.Pool(num_pool_workers)
        executor = tpp.Executor(scheduler, pool)
        asyncio.run(executor.run())

        assert set(r.status for r in resources) == {tpp.ResourceStatus.READY}

        main_pid = os.getpid()
        transition_pids = set(r.data[1] for r in resources)
        assert main_pid not in transition_pids, f'{repr(main_pid)} {repr(transition_pids)}'
        assert len(transition_pids) > 1, f'{repr(main_pid)} {repr(transition_pids)}'
