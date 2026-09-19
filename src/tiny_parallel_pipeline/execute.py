"""Scheduling and running: works out what can run now, then runs it."""


import asyncio
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
import multiprocessing.pool
import time
from typing import Self


from tiny_parallel_pipeline import (
    ResourceStatus, ResourceID, Resource, MilestoneTransition, TransitionCalculation)
from tiny_parallel_pipeline.transition import NamedResources
from tiny_parallel_pipeline.utils.text_utils import trim_list


class Scheduler:
    """Holds the dependency graph and tracks which transitions are ready to run."""
    @dataclass
    class _TransitionStatus:
        class _Status(Enum):
            UNSCHEDULED = auto()
            IN_PROGRESS = auto()
            SUCCEED = auto()
            # FAILED = auto()
        dependency_count: int = 0
        status: _Status = _Status.UNSCHEDULED
        failure_message: str | None = None

        def __repr__(self) -> str:
            return f'deps: {self.dependency_count} status: {self.status.name} msg: {self.failure_message}'

        def __str__(self) -> str:
            return repr(self)


    def __init__(self):
        # Input fields:
        self._id_2_target_resource: dict[ResourceID, Resource] = dict()
        self._transition_2_status: dict[TransitionCalculation, Scheduler._TransitionStatus] = dict()

        # From resource mappings
        self._resource_id_2_from_transition: dict[ResourceID, TransitionCalculation] = dict()
        self._resource_id_2_dependent_transitions: dict[ResourceID, list[TransitionCalculation]] = dict()

        self._compiled = False

        # Execution state:
        self._ready_to_execute_transitions: list[TransitionCalculation] = []
        self._want_transitions: set[TransitionCalculation] = set()

    def add_target_resource(self, *resources: Resource) -> Self:
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        for r in resources:
            self._id_2_target_resource[r.id] = r
        return self

    def add_transitions(self, *transitions: TransitionCalculation) -> Self:
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        for t in transitions:
            self._transition_2_status[t] = Scheduler._TransitionStatus()
        return self

    def compile(self) -> tuple[bool, str | None]:
        """Builds the graph. Fails if a resource has no producer or the dependencies loop."""

        if self._compiled:
            return True, None
        self._compiled = True

        for t in self._transition_2_status:  # Lazy available might cut off beginning of the DAG
            t.check_lazy_available()

        #
        # Build mapping: _resource_id_2 _from_ / _dependent_
        #

        for t, s in self._transition_2_status.items():
            seen_resource_ids = set()
            for r in t.in_resources_flat():
                if r.status != ResourceStatus.READY:
                    if r.id in seen_resource_ids:
                        return False, f'{r!r} multiple times in {t!r}'
                    seen_resource_ids.add(r.id)
                    s.dependency_count += 1
                    self._resource_id_2_dependent_transitions.setdefault(r.id, []).append(t)
            for r in t.out_resources_flat():
                if (from_t := self._resource_id_2_from_transition.get(r.id)) is not None:
                    return False, f'{r!r} out of multiple transitions {t!r} + {from_t!r}'
                self._resource_id_2_from_transition[r.id] = t

        #
        # DFS dependencies
        #

        resource_id_2_status: dict[ResourceID, str] = dict()
        dependency_stack: list[str] = []
        def dfs(r: Resource) -> str | None:
            status = resource_id_2_status.get(r.id, '')
            if r.status == ResourceStatus.READY or status == 'satisfied':
                return None
            elif status == 'in_stack':
                return '\n'.join(['Dependency loop'] + dependency_stack + [repr(r)])
            elif r.id not in self._resource_id_2_from_transition:
                return f'No transition to calculate {r!r}.'
            dependency_stack.append(repr(r))
            resource_id_2_status[r.id] = 'in_stack'
            t = self._resource_id_2_from_transition[r.id]
            self._want_transitions.add(t)
            dependency_stack.append(repr(t))
            for dr in t.in_resources_flat():
                err_msg = dfs(dr)
                if err_msg is not None:
                    return err_msg
            dependency_stack.pop()
            dependency_stack.pop()
            resource_id_2_status[r.id] = 'satisfied'
            return None

        # prepare DFS
        for t in self._transition_2_status:  # handle forcing Milestones
            if isinstance(t, MilestoneTransition) and t.force_schedule:
                self._id_2_target_resource.update({r.id: r for r in t.in_resources_flat()})

        # run DFS
        for r in sorted(self._id_2_target_resource.values()):
            if (err_msg := dfs(r)) is not None:
                return False, err_msg

        # post-process DFS
        res_2_from_milestone = {r.id: t
                                for t in self._transition_2_status if isinstance(t, MilestoneTransition)
                                for r in t.out_resources_flat()}
        res_scheduled = lambda r: self._resource_id_2_from_transition.get(r.id) in self._want_transitions
        for t in self._transition_2_status:  # handle opportunistic Milestones
            if isinstance(t, MilestoneTransition):
                for r in t.in_resources_flat():
                    if (from_m := res_2_from_milestone.get(r.id)) is not None:
                        raise ValueError(f'Milestone {from_m!r} not supposed to make {r!r} for another milestone {t!r}')
                if all(r.status == ResourceStatus.READY or res_scheduled(r)
                       for r in t.in_resources_flat()):
                    self._want_transitions.add(t)
        for dependents in self._resource_id_2_dependent_transitions.values():  # cleanup dependents, enable GC
            dependents[:] = [t for t in dependents if t in self._want_transitions]

        #
        # Build start-ready transitions
        #

        for t in self._want_transitions:
            s = self._transition_2_status[t]
            if s.dependency_count == 0:
                self._ready_to_execute_transitions.append(t)

        return True, None

    def get_ready_to_execute_transitions(self) -> list[TransitionCalculation]:
        return list(self._ready_to_execute_transitions)

    def mark_transitions_in_progress(self, *transitions: TransitionCalculation) -> None:
        for t in transitions:
            self._transition_2_status[t].status = Scheduler._TransitionStatus._Status.IN_PROGRESS
        self._ready_to_execute_transitions = [t for t in self._ready_to_execute_transitions
            if self._transition_2_status[t].status == Scheduler._TransitionStatus._Status.UNSCHEDULED]

    def on_transition_succeed(self, transition: TransitionCalculation) -> None:
        """Marks the outputs ready and unlocks the transitions that waited for them."""
        self._transition_2_status[transition].status = Scheduler._TransitionStatus._Status.SUCCEED
        self._want_transitions.remove(transition)
        for r in transition.out_resources_flat():
            assert r.status == ResourceStatus.READY
            for t in self._resource_id_2_dependent_transitions.get(r.id, ()):
                s = self._transition_2_status[t]
                assert s.status == Scheduler._TransitionStatus._Status.UNSCHEDULED
                s.dependency_count -= 1
                if s.dependency_count == 0 and t in self._want_transitions:
                    self._ready_to_execute_transitions.append(t)
        self._collect_garbage(transition)

    _collect_garbage_LOG_GC_PERIOD_S = 2.0
    _collect_garbage_LOG_GC_LAST_TS = 0.0

    def _collect_garbage(self, complete_transition: TransitionCalculation) -> None:
        """GC res.data once no transition left to run still has to read it."""
        for r in complete_transition.in_resources_flat():
            if not r.garbage_collection_allowed or r.id in self._id_2_target_resource:
                continue
            deps = self._resource_id_2_dependent_transitions.get(r.id)
            if deps is None:
                continue
            assert complete_transition in deps, f'{complete_transition!r} not in {deps!r}'
            deps.remove(complete_transition)
            if not deps:
                r.data = f'GC-ed after {complete_transition.name}'
                r.update_status(ResourceStatus.GARBAGE_COLLECTED)
                if (time.monotonic() - Scheduler._collect_garbage_LOG_GC_LAST_TS
                        >= Scheduler._collect_garbage_LOG_GC_PERIOD_S):
                    Scheduler._collect_garbage_LOG_GC_LAST_TS = time.monotonic()
                    print(f'[INFO] {r.id} {r.data}')


class Executor:
    """Runs ready transitions concurrently until every wanted transition has run."""
    def __init__(self, pipeline: 'Pipeline | _PipelineChain',
                 pool: multiprocessing.pool.Pool | None = None,
                 log_len: int = 100, log_period_s: float = 2.0):
        self._pipeline = pipeline.as_pipeline() if hasattr(pipeline, 'as_pipeline') else pipeline
        self._scheduler: Scheduler | None = None
        self._pool = pool
        self._log_len = log_len  # a trimmed list still has to show the ids, not just their edges
        self._log_period_s = log_period_s  # else a fast fan-out scrolls a line per completion

    def compile_scheduler(self, *targets: Resource) -> Self:
        """Compiles the pipeline for these targets and hands the Executor back, ready to run."""
        transitions = [t.compile() for t in self._pipeline.transitions.walk_values()]
        self._scheduler = Scheduler().add_transitions(*transitions).add_target_resource(
            *(targets or _pull_all_target_resources_from_transitions(transitions)))
        is_ok, err_msg = self._scheduler.compile()
        assert is_ok, err_msg
        return self

    async def run(self) -> tuple[bool, str | None]:
        """Main loop: start every ready transition, wait for the first to end, repeat."""
        assert self._scheduler is not None, f'{self._pipeline}: compile_scheduler first'
        log_info_last_ts = 0.0
        log_info_next_pending = True

        task2time_started: dict[asyncio.Task, float] = dict()
        task2transition: dict[asyncio.Task, TransitionCalculation] = dict()

        pending: set[asyncio.Task] = set()

        while len(self._scheduler._want_transitions) > 0 or len(pending) > 0:
            transition_bucket = self._scheduler.get_ready_to_execute_transitions()
            assert len(transition_bucket) > 0 or len(pending) > 0
            if transition_bucket and log_info_next_pending:
                print(f'       pending {len(pending) + len(transition_bucket): 5d} '
                      f'{trim_list(_task_names(pending), max_len=self._log_len)} '
                      f'+= {trim_list([t.name for t in transition_bucket], max_len=self._log_len)}')
            self._scheduler.mark_transitions_in_progress(*transition_bucket)
            for transition in transition_bucket:
                if self._pool is not None and transition.allow_multiprocess_pool:
                    task = _transition_as_asyncio_task_in_pool(transition, self._pool)
                else:
                    task = _transition_as_asyncio_task(transition)
                pending.add(task)
                task2time_started[task] = time.monotonic()
                task2transition[task] = transition

            done_tasks, still_pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            pending = still_pending
            done_names = _task_names(done_tasks, task2time_started)
            done_transitions = []
            for task in done_tasks:
                del task2time_started[task]
                del task2transition[task]
                transition, is_ok, err_msg = task.result()
                if not is_ok:
                    for t in pending:
                        task2transition[t].terminate()
                    await asyncio.gather(*pending, return_exceptions=True)
                    return False, f'{transition.name} failed: {err_msg}'
                self._scheduler.on_transition_succeed(transition)
                done_transitions.append(transition)

            if log_info_next_pending := time.monotonic() - log_info_last_ts >= self._log_period_s:
                log_info_last_ts = time.monotonic()
                ready_resources = [repr(r) for t in done_transitions for r in t.out_resources_flat()]
                print(f'[INFO] pending {len(pending): 5d} '
                      f'{trim_list(_task_names(pending), max_len=self._log_len)} '
                      f'-= {trim_list(done_names, max_len=self._log_len)}\n'
                      f'       ready resources: '
                      f'{trim_list(ready_resources, max_len=self._log_len)}')

        return True, None


def _pull_all_target_resources_from_transitions(
        transitions: list[TransitionCalculation]) -> Iterator[Resource]:
    """Everything the transitions touch: what a caller that named no target is asking for."""
    for t in transitions:
        yield from t.in_resources_flat()
        yield from t.out_resources_flat()


def _task_names(tasks: set[asyncio.Task], task2time_started: dict[asyncio.Task, float] | None = None) -> list[str]:
    if task2time_started is None:
        return sorted(t.get_name() for t in tasks)
    return sorted(
        f'{t.get_name()} took: '
        f'{timedelta(seconds=round(time.monotonic() - task2time_started[t], 3))}'
        for t in tasks)


def _transition_as_asyncio_task(
        transition: TransitionCalculation
        ) -> asyncio.Task[tuple[TransitionCalculation, bool, str | None]]:
    async def impl():
        is_ok, err_msg = await transition.execute()
        return transition, is_ok, err_msg
    return asyncio.create_task(impl(), name=transition.name)


def _transition_as_asyncio_task_in_pool(
        transition: TransitionCalculation, pool: multiprocessing.pool.Pool
        ) -> asyncio.Task[tuple[TransitionCalculation, bool, str | None]]:
    async def impl():
        loop = asyncio.get_event_loop()
        is_ok, err_msg, out_resources = await loop.run_in_executor(
            None,
            lambda: pool.apply(_run_transition_execute, (transition, )))
        if is_ok:
            transition.post_execute_populate_out_resource_data(out_resources)
        return transition, is_ok, err_msg
    return asyncio.create_task(impl(), name=transition.name)


def _run_transition_execute(transition: TransitionCalculation) -> tuple[bool, str | None, NamedResources]:
    is_ok, err_msg = asyncio.run(transition.execute())
    return is_ok, err_msg, transition._out_resources
