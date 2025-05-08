import asyncio
from dataclasses import dataclass
from enum import Enum, auto
import multiprocessing
import queue
import threading


from tiny_parallel_pipeline import (
    ResourceStatus, ResourceID, Resource, TransitionCalculation)


class Scheduler:
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

        def __repr__(self):
            return (f'deps: {self.dependency_count} status: {self.status.name} '
                    f'msg: {self.failure_message}')

        def __str__(self):
            return repr(self)


    def __init__(self):
        self._id2resource: dict[ResourceID, Resource] = dict()
        self._transition2status: dict[TransitionCalculation, Scheduler._TransitionStatus] = dict()

        self._resource_id_2_from_transition: dict[ResourceID, TransitionCalculation] = dict()
        self._resource_id_2_dependent_transitions: dict[ResourceID,
                                                        list[TransitionCalculation]] = dict()

        self._ready_to_execute_transitions = []

        self._want_resource_ids = set()
        self._want_transitions = set()

        self._compiled = False

    def add_resources(self, *resources) -> 'Scheduler':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        for r in resources:
            self._id2resource[r.id] = r
        return self

    def add_transitions(self, *transitions) -> 'Scheduler':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        for t in transitions:
            self._transition2status[t] = Scheduler._TransitionStatus()
        return self

    def pull_all_resources_from_transitions(self) -> 'Scheduler':
        if self._compiled:
            raise ValueError('Frozen after compiled.')
        self._id2resource.clear()
        for t in self._transition2status.keys():
# #
#             if (t._in_resources is None):
#                 print(
# f't: {t.name}'
# )
# #
            for r in t._in_resources + t._out_resources:
                self._id2resource[r.id] = r
        return self

    def compile(self) -> tuple[bool, str | None]:
        if self._compiled:
            return (True, None)
        self._compiled = True

        for t, s in self._transition2status.items():
            seen_resource_ids = set();
            for r in t._in_resources:
                if r.status == ResourceStatus.EMPTY:
                    if r.id in seen_resource_ids:
                        return (False, f'{repr(r)} multiple times in {repr(t)}')
                    seen_resource_ids.add(r.id);
                    s.dependency_count += 1
                    if r.id not in self._resource_id_2_dependent_transitions:
                        self._resource_id_2_dependent_transitions[r.id] = []
                    self._resource_id_2_dependent_transitions[r.id].append(t)
            for r in t._out_resources:
                if r.id in self._resource_id_2_from_transition:
                    return (
                        False, f'{repr(r)} out of multiple transitions {repr(t)} and ' +
                        repr(self._resource_id_2_from_transition[r.id]))
                self._resource_id_2_from_transition[r.id] = t

        resource_id_2_status = dict();
        dependency_stack = []
        def dfs(r: Resource) -> str | None:
            if (r.status == ResourceStatus.READY or
                    resource_id_2_status.get(r.id, '') == 'satisfied'):
                return None
            elif resource_id_2_status.get(r.id, '') == 'in_stack':
                return '\n'.join(['Dependency loop'] + dependency_stack + [repr(r)])
            elif r.id not in self._resource_id_2_from_transition:
                return f'No transition to calculate {repr(r)}.'
            dependency_stack.append(repr(r))
            resource_id_2_status[r.id] = 'in_stack'
            t = self._resource_id_2_from_transition[r.id]
            self._want_transitions.add(t)
            dependency_stack.append(repr(t))
            for dr in t._in_resources:
                err_msg = dfs(dr)
                if err_msg is not None:
                    return err_msg
            dependency_stack.pop()
            dependency_stack.pop()
            resource_id_2_status[r.id] = 'satisfied'
            return None

        for r in self._id2resource.values():
            if r.status == ResourceStatus.EMPTY:
                self._want_resource_ids.add(r.id)

        for rid in sorted(self._want_resource_ids):
            err_msg = dfs(self._id2resource[rid])
            if err_msg is not None:
                return (False, err_msg)

        for t in self._want_transitions:
            s = self._transition2status[t]
            if s.dependency_count == 0:
                self._ready_to_execute_transitions.append(t)

        return (True, None)

    def get_ready_to_execute_transitions(self) -> None:
        return list(self._ready_to_execute_transitions)

    def mark_transitions_in_progress(self, *transitions: list[TransitionCalculation]) -> None:
        for t in transitions:
            self._transition2status[t].status = Scheduler._TransitionStatus._Status.IN_PROGRESS
        self._refilter_ready_to_execute_transitions()

    def on_transition_succeed(self, transition: TransitionCalculation) -> None:
        self._transition2status[transition].status = Scheduler._TransitionStatus._Status.SUCCEED
        self._want_transitions.remove(transition)
        for r in transition._out_resources:
            assert r.status == ResourceStatus.READY
            self._want_resource_ids.remove(r.id)
            if r.id not in self._resource_id_2_dependent_transitions:
                continue
            for t in self._resource_id_2_dependent_transitions[r.id]:
                s = self._transition2status[t]
                assert s.status == Scheduler._TransitionStatus._Status.UNSCHEDULED
                s.dependency_count -= 1
                if s.dependency_count == 0 and t in self._want_transitions:
                    self._ready_to_execute_transitions.append(t)
        self._refilter_ready_to_execute_transitions()

    def remaining_resources_count(self):
        return len(self._want_resource_ids)

    def _refilter_ready_to_execute_transitions(self):
        self._ready_to_execute_transitions = [t for t in self._ready_to_execute_transitions
            if self._transition2status[t].status == Scheduler._TransitionStatus._Status.UNSCHEDULED]


class Executor:
    def __init__(self, scheduler: Scheduler,
                 pool: multiprocessing.Pool = None):
        self._scheduler = scheduler
        self._pool = pool

    async def run(self):
        pending: set[asyncio.Task] = set()
        while self._scheduler.remaining_resources_count() > 0 or len(pending) > 0:
            transition_bucket = self._scheduler.get_ready_to_execute_transitions()
            assert len(transition_bucket) > 0 or len(pending) > 0
            self._scheduler.mark_transitions_in_progress(*transition_bucket)
            for transition in transition_bucket:
                if self._pool is not None and transition.allow_multiprocess_pool:
                    task = _transition_as_asyncio_task_in_pool(transition, self._pool)
                else:
                    task = _transition_as_asyncio_task(transition)
                pending.add(task)

            done_tasks, still_pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED)
            # asyncio.ALL_COMPLETED
            pending = still_pending
            for task in done_tasks:
                transition, is_ok, err_msg = task.result()
                if is_ok:
                    self._scheduler.on_transition_succeed(transition)
                # else:
                #     self._scheduler.on_transition_failed(transition)


def _transition_as_asyncio_task(
        transition: TransitionCalculation
        ) -> asyncio.Task[tuple[TransitionCalculation, bool, str]]:
    async def impl():
        is_ok, err_msg = await transition.execute()
        return transition, is_ok, err_msg
    return asyncio.create_task(impl())


def _transition_as_asyncio_task_in_pool(transition: TransitionCalculation,
                                        pool: multiprocessing.Pool) -> asyncio.Task:
    async def impl():
        loop = asyncio.get_event_loop()
        is_ok, err_msg, out_resources = await loop.run_in_executor(
            None,
            lambda: pool.apply(_run_transition_execute, (transition, )))
        if is_ok:
# #
#             print(
# f'\nout_resources:\n{repr(out_resources)}\n'
# )
# #
            transition.post_execute_populate_out_resource_data(out_resources)
        return transition, is_ok, err_msg
    return asyncio.create_task(impl())


def _run_transition_execute(transition: TransitionCalculation):
    is_ok, err_msg = asyncio.run(transition.execute())
    return is_ok, err_msg, transition._out_resources
