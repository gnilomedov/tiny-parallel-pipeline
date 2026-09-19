r"""
                     (nap_00)     ...     (nap_NN)       each worker naps five times and
                         |                    |          writes one `<began> .. <ended>`
                         v                    v          line per nap
         +----------[times_00]    ...    [times_NN]
         |               |                    |
         v               v                    v
      (total)     (synthesize_00) ...  (synthesize_NN)   32Mb of one worker's lines apiece,
         |               |                    |          held in RAM and never archived
         v               v                    v
    [total_txt]  [synthesized_00] ... [synthesized_NN]
         |               |                    |
         |               +---------+----------+
         |                         v
         |               (measure-ram-ahead-gc)          resident RAM while every
         |                         |                     copy is still alive
         |                         v
         |                  [ram_ahead_gc]               that reading, and the copies go here
         |                         |
         +------------+------------+
                      v
            (measure-ram-post-gc)                        a forcing milestone: it makes
                                                         nothing and asks for all of the
                                                         above, collects, prints again

`total` stores every worker's lines in one .zip, reads them back and adds up how long
the run actually spent asleep. A second run finds the members already stored: the naps
are skipped and the total is read back out, which is what ResourcesLazyZipArchive is for.
"""


import argparse
import asyncio
import datetime
import gc
import os
import random
import sys
import tempfile
from types import SimpleNamespace
from typing import override

import tiny_parallel_pipeline as tpp


NAPS_PER_WORKER = 5
NAP_SECONDS = (0.001, 2.000)
STAMP = '%Y-%m-%d %H:%M:%S.%f'      # human readable, down to the microsecond
SEPARATOR = ' .. '
TIMES_MEMBER = 'naps/{key}.txt'
TOTAL_MEMBER = 'total.txt'
SYNTHESIZED_BYTES = 32 * 1024 * 1024


class NapTimesResource(tpp.Resource):
    """.data: str -- one `<began> .. <ended>` line per nap this worker took."""

    def __init__(self, worker: str):
        super().__init__(in_class_id=worker)


class SynthesizedTimesResource(tpp.Resource):
    """.data: str -- one worker's lines blown up to SYNTHESIZED_BYTES, held in RAM and nowhere else."""

    def __init__(self, worker: str):
        super().__init__(in_class_id=worker)


class RamConsumptionMeasurementResource(tpp.Resource):
    """.data: str -- what the process held, as one line ready for the log."""

    def __init__(self, when: str):
        super().__init__(in_class_id=when)


class TotalTxtResource(tpp.Resource):
    """.data: str -- how long every worker slept, added up."""

    def __init__(self):
        super().__init__(in_class_id='total')


class NapTransition(tpp.TransitionCalculation):
    """One worker: a few short naps, each timed and written down."""

    def __init__(self, name: str, archive: tpp.ResourcesLazyZipArchive,
                 out_times: NapTimesResource):
        super().__init__(name)
        self._set_out_resources(times=out_times)
        self._worker = out_times.id.in_class_id
        self._archive = archive

    @override
    def _check_lazy_available_impl(self, in_resources: SimpleNamespace,
                                   out_resources: SimpleNamespace) -> None:
        if self._archive.is_stored(self._worker, TIMES_MEMBER):
            out_resources.times.update_status(tpp.ResourceStatus.LAZY_AVAILABLE)

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        if self._archive.populate_if_stored(self._worker, TIMES_MEMBER, str,
                                            out_resources.times):
            return True, None
        lines = []
        for _ in range(NAPS_PER_WORKER):
            began = datetime.datetime.now()
            await asyncio.sleep(random.uniform(*NAP_SECONDS))
            lines.append(f'{began:{STAMP}}{SEPARATOR}{datetime.datetime.now():{STAMP}}')
        out_resources.times.populate_data('\n'.join(lines))
        return True, None


class SynthesizeNapTransition(tpp.TransitionCalculation):
    """Blows one worker's lines up to something worth collecting."""

    def __init__(self, name: str, in_times: NapTimesResource,
                 out_synthesized: SynthesizedTimesResource):
        super().__init__(name)
        self._set_in_resources(times=in_times)
        self._set_out_resources(synthesized=out_synthesized)

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        block = in_resources.times.data + '\n'
        out_resources.synthesized.populate_data(block * (SYNTHESIZED_BYTES // len(block)))
        return True, None


class TotalTransition(tpp.TransitionCalculation):
    """Stores every worker's lines, then reads them back and adds the naps up."""

    def __init__(self, name: str, archive: tpp.ResourcesLazyZipArchive,
                 in_times: dict[str, NapTimesResource], out_total: TotalTxtResource):
        super().__init__(name)
        self._set_in_resources(times=in_times)
        self._set_out_resources(total=out_total)
        self._archive = archive

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        self._archive.add_all([(worker, r.data) for worker, r
                               in sorted(in_resources.times.items())
                               if not self._archive.is_stored(worker, TIMES_MEMBER)],
                              TIMES_MEMBER)
        # Read back rather than summed in memory: the archive is the record either way.
        slept = datetime.timedelta()
        for worker in self._archive.stored_keys(TIMES_MEMBER):
            for line in self._archive.read_or_none(worker, TIMES_MEMBER).splitlines():
                began, ended = (datetime.datetime.strptime(t, STAMP)
                                for t in line.split(SEPARATOR))
                slept += ended - began
        out_resources.total.populate_data(
            f'{len(self._archive.stored_keys(TIMES_MEMBER))} workers slept {slept}')
        return True, None


class MeasureRamAheadGcTransition(tpp.TransitionCalculation):
    """Last reader of the copies: they cannot be collected before it has taken its reading."""

    def __init__(self, name: str, in_synthesized: dict[str, SynthesizedTimesResource],
                 out_measurement: RamConsumptionMeasurementResource):
        super().__init__(name)
        self._set_in_resources(synthesized=in_synthesized)
        self._set_out_resources(measurement=out_measurement)

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        held = sum(len(r.data) for r in in_resources.synthesized.values())
        out_resources.measurement.populate_data(
            f'{_resident_mb():.0f} Mb resident, of it {held / 1024 ** 2:.0f} Mb '
            f'in {len(in_resources.synthesized)} copies')
        print(f'[INFO] ahead of GC: {out_resources.measurement.data}')
        return True, None


class MeasureRamPostGcMilestone(tpp.MilestoneTransition):
    """Reports and makes nothing: forcing it is what asks for everything upstream of it."""

    def __init__(self, name: str, in_total: TotalTxtResource,
                 in_ahead_measurement: RamConsumptionMeasurementResource):
        super().__init__(name)
        self._set_in_resources(total=in_total, ahead_measurement=in_ahead_measurement)
        self.force_schedule = True

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        gc.collect()
        print(f'[INFO] post GC: {_resident_mb():.0f} Mb resident')
        return True, None


class NapPipeline(tpp.Pipeline):
    """Ten nappers and the one writer that archives and totals them."""

    class Resources(tpp.ResourcesDir):
        def __init__(self, workers: int):
            self.times = {f'nap_{i:02d}': NapTimesResource(f'nap_{i:02d}')
                          for i in range(workers)}
            self.synthesized = {worker: SynthesizedTimesResource(worker) for worker in self.times}
            self.total = TotalTxtResource()
            self.ram_ahead_gc = RamConsumptionMeasurementResource('ram-ahead-gc')

    class Transitions(tpp.TransitionsDir):
        def __init__(self, res: 'NapPipeline.Resources',
                     archive: tpp.ResourcesLazyZipArchive):
            self.naps = [NapTransition(f'nap-{worker}', archive, r)
                         for worker, r in sorted(res.times.items())]
            self.synthesize = [SynthesizeNapTransition(f'synthesize-{worker}', res.times[worker], r)
                               for worker, r in sorted(res.synthesized.items())]
            self.total = TotalTransition('total', archive, res.times, res.total)
            self.ram_ahead_gc = MeasureRamAheadGcTransition('measure-ram-ahead-gc',
                                                            res.synthesized, res.ram_ahead_gc)
            self.ram_post_gc = MeasureRamPostGcMilestone('measure-ram-post-gc', res.total,
                                                         res.ram_ahead_gc)

    def __init__(self, args: argparse.Namespace):
        archive = tpp.ResourcesLazyZipArchive(args.zip)
        resources = NapPipeline.Resources(args.workers)
        super().__init__('nap', resources, NapPipeline.Transitions(resources, archive))


def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('-w', '--workers', type=int, default=32)
    ap.add_argument('-z', '--zip', default=os.path.join(tempfile.gettempdir(), 'naps.zip'),
                    help='run twice against the same one to see the naps skipped [%(default)s]')
    return ap.parse_args()


def _resident_mb() -> float:
    """What the process holds right now, not the peak: only a drop shows GC worked."""
    with open('/proc/self/statm') as f:
        return int(f.read().split()[1]) * os.sysconf('SC_PAGE_SIZE') / 1024 ** 2


def main() -> None:
    args = _get_args()
    pipeline = NapPipeline(args)
    lazy = sum(r.status == tpp.ResourceStatus.LAZY_AVAILABLE for r in pipeline.resources.times.values())
    is_ok, err_msg = asyncio.run(tpp.Executor(pipeline).compile_scheduler(pipeline.resources.total).run())

    print(f'[INFO] {args.workers - lazy} workers napped, {lazy} loaded from {args.zip}')
    print(f'[INFO] {pipeline.resources.total.data}')
    if not is_ok:
        print(f'[ERROR] {err_msg}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
