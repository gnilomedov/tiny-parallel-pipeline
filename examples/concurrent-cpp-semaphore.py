r"""
        (gate.write)
             |
             v
        [gate.cpp]
             |
             v
        (gate.build)
             |
             v
         [gate.so]
             |
     +-------+-------+ ... +-------+       all released in one bucket, then each
     v       v       v     v       v       one is dispatched into C++ by the
 (wait_A)(wait_B)(wait_C)  ...  (wait_N)   chosen --strategy and blocks on the
     |       |       |     |       |       one static gate until all 8 arrive
     v       v       v     v       v
 [res_A] [res_B] [res_C]  ...  [res_N]
"""


import argparse
import asyncio
import functools
import os
import random
import sys
import sysconfig
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from typing import override

import tiny_parallel_pipeline as tpp


_STRATEGIES = ['to-thread', 'run-in-executor']
_EXECUTOR = None
_GATE_CPP = '''
#include <chrono>
#include <iostream>
#include <latch>
#include <random>
#include <string>
#include <thread>

#include <unistd.h>

#include <pybind11/pybind11.h>

namespace py = pybind11;

constexpr int kWaitForThreads = 8;
constexpr unsigned kSleepSeed = 29;

// static: one gate for the whole process, shared by every calling thread
static std::latch g_gate(kWaitForThreads);

void sleep_random(std::mt19937& rng) {
    std::this_thread::sleep_for(
        std::chrono::duration<double>(std::uniform_real_distribution<double>(0.0, 1.0)(rng)));
}

int wait_at_gate(const std::string& name) {
    const auto started = std::chrono::steady_clock::now();
    unsigned seed = kSleepSeed;  // per name, so the stagger repeats run to run
    for (char c : name)
        seed = seed * 131 + static_cast<unsigned char>(c);
    std::mt19937 rng(seed);
    sleep_random(rng);

    // one << keeps the line whole when threads print at once
    std::cout << ("[GATE] enter: thread " + name +
                  " (pid " + std::to_string(getpid()) +
                  " tid " + std::to_string(gettid()) + ")\\n") << std::flush;
    g_gate.arrive_and_wait();  // no timed wait on a latch: too few threads block here forever
    sleep_random(rng);         // stagger the exits the way the arrivals were staggered
    std::cout << ("[GATE] exit: thread " + name + "\\n") << std::flush;

    return static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - started).count());
}

PYBIND11_MODULE(gate, m) {
    m.attr("WAIT_FOR_THREADS") = kWaitForThreads;
    m.def("wait_at_gate", &wait_at_gate, py::arg("name"),
          py::call_guard<py::gil_scoped_release>());
}
'''


class CompileCppTransition(tpp.TransitionCalculation):
    """Compiles the `cpp_file` input into the `so_file` output, skipping when the .so is newer."""
    def __init__(self, name, allow_multiprocess_pool,
                 in_cpp_file: tpp.Resource, out_so_file: tpp.Resource):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._set_in_resources(cpp_file=in_cpp_file)
        self._set_out_resources(so_file=out_so_file)

    @override
    async def _execute_impl(self, in_resources, out_resources):
        cpp_path = in_resources.cpp_file.data
        so_path = out_resources.so_file.expect_ready_file_at
        if os.path.exists(so_path) and os.path.getmtime(so_path) > os.path.getmtime(cpp_path):
            print(f'[INFO] compile skipped, {so_path} is newer than the .cpp')
        else:
            if os.path.exists(so_path):
                os.remove(so_path)
            import pybind11
            print(f'[INFO] compiling {cpp_path}')
            is_ok, err_msg = await tpp.run_shell(
                f'c++ -O2 -Wall -shared -std=c++23 -fPIC '
                f'-I{pybind11.get_include()} -I{sysconfig.get_paths()["include"]} '
                f'"{cpp_path}" -o "{so_path}"')
            if not is_ok:
                return False, err_msg
            print(f'[INFO] compiled {so_path}')
        out_resources.so_file.populate_data(so_path).update_status(tpp.ResourceStatus.READY)
        return True, None


class WaitAtGateTransition(tpp.TransitionCalculation):
    """Sleeps, then sends one worker into the C++ gate the way `--strategy` asks for."""
    def __init__(self, name, worker_name, args: argparse.Namespace,
                 in_so_file: tpp.Resource, out_result_txt: tpp.Resource):
        super().__init__(name)
        self._set_in_resources(so_file=in_so_file)
        self._set_out_resources(result_txt=out_result_txt)
        self._worker_name = worker_name
        self._args = args

    @override
    async def _execute_impl(self, in_resources, out_resources):
        if self._args.gate_dir not in sys.path:  # .so built by the gate-build transition
            sys.path.insert(0, self._args.gate_dir)
        import gate
        await asyncio.sleep(random.Random(f'239-{self._worker_name}').random())
        # C++ releases the GIL, so a worker on a thread lets the others reach the gate too.
        if self._args.strategy == 'run-in-executor':  # our own pool, exactly --pool-size threads
            total_ms = await asyncio.get_running_loop().run_in_executor(
                _shared_executor(self._args.pool_size),
                functools.partial(gate.wait_at_gate, self._worker_name))
        else:  # to-thread: the loop's shared pool, min(32, cpu + 4) threads
            total_ms = await asyncio.to_thread(gate.wait_at_gate, self._worker_name)
        out_resources.result_txt.populate_data(
            f'{self._worker_name} passed the gate in {total_ms}ms')
        return True, None


class ConcurrentGatePipeline(tpp.Pipeline):
    class Resources(tpp.ResourcesDir):
        def __init__(self, args: argparse.Namespace):
            self.cpp = tpp.FileResource('gate-cpp', os.path.join(args.gate_dir, 'gate.cpp'))
            self.so = tpp.FileResource('gate-so', os.path.join(
                args.gate_dir, f'gate{sysconfig.get_config_var("EXT_SUFFIX")}'))
            self.results = [tpp.TxtResource(f'gate-result-{_worker_name(i)}')
                            for i in range(args.workers)]

    class Transitions(tpp.TransitionsDir):
        def __init__(self, args: argparse.Namespace, res: 'ConcurrentGatePipeline.Resources'):
            self.write = tpp.WriteTextFileTransition('gate-write', _GATE_CPP, False, res.cpp)
            self.build = CompileCppTransition('gate-build', False, res.cpp, res.so)
            self.wait = [
                WaitAtGateTransition(f'gate-wait-{_worker_name(i)}', _worker_name(i), args,
                                     res.so, r)
                for i, r in enumerate(res.results)]

    def __init__(self, args: argparse.Namespace):
        resources = ConcurrentGatePipeline.Resources(args)
        super().__init__(
            resources=resources,
            transitions=ConcurrentGatePipeline.Transitions(args, resources))


def _shared_executor(pool_size: int) -> ThreadPoolExecutor:
    """One pool for every worker, so `--pool-size` really is the cap they compete for."""
    global _EXECUTOR
    if _EXECUTOR is None:
        _EXECUTOR = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix='gate')
    return _EXECUTOR


def _worker_name(i: int) -> str:
    """`A`, `B`, ... `Z`, then `AA`, `AB`, ... - the label the C++ side prints."""
    name = ''
    while True:
        name = chr(ord('A') + i % 26) + name
        i = i // 26 - 1
        if i < 0:
            return name


def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description='Ways of dispatching a blocking C++ call, raced against one static gate')
    ap.add_argument('-d', '--gate-dir',
                    help='Where the generated .cpp and the built .so live, '
                         'by default a fresh system temp dir')
    ap.add_argument('-s', '--strategy',
                    default=_STRATEGIES[0], choices=_STRATEGIES,
                    help='How the blocking C++ call is dispatched off the event loop')
    ap.add_argument('-w', '--workers',
                    type=int, default=8,
                    help='Threads to send at the gate; the C++ side waits for a fixed count')
    ap.add_argument('-p', '--pool-size',
                    type=int, default=8,
                    help='Threads in the pool used by run-in-executor')
    args = ap.parse_args()
    args.gate_dir = (os.path.abspath(args.gate_dir) if args.gate_dir
                     else tempfile.mkdtemp(prefix='gate-'))
    return args


def main():
    args = _get_args()

    pipeline = ConcurrentGatePipeline(args)
    started = time.monotonic()
    is_ok, err_msg = asyncio.run(tpp.Executor(pipeline.scheduler()).run())
    took = time.monotonic() - started

    def print_result(resource):
        if resource.status == tpp.ResourceStatus.READY:
            print(f'[INFO] {resource.data}')
        else:
            reason = f': {resource.failed_reason}' if resource.failed_reason else ''
            print(f'[ERROR] {resource.id} is {resource.status.name}{reason}', file=sys.stderr)

    for r in pipeline.resources.results:
        print_result(r)

    import gate  # built and imported by the gate-wait transitions
    passed = sum(r.status == tpp.ResourceStatus.READY for r in pipeline.resources.results)
    print(f'[INFO] {args.strategy}: {passed}/{args.workers} workers passed a gate wanting '
          f'{gate.WAIT_FOR_THREADS}, took {took:.1f}s')

    if not is_ok:
        print(f'[ERROR] {err_msg}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
