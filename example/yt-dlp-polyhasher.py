r"""
    [yt_dlp.url]*                        (poly_hash.write)
         |                                      |
         v                                      v
    (yt_dlp_wget)                        [poly_hash.cpp]
         |                                      |
         v                                      v
    [yt_dlp.bin]                         (poly_hash.build)
         |                                      |
         |                                      v
         |                               [poly_hash.so]
         |                                   |      |
         +--> (poly_hash.yt_dlp_hash) <------+      |
         |              |                           |
         |              v                           |
         |    [poly_hash.yt_dlp_hash] -------.      |
         |                                    |     |
         +--> (video.info) <- [video.url]*    |     |
         |              |                     |     |
         |              v                     |     |
         |        [video.info]                |     |
         |              |                     +-----+
         |              v                     |
         +--> (video.download) <- [video.url]*|
                        |                     |
                        v                     |
                  [video.file]                |
                        |                     |
                        +--> (poly_hash.video_hash)
                                     |
                                     v
                            [poly_hash.video]
"""


import argparse
import asyncio
import multiprocessing
import os
import sys
import sysconfig
import tempfile
from typing import override

import tiny_parallel_pipeline as tpp


_POLY_HASH_DIR = os.path.join(tempfile.gettempdir(), 'tiny_parallel_pipeline')
_POLY_HASH_SO = os.path.join(
    _POLY_HASH_DIR, f'poly_hash{sysconfig.get_config_var("EXT_SUFFIX")}')
_POLY_HASH_CPP = '''
#include <pybind11/pybind11.h>

#include <cstdint>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

namespace py = pybind11;


namespace {

constexpr std::uint64_t kBase = 1e6 + 3;
constexpr std::uint64_t kMod = 1e9 + 7;

std::uint64_t poly_hash_file(const std::string& path, std::int64_t calcs_count,
                             std::uint64_t beg_h) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open " + path);
    std::vector<unsigned char> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::uint64_t h = beg_h % kMod;
    for (std::int64_t i = 0, p = 0; i < calcs_count; ++i, ++p, p = p < std::ssize(bytes) ? p : 0)
        h = (h * kBase + bytes[p] + i) % kMod;
    return h;
}

}  // namespace


PYBIND11_MODULE(poly_hash, m) {
    m.def("poly_hash_file", &poly_hash_file,
          py::arg("path"), py::arg("calcs_count") = 1 << 20, py::arg("beg_h") = 0,
          py::call_guard<py::gil_scoped_release>());
}
'''


class CompileCppTransition(tpp.TransitionCalculation):
    """Compiles the input .cpp into the output .so, skipping when the .so is newer."""
    def __init__(self, name, allow_multiprocess_pool):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(in_resources) == 1
        assert len(out_resources) == 1
        cpp_path = in_resources[0].data
        so_path = out_resources[0].expect_ready_file_at
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
            if self._termination_requested:
                return False, 'terminated'
            print(f'[INFO] compiled {so_path}')
        out_resources[0].populate_data(so_path).update_status(tpp.ResourceStatus.READY)
        return True, None


class DownloadVideoTransition(tpp.TransitionCalculation):
    """Downloads the video with yt-dlp, using the formats found in the info text."""
    def __init__(self, name, allow_multiprocess_pool):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(in_resources) == 3
        assert len(out_resources) == 1
        out_file_path = out_resources[0].expect_ready_file_at
        yt_dlp_bin = in_resources[0].data
        video_url = in_resources[1].data
        info_txt = in_resources[2].data
        audio_fmt = self._get(info_txt, 'audio')
        video_fmt = self._get(info_txt, 'video')
        if audio_fmt is None or video_fmt is None:
            return False, f'No audio/video format in:\n{info_txt}'
        cmd = (f'{yt_dlp_bin} -f {audio_fmt}+{video_fmt} '
               f'"{video_url}" -o "{out_file_path}"')
        is_ok, err_msg = await tpp.run_shell(cmd) # %(ext)
        if not is_ok:
            return False, err_msg
        if self._termination_requested:
            return False, 'terminated'
        if not os.path.exists(out_file_path):
            return False, f'`{cmd}` left no {out_file_path}, merged into another container?'
        out_resources[0].populate_data(out_file_path)
        return True, None

    def _get(self, txt, token):
        lines = txt.splitlines()
        for line in lines:
            if token in line:
                tokens = [t for t in line.split(' ') if t and t != '|']
                return tokens[0]


class CalcPolyHashTransition(tpp.TransitionCalculation):
    """Hashes the input file in C++, off the event loop, so other steps keep running."""
    def __init__(self, name, rounds, allow_multiprocess_pool):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._rounds = rounds

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(in_resources) in (2, 3)  # file, poly-hash-so, [beginning hash]
        assert len(out_resources) == 1
        if _POLY_HASH_DIR not in sys.path:  # .so built by the poly-hash-cpp transition
            sys.path.insert(0, _POLY_HASH_DIR)
        import poly_hash
        beg_h = int(in_resources[2].data) if len(in_resources) == 3 else 0
        # C++ releases the GIL => the event loop keeps driving the other transitions.
        hash_value = await asyncio.to_thread(
            poly_hash.poly_hash_file, in_resources[0].data, self._rounds, beg_h)
        if self._termination_requested:
            return False, 'terminated'
        out_resources[0].populate_data(str(hash_value)).update_status(tpp.ResourceStatus.READY)
        return True, None


class YtDlHasherPipeline(tpp.Pipeline):
    class YtDlpResources(tpp.ResourcesDir):
        def __init__(self, args: argparse.Namespace):
            self.url = tpp.UrlStrResource('yt-dlp-url', args.yt_dlp_url)
            self.bin = tpp.FileResource('yt-dlp-bin', args.yt_dlp_local_bin)

    class PolyHashResources(tpp.ResourcesDir):
        def __init__(self):
            self.cpp = tpp.FileResource(
                'poly-hash-cpp', os.path.join(_POLY_HASH_DIR, 'poly_hash.cpp'))
            self.so = tpp.FileResource('poly-hash-so', _POLY_HASH_SO)
            self.yt_dlp_hash = tpp.TxtResource('poly-hash-yt-dlp')
            self.video = tpp.TxtResource('poly-hash-video')

    class VideoResources(tpp.ResourcesDir):
        def __init__(self, args: argparse.Namespace):
            self.url = tpp.UrlStrResource('video-url', args.video_url)
            self.info = tpp.TxtResource('video-info')
            self.file = tpp.FileResource('video-file', args.video_local_path)

    class Resources(tpp.ResourcesDir):
        def __init__(self, args: argparse.Namespace):
            self.yt_dlp = YtDlHasherPipeline.YtDlpResources(args)
            self.poly_hash = YtDlHasherPipeline.PolyHashResources()
            self.video = YtDlHasherPipeline.VideoResources(args)

    class CalcPolyHashTransitions(tpp.TransitionsDir):
        def __init__(self, args: argparse.Namespace, res: 'YtDlHasherPipeline.Resources'):
            self.write = (tpp.WriteTextFileTransition(
                    'poly-hash-write', _POLY_HASH_CPP, True)
                .set_out_resources(res.poly_hash.cpp))
            self.build = (CompileCppTransition('poly-hash-build', True)
                .set_in_resources(res.poly_hash.cpp)
                .set_out_resources(res.poly_hash.so))
            self.yt_dlp_hash = (CalcPolyHashTransition(
                    'poly-hash-yt-dlp', args.hash_rounds, True)
                .set_in_resources(res.yt_dlp.bin, res.poly_hash.so)
                .set_out_resources(res.poly_hash.yt_dlp_hash))
            self.video_hash = (CalcPolyHashTransition(
                    'poly-hash-video', args.hash_rounds, True)
                .set_in_resources(res.video.file, res.poly_hash.so, res.poly_hash.yt_dlp_hash)
                .set_out_resources(res.poly_hash.video))

    class VideoTransitions(tpp.TransitionsDir):
        def __init__(self, res: 'YtDlHasherPipeline.Resources'):
            self.info = (tpp.CaptureCliStdoutTransition(
                    'video-info', True, '{0}', '-F', '"{1}"')
                .set_in_resources(res.yt_dlp.bin, res.video.url)
                .set_out_resources(res.video.info))
            self.download = (DownloadVideoTransition('video-download', True)
                .set_in_resources(res.yt_dlp.bin, res.video.url, res.video.info)
                .set_out_resources(res.video.file))

    class Transitions(tpp.TransitionsDir):
        def __init__(self, args: argparse.Namespace, res: 'YtDlHasherPipeline.Resources'):
            self.yt_dlp_wget = (tpp.WgetUrlTransition('yt-dlp-wget', True, 'chmod 755')
                .set_in_resources(res.yt_dlp.url)
                .set_out_resources(res.yt_dlp.bin))
            self.poly_hash = YtDlHasherPipeline.CalcPolyHashTransitions(args, res)
            self.video = YtDlHasherPipeline.VideoTransitions(res)

    def __init__(self, args: argparse.Namespace):
        resources = YtDlHasherPipeline.Resources(args)
        super().__init__(
            resources=resources,
            transitions=YtDlHasherPipeline.Transitions(args, resources))


def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='YouTube downloader using parallel pipeline')
    ap.add_argument('-d', '--yt-dlp-url',
                    default='https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp',
                    help='URL to download yt-dlp binary from')
    ap.add_argument('-e', '--yt-dlp-local-bin',
                    default=os.path.join(tempfile.gettempdir(), 'yt-dlp'),
                    help='Local yt-dlp location')
    ap.add_argument('-v', '--video-url', required=True,
                    help='URL of the video to download')
    ap.add_argument('-o', '--video-local-path',
                    default=os.path.join(tempfile.gettempdir(), 'video.mp4'),
                    help='URL to download yt-dlp binary from')
    ap.add_argument('-r', '--hash-rounds',
                    type=int, default=2 ** 32,
                    help='Passes over the yt-dlp binary in the C++ polynomial hash')
    ap.add_argument('-p', '--pool-workers',
                    type=int, default=0,
                    help='Run allow_multiprocess_pool transitions in a pool of N processes')
    return ap.parse_args()


def main():
    args = _get_args()

    pipeline = YtDlHasherPipeline(args)
    pool = multiprocessing.Pool(args.pool_workers) if args.pool_workers else None
    executor = tpp.Executor(pipeline.scheduler(), pool)

    is_ok, err_msg = asyncio.run(executor.run())

    if pool is not None:
        pool.close()
        pool.join()

    def print_hash(label, resource):
        if resource.status == tpp.ResourceStatus.READY:
            print(f'[INFO] {label} hash: {resource.data}')
        else:
            reason = f': {resource.failed_reason}' if resource.failed_reason else ''
            print(f'[ERROR] {label} hash is {resource.status.name}{reason}', file=sys.stderr)

    print_hash('yt-dlp', pipeline.resources.poly_hash.yt_dlp_hash)
    print_hash('video ', pipeline.resources.poly_hash.video)

    if not is_ok:
        print(f'[ERROR] {err_msg}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
