import argparse
import asyncio
from asyncio.subprocess import PIPE
import multiprocessing
import os
import subprocess
from typing import List, Optional
from typing import override

import tiny_parallel_pipeline as tpp


class UrlStrResource(tpp.Resource):
    def __init__(self, in_class_id: str, data_url: str):
        super().__init__(in_class_id=in_class_id)
        self.populate_data(data_url).update_status(tpp.ResourceStatus.READY)

class FileResource(tpp.Resource):
    def __init__(self, in_class_id: str, ready_file_data: str | None = None):
        super().__init__(in_class_id=in_class_id)
        if ready_file_data is not None:
            self.populate_data(ready_file_data).update_status(tpp.ResourceStatus.READY)

class TxtResource(tpp.Resource):
    def __init__(self, in_class_id: str, ready_txt_data: str | None = None):
        super().__init__(in_class_id=in_class_id)
        if ready_txt_data is not None:
            self.populate_data(ready_txt_data).update_status(tpp.ResourceStatus.READY)


class WgetUrlTransition(tpp.TransitionCalculation):
    def __init__(self,
                 name, out_file_path, allow_multiprocess_pool,
                 *post_wget_file_commands):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._out_file_path = out_file_path
        self._post_wget_file_commands = post_wget_file_commands

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(in_resources) == 1
        assert len(out_resources) == 1
        url_str = in_resources[0].data
        os.system(f'wget "{url_str}" -O "{self._out_file_path}"')
        for c in self._post_wget_file_commands:
            os.system(f'{c} "{self._out_file_path}"')
        out_resources[0].populate_data(self._out_file_path).update_status(tpp.ResourceStatus.READY)
        return True, None


class CliCommandStdoutTransition(tpp.TransitionCalculation):
    def __init__(self, name, allow_multiprocess_pool, *cmd: list[str]):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._cmd = list(cmd)
        self._timeout = None

    def set_timeout(self, timeout: int | None):
        self._timeout = timeout
        return self

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(out_resources) == 1
        # process = await asyncio.create_subprocess_exec(
        #     *(self._cmd),
        process = await asyncio.create_subprocess_shell(
            ' '.join(self._cmd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        stdout_text = stdout.decode('utf-8')
        stderr_text = stderr.decode('utf-8')
        # if process.returncode != 0:
        #     raise RuntimeError(f"yt-dlp command failed with error: {stderr_text}")
#
        print(
f'\n{self.__class__.__name__}\n{repr(self._cmd)}\n{' '.join(self._cmd)}\n=+=\n{stdout_text}\n=+=\n'
)
#
        out_resources[0].populate_data(stdout_text).update_status(tpp.ResourceStatus.READY)

        return True, None


class YtDlpTransition(tpp.TransitionCalculation):
    def __init__(self, name, out_file_path, allow_multiprocess_pool):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._out_file_path = out_file_path

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(in_resources) == 3
        assert len(out_resources) == 1
        yt_dlp_bin = in_resources[0].data
        video_url = in_resources[1].data
        info_txt = in_resources[2].data
        cmd = (f'{yt_dlp_bin} -f {self._get(info_txt, 'audio')}+{self._get(info_txt, 'video')} '
               f'"{video_url}" -o "{self._out_file_path}"')
# #
#         print(
# f'\n{cmd}\n'
# )
# #
        os.system(cmd) # %(ext)
        out_resources[0].populate_data(self._out_file_path)
        return True, None

    def _get(self, txt, token):
        lines = txt.splitlines()
        for line in lines:
            if token in line:
                tokens = [t for t in line.split(' ') if t and t != '|']
# #
#                 print(
# f'{txt}\n===\n\n{line}\n{repr(tokens)}\n{repr(tokens[0])}'
# )
# #
                return tokens[0]


def main():
    ap = argparse.ArgumentParser(description='YouTube downloader using parallel pipeline')
    ap.add_argument('-d', '--yt-dlp-url',
                    default='https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp',
                    help='URL to download yt-dlp binary from')
    ap.add_argument('-e', '--yt-dlp-local-bin',
                    default='/tmp/yt-dlp',
                    help='Local yt-dlp location')
    ap.add_argument('-v', '--video-url',
                    # default='youtu.be/tIeHLnjs5U8',
                    default='https://www.youtube.com/watch?v=tIeHLnjs5U8',
                    help='URL to download yt-dlp binary from')
    ap.add_argument('-o', '--video-local-path',
                    default='/tmp/video.mp4',
                    # default='youtu.be/tIeHLnjs5U8',
                    help='URL to download yt-dlp binary from')
    args = ap.parse_args()

    yt_dlp_bin_res = FileResource(args.yt_dlp_local_bin)
    wget_yt_dlp_transition = (WgetUrlTransition(
            'yt-dlp', args.yt_dlp_local_bin, True, 'chmod 755')
        .set_in_resources(UrlStrResource('yt-dlp', args.yt_dlp_url))
        .set_out_resources(yt_dlp_bin_res)
        .compile())

    video_url_res = UrlStrResource('video', args.video_url)
    video_info_txt_res = TxtResource(f'yt-info-{args.video_url}')
    yt_dlp_get_info_transition = (CliCommandStdoutTransition(
            'yt-dlp-F', True,
            args.yt_dlp_local_bin, '-F', f'"{video_url_res.data}"')
        # .set_timeout(10)
        .set_out_resources(video_info_txt_res)
        .compile())

    local_video_res = FileResource(args.video_local_path)
    yt_dlp_download_transition = (YtDlpTransition(
            f'yt-dlp-{video_url_res.data}', args.video_local_path, True)
        .set_in_resources(yt_dlp_bin_res, video_url_res, video_info_txt_res)
        .set_out_resources(local_video_res)
        .compile())

    scheduler = (tpp.Scheduler()
            .add_transitions(wget_yt_dlp_transition,
                             yt_dlp_get_info_transition,
                             yt_dlp_download_transition)
            .pull_all_resources_from_transitions())
    is_ok, err_msg = scheduler.compile()
    if not is_ok:
        print(f'Pipeline compilation failed: {err_msg}')
        return

    num_pool_workers = 3
    # pool = multiprocessing.Pool(num_pool_workers)
    # executor = tpp.Executor(scheduler, pool)
    executor = tpp.Executor(scheduler)

    asyncio.run(executor.run())

    # pool.close()
    # pool.join()


if __name__ == '__main__':
    # asyncio.run(main())
    main()
