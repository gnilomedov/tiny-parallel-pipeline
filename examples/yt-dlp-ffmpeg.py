r"""
 [yt_dlp_url]*
      |
      v
 (wget_yt_dlp)
      |
      v
 [yt_dlp_bin] --------------------.
      |                            |
      v                            v
 (fetch_info) <- [url]*     (fetch_metadata) <- [url]*
      |                            |
      v                            v
 [yt_entry_info]          [yt_entry_metadata]
      |                            |
      v                            |
 (download_audio_file) <- [url]*   |
      |  ^-- [yt_dlp_bin]          |
      v                            |
 [audio_file]                      |
      |                            |
      +--> (convert_mp3_file) <----+
                   |
                   v
              [mp3_file]
"""


import argparse
import asyncio
import multiprocessing
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime
from typing import override


try:
    from ffmpeg_yt_dlp_lg_channels import CHANNEL_ID_2_SHORT_NAME
except ImportError:
    CHANNEL_ID_2_SHORT_NAME = {}


import tiny_parallel_pipeline as tpp


class YtInfo:
    """The audio format to ask yt-dlp for, derived from its `-F` text."""
    F_AUDIO = ['139', '233-1', '249-1', '233', '234'] + ['dash_webm-5']

    def __init__(self, info_txt: str):
        self.audio_formats = [self._token(l) for l in info_txt.splitlines() if 'audio only' in l]

    @property
    def audio_format(self) -> str | None:
        for f in YtInfo.F_AUDIO:
            if f in self.audio_formats:
                return f
        return self.audio_formats[0] if self.audio_formats else None

    def _token(self, line: str) -> str:
        return [t for t in line.split(' ') if t and t != '|'][0]


class FetchYtDlpInfoTransition(tpp.TransitionCalculation):
    """Runs one read-only `yt-dlp` query on `url_str`, echoes it and stores it in `entry_txt`."""
    def __init__(self, name, cmd_template, in_yt_dlp_bin: tpp.Resource,
                 in_url_str: tpp.Resource, out_entry_txt: tpp.Resource):
        super().__init__(name, allow_multiprocess_pool=True, retries_count=3)
        self._set_in_resources(yt_dlp_bin=in_yt_dlp_bin, url_str=in_url_str)
        self._set_out_resources(entry_txt=out_entry_txt)
        self._cmd_template = cmd_template

    @override
    async def _execute_impl(self, in_resources, out_resources):
        is_ok, out = await _run_shell(
            self._cmd_template.format(in_resources.yt_dlp_bin.data,
                                      in_resources.url_str.data), capture=True)
        if not is_ok:
            return False, out
        print(out, file=sys.stderr)
        out_resources.entry_txt.populate_data(out)
        return True, None


class DownloadAudioFileTransition(tpp.TransitionCalculation):
    """Asks yt-dlp for the output file name, fetches the audio, yields it as `audio_file`."""
    def __init__(self, name, in_yt_dlp_bin: tpp.Resource, in_info_txt: tpp.Resource,
                 in_url_str: tpp.Resource, out_audio_file: tpp.Resource):
        super().__init__(name, allow_multiprocess_pool=True)
        self._set_in_resources(yt_dlp_bin=in_yt_dlp_bin, info_txt=in_info_txt,
                               url_str=in_url_str)
        self._set_out_resources(audio_file=out_audio_file)

    @override
    async def _execute_impl(self, in_resources, out_resources):
        yt_dlp_bin: str = in_resources.yt_dlp_bin.data
        url_str: str = in_resources.url_str.data
        download_format: str | None = YtInfo(in_resources.info_txt.data).audio_format
        if download_format is None:
            return False, f'No audio format found in:\n{in_resources.info_txt.data}'
        is_ok, file_name = await _run_shell(  # file_name: str
            f"{yt_dlp_bin} --print filename -f {download_format} '{url_str}'", capture=True)
        if not is_ok:
            return False, file_name
        is_ok, err_msg = await _run_shell(
            self._yt_dlp_cmd(yt_dlp_bin, url_str, download_format))
        if not is_ok:
            return False, err_msg
        out_resources.audio_file.populate_data(file_name)
        return True, None

    @staticmethod
    def _yt_dlp_cmd(yt_dlp_bin: str, url_str: str, download_format: str) -> str:
        return f"{yt_dlp_bin} -N 4 -f {download_format} '{url_str}'"


class ConvertMp3FileTransition(tpp.TransitionCalculation):
    """Re-encodes the `audio_file` input to a 1.9x accelerated 32k .mp3 as `mp3_file`."""
    def __init__(self, name, in_audio_file: tpp.Resource, in_metadata_txt: tpp.Resource,
                 out_mp3_file: tpp.Resource):
        super().__init__(name, allow_multiprocess_pool=True)
        self._set_in_resources(audio_file=in_audio_file, metadata_txt=in_metadata_txt)
        self._set_out_resources(mp3_file=out_mp3_file)

    @override
    async def _execute_impl(self, in_resources, out_resources):
        mp3_name = f'{self._podcast_name(in_resources.metadata_txt.data)}.mp3'
        is_ok, err_msg = await _run_shell(
            self._ffmpeg_cmd(in_resources.audio_file.data, mp3_name))
        if not is_ok:
            return False, err_msg
        out_resources.mp3_file.populate_data(mp3_name)
        return True, None

    @staticmethod
    def _ffmpeg_cmd(downloaded_file: str, mp3_name: str) -> str:
        return (f"ffmpeg -i '{downloaded_file}' -map 0:a:0 -b:a 32k "
                f"-filter:a atempo=1.9 '{mp3_name}'")

    @staticmethod
    def _podcast_name(metadata_txt: str) -> str:
        """`<2 lowercase alpha chars>-<today>-<upload date>-<channel>-<title>`, by prefix."""
        upload_date, channel_id, title = metadata_txt.splitlines()
        prefix_pattern = re.compile(r'^([a-z]{2})-')
        used = [(ord(m.group(1)[0]) - ord('a')) * 26 + ord(m.group(1)[1]) - ord('a')
                    for f in os.listdir('.')  # .mp3 only: the `yt-dlp` binary sits here too
                    if f.endswith('.mp3') and (m := prefix_pattern.match(f))]
        index = max(used, default=-1) + 1
        return '-'.join([
            chr(ord('a') + index // 26) + chr(ord('a') + index % 26),
            datetime.now().strftime('%Y%m%d'),
            upload_date,
            CHANNEL_ID_2_SHORT_NAME.get(channel_id, channel_id),
            re.sub(r'[\s\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]', '-', title[:5]).lower(),
        ])


class YtDlpFfmpegPipeline(tpp.Pipeline):
    class Resources(tpp.ResourcesDir):
        def __init__(self, args: argparse.Namespace):
            self.yt_dlp_url = tpp.UrlStrResource('yt-dlp-url', args.yt_dlp_url)
            self.yt_dlp_bin = tpp.FileResource('yt-dlp-bin', args.yt_dlp_local_bin)
            self.url = tpp.UrlStrResource('source-url', args.source)
            self.yt_entry_info = tpp.TxtResource('yt-entry-info')
            self.yt_entry_metadata = tpp.TxtResource('yt-entry-metadata')
            self.audio_file = tpp.TxtResource('audio-file')
            self.mp3_file = tpp.TxtResource('podcast-mp3')

    class Transitions(tpp.TransitionsDir):
        def __init__(self, res: 'YtDlpFfmpegPipeline.Resources'):
            self.wget_yt_dlp = tpp.WgetUrlTransition(
                'yt-dlp-wget', True, res.yt_dlp_url, res.yt_dlp_bin, 'chmod 755')
            self.fetch_info = FetchYtDlpInfoTransition(
                'fetch-info', "{0} -F '{1}'", res.yt_dlp_bin, res.url, res.yt_entry_info)
            self.fetch_metadata = FetchYtDlpInfoTransition(
                'fetch-metadata',
                "{0} --print upload_date --print channel_id --print title '{1}'",
                res.yt_dlp_bin, res.url, res.yt_entry_metadata)
            self.download_audio_file = DownloadAudioFileTransition(
                'download-audio-file', res.yt_dlp_bin, res.yt_entry_info, res.url,
                res.audio_file)
            self.convert_mp3_file = ConvertMp3FileTransition(
                'convert-mp3-file', res.audio_file, res.yt_entry_metadata, res.mp3_file)

    def __init__(self, args: argparse.Namespace):
        resources = YtDlpFfmpegPipeline.Resources(args)
        super().__init__(
            resources=resources,
            transitions=YtDlpFfmpegPipeline.Transitions(resources))


def _get_clipboard_content() -> str | None:
    for clipboard_cmd in [['wl-paste'], ['xclip', '-o'], ['pbpaste']]:
        try:
            result = subprocess.run(clipboard_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
        except FileNotFoundError:
            pass
    return None


async def _run_shell(cmd: str, capture: bool = False) -> tuple[bool, str | None]:
    print(f'\n{cmd}\n', file=sys.stderr)
    return await tpp.run_shell(cmd, asyncio.subprocess.PIPE if capture else None)


def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Download a video as an accelerated podcast .mp3')
    ap.add_argument('-s', '--source', help='URL of the video, else the clipboard is used')
    ap.add_argument('-o', '--out-dir',
                    help='Where the yt-dlp binary, the audio and the .mp3 land, '
                         'by default a fresh system temp dir')
    ap.add_argument('-d', '--yt-dlp-url',
                    default='https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp',
                    help='URL to download the yt-dlp binary from')
    ap.add_argument('-p', '--pool-workers',
                    type=int, default=0,
                    help='Run the transitions in a pool of N processes instead of threads')
    ap.add_argument('-e', '--yt-dlp-local-bin',
                    help='Where that binary lands, by default `yt-dlp` inside --out-dir')
    args = ap.parse_args()
    args.source = args.source or _get_clipboard_content()
    if not args.source:
        sys.exit('[FATAL] No URL provided via command line or clipboard')
    args.out_dir = (os.path.abspath(args.out_dir) if args.out_dir
                    else tempfile.mkdtemp(prefix='yt-dlp-ffmpeg-'))
    args.yt_dlp_local_bin = args.yt_dlp_local_bin or os.path.join(args.out_dir, 'yt-dlp')
    return args


def main():
    args = _get_args()
    os.makedirs(args.out_dir, exist_ok=True)
    os.chdir(args.out_dir)  # yt-dlp, ffmpeg and the `aa-` prefix scan all work here
    print(f'[INFO] out dir: {args.out_dir}', file=sys.stderr)

    pipeline = YtDlpFfmpegPipeline(args)
    target = pipeline.resources.mp3_file
    if args.pool_workers:
        with multiprocessing.Pool(args.pool_workers) as pool:
            is_ok, err_msg = asyncio.run(tpp.Executor(pipeline.scheduler(target), pool).run())
    else:
        is_ok, err_msg = asyncio.run(tpp.Executor(pipeline.scheduler(target)).run())

    print(target.data)
    subprocess.run(['ls', '-lht', '--color=always', args.out_dir], stdout=sys.stderr)

    if not is_ok:
        print(f'[ERROR] {err_msg}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
