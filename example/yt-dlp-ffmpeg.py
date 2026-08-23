r"""
    [url]*                      [url]*
      |                            |
      v                            v
 (fetch_info)              (fetch_metadata)
      |                            |
      v                            v
 [yt_entry_info]          [yt_entry_metadata]
      |                            |
      v                            |
 (download_audio_file) <- [url]*   |
      |                            |
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
import os
import re
import subprocess
import sys
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


def _ffmpeg_cmd(downloaded_file: str, mp3_name: str) -> str:
    return f"ffmpeg -i '{downloaded_file}' -map 0:a:0 -b:a 32k -filter:a atempo=1.9 '{mp3_name}'"


def _podcast_name(metadata_txt: str) -> str:
    """`<2 lowercase alpha chars>-<today>-<upload date>-<channel>-<title>`, ordered by prefix."""
    upload_date, channel_id, title = metadata_txt.splitlines()
    prefix_pattern = re.compile(r'^([a-z]{2})-')
    used = [(ord(m.group(1)[0]) - ord('a')) * 26 + ord(m.group(1)[1]) - ord('a')
                for f in os.listdir('.') if (m := prefix_pattern.match(f))]
    index = max(used, default=-1) + 1
    return '-'.join([
        chr(ord('a') + index // 26) + chr(ord('a') + index % 26),
        datetime.now().strftime('%Y%m%d'),
        upload_date,
        CHANNEL_ID_2_SHORT_NAME.get(channel_id, channel_id),
        re.sub(r'[\s\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]', '-', title[:5]).lower(),
    ])


async def _run_shell(cmd: str, capture: bool = False) -> tuple[bool, str | None]:
    print(f'\n{cmd}\n', file=sys.stderr)
    return await tpp.run_shell(cmd, asyncio.subprocess.PIPE if capture else None)


def _yt_dlp_cmd(url_str: str, download_format: str) -> str:
    return f"yt-dlp -N 4 -f {download_format} '{url_str}'"


class FetchYtDlpInfoTransition(tpp.TransitionCalculation):
    """Runs one read-only `yt-dlp` query, echoes its output and stores it."""
    def __init__(self, name, cmd_template):
        super().__init__(name, retries_count=3)
        self._cmd_template = cmd_template

    @override
    async def _execute_impl(self, in_resources, out_resources):
        url_str: str = in_resources[0].data
        is_ok, out = await _run_shell(self._cmd_template.format(url_str), capture=True)
        if not is_ok:
            return False, out
        print(out, file=sys.stderr)
        out_resources[0].populate_data(out)
        return True, None


class DownloadAudioFileTransition(tpp.TransitionCalculation):
    """Asks yt-dlp for the output file name, fetches the audio, yields that file."""
    @override
    async def _execute_impl(self, in_resources, out_resources):
        info_txt: str = in_resources[0].data
        url_str: str = in_resources[1].data
        download_format: str | None = YtInfo(info_txt).audio_format
        if download_format is None:
            return False, f'No audio format found in:\n{info_txt}'
        is_ok, file_name = await _run_shell(  # file_name: str
            f"yt-dlp --print filename -f {download_format} '{url_str}'", capture=True)
        if not is_ok:
            return False, file_name
        is_ok, err_msg = await _run_shell(_yt_dlp_cmd(url_str, download_format))
        if not is_ok:
            return False, err_msg
        out_resources[0].populate_data(file_name)
        return True, None


class ConvertMp3FileTransition(tpp.TransitionCalculation):
    """Re-encodes the downloaded audio to a 1.9x accelerated 32k .mp3."""
    @override
    async def _execute_impl(self, in_resources, out_resources):
        audio_file: str = in_resources[0].data
        metadata_txt: str = in_resources[1].data
        mp3_name = f'{_podcast_name(metadata_txt)}.mp3'
        is_ok, err_msg = await _run_shell(_ffmpeg_cmd(audio_file, mp3_name))
        if not is_ok:
            return False, err_msg
        out_resources[0].populate_data(mp3_name)
        return True, None


class YtDlpFfmpegPipeline(tpp.Pipeline):
    class Resources(tpp.ResourcesDir):
        def __init__(self, args: argparse.Namespace):
            self.url = tpp.UrlStrResource('source-url', args.source)
            self.yt_entry_info = tpp.TxtResource('yt-entry-info')
            self.yt_entry_metadata = tpp.TxtResource('yt-entry-metadata')
            self.audio_file = tpp.TxtResource('audio-file')
            self.mp3_file = tpp.TxtResource('podcast-mp3')

    class Transitions(tpp.TransitionsDir):
        def __init__(self, res: 'YtDlpFfmpegPipeline.Resources'):
            self.fetch_info = (FetchYtDlpInfoTransition('fetch-info', "yt-dlp -F '{0}'")
                .set_in_resources(res.url)
                .set_out_resources(res.yt_entry_info))
            self.fetch_metadata = (FetchYtDlpInfoTransition('fetch-metadata',
                    "yt-dlp --print upload_date --print channel_id --print title '{0}'")
                .set_in_resources(res.url)
                .set_out_resources(res.yt_entry_metadata))
            self.download_audio_file = (DownloadAudioFileTransition('download-audio-file')
                .set_in_resources(res.yt_entry_info, res.url)
                .set_out_resources(res.audio_file))
            self.convert_mp3_file = (ConvertMp3FileTransition('convert-mp3-file')
                .set_in_resources(res.audio_file, res.yt_entry_metadata)
                .set_out_resources(res.mp3_file))

    def __init__(self, args: argparse.Namespace):
        resources = YtDlpFfmpegPipeline.Resources(args)
        super().__init__(
            resources=resources,
            transitions=YtDlpFfmpegPipeline.Transitions(resources))


def _get_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Download a video as an accelerated podcast .mp3')
    ap.add_argument('-s', '--source', help='URL of the video, else the clipboard is used')
    args = ap.parse_args()
    args.source = args.source or _get_clipboard_content()
    if not args.source:
        sys.exit('[FATAL] No URL provided via command line or clipboard')
    return args


def _get_clipboard_content() -> str | None:
    for clipboard_cmd in [['wl-paste'], ['xclip', '-o'], ['pbpaste']]:
        try:
            result = subprocess.run(clipboard_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                return result.stdout.strip()
        except FileNotFoundError:
            pass
    return None


def main():
    args = _get_args()

    pipeline = YtDlpFfmpegPipeline(args)
    target = pipeline.resources.mp3_file
    is_ok, err_msg = asyncio.run(tpp.Executor(pipeline.scheduler(target)).run())

    print(target.data)

    if not is_ok:
        print(f'[ERROR] {err_msg}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
