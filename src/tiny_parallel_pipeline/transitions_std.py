"""Common transitions: run a shell command, fetch a URL, write a file."""


import asyncio
import os
from types import SimpleNamespace
from typing import override, Self


from tiny_parallel_pipeline import Resource, ResourceStatus, TransitionCalculation


class CaptureCliStdoutTransition(TransitionCalculation):
    """Runs a shell command; `{name}` in it is filled from the inputs, stdout goes to `stdout_txt`."""
    def __init__(self, name: str, allow_multiprocess_pool: bool, out_stdout_txt: Resource,
                 in_name_2_resource: dict[str, Resource], *cmd: str):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._set_in_resources(**in_name_2_resource)
        self._set_out_resources(stdout_txt=out_stdout_txt)
        self._cmd = list(cmd)
        self._timeout = None

    def set_timeout(self, timeout: int | None) -> Self:
        self._timeout = timeout
        return self

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        cmd = ' '.join(c.format(**{n: r.data for n, r in vars(in_resources).items()})
                       for c in self._cmd)
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), self._timeout)
        except TimeoutError:
            process.kill()
            await process.wait()
            return False, f'`{cmd}` timed out after {self._timeout}s'
        stdout_text = stdout.decode('utf-8')
        stderr_text = stderr.decode('utf-8')
        if process.returncode != 0:
            return False, f'`{cmd}` exited with {process.returncode}: {stderr_text}'
        out_resources.stdout_txt.populate_data(stdout_text).update_status(ResourceStatus.READY)

        return True, None


class WgetUrlTransition(TransitionCalculation):
    """Downloads the `url_str` input to the `dst_file` output, then runs any post commands."""
    def __init__(self, name: str, allow_multiprocess_pool: bool, in_url_str: Resource,
                 out_dst_file: Resource, *post_wget_file_commands: str):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._set_in_resources(url_str=in_url_str)
        self._set_out_resources(dst_file=out_dst_file)
        self._post_wget_file_commands = post_wget_file_commands

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        out_file_path = out_resources.dst_file.expect_ready_file_at
        is_ok, err_msg = await run_shell(
            f'wget "{in_resources.url_str.data}" -O "{out_file_path}"')
        if not is_ok:
            return False, err_msg
        for c in self._post_wget_file_commands:
            if self._termination_requested:
                return False, 'terminated'
            is_ok, err_msg = await run_shell(f'{c} "{out_file_path}"')
            if not is_ok:
                return False, err_msg
        if not os.path.exists(out_file_path):
            return False, f'wget left no {out_file_path}'
        out_resources.dst_file.populate_data(out_file_path).update_status(ResourceStatus.READY)
        return True, None


class WriteTextFileTransition(TransitionCalculation):
    """Writes fixed text to the `dst_file` output, but only when the text changed."""
    def __init__(self, name: str, text: str, allow_multiprocess_pool: bool,
                 out_dst_file: Resource):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._set_out_resources(dst_file=out_dst_file)
        self._text = text

    @override
    async def _execute_impl(self, in_resources: SimpleNamespace,
                            out_resources: SimpleNamespace) -> tuple[bool, str | None]:
        out_file_path = out_resources.dst_file.expect_ready_file_at
        os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
        # Keep mtime stable when unchanged so downstream staleness checks can skip work.
        if not os.path.exists(out_file_path) or open(out_file_path).read() != self._text:
            with open(out_file_path, 'w') as f:
                f.write(self._text)
        out_resources.dst_file.populate_data(out_file_path).update_status(ResourceStatus.READY)
        return True, None


async def run_shell(cmd: str, stdout: int = asyncio.subprocess.DEVNULL
                    ) -> tuple[bool, str | None]:
    """Runs a shell command. Returns (False, stderr) when it fails, else the captured stdout."""
    process = await asyncio.create_subprocess_shell(
        cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=stdout,
        stderr=asyncio.subprocess.PIPE)
    out, stderr = await process.communicate()
    if process.returncode != 0:
        return False, f'`{cmd}` exited with {process.returncode}: {stderr.decode('utf-8')}'
    return True, out.decode('utf-8').strip() if out is not None else None
