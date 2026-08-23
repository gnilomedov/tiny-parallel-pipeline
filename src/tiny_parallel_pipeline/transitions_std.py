"""Common transitions: run a shell command, fetch a URL, write a file."""


import asyncio
import os
from typing import override


from tiny_parallel_pipeline import ResourceStatus, TransitionCalculation


class CaptureCliStdoutTransition(TransitionCalculation):
    """Runs a shell command and stores its stdout in the output resource."""
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
        cmd = ' '.join(c.format(*[r.data for r in in_resources]) for c in self._cmd)
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
        if self._termination_requested:
            return False, 'terminated'
        stdout_text = stdout.decode('utf-8')
        stderr_text = stderr.decode('utf-8')
        if process.returncode != 0:
            return False, f'`{cmd}` exited with {process.returncode}: {stderr_text}'
        out_resources[0].populate_data(stdout_text).update_status(ResourceStatus.READY)

        return True, None


class WgetUrlTransition(TransitionCalculation):
    """Downloads the input URL to the output file, then runs any post commands."""
    def __init__(self, name, allow_multiprocess_pool, *post_wget_file_commands):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._post_wget_file_commands = post_wget_file_commands

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(in_resources) == 1
        assert len(out_resources) == 1
        url_str = in_resources[0].data
        out_file_path = out_resources[0].expect_ready_file_at
        is_ok, err_msg = await run_shell(f'wget "{url_str}" -O "{out_file_path}"')
        if not is_ok:
            return False, err_msg
        if self._termination_requested:
            return False, 'terminated'
        for c in self._post_wget_file_commands:
            is_ok, err_msg = await run_shell(f'{c} "{out_file_path}"')
            if not is_ok:
                return False, err_msg
            if self._termination_requested:
                return False, 'terminated'
        if not os.path.exists(out_file_path):
            return False, f'wget left no {out_file_path}'
        out_resources[0].populate_data(out_file_path).update_status(ResourceStatus.READY)
        return True, None


class WriteTextFileTransition(TransitionCalculation):
    """Writes fixed text to the output file, but only when the text changed."""
    def __init__(self, name, text, allow_multiprocess_pool):
        super().__init__(name, allow_multiprocess_pool=allow_multiprocess_pool)
        self._text = text

    @override
    async def _execute_impl(self, in_resources, out_resources):
        assert len(out_resources) == 1
        out_file_path = out_resources[0].expect_ready_file_at
        os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
        # Keep mtime stable when unchanged so downstream staleness checks can skip work.
        if not os.path.exists(out_file_path) or open(out_file_path).read() != self._text:
            with open(out_file_path, 'w') as f:
                f.write(self._text)
        out_resources[0].populate_data(out_file_path).update_status(ResourceStatus.READY)
        return True, None


async def run_shell(cmd: str, stdout=asyncio.subprocess.DEVNULL) -> tuple[bool, str | None]:
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
