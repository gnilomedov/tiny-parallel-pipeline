import asyncio
import pytest


import tiny_parallel_pipeline as tpp


from tiny_parallel_pipeline import transitions_std as std


#
# Test-specific fakes
#

class Mocks:
    def __init__(self):
        self.returncode, self.stdout, self.stderr, self.delay = 0, b'OUT', b'ERR', 0.0
        self.processes, self.files, self.made = [], {}, []

    @property
    def shell(self):
        return [p.cmd for p in self.processes]


class FakeProcess:
    def __init__(self, mocks, cmd):
        self._mocks, self.cmd, self.killed = mocks, cmd, False
        self.returncode = mocks.returncode

    async def communicate(self):
        await asyncio.sleep(self._mocks.delay)
        return self._mocks.stdout, self._mocks.stderr

    def kill(self):
        self.killed = True

    async def wait(self):
        return self.returncode


#
# Base test
#

class BaseTest:
    @pytest.fixture
    def mocks(self, monkeypatch):
        m, real_open = Mocks(), open
        async def create_subprocess_shell(cmd, **kwargs):
            m.processes.append(FakeProcess(m, cmd))
            return m.processes[-1]
        class FakeFile:
            def __init__(self, path):
                self.path = path
            def read(self):
                return m.files[self.path]
            def write(self, text):
                m.files[self.path] = text
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False
        monkeypatch.setattr(std.asyncio, 'create_subprocess_shell', create_subprocess_shell)
        monkeypatch.setattr(std.os.path, 'exists', lambda p: p in m.files)
        monkeypatch.setattr(std.os, 'makedirs', lambda d, exist_ok=False: m.made.append(d))
        monkeypatch.setattr('builtins.open', lambda p, *a, **k:
                            FakeFile(p) if p.startswith('/x/') else real_open(p, *a, **k))
        return m

    @staticmethod
    def run(transition):
        return asyncio.run(transition.compile().execute())


#
# Tests
#

class TestRunShell(BaseTest):  # real shell, harmless commands only
    def test_ok(self):
        assert asyncio.run(std.run_shell('printf "b\\na\\na\\n" | sort -u')) == (True, None)

    def test_failure_reports_returncode_and_stderr(self):
        assert asyncio.run(std.run_shell('echo boom >&2; exit 3')) == (
            False, '`echo boom >&2; exit 3` exited with 3: boom\n')

    def test_stdin_is_devnull(self):
        assert asyncio.run(std.run_shell('read line'))[0] is False  # immediate EOF


class TestCaptureCliStdoutTransition(BaseTest):
    def test_formats_cmd_and_captures_stdout(self, mocks):
        out = tpp.TxtResource('out')
        assert self.run(std.CaptureCliStdoutTransition(
            'T', False, out,
            {'bin_file': tpp.TxtResource('bin', '/bin/x'),
             'url_str': tpp.TxtResource('url', 'u')},
            '{bin_file}', '-F', '"{url_str}"')) == (True, None)
        assert mocks.shell == ['/bin/x -F "u"']
        assert (out.data, out.status.name) == ('OUT', 'READY')

    def test_non_zero_returncode(self, mocks):
        mocks.returncode, mocks.stderr = 2, b'nope'
        out = tpp.TxtResource('out')
        assert self.run(std.CaptureCliStdoutTransition('T', False, out, {}, 'x')) == (
            False, '`x` exited with 2: nope')
        assert (out.data, out.status.name) == (None, 'FAILED')

    def test_timeout_kills_the_process(self, mocks):
        mocks.delay = 1.0
        assert self.run(
            std.CaptureCliStdoutTransition('T', False, tpp.TxtResource('out'), {}, 'x')
                .set_timeout(0)) == (False, '`x` timed out after 0s')
        assert mocks.processes[-1].killed

class TestWgetUrlTransition(BaseTest):
    def _transition(self, *post_cmds, out=None):
        return std.WgetUrlTransition(
            'W', False, tpp.UrlStrResource('url', 'http://u'),
            out or tpp.FileResource('bin', '/x/bin'), *post_cmds)

    def test_wget_then_post_commands(self, mocks):
        mocks.files['/x/bin'] = ''
        out = tpp.FileResource('bin', '/x/bin')
        assert self.run(self._transition('chmod 755', 'strip', out=out)) == (True, None)
        assert mocks.shell == ['wget "http://u" -O "/x/bin"',
                               'chmod 755 "/x/bin"', 'strip "/x/bin"']
        assert (out.data, out.status.name) == ('/x/bin', 'READY')

    def test_wget_failure_skips_post_commands(self, mocks):
        mocks.returncode, mocks.stderr = 8, b'404'
        is_ok, err_msg = self.run(self._transition('chmod 755'))
        assert is_ok is False and '404' in err_msg
        assert len(mocks.shell) == 1

    def test_missing_output_file(self, mocks):
        assert self.run(self._transition()) == (False, 'wget left no /x/bin')

    def test_terminated(self, mocks):
        mocks.files['/x/bin'] = ''
        t = self._transition('chmod 755')
        t.terminate()
        assert self.run(t) == (False, 'terminated')
        assert len(mocks.shell) == 1  # bailed before the post command


class TestWriteTextFileTransition(BaseTest):
    def _transition(self, text, out=None):
        return std.WriteTextFileTransition(
            'W', text, False, out or tpp.FileResource('cpp', '/x/poly.cpp'))

    def test_creates_dir_and_writes(self, mocks):
        out = tpp.FileResource('cpp', '/x/poly.cpp')
        assert self.run(self._transition('CODE', out)) == (True, None)
        assert (mocks.made, mocks.files) == (['/x'], {'/x/poly.cpp': 'CODE'})
        assert (out.data, out.status.name) == ('/x/poly.cpp', 'READY')

    def test_same_text_keeps_mtime_stable(self, mocks):
        mocks.files['/x/poly.cpp'] = 'CODE'
        assert self.run(self._transition('CODE')) == (True, None)
        assert mocks.files == {'/x/poly.cpp': 'CODE'}

    def test_changed_text_is_rewritten(self, mocks):
        mocks.files['/x/poly.cpp'] = 'OLD'
        assert self.run(self._transition('NEW')) == (True, None)
        assert mocks.files == {'/x/poly.cpp': 'NEW'}
