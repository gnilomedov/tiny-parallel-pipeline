from unittest import mock

from tiny_parallel_pipeline.utils.resources_lazy_zip_archive import ResourcesLazyZipArchive


MEMBER = 'src-edgar/{key}-facts.json'
PATH = '/nowhere/corpus-anchor.zip'
BY_ORIGIN = ('by-origin', r'(.+?)_syn_\d+of\d+')


@mock.patch('os.path.exists', return_value=False)
@mock.patch('zipfile.ZipFile')
def test_add(zipfile_zipfile, _exists):
    assert ResourcesLazyZipArchive(PATH).add('AAPL', 'a,b\n1,2\n', '{key}.csv') == 8

    written = zipfile_zipfile.return_value.__enter__.return_value.writestr
    written.assert_called_once_with('AAPL.csv', 'a,b\n1,2\n')
    # Always appended to: mode 'a' creates the archive when it is not there yet.
    assert zipfile_zipfile.call_args.args[1] == 'a'


@mock.patch('subprocess.run')
@mock.patch('os.replace')
@mock.patch('zipfile.ZipFile')
@mock.patch('shutil.which')
@mock.patch.object(ResourcesLazyZipArchive, 'read_or_none', return_value='old')
def test_replace(_read_or_none, which, zipfile_zipfile, os_replace, subprocess_run):
    which.return_value = '/usr/bin/zip'
    archive = ResourcesLazyZipArchive(PATH)

    assert archive.replace('', 'new', 'manifest.json') == 3

    # A zip cannot overwrite a member, so the stale one goes first; the CLI moves compressed
    # bytes, while the python fallback re-encodes every member.
    subprocess_run.assert_called_once_with(
        ['zip', '--quiet', '--delete', PATH, 'manifest.json'], check=True)
    zipfile_zipfile.return_value.__enter__.return_value.writestr.assert_called_once_with(
        'manifest.json', 'new')

    which.return_value = None
    src = zipfile_zipfile.return_value.__enter__.return_value
    src.infolist.return_value = [mock.Mock(filename=n) for n in ('AAPL.csv', 'manifest.json')]
    src.writestr.reset_mock()

    assert archive.replace('', 'new', 'manifest.json') == 3

    kept = [c.args[0].filename for c in src.writestr.call_args_list
            if not isinstance(c.args[0], str)]
    assert kept == ['AAPL.csv']
    os_replace.assert_called_once_with(f'{PATH}.tmp', PATH)


@mock.patch.object(ResourcesLazyZipArchive, '_reader_zip')
def test_is_stored(reader_zip):
    reader_zip.return_value = mock.Mock(
        namelist=mock.Mock(return_value=['src-edgar/AAPL-facts.json']))
    archive = ResourcesLazyZipArchive(PATH)

    # Presence only, off the central directory: nothing is read or parsed.
    assert archive.is_stored('AAPL', MEMBER)
    assert not archive.is_stored('NVDA', MEMBER)


@mock.patch.object(ResourcesLazyZipArchive, '_reader_zip')
def test_stored_keys_and_stored_group(reader_zip):
    names = [MEMBER.format(key=k) for k in ('AAPL', 'AAPL_syn_000of002', 'NVDA')]
    reader_zip.return_value = mock.Mock(namelist=mock.Mock(return_value=names))
    archive = ResourcesLazyZipArchive(PATH)

    assert archive.stored_keys(MEMBER) == ['AAPL', 'AAPL_syn_000of002', 'NVDA']
    # A second index over the same member, filing the copies under their origin ticker. The
    # origin itself does not match the regexp, so it stays out of its own group.
    assert archive.stored_group('AAPL', MEMBER, *BY_ORIGIN) == ['AAPL_syn_000of002']
    assert archive.stored_group('NVDA', MEMBER, *BY_ORIGIN) == []


@mock.patch.object(ResourcesLazyZipArchive, '_reader_zip')
def test_index(reader_zip):
    reader_zip.return_value = mock.Mock(namelist=mock.Mock(return_value=[
        'AAPL.csv', 'ZZZZ.csv', 'src-edgar/AAPL-facts.json', 'src-edgar/AAPL-sub.json',
        'manifest.txt']))
    archive = ResourcesLazyZipArchive(PATH)

    # Head and tail both set: the key is what sits between, and a sibling template is skipped.
    assert archive.stored_keys(MEMBER) == ['AAPL']
    assert archive.stored_keys('{key}.csv') == ['AAPL', 'ZZZZ']         # tail only
    # No {key} at all: one fixed member, filed under the empty key.
    assert archive.stored_keys('manifest.txt') == ['']
    assert archive.is_stored('', 'manifest.txt')

    reader_zip.return_value = None

    # Nothing to index while the archive is not there yet.
    assert ResourcesLazyZipArchive('/nope.zip').stored_keys(MEMBER) == []


@mock.patch('os.path.exists', return_value=True)
@mock.patch('zipfile.ZipFile')
def test_invalidate(zipfile_zipfile, _exists):
    reader = zipfile_zipfile.return_value
    reader.namelist.return_value = ['src-edgar/AAPL-facts.json']
    archive = ResourcesLazyZipArchive(PATH)
    assert archive.stored_keys(MEMBER) == ['AAPL']
    assert archive.stored_group('AAPL', MEMBER, *BY_ORIGIN) == []       # a second index

    reader.namelist.return_value = ['src-edgar/NVDA-facts.json']
    archive.add('NVDA', 'x', MEMBER)

    # Both indexes go, and the handle they were read off is closed rather than left stale.
    reader.close.assert_called_once()
    assert archive.stored_keys(MEMBER) == ['NVDA']
    assert archive.stored_group('NVDA', MEMBER, *BY_ORIGIN) == []
