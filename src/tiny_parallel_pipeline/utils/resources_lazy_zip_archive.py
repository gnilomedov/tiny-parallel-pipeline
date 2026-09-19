"""A .zip used as a keyed store: members addressed by template and key, with lazy indexes."""


import io
import json
import os
import re
import shutil
import subprocess
import threading
import zipfile
from collections.abc import Callable

from tiny_parallel_pipeline.resource import Resource, ResourceStatus


class ResourcesLazyZipArchive:
    """Stores resources for lazy availability: tiny indexes give fast presence and group lookup.

    Tuned for the pattern: many reads -> many writes. NOT for frequent read -> write -> read
    flips: a write drops the read handle and every index with it, and the next read rebuilds.

    Naming recommendation: `self._lzy_res_archive`, or `self._<what_it_holds>_lzy_res_archive`.

    Every method carries a `# self._lock:` tag saying whether it takes that lock (reentrant)
    or expects a caller to hold it already.
    """

    _PATH_2_ARCHIVE: dict[str, 'ResourcesLazyZipArchive'] = dict()
    _PATH_2_ARCHIVE_LOCK = threading.Lock()

    def __init__(self, path: str):
        self._path = path
        self._lock = threading.RLock()
        self._reader: zipfile.ZipFile | None = None
        self._idx_name_2_key_2_members: dict[str, dict[str, list[str]]] = dict()

    def __reduce__(self):
        """Its lock and open handle cannot cross to a worker: rebuild there from the path."""
        return type(self)._for_path, (self._path,)

    @classmethod
    def _for_path(cls, path: str) -> 'ResourcesLazyZipArchive':
        """One instance per path per process, so a pool worker opens the zip once, not per task."""
        with cls._PATH_2_ARCHIVE_LOCK:
            return cls._PATH_2_ARCHIVE.setdefault(path, cls(path))

    # self._lock: acquires, through add_all.
    def add(self, key: str, text: str, member: str) -> int:
        return self.add_all([(key, text)], member)

    # self._lock: acquires.
    def add_all(self, items: list[tuple[str, str]], member: str) -> int:
        """One open for a whole set: appending per member re-reads a growing directory."""
        with self._lock:
            with zipfile.ZipFile(self._path, 'a', zipfile.ZIP_DEFLATED, compresslevel=1) as z:
                for key, text in items:
                    z.writestr(member.format(key=key), text)
            self._invalidate()
        return sum(len(text) for _, text in items)

    # self._lock: acquires.
    def replace(self, key: str, text: str, member: str) -> int:
        """`add` for a rewritten member: zipfile has no remove, so this copies the archive."""
        with self._lock:
            if self.read_or_none(key, member) is not None:
                # Dropping one member is O(archive) either way. `zip -d` moves the compressed
                # bytes; the loop below re-encodes every member: 7.5s against 0.07s on 150MB.
                name = member.format(key=key)
                self._invalidate()
                if shutil.which('zip') is not None:
                    subprocess.run(['zip', '--quiet', '--delete', self._path, name], check=True)
                else:
                    tmp = f'{self._path}.tmp'
                    with (zipfile.ZipFile(self._path) as src,
                          zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED, 1) as dst):
                        for info in src.infolist():
                            if info.filename != name:
                                dst.writestr(info, src.read(info.filename))
                    os.replace(tmp, self._path)
            return self.add(key, text, member)

    # self._lock: acquires.
    def read_or_none(self, key: str, member: str) -> str | None:
        """One member's text, or None when the archive -- or that member -- is not there."""
        with self._lock:
            if not self.is_stored(key, member):
                return None
            return self._reader.read(member.format(key=key)).decode()

    # self._lock: acquires, through _index.
    def is_stored(self, key: str, member: str) -> bool:
        """Presence only, off the index: nothing is read or parsed to answer it."""
        return bool(self._index(member).get(key))

    # self._lock: acquires, through read_or_none.
    def populate_if_stored(self, key: str, member: str, parse_fn: Callable[[str], object],
                           out_resource: Resource) -> bool:
        """The lazy load, for `_execute_impl`: False means nothing was there to load."""
        text = self.read_or_none(key, member)
        if text is None:
            return False
        out_resource.populate_data(parse_fn(text)).update_status(ResourceStatus.READY)
        return True

    # self._lock: acquires, through _index.
    def stored_group(self, group_key: str, member: str, idx_name: str,
                     extract_key_regexp: str) -> list[str]:
        """The keys a named index files under `group_key`, sorted; a ticker's copies, say.

        `idx_name` must be unique per regexp: one name is one index.
        """
        return self._index(member, idx_name, extract_key_regexp).get(group_key, [])

    # self._lock: acquires, through _index.
    def stored_keys(self, member: str) -> list[str]:
        """Every key stored under this member template, sorted."""
        return list(self._index(member))

    # self._lock: acquires.
    def _index(self, member: str, idx_name: str | None = None,
               extract_key_regexp: str | None = None) -> dict[str, list[str]]:
        """Keys grouped by what the regexp pulls out of them; built once, dropped by a write."""
        idx_name = idx_name or member
        # A regexp makes a different index over the same member, so it needs its own name.
        assert extract_key_regexp is None or idx_name != member, f'grouped index needs a name: {member}'
        # partition, not split: a member with no {key} is one fixed name stored under the key ''.
        head, _, tail = member.partition('{key}')
        # One match per name: group 1 is the key, group 2 is what the index files it under. The
        # caller's regexp carries that inner group; the default files every key under itself.
        of_name = re.compile(
            f'{re.escape(head)}({extract_key_regexp or "(.*)"}){re.escape(tail)}').fullmatch
        with self._lock:
            if idx_name not in self._idx_name_2_key_2_members:
                index: dict[str, list[str]] = dict()
                for name in sorted(self._names()):
                    if (at := of_name(name)) is not None:
                        index.setdefault(at.group(2), []).append(at.group(1))
                self._idx_name_2_key_2_members[idx_name] = index
            return self._idx_name_2_key_2_members[idx_name]

    # self._lock: requires it held.
    def _invalidate(self) -> None:
        """Drops the read handle and the indexes with it: a write just made both stale."""
        self._idx_name_2_key_2_members.clear()
        if self._reader is not None:
            self._reader.close()
            self._reader = None

    # self._lock: requires it held.
    def _names(self) -> list[str]:
        z = self._reader_zip()
        return z.namelist() if z is not None else []

    # self._lock: requires it held.
    def _reader_zip(self) -> 'zipfile.ZipFile | None':
        """The shared read handle, opened on first use; None while the archive does not exist."""
        if self._reader is None and os.path.exists(self._path):
            self._reader = zipfile.ZipFile(self._path)
        return self._reader


def parse_as_text(text: str) -> str:
    """Identity: hold the member as it landed and parse only where it is actually used."""
    return text


def parse_json(text: str) -> dict:
    return json.loads(text)


def parse_csv_2_df(text: str):
    """A stored frame, first column back as the index, as the CSV left it."""
    import pandas  # not a dep of this package
    return pandas.read_csv(io.StringIO(text), index_col=0)


def make_parse_csv_2_df_fn(index_type: str) -> Callable[[str], object]:
    """Same as parse_csv_2_df, but casting the index."""
    def parse_csv_2_df_with_index_type(text: str):
        df = parse_csv_2_df(text)
        df.index = df.index.astype(index_type)
        return df
    return parse_csv_2_df_with_index_type
