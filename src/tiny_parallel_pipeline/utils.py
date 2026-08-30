"""Text helpers: shrink a long list or a long string down to one readable line."""


_CUT = '~...~'
_GAP = ' ... '


def trim_list(items: list, edge: int = 3, max_len: int = 50) -> str:
    """Keeps `edge` items at each end, then caps the whole thing at `max_len`."""
    if not isinstance(items, list) or len(items) <= 2 * edge:
        return trim_text(str(items), max_len)
    return _join_trimmed(repr(items[:edge])[:-1], repr(items[-edge:])[1:], _GAP, max_len)


def trim_text(text: str, max_len: int = 50) -> str:
    """Cuts the middle out, keeping both ends, when `text` is longer than `max_len`."""
    if not isinstance(text, str):
        return repr(text)
    if len(text) <= max_len:
        return text
    half = len(text) // 2
    return _join_trimmed(text[:half], text[half:], '', max_len)


def _join_trimmed(beg: str, end: str, gap: str, max_len: int) -> str:
    to_reduce = len(beg) + len(end) + len(_CUT) - max_len
    if to_reduce <= 0:
        return f'{beg}{gap}{end}'
    cut_beg = to_reduce // 2
    return ''.join([beg[:max(0, len(beg) - cut_beg)], _CUT,
                    end[min(len(end), to_reduce - cut_beg):]])
