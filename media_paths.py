"""Name safety for media referenced by an uploaded deck.

Kept out of `renderer.py` so it can be imported — and tested — without pulling
in `anki`, which is a heavyweight AGPL dependency present only in the worker
image. Nothing here touches the collection; it is pure string work.

Every name judged here arrives inside a user-uploaded `.apkg`: in its media list,
in an `<img src>` in card HTML, or as an AV tag's filename. None is trusted. The
renderer reads media from the package by listed name into temp files it names
itself, so no name reaches a path; this keeps the names to what Anki can hold.
"""

from __future__ import annotations

import os


def safe_media_name(fname: str) -> str | None:
    """A bare Anki media filename, or None if it is anything else.

    Anki's media directory is FLAT — every reference is a bare filename — so a
    name carrying a separator, a parent ref, a NUL or an absolute root is not a
    media file, it is an attempt to leave the directory.
    """
    if not fname or "\x00" in fname:
        return None
    if fname in {".", ".."}:
        return None
    # `basename` collapses "../x" to "x", so an inequality means the name held a
    # separator. Backslash is not a separator on posix, hence the explicit test.
    if fname != os.path.basename(fname) or "\\" in fname or os.path.isabs(fname):
        return None
    return fname

