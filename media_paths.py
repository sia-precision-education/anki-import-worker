"""Path safety for media referenced by an uploaded deck.

Kept out of `renderer.py` so it can be imported — and tested — without pulling
in `anki`, which is a heavyweight AGPL dependency present only in the worker
image. Nothing here touches the collection; it is pure string and path work.

Every name these functions judge arrives inside a user-uploaded `.apkg`, from
either an `<img src>` in card HTML or an AV tag's filename. Neither is trusted.
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


def within_dir(base: str, target: str) -> bool:
    """Whether `target` really resolves inside `base` — symlinks included.

    Belt to `safe_media_name`'s braces: a deck cannot name its way out, and a
    symlink planted in the extracted media dir cannot point its way out either.
    """
    base_real = os.path.realpath(base)
    target_real = os.path.realpath(target)
    return target_real == base_real or target_real.startswith(base_real + os.sep)
