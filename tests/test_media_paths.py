"""A crafted deck's media names stay bare filenames.

Media was once read from Anki's media folder by joined path, and
`os.path.join(media_dir, fname)` does NOT contain a traversal — an absolute
second argument replaces the first outright — so an unchecked reference let a
deck read any file the worker could (`/proc/self/environ` carries the storage
connection string and callback secret). Media is now read from the package by
listed name, but names are still held to what Anki's flat media folder holds.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from media_paths import safe_media_name


@pytest.mark.parametrize(
    "fname",
    [
        "../../../etc/passwd",
        "../../proc/self/environ",
        "/etc/passwd",
        "/proc/self/environ",
        "a/../../../etc/passwd",
        "./../secret",
        "....//....//etc/passwd",
        "sub/dir/file.jpg",
        "..\\..\\windows\\system32\\config\\sam",
        "..",
        ".",
        "",
        "x\x00.jpg",
    ],
)
def test_escaping_names_are_rejected(fname):
    assert safe_media_name(fname) is None


@pytest.mark.parametrize(
    "fname",
    [
        "image.jpg",
        "café.png",
        "日本語.png",
        "my file (1).webp",
        "a.b.c.mp3",
        "paste-1234567890.jpg",
        "file-name_2.gif",
    ],
)
def test_real_media_names_are_accepted(fname):
    """Anki names are unicode and messy; the guard must not become a whitelist."""
    assert safe_media_name(fname) == fname

