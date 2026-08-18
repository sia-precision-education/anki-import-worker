"""A crafted deck must not be able to name a file outside its media directory.

The exploit these pin: `os.path.join(media_dir, fname)` does NOT contain a
traversal — an absolute second argument replaces the first outright — so an
unchecked reference let a deck read any file the worker process could and hand
it to the media sink, which uploads it to the uploader's own container and
substitutes a fetchable reference into the card HTML. `/proc/self/environ`
carries AZURE_STORAGE_CONNECTION_STRING and ANKI_CALLBACK_SECRET.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from media_paths import safe_media_name, within_dir


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


def test_join_would_escape_without_the_guard():
    """Documents WHY the guard exists, so nobody 'simplifies' it away."""
    assert os.path.join("/tmp/media", "/etc/passwd") == "/etc/passwd"
    assert "/tmp/media" not in os.path.normpath(os.path.join("/tmp/media", "../../etc/passwd"))


class TestWithinDir:
    def test_a_plain_child_is_inside(self, tmp_path):
        child = tmp_path / "image.jpg"
        child.write_bytes(b"x")
        assert within_dir(str(tmp_path), str(child))

    def test_a_parent_escape_is_outside(self, tmp_path):
        assert not within_dir(str(tmp_path), str(tmp_path / ".." / "elsewhere.jpg"))

    def test_a_symlink_out_of_the_dir_is_outside(self, tmp_path):
        """The case `safe_media_name` cannot see: a bare name that is a symlink."""
        outside = tmp_path.parent / "outside_secret.txt"
        outside.write_text("secret")
        media = tmp_path / "media"
        media.mkdir()
        link = media / "innocent.jpg"
        os.symlink(outside, link)
        assert safe_media_name("innocent.jpg") == "innocent.jpg"
        assert not within_dir(str(media), str(link))

    def test_a_sibling_prefix_is_not_inside(self, tmp_path):
        """"/a/media-evil" must not count as inside "/a/media"."""
        base = tmp_path / "media"
        base.mkdir()
        sibling = tmp_path / "media-evil"
        sibling.mkdir()
        assert not within_dir(str(base), str(sibling))
