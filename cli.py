# Copyright (C) 2026 SIA Precision Education — AGPL-3.0 (see LICENSE).
"""Local renderer — prove a .apkg renders before any backend is involved.

    python cli.py <deck.apkg> [outdir]

Writes <outdir>/cards.json, <outdir>/preview.html and <outdir>/media/. Open
preview.html in a browser to eyeball fidelity. No Azure / SIA needed.
"""

from __future__ import annotations

import html
import json
import os
import shutil
import sys

from renderer import render_apkg


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python cli.py <deck.apkg> [outdir]", file=sys.stderr)
        return 2

    apkg = sys.argv[1]
    outdir = sys.argv[2] if len(sys.argv) > 2 else "out"
    media_dir = os.path.join(outdir, "media")
    os.makedirs(media_dir, exist_ok=True)

    def sink(local_path: str, fname: str) -> "str | None":
        safe = os.path.basename(fname) or "media.bin"
        try:
            shutil.copyfile(local_path, os.path.join(media_dir, safe))
        except OSError:
            return None
        return f"media/{safe}"

    result = render_apkg(apkg, sink)

    with open(os.path.join(outdir, "cards.json"), "w", encoding="utf-8") as fh:
        json.dump(
            {
                "deck_name": result.deck_name,
                "imported": result.imported,
                "skipped": result.skipped,
                "degraded": result.degraded,
                "cards": [vars(c) for c in result.cards],
            },
            fh,
            indent=2,
            ensure_ascii=False,
        )

    sections = []
    for i, c in enumerate(result.cards):
        # Isolated <style> per card mirrors how the app scopes note-type CSS.
        sections.append(
            f'<section style="border:1px solid #ccc;margin:1rem;padding:1rem;border-radius:8px">'
            f'<div style="font:12px monospace;color:#888">#{i + 1} · {html.escape(c.deck)} · '
            f'{html.escape(c.note_type)}{" · cloze" if c.cloze else ""}</div>'
            f"<style>{c.css}</style>"
            f'<div class="card">{c.front_html}</div><hr>'
            f'<div class="card">{c.back_html}</div></section>'
        )
    preview = (
        "<!doctype html><meta charset='utf-8'>"
        f"<title>{html.escape(result.deck_name)} preview</title>"
        f"<h1>{html.escape(result.deck_name)} — {result.imported} cards "
        f"({result.skipped} skipped)</h1>" + "".join(sections)
    )
    with open(os.path.join(outdir, "preview.html"), "w", encoding="utf-8") as fh:
        fh.write(preview)

    print(
        f"deck={result.deck_name!r} imported={result.imported} "
        f"skipped={result.skipped} degraded={result.degraded}"
    )
    print(f"wrote {outdir}/cards.json and {outdir}/preview.html")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
