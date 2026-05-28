#!/usr/bin/env python3
"""Produce a small-footprint copy of logos/ for deploying with the app.

The full ``logos/`` directory is ~210 MB at 256x256 — too heavy to
commit to git on every push. The dashboard renders these at a 40px
circle, so 80x80 source images are more than enough resolution. This
script:

  1. Reads every PNG in ``logos/``,
  2. Downscales to 80x80 (preserving aspect, then centering on a
     transparent canvas if the source wasn't square),
  3. Saves to ``logos_small/<unitid>.png`` with PNG optimization.

Total output size lands around 25–35 MB — comfortable for git.

Idempotent: re-running just overwrites any files whose source is
newer than the destination.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from PIL import Image

SRC = Path(__file__).parent / "logos"
DST = Path(__file__).parent / "logos_small"
SIZE = 80


def main() -> int:
    if not SRC.exists():
        print(f"ERROR: {SRC} not found.")
        return 2
    DST.mkdir(exist_ok=True)

    sources = sorted(SRC.glob("*.png"))
    print(f"Optimizing {len(sources):,} PNGs from {SRC} -> {DST} @ {SIZE}x{SIZE}...")

    for i, src in enumerate(sources, 1):
        dst = DST / src.name
        # Skip if dest is up-to-date with source.
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
            continue
        try:
            img = Image.open(src).convert("RGBA")
        except Exception as exc:
            print(f"  skip {src.name}: {exc}")
            continue
        img.thumbnail((SIZE, SIZE), Image.LANCZOS)
        if img.size != (SIZE, SIZE):
            canvas = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
            canvas.paste(
                img,
                ((SIZE - img.size[0]) // 2, (SIZE - img.size[1]) // 2),
                img,
            )
            img = canvas
        img.save(dst, "PNG", optimize=True)
        if i % 500 == 0:
            print(f"  {i}/{len(sources)}")

    # Copy the manifest alongside so the deployed app has the source
    # provenance per institution.
    manifest_src = SRC / "_manifest.csv"
    if manifest_src.exists():
        shutil.copy2(manifest_src, DST / "_manifest.csv")

    total = sum(p.stat().st_size for p in DST.glob("*.png"))
    print(f"\nDone. {len(list(DST.glob('*.png'))):,} files, {total/1024/1024:.1f} MB total in {DST}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
