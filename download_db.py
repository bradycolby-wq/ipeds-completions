"""
Download ipeds.db from GitHub Releases during the Render build step,
so it's available on disk before the app starts.
"""
import urllib.request
from pathlib import Path

DB_URL = (
    "https://github.com/bradycolby-wq/ipeds-completions/releases/"
    "download/v1.6/ipeds.db"
)

dest = Path(__file__).parent / "ipeds.db"

if dest.exists():
    print(f"ipeds.db already present ({dest.stat().st_size / 1e6:.0f} MB), skipping download.")
else:
    print("Downloading ipeds.db from GitHub Releases...")
    urllib.request.urlretrieve(DB_URL, dest)
    print(f"Done. ({dest.stat().st_size / 1e6:.0f} MB)")
