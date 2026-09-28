from __future__ import annotations

import os
from pathlib import Path

from egnyte import EgnyteClient


DOMAIN = "scadadata.egnyte.com"
SALA_FOLDER = "/Shared/SALA"


def main() -> None:
    token = os.environ.get("EGNYTE_ACCESS_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing EGNYTE_ACCESS_TOKEN environment variable.")

    client = EgnyteClient(DOMAIN, token)
    files = client.list_files(SALA_FOLDER, count=100)

    candidates = [
        item for item in files
        if Path(item.name).suffix.casefold() in {"", ".csv"}
    ]

    if not candidates:
        print("No CSV candidates found in /Shared/SALA.")
        return

    latest = max(candidates, key=lambda item: (item.uploaded, item.name))

    print(f"Folder: {SALA_FOLDER}")
    print(f"Files returned: {len(files)}")
    print(f"CSV candidates: {len(candidates)}")
    print("Latest candidate:")
    print(f"  name: {latest.name}")
    print(f"  path: {latest.path}")
    print(f"  size: {latest.size}")
    print(f"  group_id: {latest.group_id}")
    print(f"  entry_id: {latest.entry_id}")
    print(f"  uploaded: {latest.uploaded}")


if __name__ == "__main__":
    main()
