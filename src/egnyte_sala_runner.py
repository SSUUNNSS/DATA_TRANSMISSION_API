"""Compatibility API for older SALA-only Egnyte commands and tests."""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from egnyte import EgnyteClient, EgnyteFile
from egnyte_runner import (
    DEFAULT_TIMEZONE,
    OUTPUT_ROOT as SALA_OUTPUT_ROOT,
    PROJECT_ROOT,
    SCHEDULED_CHECK_MINUTES,
    EgnyteSite,
    egnyte_uploaded_datetime,
    file_version_key,
    filter_new_files as _filter_new_files,
    get_due_check_times,
    initialize_site_state,
    load_state,
    parse_csv_stream,
    process_csv_bytes,
    process_site_file,
    run_site_cycle,
    save_state,
)

LOGGER = logging.getLogger("sala_egnyte")
SALA_ROOT = "/Shared/SALA"
SALA_OUTPUT_ROOT = PROJECT_ROOT / "src" / "sourceData" / "normal"
SALA_STATE_PATH = PROJECT_ROOT / "state" / "sala_egnyte_state.json"


def filter_new_files(files: list[EgnyteFile], processed_ids: set[str], *,
                     baseline_cutoff: datetime | None = None,
                     tz: ZoneInfo | None = None) -> list[EgnyteFile]:
    selected, _ = _filter_new_files(
        files,
        processed_ids,
        baseline_cutoff,
        tz or ZoneInfo(DEFAULT_TIMEZONE),
    )
    return selected


def process_sala_csv_bytes(*, csv_bytes: bytes, source_name: str, date_label: str,
                           output_root: Path, file_stem: str) -> Path:
    return process_csv_bytes(
        csv_bytes=csv_bytes,
        source_name=source_name,
        date_label=date_label,
        output_station="sala",
        output_root=output_root,
        file_stem=file_stem,
    )


def _sala_site() -> EgnyteSite:
    return EgnyteSite(
        name="sala",
        egnyte_folder=SALA_ROOT,
        output_station="sala",
        config_path=PROJECT_ROOT / "config" / "sala_local.json",
    )


def process_sala_file(file: EgnyteFile, *, client: EgnyteClient,
                      state_path: Path = SALA_STATE_PATH,
                      output_root: Path = SALA_OUTPUT_ROOT,
                      dry_run: bool = False,
                      state: dict | None = None) -> bool:
    current_state = state if state is not None else load_state(state_path)
    if dry_run:
        LOGGER.info("[SALA][EGNYTE] dry-run; not downloading %s", file.name)
        return False
    return process_site_file(
        _sala_site(), file, client=client, state=current_state,
        state_path=state_path, output_root=output_root,
    )


def initialize_sala_state(*, client: EgnyteClient | None = None,
                           state_path: Path = SALA_STATE_PATH,
                           now: datetime | None = None,
                           tz_name: str = DEFAULT_TIMEZONE,
                           baseline_before: str) -> Path:
    tz = ZoneInfo(tz_name)
    return initialize_site_state(
        _sala_site(),
        tz=tz,
        baseline_before=baseline_before,
        state_path=state_path,
    )


def run_sala_cycle(*, client: EgnyteClient | None = None,
                   state_path: Path = SALA_STATE_PATH,
                   output_root: Path = SALA_OUTPUT_ROOT,
                   dry_run: bool = False,
                   startup: bool = False,
                   now: datetime | None = None,
                   tz_name: str = DEFAULT_TIMEZONE,
                   state: dict | None = None) -> int:
    tz = ZoneInfo(tz_name)
    state = state if state is not None else load_state(state_path)
    client = client or EgnyteClient.from_environment()
    return run_site_cycle(
        _sala_site(), client=client, state=state, state_path=state_path,
        output_root=output_root, dry_run=dry_run,
        now=now or datetime.now(tz), tz=tz,
    )


def run_scheduler(*, client: EgnyteClient | None = None,
                  state_path: Path = SALA_STATE_PATH,
                  output_root: Path = SALA_OUTPUT_ROOT,
                  tz_name: str = DEFAULT_TIMEZONE) -> None:
    from egnyte_runner import run_scheduler as run_shared_scheduler

    site = _sala_site()
    state_root = state_path.parent / "egnyte"
    run_shared_scheduler(
        [site],
        client=client or EgnyteClient.from_environment(),
        tz_name=tz_name,
        output_root=output_root,
        state_root=state_root,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compatibility SALA Egnyte runner.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--initialize-state", action="store_true")
    parser.add_argument("--baseline-before")
    parser.add_argument("--state-path", type=Path, default=SALA_STATE_PATH)
    parser.add_argument("--output-root", type=Path, default=SALA_OUTPUT_ROOT)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.initialize_state:
        if not args.baseline_before:
            raise SystemExit("--initialize-state requires --baseline-before <timestamp>.")
        initialize_sala_state(
            state_path=args.state_path, tz_name=args.timezone,
            baseline_before=args.baseline_before,
        )
    elif args.once:
        client = EgnyteClient.from_environment()
        run_sala_cycle(
            client=client, state_path=args.state_path, output_root=args.output_root,
            dry_run=args.dry_run, now=datetime.now(ZoneInfo(args.timezone)),
            tz_name=args.timezone,
        )
    else:
        client = EgnyteClient.from_environment()
        run_scheduler(client=client, state_path=args.state_path,
                      output_root=args.output_root, tz_name=args.timezone)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())