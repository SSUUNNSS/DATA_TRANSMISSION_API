"""Configurable, sequential Egnyte ingestion for all enabled sites."""
from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from egnyte import EgnyteClient, EgnyteFile
from preprocessing.Ingrid.preprocessing_utils import Record, parse_csv_stream as _parse_csv_stream, resample_records

LOGGER = logging.getLogger("sala_egnyte")
SITES_CONFIG_PATH = PROJECT_ROOT / "config" / "egnyte_sites.json"
STATE_ROOT = PROJECT_ROOT / "state" / "egnyte"
LEGACY_SALA_STATE_PATH = PROJECT_ROOT / "state" / "sala_egnyte_state.json"
OUTPUT_ROOT = PROJECT_ROOT / "src" / "sourceData" / "normal"
DEFAULT_TIMEZONE = "Europe/Stockholm"
SCHEDULED_CHECK_MINUTES = (12, 27, 42, 57)


@dataclass(frozen=True)
class EgnyteSite:
    name: str
    egnyte_folder: str
    output_station: str
    config_path: Path
    enabled: bool = True


def load_sites(config_path: Path = SITES_CONFIG_PATH) -> list[EgnyteSite]:
    with config_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    site_items = payload.get("sites")
    if not isinstance(site_items, list):
        raise ValueError("Egnyte sites config must contain a 'sites' array.")

    sites: list[EgnyteSite] = []
    names: set[str] = set()
    for item in site_items:
        name = str(item.get("name", "")).strip().lower()
        folder = str(item.get("egnyte_folder", "")).strip().rstrip("/")
        output_station = str(item.get("output_station", "")).strip().lower()
        config_value = str(item.get("watcher_config", f"{name}_local.json"))
        if not name or not folder.startswith("/Shared/") or not output_station:
            raise ValueError("Each Egnyte site needs name, /Shared egnyte_folder, and output_station.")
        if name in names:
            raise ValueError(f"Duplicate Egnyte site name: {name}")
        names.add(name)
        sites.append(EgnyteSite(
            name=name,
            egnyte_folder=folder,
            output_station=output_station,
            config_path=(PROJECT_ROOT / "config" / config_value).resolve(),
            enabled=bool(item.get("enabled", True)),
        ))
    return sites


def state_path_for(site: EgnyteSite, state_root: Path = STATE_ROOT) -> Path:
    return state_root / f"{site.name}.json"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {"processed_files": {}}
    try:
        with path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
    except (json.JSONDecodeError, OSError) as error:
        raise ValueError(f"State file is not valid JSON: {path}") from error
    if not isinstance(state, dict):
        raise ValueError(f"State file must contain a JSON object: {path}")
    if not isinstance(state.get("processed_files", {}), dict):
        raise ValueError(f"State value 'processed_files' must be an object: {path}")
    state.setdefault("processed_files", {})
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2)
        handle.flush()
    temp_path.replace(path)


def load_site_state(
    site: EgnyteSite,
    *,
    state_root: Path = STATE_ROOT,
    legacy_sala_path: Path = LEGACY_SALA_STATE_PATH,
) -> dict[str, Any]:
    path = state_path_for(site, state_root)
    if path.exists():
        return load_state(path)
    if site.name == "sala" and legacy_sala_path.exists():
        state = load_state(legacy_sala_path)
        save_state(path, state)
        LOGGER.info("[SALA][STATE] migrated legacy state to %s", path)
        return state
    return {"processed_files": {}}


def file_version_key(file: EgnyteFile) -> str:
    group_id = str(file.group_id or "").strip()
    entry_id = str(file.entry_id or "").strip()
    if not group_id or not entry_id:
        raise ValueError(f"Egnyte file is missing group_id or entry_id: {file.name}")
    return f"{group_id}:{entry_id}"


def egnyte_uploaded_datetime(uploaded_ms: int, tz: ZoneInfo) -> datetime:
    return datetime.fromtimestamp(uploaded_ms / 1000.0, timezone.utc).astimezone(tz)


def parse_baseline_cutoff(value: str, tz: ZoneInfo) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"Invalid baseline cutoff: {value!r}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def filter_new_files(
    files: list[EgnyteFile],
    processed_ids: set[str],
    baseline_cutoff: datetime | None,
    tz: ZoneInfo,
) -> tuple[list[EgnyteFile], int]:
    eligible: list[EgnyteFile] = []
    historical_count = 0
    for file in files:
        if baseline_cutoff is not None and egnyte_uploaded_datetime(file.uploaded, tz) <= baseline_cutoff:
            historical_count += 1
            continue
        eligible.append(file)
    candidates: list[EgnyteFile] = []
    seen_ids = set(processed_ids)
    for file in eligible:
        if Path(file.name).suffix.lower() not in {"", ".csv"}:
            continue
        key = file_version_key(file)
        if key in seen_ids:
            continue
        seen_ids.add(key)
        candidates.append(file)
    return sorted(candidates, key=lambda item: (item.uploaded, item.name)), historical_count


def get_due_check_times(reference_time: datetime | None = None, tz_name: str = DEFAULT_TIMEZONE) -> list[datetime]:
    tz = ZoneInfo(tz_name)
    reference = (reference_time or datetime.now(tz)).astimezone(tz)
    current = reference.replace(second=0, microsecond=0)
    due: list[datetime] = []
    cursor = current
    for _ in range(4):
        for minute in SCHEDULED_CHECK_MINUTES:
            candidate = cursor.replace(minute=minute)
            if candidate > reference:
                due.append(candidate)
                if len(due) == 4:
                    return due
        cursor = (cursor + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return due


class FixedClockSchedule:
    def __init__(self) -> None:
        self.last_run_slot: datetime | None = None

    def next_run(self, reference_time: datetime, tz_name: str = DEFAULT_TIMEZONE) -> datetime:
        for candidate in get_due_check_times(reference_time, tz_name):
            if self.last_run_slot is None or candidate > self.last_run_slot:
                if candidate <= reference_time:
                    continue
                return candidate
        raise RuntimeError("No future Egnyte polling boundary could be calculated.")

    def claim(self, scheduled_slot: datetime) -> bool:
        if self.last_run_slot is not None and scheduled_slot <= self.last_run_slot:
            return False
        self.last_run_slot = scheduled_slot
        return True


def parse_csv_stream(stream, source_name: str = "<stream>") -> list[Record]:
    records = list(_parse_csv_stream(stream, source_name))
    if not records:
        raise ValueError(f"CSV data is empty for {source_name}")
    return records


def process_csv_bytes(
    *,
    csv_bytes: bytes,
    source_name: str,
    date_label: str,
    output_station: str,
    output_root: Path = OUTPUT_ROOT,
    file_stem: str | None = None,
) -> Path:
    records = parse_csv_stream(io.BytesIO(csv_bytes), source_name)
    resampled = list(resample_records(records))
    if not resampled:
        raise ValueError(f"No valid records generated from {source_name}")

    output_dir = output_root / output_station / date_label
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = file_stem or Path(source_name).stem
    output_path = output_dir / f"{stem}.parquet"
    temp_path = output_dir / f"{stem}.parquet.tmp"
    ts_values = [int(record.ts_utc.timestamp() * 1_000_000) for record in resampled]
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "metric": pa.array([str(record.metric) for record in resampled], type=pa.string()),
        "value": pa.array([float(record.value) for record in resampled], type=pa.float64()),
    })
    try:
        pq.write_table(table, temp_path)
        if not temp_path.is_file() or temp_path.stat().st_size == 0:
            raise OSError(f"Temporary Parquet output is empty: {temp_path}")
        temp_path.replace(output_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return output_path


def process_site_file(
    site: EgnyteSite,
    file: EgnyteFile,
    *,
    client: EgnyteClient,
    state: dict[str, Any],
    state_path: Path,
    output_root: Path = OUTPUT_ROOT,
) -> bool:
    key = file_version_key(file)
    if key in state["processed_files"]:
        return False
    LOGGER.info("[%s][EGNYTE] downloading %s", site.name.upper(), file.name)
    csv_bytes = client.download_file(file)
    if not csv_bytes:
        raise ValueError(f"Downloaded CSV for {file.name} is empty")
    date_label = file.path.strip("/").split("/")[-2]
    LOGGER.info("[%s][PREPROCESS] preprocessing %s", site.name.upper(), file.name)
    output_path = process_csv_bytes(
        csv_bytes=csv_bytes,
        source_name=file.name,
        date_label=date_label,
        output_station=site.output_station,
        output_root=output_root,
    )
    expected_path = output_root / site.output_station / date_label / f"{Path(file.name).stem}.parquet"
    if output_path != expected_path or not output_path.is_file() or output_path.stat().st_size == 0:
        raise OSError(f"Parquet handoff was not finalized as expected: {expected_path}")
    LOGGER.info("[%s][PARQUET] wrote %s", site.name.upper(), output_path)
    state["processed_files"][key] = {
        "filename": file.name,
        "remote_path": file.path,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "parquet_path": str(output_path),
    }
    save_state(state_path, state)
    LOGGER.info("[%s][STATE] marked %s completed", site.name.upper(), key)
    return True


def initialize_site_state(
    site: EgnyteSite,
    *,
    tz: ZoneInfo,
    baseline_before: str,
    state_root: Path | None = None,
    legacy_sala_path: Path | None = None,
    state_path: Path | None = None,
) -> Path:
    cutoff = parse_baseline_cutoff(baseline_before, tz)
    state_root = state_root or STATE_ROOT
    legacy_sala_path = legacy_sala_path or LEGACY_SALA_STATE_PATH
    path = state_path or state_path_for(site, state_root)
    if state_path is not None:
        state = load_state(path)
    else:
        state = load_site_state(site, state_root=state_root, legacy_sala_path=legacy_sala_path)
    state["baseline_before"] = cutoff.isoformat()
    save_state(path, state)
    LOGGER.info("[%s][STATE] baseline set locally to %s in %s", site.name.upper(), cutoff.isoformat(), path)
    return path


def run_site_cycle(
    site: EgnyteSite,
    *,
    client: EgnyteClient,
    state: dict[str, Any],
    state_path: Path,
    output_root: Path = OUTPUT_ROOT,
    dry_run: bool = False,
    now: datetime,
    tz: ZoneInfo,
) -> int:
    date_label = now.astimezone(tz).strftime("%Y-%m-%d")
    folder = f"{site.egnyte_folder}/{date_label}"
    prefix = site.name.upper()
    LOGGER.info("[%s][EGNYTE] checking %s", prefix, folder)
    files = client.list_files(folder, count=1000)
    LOGGER.info("[%s][EGNYTE] %d files returned", prefix, len(files))
    stored_baseline = state.get("baseline_before")
    cutoff = parse_baseline_cutoff(str(stored_baseline), tz) if stored_baseline else None
    if cutoff:
        LOGGER.info("[%s][BASELINE] cutoff = %s", prefix, cutoff.isoformat())
    new_files, historical_count = filter_new_files(files, set(state["processed_files"]), cutoff, tz)
    if cutoff:
        LOGGER.info("[%s][BASELINE] %d historical file(s) excluded", prefix, historical_count)
    eligible_count = len(files) - historical_count
    LOGGER.info("[%s][EGNYTE] %d eligible post-baseline file(s)", prefix, eligible_count)
    LOGGER.info("[%s][EGNYTE] %d new file(s) found", prefix, len(new_files))
    processed = 0
    for file in new_files:
        if dry_run:
            LOGGER.info("[%s][EGNYTE] dry-run; not downloading %s", prefix, file.name)
            continue
        try:
            if process_site_file(
                site, file, client=client, state=state, state_path=state_path, output_root=output_root
            ):
                processed += 1
        except Exception:
            LOGGER.exception("[%s][EGNYTE] failed to process %s", prefix, file.name)
    return processed


def run_sites_cycle(
    sites: list[EgnyteSite],
    *,
    client: EgnyteClient,
    now: datetime,
    tz_name: str = DEFAULT_TIMEZONE,
    dry_run: bool = False,
    output_root: Path = OUTPUT_ROOT,
    state_root: Path = STATE_ROOT,
    legacy_sala_path: Path = LEGACY_SALA_STATE_PATH,
) -> dict[str, int]:
    tz = ZoneInfo(tz_name)
    results: dict[str, int] = {}
    enabled_sites = [site for site in sites if site.enabled]
    if enabled_sites:
        try:
            client.ensure_authenticated()
        except Exception:
            LOGGER.exception("[EGNYTE][AUTH] authentication failed before site processing")
            return {site.name: 0 for site in enabled_sites}

    for site in sites:
        if not site.enabled:
            LOGGER.info("[%s][EGNYTE] site disabled; skipping", site.name.upper())
            continue
        path = state_path_for(site, state_root)
        has_existing_state = (path.exists() and path.stat().st_size > 0) or (
            site.name == "sala"
            and legacy_sala_path.exists()
            and legacy_sala_path.stat().st_size > 0
        )
        if not has_existing_state:
            LOGGER.warning(
                "[%s][STATE] no initialized state/baseline; skipping site until initialized",
                site.name.upper(),
            )
            results[site.name] = 0
            continue
        try:
            state = load_site_state(site, state_root=state_root, legacy_sala_path=legacy_sala_path)
            results[site.name] = run_site_cycle(
                site,
                client=client,
                state=state,
                state_path=path,
                output_root=output_root,
                dry_run=dry_run,
                now=now,
                tz=tz,
            )
        except Exception:
            LOGGER.exception("[%s][EGNYTE] site cycle failed; continuing to next site", site.name.upper())
            results[site.name] = 0
    return results


def run_scheduler(
    sites: list[EgnyteSite],
    *,
    client: EgnyteClient,
    tz_name: str = DEFAULT_TIMEZONE,
    output_root: Path = OUTPUT_ROOT,
    state_root: Path = STATE_ROOT,
) -> None:
    tz = ZoneInfo(tz_name)
    schedule = FixedClockSchedule()
    scheduler_label = sites[0].name.upper() if len(sites) == 1 else "EGNYTE"
    LOGGER.info("[EGNYTE] startup cycle (today-only)")
    run_sites_cycle(sites, client=client, now=datetime.now(tz), tz_name=tz_name,
                    output_root=output_root, state_root=state_root)
    next_run = schedule.next_run(datetime.now(tz), tz_name)
    LOGGER.info("[%s][SCHEDULER] cycle complete", scheduler_label)
    LOGGER.info("[%s][SCHEDULER] next run: %s", scheduler_label, next_run.isoformat())
    while True:
        next_run = schedule.next_run(datetime.now(tz), tz_name)
        while True:
            now = datetime.now(tz)
            if now >= next_run:
                break
            time.sleep((next_run - now).total_seconds())
        if not schedule.claim(next_run):
            continue
        run_sites_cycle(sites, client=client, now=datetime.now(tz), tz_name=tz_name,
                        output_root=output_root, state_root=state_root)
        next_run = schedule.next_run(datetime.now(tz), tz_name)
        LOGGER.info("[%s][SCHEDULER] cycle complete", scheduler_label)
        LOGGER.info("[%s][SCHEDULER] next run: %s", scheduler_label, next_run.isoformat())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run configured Egnyte site ingestion.")
    parser.add_argument("--once", action="store_true", help="Run one today-only cycle and exit.")
    parser.add_argument("--dry-run", action="store_true", help="List eligible files without downloading.")
    parser.add_argument("--site", help="Run one configured site by lowercase canonical name.")
    parser.add_argument("--all-sites", action="store_true", help="Initialize baseline state for every configured site (local only).")
    parser.add_argument("--initialize-state", action="store_true", help="Initialize one site's baseline state.")
    parser.add_argument("--baseline-before", help="Baseline cutoff YYYY-MM-DD HH:MM in the selected timezone.")
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--sites-config", type=Path, default=SITES_CONFIG_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        sites = load_sites(args.sites_config)
        if args.initialize_state:
            if args.all_sites and args.site:
                raise SystemExit("Use either --site or --all-sites with --initialize-state, not both.")
            if not args.all_sites and not args.site:
                raise SystemExit("--initialize-state requires --site <name> or --all-sites.")
            if not args.baseline_before:
                raise SystemExit("--initialize-state requires --baseline-before <timestamp>.")
            if args.site:
                sites = [site for site in sites if site.name == args.site.strip().lower()]
                if not sites:
                    raise SystemExit(f"Unknown Egnyte site: {args.site}")
            tz = ZoneInfo(args.timezone)
            for site in sites:
                initialize_site_state(
                    site,
                    tz=tz,
                    baseline_before=args.baseline_before,
                )
            return 0

        if args.all_sites:
            raise SystemExit("--all-sites is only valid with --initialize-state.")
        if args.site:
            sites = [site for site in sites if site.name == args.site.strip().lower()]
            if not sites:
                raise SystemExit(f"Unknown Egnyte site: {args.site}")
        client = EgnyteClient.from_environment()
        try:
            client.ensure_authenticated()
        except Exception:
            LOGGER.exception("[EGNYTE][AUTH] authentication failed before runner startup")
            return 1
        tz = ZoneInfo(args.timezone)
        now = datetime.now(tz)
        if args.once:
            run_sites_cycle(sites, client=client, now=now, tz_name=args.timezone, dry_run=args.dry_run)
        else:
            run_scheduler(sites, client=client, tz_name=args.timezone)
    except Exception:
        LOGGER.exception("[EGNYTE] runner failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())