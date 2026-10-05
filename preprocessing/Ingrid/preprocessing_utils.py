from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Iterable

import pyarrow.parquet as pq


@dataclass(frozen=True)
class Record:
    ts_utc: datetime
    metric: str
    value: float


@dataclass(frozen=True)
class Batch:
    name: str
    files: list[Path]


REQUIRED_COLUMNS = ("ts_utc", "metric", "value")


def list_batches(source_dir: Path, batch_by_subdirectory: bool = False) -> list[Batch]:
    if not source_dir.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    if source_dir.is_file():
        return [Batch(source_dir.stem, [source_dir])]

    if batch_by_subdirectory:
        batches = []
        for child in sorted(path for path in source_dir.iterdir() if path.is_dir()):
            files = _data_files(child)
            if files:
                batches.append(Batch(child.name, files))
        if not batches:
            raise FileNotFoundError(f"No supported data files found in {source_dir}")
        return batches

    files = _data_files(source_dir)
    if not files:
        raise FileNotFoundError(f"No supported data files found in {source_dir}")
    return [Batch(source_dir.name, files)]


def iter_records(batch: Batch) -> Iterator[Record]:
    for file_path in batch.files:
        if file_path.suffix.lower() in {".csv", ""}:
            yield from _iter_text(file_path)
        elif file_path.suffix.lower() == ".parquet":
            yield from _iter_parquet(file_path)


def iter_resampled(batch: Batch) -> Iterator[Record]:
    yield from resample_records(iter_records(batch))


def resample_records(records: Iterable[Record]) -> Iterator[Record]:
    earliest: dict[tuple[str, datetime], Record] = {}
    for record in records:
        bucket = record.ts_utc.replace(
            minute=record.ts_utc.minute,
            second=0,
            microsecond=0,
        )
        key = (record.metric, bucket)
        current = earliest.get(key)
        if current is None or record.ts_utc < current.ts_utc:
            earliest[key] = record

    yield from sorted(earliest.values(), key=lambda record: (record.ts_utc, record.metric))


def _data_files(directory: Path) -> list[Path]:
    return sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.suffix.lower() in {".csv", ".parquet", ""}
    )


def _iter_parquet(file_path: Path) -> Iterator[Record]:
    parquet_file = pq.ParquetFile(file_path)
    missing = [column for column in REQUIRED_COLUMNS if column not in parquet_file.schema_arrow.names]
    if missing:
        raise ValueError(f"Parquet file {file_path} is missing columns: {missing}")

    for row_group_index in range(parquet_file.metadata.num_row_groups):
        table = parquet_file.read_row_group(row_group_index, columns=list(REQUIRED_COLUMNS))
        for row in table.to_pylist():
            yield _make_record(row["ts_utc"], row["metric"], row["value"], file_path)


def _iter_text(file_path: Path) -> Iterator[Record]:
    with file_path.open("rb") as stream:
        yield from parse_csv_stream(stream, str(file_path))


def parse_csv_stream(stream, source_name: str = "<stream>") -> Iterator[Record]:
    """Parse a text or seekable binary stream without persisting raw data."""
    prefix = stream.read(4)
    stream.seek(0)
    wrapper = None
    if isinstance(prefix, bytes):
        encoding = "utf-16" if prefix.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
        wrapper = io.TextIOWrapper(stream, encoding=encoding, newline="")
    try:
        reader = csv.reader(wrapper or stream, delimiter=";", strict=True)
        for line_number, row in enumerate(reader, start=1):
            if not row or not any(field.strip() for field in row):
                continue
            if len(row) >= 7:
                timestamp = f"{row[0].strip()} {row[1].strip()}"
                metric = f"{row[2].strip()}.{row[6].strip()}"
                value = row[3].strip()
            elif len(row) >= 3:
                timestamp, metric, value = (field.strip() for field in row[:3])
            else:
                raise ValueError(f"Invalid data row in {source_name} at line {line_number}")
            yield _make_record(timestamp, metric, value, source_name)
    finally:
        if wrapper is not None:
            wrapper.detach()


def _detect_encoding(file_path: Path) -> str:
    with file_path.open("rb") as data_file:
        prefix = data_file.read(4)
    if prefix.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    if prefix.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"


def _make_record(ts_utc, metric, value, file_path: Path) -> Record:
    if isinstance(ts_utc, str):
        ts_utc = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)
    try:
        numeric_value = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Non-numeric value in {file_path}: {value!r}") from error
    return Record(ts_utc=ts_utc, metric=str(metric), value=numeric_value)
