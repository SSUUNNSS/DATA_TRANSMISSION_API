"""把站点各日期子目录的原始数据重采样成三列 parquet，每天一个文件。

目录结构（数据已按日期分目录）：
    src/sourceData/Falkoping/
        2026-08-01/  （80 个文件）
        2026-08-02/  （96 个文件）
        ...

本脚本遍历每个日期子目录，对当天的数据做 1 分钟重采样，每天输出一个
以日期命名的文件夹，内含对应日期的 parquet（便于按天上传到云端）。
parquet 统一输出到站点文件夹内的 <站点>_parquet 子目录：
    src/sourceData/normal/Falkoping/2026-08-01/2026-08-01.parquet
    src/sourceData/normal/Falkoping/2026-08-02/2026-08-02.parquet
    ...

三列 = ts_utc / metric / value，schema 对齐 octopus 的 parquet：
    ts_utc: timestamp[us, tz=UTC]
    metric: string（`测点.描述` 点分格式）
    value : double

用法（在项目根目录下）：
    .venv\\Scripts\\python.exe preprocessing\\Ingrid\\export_daily_parquet.py

    # 指定站点源目录；输出自动放到 normal\\<站点>\\<日期>
    .venv\\Scripts\\python.exe preprocessing\\Ingrid\\export_daily_parquet.py \\
        --source-dir src\\sourceData\\raw_data\\sala
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

try:
    from .preprocessing_utils import Batch, iter_resampled, list_batches
except ImportError:
    from preprocessing_utils import Batch, iter_resampled, list_batches

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "src" / "sourceData" / "normal" / "falkoping"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "src" / "sourceData" / "normal"
CHUNK_SIZE = 200_000
# 日期子目录名：形如 2026-08-21。用于跳过 <站点>_parquet 等非日期目录。
DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_args() -> argparse.Namespace:
    """解析命令行参数，支持指定站点源目录与输出目录。"""
    parser = argparse.ArgumentParser(
        description="Resample per-day raw station data into three-column parquet."
    )
    parser.add_argument(
        "--source-dir",
        default=str(DEFAULT_SOURCE_DIR),
        help="Directory containing per-day raw data subdirectories.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output root; station/date folders are added automatically.",
    )
    parser.add_argument(
        "--only-date",
        default="",
        help="Only process this date (subdirectory name), e.g. 2026-08-01.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rewrite output files even when they already exist.",
    )
    parser.add_argument(
        "--input-file",
        default="",
        help="Process only this raw file and name the output after that file.",
    )
    return parser.parse_args()


def _to_micros(ts) -> int:
    """时间戳 → UTC 微秒整数（parquet timestamp[us, tz=UTC]）。"""
    return int(ts.timestamp() * 1_000_000)


def _write_chunk(
    writer: pq.ParquetWriter,
    ts_list: list[int],
    metric_list: list[str],
    value_list: list[float],
) -> None:
    """把一块数据打包成 Table 写入 parquet。"""
    table = pa.table(
        {
            "ts_utc": pa.array(ts_list, type=pa.timestamp("us", tz="UTC")),
            "metric": pa.array(metric_list, type=pa.string()),
            "value": pa.array(value_list, type=pa.float64()),
        }
    )
    writer.write_table(table)


def export_batch(batch, output_path: Path) -> int:
    """把一天（一个 batch）重采样后写入一个 parquet，返回写入行数。"""
    return export_records(iter_resampled(batch), output_path)


def export_records(records, output_path: Path) -> int:
    """Write already resampled records using the shared schema and chunking."""
    schema = pa.schema(
        [
            pa.field("ts_utc", pa.timestamp("us", tz="UTC")),
            pa.field("metric", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )

    writer = pq.ParquetWriter(output_path, schema)
    ts_list: list[int] = []
    metric_list: list[str] = []
    value_list: list[float] = []
    total = 0

    try:
        for record in records:
            ts_list.append(_to_micros(record.ts_utc))
            metric_list.append(str(record.metric))
            value_list.append(float(record.value))
            total += 1
            if len(ts_list) >= CHUNK_SIZE:
                _write_chunk(writer, ts_list, metric_list, value_list)
                ts_list, metric_list, value_list = [], [], []
        if ts_list:
            _write_chunk(writer, ts_list, metric_list, value_list)
    finally:
        writer.close()

    return total


def main() -> None:
    args = parse_args()
    source_dir = Path(args.source_dir).expanduser().resolve()
    output_root = Path(args.output_dir).expanduser().resolve()
    output_dir = output_root / source_dir.name
    only_date = args.only_date.strip()

    output_dir.mkdir(parents=True, exist_ok=True)

    if args.input_file:
        input_file = Path(args.input_file).expanduser().resolve()
        if not input_file.is_file():
            raise SystemExit(f"Input file not found: {input_file}")
        if input_file.parent.name != only_date:
            raise SystemExit(f"Input file is not inside date directory {only_date}: {input_file}")
        batches = [Batch(name=only_date, files=[input_file])]
    else:
        batches = list_batches(source_dir, batch_by_subdirectory=True)

    # 只处理日期命名的子目录，跳过 Falkoping_parquet 这类非日期目录。
    batches = [batch for batch in batches if DATE_DIR_RE.match(batch.name)]

    if only_date:
        batches = [batch for batch in batches if batch.name == only_date]
        if not batches:
            raise SystemExit(f"Date {only_date} not found in {source_dir}")
    if not batches:
        raise SystemExit(f"No date subdirectories found in {source_dir}")

    grand_total = 0
    skipped = 0
    for batch in batches:
        # 每天一个以日期命名的子文件夹，内含同名 parquet，方便按天上传。
        day_dir = output_dir / batch.name
        day_dir.mkdir(parents=True, exist_ok=True)
        if args.input_file:
            output_name = f"{batch.files[0].stem}.parquet"
        else:
            output_name = f"{batch.name}.parquet"
        output_path = day_dir / output_name

        # 增量：已生成过的日期直接跳过（文件存在且非空即视为已完成）。
        if not args.force and output_path.exists() and output_path.stat().st_size > 0:
            print(f"{batch.name}: skip (already exists)")
            skipped += 1
            continue

        rows = export_batch(batch, output_path)
        size_mb = output_path.stat().st_size / (1024 * 1024)
        grand_total += rows
        print(f"{batch.name}: {rows} rows -> {output_path.name} ({size_mb:.1f} MB)")

    print(
        f"Done. {len(batches)} dates, {len(batches) - skipped} generated "
        f"({skipped} skipped), {grand_total} rows total in {output_dir}"
    )


if __name__ == "__main__":
    main()
