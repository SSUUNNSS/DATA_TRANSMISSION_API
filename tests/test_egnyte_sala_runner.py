from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from egnyte import EgnyteClient, EgnyteFile
from egnyte_sala_runner import (
    SCHEDULED_CHECK_MINUTES,
    egnyte_uploaded_datetime,
    filter_new_files,
    get_due_check_times,
    initialize_sala_state,
    load_state,
    parse_csv_stream,
    process_sala_csv_bytes,
    process_sala_file,
    run_sala_cycle,
    save_state,
)
from egnyte_runner import EgnyteSite, initialize_site_state


def _uploaded_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


class EgnyteSalaRunnerTests(unittest.TestCase):
    def test_selecting_new_files_from_folder_listing(self) -> None:
        processed = {"g1:e1", "g2:e2"}
        files = [
            EgnyteFile(name="a.csv", path="/Shared/SALA/2026-10-01/a.csv", group_id="g1", entry_id="e1", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)), size=10, last_modified=""),
            EgnyteFile(name="b.csv", path="/Shared/SALA/2026-10-01/b.csv", group_id="g2", entry_id="e2", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 1, tzinfo=timezone.utc)), size=10, last_modified=""),
            EgnyteFile(name="c.csv", path="/Shared/SALA/2026-10-01/c.csv", group_id="g3", entry_id="e3", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 2, tzinfo=timezone.utc)), size=10, last_modified=""),
        ]

        self.assertEqual(
            [file.name for file in filter_new_files(files, processed)],
            ["c.csv"],
        )

    def test_ignores_already_processed_group_and_entry_ids(self) -> None:
        processed = {"g1:e1"}
        files = [
            EgnyteFile(name="dup.csv", path="/Shared/SALA/2026-10-01/dup.csv", group_id="g1", entry_id="e1", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)), size=10, last_modified=""),
            EgnyteFile(name="new.csv", path="/Shared/SALA/2026-10-01/new.csv", group_id="g1", entry_id="e2", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 1, tzinfo=timezone.utc)), size=10, last_modified=""),
        ]

        self.assertEqual(
            [file.name for file in filter_new_files(files, processed)],
            ["new.csv"],
        )

    def test_multiple_missed_files_are_processed_oldest_first(self) -> None:
        processed = set()
        files = [
            EgnyteFile(name="late.csv", path="/Shared/SALA/2026-10-01/late.csv", group_id="g3", entry_id="e3", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 2, tzinfo=timezone.utc)), size=10, last_modified=""),
            EgnyteFile(name="early.csv", path="/Shared/SALA/2026-10-01/early.csv", group_id="g1", entry_id="e1", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)), size=10, last_modified=""),
            EgnyteFile(name="middle.csv", path="/Shared/SALA/2026-10-01/middle.csv", group_id="g2", entry_id="e2", uploaded=_uploaded_ms(datetime(2026, 10, 1, 10, 1, tzinfo=timezone.utc)), size=10, last_modified=""),
        ]

        self.assertEqual(
            [file.name for file in sorted(filter_new_files(files, processed), key=lambda item: item.uploaded)],
            ["early.csv", "middle.csv", "late.csv"],
        )

    def test_csv_bytes_preprocess_without_writing_raw_csv_to_disk(self) -> None:
        csv_bytes = (
            "2026-10-01;00:00:00;TC1;1.0;0;0;A\n"
            "2026-10-01;00:00:30;TC1;1.0;0;0;A\n"
        ).encode("utf-8")

        stream = io.BytesIO(csv_bytes)
        records = list(parse_csv_stream(stream, source_name="sala-test"))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].metric, "TC1.A")
        self.assertEqual(records[0].value, 1.0)
        self.assertEqual(stream.tell() > 0, True)

    def test_state_is_updated_only_after_successful_parquet_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            source_dir = Path(tmp_dir) / "src" / "sourceData" / "normal" / "sala" / "2026-10-01"
            source_dir.mkdir(parents=True)
            state = {"processed_files": {}}
            save_state(state_path, state)

            csv_bytes = "2026-10-01;00:00:00;TC1;1.0;0;0;A\n".encode("utf-8")
            output_path = process_sala_csv_bytes(
                csv_bytes=csv_bytes,
                source_name="sala-test",
                date_label="2026-10-01",
                output_root=source_dir.parent.parent.parent,
                file_stem="sample",
            )

            state = load_state(state_path)
            self.assertTrue(output_path.exists())
            self.assertNotIn("g1:e1", state["processed_files"])

    def test_process_sala_file_orders_download_preprocessing_parquet_then_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_path = root / "sala_egnyte_state.json"
            output_root = root / "src" / "sourceData" / "normal"
            file = EgnyteFile(
                "sample.csv",
                "/Shared/SALA/2026-10-02/sample.csv",
                "group-1",
                "entry-1",
                _uploaded_ms(datetime(2026, 10, 2, 10, 0, tzinfo=timezone.utc)),
                100,
                "",
            )
            csv_bytes = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")
            events: list[str] = []
            client = Mock()
            client.download_file.side_effect = lambda candidate: events.append("download") or csv_bytes
            real_write_table = __import__("pyarrow.parquet", fromlist=["write_table"]).write_table

            def preprocess(stream, source_name):
                events.append("preprocess")
                return parse_csv_stream(stream, source_name)

            def write_parquet(table, path):
                temp_path = Path(path)
                final_path = temp_path.with_suffix("")
                self.assertEqual(temp_path.name, "sample.parquet.tmp")
                self.assertFalse(final_path.exists())
                events.append("parquet-temp-write")
                real_write_table(table, temp_path)

            def save_and_record(path, state):
                final_path = output_root / "sala" / "2026-10-02" / "sample.parquet"
                self.assertTrue(final_path.is_file())
                self.assertGreater(final_path.stat().st_size, 0)
                events.append("state-save")
                save_state(path, state)

            with (
                patch("egnyte_runner.parse_csv_stream", side_effect=preprocess),
                patch("egnyte_runner.pq.write_table", side_effect=write_parquet),
                patch("egnyte_runner.save_state", side_effect=save_and_record),
            ):
                processed = process_sala_file(
                    file,
                    client=client,
                    state_path=state_path,
                    output_root=output_root,
                )

            parquet_path = output_root / "sala" / "2026-10-02" / "sample.parquet"
            self.assertTrue(processed)
            self.assertEqual(events, ["download", "preprocess", "parquet-temp-write", "state-save"])
            self.assertTrue(parquet_path.is_file())
            self.assertTrue((output_root / "sala" / "2026-10-02" / "sample.parquet").exists())
            self.assertFalse((output_root / "sala" / "2026-10-02" / "sample.csv").exists())
            self.assertEqual(set(load_state(state_path)["processed_files"]), {"group-1:entry-1"})

    def test_preprocessing_failure_does_not_update_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            file = EgnyteFile("bad.csv", "/Shared/SALA/2026-10-02/bad.csv", "group-1", "entry-1", 0, 10, "")
            client = Mock()
            client.download_file.return_value = b"bad,csv"

            with patch("egnyte_runner.parse_csv_stream", side_effect=ValueError("mock preprocessing failure")):
                with self.assertRaisesRegex(ValueError, "mock preprocessing failure"):
                    process_sala_file(file, client=client, state_path=state_path, output_root=Path(tmp_dir))

            self.assertNotIn("group-1:entry-1", load_state(state_path)["processed_files"])

    def test_parquet_write_failure_does_not_update_state_or_leave_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            output_root = Path(tmp_dir) / "normal"
            file = EgnyteFile("sample.csv", "/Shared/SALA/2026-10-02/sample.csv", "group-1", "entry-1", 0, 10, "")
            client = Mock()
            client.download_file.return_value = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")

            def fail_after_partial_write(_table, path):
                Path(path).write_bytes(b"partial parquet")
                raise OSError("mock parquet write failure")

            with patch("egnyte_runner.pq.write_table", side_effect=fail_after_partial_write):
                with self.assertRaisesRegex(OSError, "mock parquet write failure"):
                    process_sala_file(file, client=client, state_path=state_path, output_root=output_root)

            output_dir = output_root / "sala" / "2026-10-02"
            self.assertFalse((output_dir / "sample.parquet").exists())
            self.assertFalse((output_dir / "sample.parquet.tmp").exists())
            self.assertNotIn("group-1:entry-1", load_state(state_path)["processed_files"])

    def test_failed_processing_remains_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            bad_csv = b"not,a,valid,csv\n"
            with self.assertRaises(ValueError):
                process_sala_csv_bytes(
                    csv_bytes=bad_csv,
                    source_name="bad-file",
                    date_label="2026-10-01",
                    output_root=Path(tmp_dir),
                    file_stem="bad",
                )

            state = load_state(state_path)
            self.assertEqual(state["processed_files"], {})

    def test_baseline_initialization_only_updates_local_state_and_preserves_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala.json"
            state_path.write_text(json.dumps({
                "processed_files": {"existing-group:existing-entry": {"filename": "existing.csv"}},
                "other_metadata": "retained",
            }), encoding="utf-8")
            client = Mock()

            with patch("egnyte_sala_runner.EgnyteClient.from_environment") as from_environment:
                from egnyte_sala_runner import initialize_sala_state
                result_path = initialize_sala_state(
                    state_path=state_path,
                    baseline_before="2026-10-05 10:25",
                    tz_name="Europe/Stockholm",
                )

            state = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(state["baseline_before"], "2026-10-05T10:25:00+02:00")
            self.assertIn("existing-group:existing-entry", state["processed_files"])
            self.assertEqual(state["other_metadata"], "retained")
            from_environment.assert_not_called()
            client.list_files.assert_not_called()
            client.download_file.assert_not_called()

    def test_baseline_initialization_does_not_list_or_record_file_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "gavle.json"
            save_state(state_path, {"processed_files": {"existing:version": {"filename": "kept.csv"}}})

            result = initialize_site_state(
                EgnyteSite("gavle", "/Shared/GAVLE", "gavle", Path("config/gavle_local.json")),
                tz=ZoneInfo("Europe/Stockholm"),
                baseline_before="2026-10-05 10:25",
                state_path=state_path,
            )

            self.assertEqual(result, state_path)
            state = load_state(state_path)
            self.assertEqual(set(state["processed_files"]), {"existing:version"})
            self.assertEqual(state["baseline_before"], "2026-10-05T10:25:00+02:00")

    def test_startup_catch_up_skips_previous_and_same_day_historical_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            cutoff = datetime(2026, 10, 2, 10, 40, tzinfo=timezone.utc)
            yesterday_file = EgnyteFile(
                "yesterday.csv", "/Shared/SALA/2026-10-01/yesterday.csv", "g-y", "e-y",
                _uploaded_ms(datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)), 10, ""
            )
            same_day_before = EgnyteFile(
                "before.csv", "/Shared/SALA/2026-10-02/before.csv", "g-b", "e-b",
                _uploaded_ms(datetime(2026, 10, 2, 10, 39, tzinfo=timezone.utc)), 10, ""
            )
            at_cutoff = EgnyteFile(
                "at-cutoff.csv", "/Shared/SALA/2026-10-02/at-cutoff.csv", "g-c", "e-c",
                _uploaded_ms(cutoff), 10, ""
            )
            after_cutoff = EgnyteFile(
                "after.csv", "/Shared/SALA/2026-10-02/after.csv", "g-a", "e-a",
                _uploaded_ms(datetime(2026, 10, 2, 10, 41, tzinfo=timezone.utc)), 10, ""
            )
            save_state(state_path, {"processed_files": {}, "baseline_before": "2026-10-02T12:40:00+02:00"})
            client = Mock()
            client.list_files.side_effect = [[after_cutoff, same_day_before, at_cutoff]]
            client.download_file.return_value = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")

            processed = run_sala_cycle(
                client=client,
                state_path=state_path,
                output_root=Path(tmp_dir) / "normal",
                startup=True,
                now=datetime(2026, 10, 2, 13, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
                tz_name="Europe/Stockholm",
            )

            self.assertEqual(processed, 1)
            client.download_file.assert_called_once_with(after_cutoff)
            client.list_files.assert_called_once_with("/Shared/SALA/2026-10-02", count=1000)

    def test_startup_catch_up_skips_all_96_previous_day_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            save_state(state_path, {
                "processed_files": {},
                "baseline_before": "2026-10-02T12:40:00+02:00",
            })
            client = Mock()
            client.list_files.return_value = []
            now = datetime(2026, 10, 2, 13, 0, tzinfo=ZoneInfo("Europe/Stockholm"))

            with (
                patch("egnyte_runner.process_site_file") as process_file,
                patch("egnyte_runner.parse_csv_stream") as preprocess,
                patch("egnyte_runner.pq.write_table") as parquet_write,
                self.assertLogs("sala_egnyte", level="INFO") as captured,
            ):
                processed = run_sala_cycle(
                    client=client,
                    state_path=state_path,
                    output_root=Path(tmp_dir) / "normal",
                    startup=True,
                    now=now,
                    tz_name="Europe/Stockholm",
                )

            output = "\n".join(captured.output)
            self.assertEqual(processed, 0)
            client.list_files.assert_called_once_with("/Shared/SALA/2026-10-02", count=1000)
            client.download_file.assert_not_called()
            process_file.assert_not_called()
            preprocess.assert_not_called()
            parquet_write.assert_not_called()
            self.assertIn("[SALA][BASELINE] cutoff = 2026-10-02T12:40:00+02:00", output)
            self.assertNotIn("96 files returned", output)
            self.assertNotIn("skipping historical file", output)

    def test_same_day_cutoff_only_allows_files_after_1240(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            save_state(state_path, {
                "processed_files": {},
                "baseline_before": "2026-10-02T12:40:00+02:00",
            })
            times = [
                ("12-25", datetime(2026, 10, 2, 10, 25, tzinfo=timezone.utc)),
                ("12-40", datetime(2026, 10, 2, 10, 40, tzinfo=timezone.utc)),
                ("12-55", datetime(2026, 10, 2, 10, 55, tzinfo=timezone.utc)),
                ("13-10", datetime(2026, 10, 2, 11, 10, tzinfo=timezone.utc)),
            ]
            files = [
                EgnyteFile(
                    f"sala_{label}.csv",
                    f"/Shared/SALA/2026-10-02/sala_{label}.csv",
                    f"group-{label}",
                    f"entry-{label}",
                    _uploaded_ms(uploaded_utc),
                    10,
                    "",
                )
                for label, uploaded_utc in times
            ]
            client = Mock()
            client.list_files.return_value = files
            client.download_file.return_value = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")

            with self.assertLogs("sala_egnyte", level="INFO") as captured:
                processed = run_sala_cycle(
                    client=client,
                    state_path=state_path,
                    output_root=Path(tmp_dir) / "normal",
                    now=datetime(2026, 10, 2, 14, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
                    tz_name="Europe/Stockholm",
                )

            self.assertEqual(processed, 2)
            self.assertEqual(
                [call.args[0].name for call in client.download_file.call_args_list],
                ["sala_12-55.csv", "sala_13-10.csv"],
            )
            output = "\n".join(captured.output)
            self.assertIn("[SALA][BASELINE] 2 historical file(s) excluded", output)
            self.assertIn("[SALA][EGNYTE] 2 eligible post-baseline file(s)", output)
            self.assertNotIn("skipping historical file", output)

    def test_processed_ids_still_prevent_duplicates_after_baseline(self) -> None:
        cutoff = datetime(2026, 10, 2, 10, 40, tzinfo=timezone.utc)
        duplicate = EgnyteFile(
            "duplicate.csv", "/Shared/SALA/2026-10-02/duplicate.csv", "g-known", "e-known",
            _uploaded_ms(datetime(2026, 10, 2, 10, 41, tzinfo=timezone.utc)), 10, ""
        )
        candidate = EgnyteFile(
            "candidate.csv", "/Shared/SALA/2026-10-02/candidate.csv", "g-new", "e-new",
            _uploaded_ms(datetime(2026, 10, 2, 10, 42, tzinfo=timezone.utc)), 10, ""
        )

        selected = filter_new_files(
            [duplicate, candidate],
            {"g-known:e-known"},
            baseline_cutoff=cutoff,
            tz=ZoneInfo("Europe/Stockholm"),
        )

        self.assertEqual([file.name for file in selected], ["candidate.csv"])

    def test_local_initialization_preserves_existing_processed_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            existing_cutoff = "2026-10-02T12:40:00+02:00"
            save_state(state_path, {
                "processed_files": {"old-group:old-entry": {"filename": "old.csv"}},
                "baseline_before": existing_cutoff,
            })
            client = Mock()
            client.list_files.return_value = []

            initialize_sala_state(
                client=client,
                state_path=state_path,
                now=datetime(2026, 10, 2, 13, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
                baseline_before="2026-10-02 12:40",
            )

            self.assertEqual(load_state(state_path)["baseline_before"], existing_cutoff)
            self.assertIn("old-group:old-entry", load_state(state_path)["processed_files"])
            client.list_files.assert_not_called()

    def test_file_after_cutoff_is_processed_by_next_normal_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            baseline_file = EgnyteFile(
                "historical.csv", "/Shared/SALA/2026-10-02/historical.csv", "g1", "e1",
                _uploaded_ms(datetime(2026, 10, 2, 10, 0, tzinfo=timezone.utc)), 10, ""
            )
            newer_file = EgnyteFile(
                "newer.csv",
                "/Shared/SALA/2026-10-02/newer.csv",
                "g2",
                "e2",
                _uploaded_ms(datetime(2026, 10, 2, 10, 41, tzinfo=timezone.utc)),
                10,
                "",
            )
            client = Mock()
            client.list_files.side_effect = [[baseline_file, newer_file], [baseline_file, newer_file]]
            client.download_file.return_value = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")
            now = datetime(2026, 10, 2, 13, 0, tzinfo=ZoneInfo("Europe/Stockholm"))

            initialize_sala_state(
                client=client,
                state_path=state_path,
                now=now,
                tz_name="Europe/Stockholm",
                baseline_before="2026-10-02 12:40",
            )
            count = run_sala_cycle(
                client=client,
                state_path=state_path,
                output_root=Path(tmp_dir) / "normal",
                now=now,
            )

            self.assertEqual(count, 1)
            client.download_file.assert_called_once_with(newer_file)
            self.assertIn("g2:e2", load_state(state_path)["processed_files"])

    def test_local_baseline_initialization_does_not_list_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            original_state = {"processed_files": {"old-group:old-entry": {"filename": "old.csv"}}}
            save_state(state_path, original_state)
            client = Mock()
            initialize_sala_state(
                client=client,
                state_path=state_path,
                now=datetime(2026, 10, 2, 10, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
                baseline_before="2026-10-02 12:40",
            )

            self.assertEqual(load_state(state_path)["processed_files"], original_state["processed_files"])
            client.list_files.assert_not_called()
            client.download_file.assert_not_called()

    def test_new_file_after_initialization_is_processed_normally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            output_root = Path(tmp_dir) / "normal"
            existing = EgnyteFile("existing.csv", "/Shared/SALA/2026-10-02/existing.csv", "g1", "e1", _uploaded_ms(datetime(2026, 10, 2, 8, 0, tzinfo=timezone.utc)), 10, "")
            new_file = EgnyteFile("new.csv", "/Shared/SALA/2026-10-02/new.csv", "g2", "e2", _uploaded_ms(datetime(2026, 10, 2, 8, 1, tzinfo=timezone.utc)), 10, "")
            client = Mock()
            client.list_files.return_value = [existing, new_file]
            client.download_file.return_value = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")
            now = datetime(2026, 10, 2, 10, 0, tzinfo=ZoneInfo("Europe/Stockholm"))

            initialize_sala_state(
                client=client,
                state_path=state_path,
                now=now,
                baseline_before="2026-10-02 10:00",
            )
            processed_count = run_sala_cycle(
                client=client,
                state_path=state_path,
                output_root=output_root,
                now=now,
            )

            self.assertEqual(processed_count, 1)
            client.download_file.assert_called_once_with(new_file)
            client.list_files.assert_called_once()
            state = load_state(state_path)
            self.assertNotIn("g1:e1", state["processed_files"])
            self.assertIn("g2:e2", state["processed_files"])
            self.assertTrue((output_root / "sala" / "2026-10-02" / "new.parquet").exists())

    def test_missed_files_are_processed_oldest_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_path = Path(tmp_dir) / "sala_egnyte_state.json"
            files = [
                EgnyteFile("late.csv", "/Shared/SALA/2026-10-02/late.csv", "g3", "e3", _uploaded_ms(datetime(2026, 10, 2, 10, 2, tzinfo=timezone.utc)), 10, ""),
                EgnyteFile("early.csv", "/Shared/SALA/2026-10-02/early.csv", "g1", "e1", _uploaded_ms(datetime(2026, 10, 2, 10, 0, tzinfo=timezone.utc)), 10, ""),
                EgnyteFile("middle.csv", "/Shared/SALA/2026-10-02/middle.csv", "g2", "e2", _uploaded_ms(datetime(2026, 10, 2, 10, 1, tzinfo=timezone.utc)), 10, ""),
            ]
            client = Mock()
            client.list_files.return_value = files
            client.download_file.return_value = (
                "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
                "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
            ).encode("utf-8")

            processed_count = run_sala_cycle(
                client=client,
                state_path=state_path,
                output_root=Path(tmp_dir) / "normal",
                now=datetime(2026, 10, 2, 10, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
            )

            self.assertEqual(processed_count, 3)
            self.assertEqual(
                [call.args[0].name for call in client.download_file.call_args_list],
                ["early.csv", "middle.csv", "late.csv"],
            )

    def test_schedule_calculation_targets_12_27_42_57(self) -> None:
        candidate = datetime(2026, 10, 1, 9, 59, tzinfo=timezone.utc)
        self.assertNotIn(candidate.minute, SCHEDULED_CHECK_MINUTES)

        next_times = get_due_check_times(datetime(2026, 10, 1, 10, 10, tzinfo=timezone.utc))
        self.assertEqual([slot.minute for slot in next_times], [12, 27, 42, 57])

    def test_schedule_rolls_over_to_next_hour(self) -> None:
        next_times = get_due_check_times(datetime(2026, 10, 1, 10, 58, tzinfo=ZoneInfo("Europe/Stockholm")))
        self.assertEqual([slot.hour for slot in next_times], [11, 11, 11, 11])
        self.assertEqual([slot.minute for slot in next_times], [12, 27, 42, 57])

    def test_realistic_13_digit_uploaded_timestamp_converts_from_milliseconds(self) -> None:
        uploaded_ms = _uploaded_ms(datetime(2026, 10, 2, 10, 40, tzinfo=timezone.utc))

        converted = egnyte_uploaded_datetime(uploaded_ms, ZoneInfo("Europe/Stockholm"))

        self.assertGreaterEqual(len(str(uploaded_ms)), 13)
        self.assertEqual(converted, datetime(2026, 10, 2, 12, 40, tzinfo=ZoneInfo("Europe/Stockholm")))

    def test_egnyte_client_list_and_download_are_mocked(self) -> None:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "files": [
                {
                    "name": "a.csv",
                    "path": "/Shared/SALA/2026-10-01/a.csv",
                    "group_id": "g1",
                    "entry_id": "e1",
                    "uploaded": _uploaded_ms(datetime(2026, 10, 1, 10, 10, tzinfo=timezone.utc)),
                    "size": 128,
                    "last_modified": "2026-10-01T10:10:00Z",
                }
            ]
        }
        mock_response.headers = {}
        mock_response.raise_for_status.return_value = None

        mock_file = Mock()
        mock_file.status_code = 200
        mock_file.content = b"date;time;metric;value;0;0;A\n2026-10-01;10:10:00;TC1;1.0;0;0;A\n"
        mock_file.headers = {}
        mock_file.raise_for_status.return_value = None

        with patch.object(requests.Session, "get", side_effect=[mock_response, mock_file]) as get_mock:
            client = EgnyteClient("scadadata.egnyte.com", "test-token")
            files = client.list_files("/Shared/SALA/2026-10-01", count=10)
            downloaded = client.download_file(files[0])

        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].name, "a.csv")
        self.assertEqual(downloaded, mock_file.content)
        self.assertEqual(get_mock.call_count, 2)

    def test_environment_access_token_override_is_used_directly(self) -> None:
        session = Mock()
        client = EgnyteClient.from_environment({
            "EGNYTE_DOMAIN": "scadadata",
            "EGNYTE_ACCESS_TOKEN": "override-token",
        })
        client.session = session

        self.assertEqual(client.headers["Authorization"], "Bearer override-token")
        session.post.assert_not_called()

    def test_oauth_token_is_acquired_as_form_data_and_cached(self) -> None:
        token_response = Mock()
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "oauth-token"}
        api_response = Mock()
        api_response.status_code = 200
        api_response.headers = {}
        api_response.raise_for_status.return_value = None
        session = Mock()
        session.post.return_value = token_response
        session.get.return_value = api_response
        client = EgnyteClient(
            "scadadata",
            api_key="api-key-value",
            api_secret="api-secret-value",
            username="technical-user",
            password="password-value",
            session=session,
        )

        client._get("https://scadadata.egnyte.com/pubapi/v1/test")
        client._get("https://scadadata.egnyte.com/pubapi/v1/test")

        session.post.assert_called_once()
        args, kwargs = session.post.call_args
        self.assertEqual(args[0], "https://scadadata.egnyte.com/puboauth/token")
        self.assertEqual(kwargs["data"], {
            "grant_type": "password",
            "client_id": "api-key-value",
            "client_secret": "api-secret-value",
            "username": "technical-user",
            "password": "password-value",
        })
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/x-www-form-urlencoded")
        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(session.get.call_args.kwargs["headers"]["Authorization"], "Bearer oauth-token")

    def test_oauth_429_honors_retry_after_then_caches_token(self) -> None:
        limited = Mock(status_code=429, headers={"Retry-After": "7"})
        token_response = Mock(status_code=200, headers={})
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "cached-after-429"}
        session = Mock()
        session.post.side_effect = [limited, token_response]
        limiter = Mock()
        client = EgnyteClient(
            "scadadata",
            api_key="api-key-value",
            api_secret="api-secret-value",
            username="technical-user",
            password="password-value",
            session=session,
            rate_limiter=limiter,
        )

        with patch("egnyte.client.time.sleep") as sleep_mock, self.assertLogs("egnyte.client", level="WARNING") as captured:
            client.ensure_authenticated()
            client.ensure_authenticated()

        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(limiter.wait.call_count, 2)
        sleep_mock.assert_called_once_with(7.0)
        self.assertEqual(client.headers["Authorization"], "Bearer cached-after-429")
        self.assertIn("token endpoint rate limited; retrying in 7 seconds", "\n".join(captured.output))
        for secret in ("api-key-value", "api-secret-value", "technical-user", "password-value", "cached-after-429"):
            self.assertNotIn(secret, "\n".join(captured.output))

    def test_oauth_429_without_retry_after_uses_bounded_exponential_backoff(self) -> None:
        limited_responses = [Mock(status_code=429, headers={}) for _ in range(3)]
        token_response = Mock(status_code=200, headers={})
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "eventual-token"}
        session = Mock()
        session.post.side_effect = [*limited_responses, token_response]
        limiter = Mock()
        client = EgnyteClient(
            "scadadata",
            api_key="api-key-value",
            api_secret="api-secret-value",
            username="technical-user",
            password="password-value",
            session=session,
            rate_limiter=limiter,
        )

        with patch("egnyte.client.time.sleep") as sleep_mock:
            client.ensure_authenticated()

        self.assertEqual(session.post.call_count, 4)
        self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [2.0, 4.0, 8.0])
        self.assertEqual(limiter.wait.call_count, 4)
        self.assertEqual(client.headers["Authorization"], "Bearer eventual-token")

    def test_oauth_429_retries_are_bounded(self) -> None:
        session = Mock()
        session.post.side_effect = [Mock(status_code=429, headers={}) for _ in range(5)]
        limiter = Mock()
        client = EgnyteClient(
            "scadadata",
            api_key="api-key-value",
            api_secret="api-secret-value",
            username="technical-user",
            password="password-value",
            session=session,
            rate_limiter=limiter,
        )

        with patch("egnyte.client.time.sleep") as sleep_mock:
            with self.assertRaisesRegex(requests.HTTPError, "remained rate limited after 3 retries"):
                client.ensure_authenticated()

        self.assertEqual(session.post.call_count, 4)
        self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [2.0, 4.0, 8.0])
        self.assertEqual(limiter.wait.call_count, 4)

    def test_unauthorized_request_refreshes_token_and_retries_once(self) -> None:
        token_response = Mock()
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "refreshed-token"}
        unauthorized = Mock(status_code=401, headers={})
        authorized = Mock(status_code=200, headers={})
        authorized.raise_for_status.return_value = None
        session = Mock()
        session.post.return_value = token_response
        session.get.side_effect = [unauthorized, authorized]
        client = EgnyteClient(
            "scadadata.egnyte.com",
            "expired-token",
            api_key="api-key-value",
            api_secret="api-secret-value",
            username="technical-user",
            password="password-value",
            max_retries=0,
            session=session,
        )

        response = client._get("https://scadadata.egnyte.com/pubapi/v1/test")

        self.assertIs(response, authorized)
        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(session.get.call_args_list[0].kwargs["headers"]["Authorization"], "Bearer expired-token")
        self.assertEqual(session.get.call_args_list[1].kwargs["headers"]["Authorization"], "Bearer refreshed-token")
        session.post.assert_called_once()

    def test_second_unauthorized_response_fails_clearly_without_another_retry(self) -> None:
        token_response = Mock()
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "refreshed-token"}
        session = Mock()
        session.post.return_value = token_response
        session.get.side_effect = [Mock(status_code=401), Mock(status_code=401)]
        client = EgnyteClient(
            "scadadata",
            "expired-token",
            api_key="api-key-value",
            api_secret="api-secret-value",
            username="technical-user",
            password="password-value",
            session=session,
        )

        with self.assertRaisesRegex(requests.HTTPError, "HTTP 401 after token refresh"):
            client._get("https://scadadata.egnyte.com/pubapi/v1/test")

        self.assertEqual(session.get.call_count, 2)
        session.post.assert_called_once()

    def test_credentials_are_not_logged(self) -> None:
        token_response = Mock()
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "sensitive-access-token"}
        api_response = Mock(status_code=200, headers={})
        api_response.raise_for_status.return_value = None
        session = Mock()
        session.post.return_value = token_response
        session.get.return_value = api_response
        client = EgnyteClient(
            "scadadata",
            api_key="sensitive-api-key",
            api_secret="sensitive-api-secret",
            username="sensitive-username",
            password="sensitive-password",
            session=session,
        )

        with self.assertNoLogs("egnyte.client"):
            client._get("https://scadadata.egnyte.com/pubapi/v1/test")


if __name__ == "__main__":
    unittest.main()
