from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from egnyte import EgnyteClient, EgnyteFile
from egnyte.client import GLOBAL_RATE_LIMITER, GlobalRateLimiter
from egnyte_runner import (
    EgnyteSite,
    FixedClockSchedule,
    filter_new_files,
    get_due_check_times,
    load_site_state,
    load_sites,
    main,
    run_sites_cycle,
    state_path_for,
)


TZ = ZoneInfo("Europe/Stockholm")
NOW = datetime(2026, 10, 2, 14, 0, tzinfo=TZ)
CSV = (
    "2026-10-02;00:00:00;TC1;1.0;0;0;A\n"
    "2026-10-02;00:00:30;TC1;1.0;0;0;A\n"
).encode("utf-8")


def uploaded_ms(hour: int, minute: int) -> int:
    return int(datetime(2026, 10, 2, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


def make_site(name: str, folder: str | None = None, enabled: bool = True) -> EgnyteSite:
    return EgnyteSite(name, folder or f"/Shared/{name.upper()}", name, Path(f"config/{name}_local.json"), enabled)


class EgnyteRunnerTests(unittest.TestCase):
    def test_config_uses_explicit_uppercase_folders_and_lowercase_output_names(self) -> None:
        sites = load_sites()
        by_name = {site.name: site for site in sites}
        self.assertEqual(len(sites), 17)
        self.assertEqual(by_name["karlskrona"].egnyte_folder, "/Shared/KARLSKRONA")
        self.assertEqual(by_name["goteborg_skogen"].egnyte_folder, "/Shared/GOTEBORG_SKOGEN")
        self.assertEqual(by_name["sala"].output_station, "sala")
        self.assertEqual(by_name["sala"].config_path.name, "sala_local.json")
        self.assertEqual(state_path_for(by_name["sala"]).name, "sala.json")
        self.assertEqual({site.name for site in sites if site.enabled}, {"gavle", "eslov", "karlskrona"})

    def test_disabled_site_is_skipped_and_all_sites_query_only_today(self) -> None:
        sites = [make_site("sala"), make_site("gavle"), make_site("lerum", enabled=False)]
        client = Mock()
        client.list_files.return_value = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            state_root = Path(tmp_dir) / "state"
            for site in sites[:2]:
                path = state_path_for(site, state_root)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"processed_files": {}}', encoding="utf-8")
            results = run_sites_cycle(
                sites,
                client=client,
                now=NOW,
                dry_run=True,
                state_root=state_root,
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
            )

        self.assertEqual(results, {"sala": 0, "gavle": 0})
        folders = [call.args[0] for call in client.list_files.call_args_list]
        self.assertEqual(folders, ["/Shared/SALA/2026-10-02", "/Shared/GAVLE/2026-10-02"])
        self.assertFalse(any("2026-10-01" in folder for folder in folders))

    def test_duplicate_group_entry_ids_in_one_listing_are_selected_once(self) -> None:
        duplicate_first = EgnyteFile(
            "first.csv", "/Shared/SALA/2026-10-02/first.csv", "same-group", "same-entry", uploaded_ms(10, 0), 1, ""
        )
        duplicate_again = EgnyteFile(
            "renamed.csv", "/Shared/SALA/2026-10-02/renamed.csv", "same-group", "same-entry", uploaded_ms(10, 1), 1, ""
        )
        selected, historical_count = filter_new_files(
            [duplicate_first, duplicate_again], set(), None, TZ
        )

        self.assertEqual(historical_count, 0)
        self.assertEqual([file.name for file in selected], ["first.csv"])

    def test_state_isolated_by_site_and_sala_legacy_state_migrates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            legacy = root / "sala_egnyte_state.json"
            legacy.write_text(json.dumps({
                "baseline_before": "2026-10-02T12:40:00+02:00",
                "processed_files": {"old-group:old-entry": {"filename": "old.csv"}},
            }), encoding="utf-8")
            sala = make_site("sala")
            gavle = make_site("gavle")

            sala_state = load_site_state(sala, state_root=root / "egnyte", legacy_sala_path=legacy)
            gavle_state = load_site_state(gavle, state_root=root / "egnyte", legacy_sala_path=legacy)

            migrated = json.loads((root / "egnyte" / "sala.json").read_text(encoding="utf-8"))
            self.assertEqual(sala_state, migrated)
            self.assertEqual(sala_state["baseline_before"], "2026-10-02T12:40:00+02:00")
            self.assertIn("old-group:old-entry", sala_state["processed_files"])
            self.assertEqual(gavle_state["processed_files"], {})
            self.assertFalse((root / "egnyte" / "gavle.json").exists())

    def test_initialize_state_cli_is_local_and_preserves_processed_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_root = root / "state" / "egnyte"
            legacy_path = root / "state" / "legacy-sala.json"
            existing_path = state_path_for(make_site("falkoping"), state_root)
            existing_path.parent.mkdir(parents=True)
            existing_path.write_text(json.dumps({
                "processed_files": {"group-id:entry-id": {"filename": "known.csv"}},
            }), encoding="utf-8")

            with (
                patch.object(sys, "argv", [
                    "egnyte_runner.py", "--initialize-state", "--site", "falkoping",
                    "--baseline-before", "2026-10-05 10:25", "--timezone", "Europe/Stockholm",
                ]),
                patch("egnyte_runner.STATE_ROOT", state_root),
                patch("egnyte_runner.LEGACY_SALA_STATE_PATH", legacy_path),
                patch("egnyte_runner.EgnyteClient.from_environment", side_effect=AssertionError("network auth attempted")) as client_factory,
            ):
                result = main()

            state = json.loads(existing_path.read_text(encoding="utf-8"))
            self.assertEqual(result, 0)
            self.assertEqual(state["baseline_before"], "2026-10-05T10:25:00+02:00")
            self.assertIn("group-id:entry-id", state["processed_files"])
            client_factory.assert_not_called()

    def test_all_sites_baseline_initialization_is_local_and_includes_disabled_sites(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_root = Path(tmp_dir) / "state" / "egnyte"
            legacy_path = Path(tmp_dir) / "legacy-sala.json"
            with (
                patch.object(sys, "argv", [
                    "egnyte_runner.py", "--initialize-state", "--all-sites",
                    "--baseline-before", "2026-10-05 10:25", "--timezone", "Europe/Stockholm",
                ]),
                patch("egnyte_runner.STATE_ROOT", state_root),
                patch("egnyte_runner.LEGACY_SALA_STATE_PATH", legacy_path),
                patch("egnyte_runner.EgnyteClient.from_environment", side_effect=AssertionError("network auth attempted")) as client_factory,
            ):
                result = main()

            state_files = list(state_root.glob("*.json"))
            self.assertEqual(result, 0)
            self.assertEqual(len(state_files), 17)
            self.assertEqual(
                {json.loads(path.read_text(encoding="utf-8"))["baseline_before"] for path in state_files},
                {"2026-10-05T10:25:00+02:00"},
            )
            client_factory.assert_not_called()

    def test_shared_oauth_token_is_acquired_once_across_sites_and_cycles(self) -> None:
        sites = [make_site("gavle"), make_site("eslov")]
        session = Mock()
        token_response = Mock()
        token_response.raise_for_status.return_value = None
        token_response.json.return_value = {"access_token": "shared-cached-token"}
        listing_response = Mock(status_code=200, headers={})
        listing_response.raise_for_status.return_value = None
        listing_response.json.return_value = {"files": []}
        session.post.return_value = token_response
        session.get.return_value = listing_response
        client = EgnyteClient(
            "example.egnyte.com",
            api_key="key",
            api_secret="secret",
            username="user",
            password="pass",
            session=session,
        )

        with tempfile.TemporaryDirectory() as tmp_dir, patch("egnyte.client.time.sleep"):
            state_root = Path(tmp_dir) / "state"
            for site in sites:
                state_path_for(site, state_root).parent.mkdir(parents=True, exist_ok=True)
                state_path_for(site, state_root).write_text('{"processed_files": {}}', encoding="utf-8")

            run_sites_cycle(sites, client=client, now=NOW, state_root=state_root)
            run_sites_cycle(sites, client=client, now=NOW, state_root=state_root)

        session.post.assert_called_once()
        self.assertEqual(session.get.call_count, 4)
        self.assertTrue(all(
            call.kwargs["headers"]["Authorization"] == "Bearer shared-cached-token"
            for call in session.get.call_args_list
        ))

    def test_authentication_failure_is_reported_once_before_site_requests(self) -> None:
        sites = [make_site("gavle"), make_site("eslov")]
        client = Mock()
        client.ensure_authenticated.side_effect = RuntimeError("mock OAuth rejection")

        with self.assertLogs("sala_egnyte", level="ERROR") as captured:
            results = run_sites_cycle(sites, client=client, now=NOW)

        self.assertEqual(results, {"gavle": 0, "eslov": 0})
        client.ensure_authenticated.assert_called_once()
        client.list_files.assert_not_called()
        self.assertEqual(sum("authentication failed before site processing" in line for line in captured.output), 1)

    def test_uninitialized_enabled_site_is_skipped_without_listing(self) -> None:
        site = make_site("gavle")
        client = Mock()

        with tempfile.TemporaryDirectory() as tmp_dir, self.assertLogs("sala_egnyte", level="WARNING") as captured:
            results = run_sites_cycle(
                [site],
                client=client,
                now=NOW,
                state_root=Path(tmp_dir) / "state",
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
            )

        self.assertEqual(results, {"gavle": 0})
        client.list_files.assert_not_called()
        self.assertIn("no initialized state/baseline; skipping site until initialized", "\n".join(captured.output))

    def test_empty_state_file_is_not_treated_as_initialized(self) -> None:
        site = make_site("gavle")
        client = Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_root = Path(tmp_dir) / "state"
            path = state_path_for(site, state_root)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"")

            result = run_sites_cycle(
                [site],
                client=client,
                now=NOW,
                state_root=state_root,
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
            )

        self.assertEqual(result, {"gavle": 0})
        client.list_files.assert_not_called()

    def test_initialized_site_processes_only_post_baseline_files(self) -> None:
        site = make_site("gavle")
        before = EgnyteFile(
            "before.csv", "/Shared/GAVLE/2026-10-02/before.csv", "gb", "eb", uploaded_ms(10, 0), 10, ""
        )
        after = EgnyteFile(
            "after.csv", "/Shared/GAVLE/2026-10-02/after.csv", "ga", "ea", uploaded_ms(14, 0), 10, ""
        )
        client = Mock()
        client.list_files.return_value = [before, after]
        client.download_file.return_value = CSV

        with tempfile.TemporaryDirectory() as tmp_dir:
            state_root = Path(tmp_dir) / "state"
            path = state_path_for(site, state_root)
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({
                "baseline_before": "2026-10-02T12:40:00+02:00",
                "processed_files": {},
            }), encoding="utf-8")

            result = run_sites_cycle(
                [site], client=client, now=NOW, state_root=state_root,
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
                output_root=Path(tmp_dir) / "normal",
            )

        self.assertEqual(result, {"gavle": 1})
        client.download_file.assert_called_once_with(after)

    def test_one_site_listing_failure_does_not_stop_later_site(self) -> None:
        sites = [make_site("sala"), make_site("gavle"), make_site("staffan")]
        client = Mock()
        client.list_files.side_effect = [RuntimeError("SALA unavailable"), [], []]

        with tempfile.TemporaryDirectory() as tmp_dir:
            state_root = Path(tmp_dir) / "state"
            for site in sites:
                path = state_path_for(site, state_root)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"processed_files": {}}', encoding="utf-8")
            results = run_sites_cycle(
                sites,
                client=client,
                now=NOW,
                dry_run=True,
                state_root=state_root,
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
            )

        self.assertEqual(results, {"sala": 0, "gavle": 0, "staffan": 0})
        self.assertEqual(client.list_files.call_count, 3)
        self.assertEqual(client.list_files.call_args_list[2].args[0], "/Shared/STAFFAN/2026-10-02")

    def test_sites_are_processed_sequentially(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            sites = [make_site("sala"), make_site("gavle")]
            files = [
                EgnyteFile(
                    f"{site.name}.csv", f"{site.egnyte_folder}/2026-10-02/{site.name}.csv",
                    f"group-{site.name}", f"entry-{site.name}", uploaded_ms(10, 0), 10, "",
                )
                for site in sites
            ]
            events: list[str] = []
            client = Mock()
            state_root = Path(tmp_dir) / "state"
            for site in sites:
                path = state_path_for(site, state_root)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"processed_files": {}}', encoding="utf-8")

            def list_site(folder: str, **_kwargs):
                events.append(f"list:{folder}")
                return [next(file for file in files if file.path.startswith(folder))]

            def download(file: EgnyteFile):
                events.append(f"download:{file.name}")
                return CSV

            client.list_files.side_effect = list_site
            client.download_file.side_effect = download

            results = run_sites_cycle(
                sites,
                client=client,
                now=NOW,
                state_root=state_root,
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
                output_root=Path(tmp_dir) / "normal",
            )

            self.assertEqual(results, {"sala": 1, "gavle": 1})
            self.assertEqual(events, [
                "list:/Shared/SALA/2026-10-02",
                "download:sala.csv",
                "list:/Shared/GAVLE/2026-10-02",
                "download:gavle.csv",
            ])

    def test_multiple_files_are_processed_oldest_first_sequentially(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            site = make_site("sala")
            files = [
                EgnyteFile("late.csv", "/Shared/SALA/2026-10-02/late.csv", "g3", "e3", uploaded_ms(11, 0), 10, ""),
                EgnyteFile("early.csv", "/Shared/SALA/2026-10-02/early.csv", "g1", "e1", uploaded_ms(10, 0), 10, ""),
                EgnyteFile("middle.csv", "/Shared/SALA/2026-10-02/middle.csv", "g2", "e2", uploaded_ms(10, 30), 10, ""),
            ]
            order: list[str] = []
            client = Mock()
            client.list_files.side_effect = lambda folder, **_kwargs: order.append(f"list:{folder}") or files
            client.download_file.side_effect = lambda file: order.append(f"download:{file.name}") or CSV
            state_root = Path(tmp_dir) / "state"
            state_path = state_path_for(site, state_root)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text('{"processed_files": {}}', encoding="utf-8")

            results = run_sites_cycle(
                [site], client=client, now=NOW, state_root=state_root,
                output_root=Path(tmp_dir) / "normal",
                legacy_sala_path=Path(tmp_dir) / "missing-legacy.json",
            )

            self.assertEqual(results, {"sala": 3})
            self.assertEqual(
                [entry for entry in order if entry.startswith("download:")],
                ["download:early.csv", "download:middle.csv", "download:late.csv"],
            )
            self.assertEqual(order[0], "list:/Shared/SALA/2026-10-02")
            self.assertTrue((Path(tmp_dir) / "normal" / "sala" / "2026-10-02" / "early.parquet").is_file())

    def test_global_limiter_is_shared_and_enforces_700ms_with_monotonic(self) -> None:
        self.assertIs(EgnyteClient("example.egnyte.com", "token").rate_limiter, GLOBAL_RATE_LIMITER)
        limiter = GlobalRateLimiter(0.7)
        with patch("egnyte.client.time.monotonic", side_effect=[10.0, 10.0, 10.2, 10.7]), \
             patch("egnyte.client.time.sleep") as sleep_mock:
            limiter.wait()
            limiter.wait()
        sleep_mock.assert_called_once()
        self.assertAlmostEqual(sleep_mock.call_args.args[0], 0.5)

    def test_api_retries_share_limiter_for_each_network_attempt(self) -> None:
        limiter = Mock()
        session = Mock()
        first = Mock(status_code=503, headers={})
        second = Mock(status_code=200, headers={})
        second.raise_for_status.return_value = None
        session.get.side_effect = [first, second]
        client = EgnyteClient("example.egnyte.com", "token", max_retries=1, session=session, rate_limiter=limiter)

        with patch("egnyte.client.time.sleep"):
            client._get("https://example.egnyte.com/pubapi/v1/test")

        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(limiter.wait.call_count, 2)

    def test_oauth_token_request_also_uses_the_client_rate_limiter(self) -> None:
        limiter = Mock()
        session = Mock()
        token = Mock()
        token.raise_for_status.return_value = None
        token.json.return_value = {"access_token": "cached-token"}
        response = Mock(status_code=200, headers={})
        response.raise_for_status.return_value = None
        session.post.return_value = token
        session.get.return_value = response
        client = EgnyteClient(
            "example.egnyte.com", api_key="key", api_secret="secret", username="user", password="pass",
            session=session, rate_limiter=limiter,
        )

        client._get("https://example.egnyte.com/pubapi/v1/test")

        self.assertEqual(limiter.wait.call_count, 2)
        session.post.assert_called_once()

    def test_fixed_scheduler_boundary_at_startup_is_not_repeated(self) -> None:
        at_boundary = datetime(2026, 10, 2, 15, 27, tzinfo=TZ)
        future = get_due_check_times(at_boundary)
        self.assertEqual(future[0], datetime(2026, 10, 2, 15, 42, tzinfo=TZ))
        self.assertEqual(future[0].minute, 42)

    def test_schedule_always_selects_strictly_future_slot(self) -> None:
        cases = [
            (datetime(2026, 10, 2, 15, 11, 50, tzinfo=TZ), (15, 12)),
            (datetime(2026, 10, 2, 15, 12, 0, tzinfo=TZ), (15, 27)),
            (datetime(2026, 10, 2, 15, 12, 1, tzinfo=TZ), (15, 27)),
            (datetime(2026, 10, 2, 15, 26, 59, tzinfo=TZ), (15, 27)),
            (datetime(2026, 10, 2, 15, 27, 0, tzinfo=TZ), (15, 42)),
            (datetime(2026, 10, 2, 15, 27, 30, tzinfo=TZ), (15, 42)),
            (datetime(2026, 10, 2, 15, 56, 59, tzinfo=TZ), (15, 57)),
            (datetime(2026, 10, 2, 15, 57, 0, tzinfo=TZ), (16, 12)),
            (datetime(2026, 10, 2, 15, 57, 1, tzinfo=TZ), (16, 12)),
            (datetime(2026, 10, 2, 15, 57, 14, tzinfo=TZ), (16, 12)),
            (datetime(2026, 10, 2, 15, 57, 59, tzinfo=TZ), (16, 12)),
        ]
        schedule = FixedClockSchedule()
        for now, (expected_hour, expected_minute) in cases:
            with self.subTest(now=now):
                next_run = schedule.next_run(now)
                self.assertGreater(next_run, now)
                self.assertEqual((next_run.hour, next_run.minute), (expected_hour, expected_minute))

    def test_cycle_taking_15_seconds_advances_past_claimed_slot(self) -> None:
        schedule = FixedClockSchedule()
        slot = datetime(2026, 10, 2, 15, 57, tzinfo=TZ)
        self.assertTrue(schedule.claim(slot))

        after_cycle = datetime(2026, 10, 2, 15, 57, 15, tzinfo=TZ)
        self.assertEqual(schedule.next_run(after_cycle), datetime(2026, 10, 2, 16, 12, tzinfo=TZ))
        self.assertFalse(schedule.claim(slot))

    def test_startup_cycle_at_scheduled_minute_does_not_duplicate_boundary(self) -> None:
        schedule = FixedClockSchedule()
        startup_completed = datetime(2026, 10, 2, 15, 57, 20, tzinfo=TZ)

        self.assertEqual(schedule.next_run(startup_completed), datetime(2026, 10, 2, 16, 12, tzinfo=TZ))

    def test_rate_limiter_does_not_mutate_or_schedule_polling_slots(self) -> None:
        schedule = FixedClockSchedule()
        limiter = GlobalRateLimiter(0.7)
        with patch("egnyte.client.time.monotonic", side_effect=[1.0, 1.0]), patch("egnyte.client.time.sleep"):
            limiter.wait()

        self.assertIsNone(schedule.last_run_slot)
        self.assertEqual(
            schedule.next_run(datetime(2026, 10, 2, 15, 57, 14, tzinfo=TZ)),
            datetime(2026, 10, 2, 16, 12, tzinfo=TZ),
        )


if __name__ == "__main__":
    unittest.main()
