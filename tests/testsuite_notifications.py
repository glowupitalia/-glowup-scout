import hashlib
import os
import sqlite3
import stat
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from openpyxl import Workbook

from notifications import (
    AttachmentDecision,
    DEFAULT_RECIPIENT,
    DISCOVERY_COMPLETED,
    DISCOVERY_COMPLETED_ZERO_RESULTS,
    DISCOVERY_FAILED,
    EmailAttachment,
    EmailConfig,
    NotificationOutbox,
    SMTPEmailTransport,
    discovery_terminal_event,
    prepare_discovery_attachment,
    preview_discovery_terminal_notification,
    render_discovery_notification,
    send_discovery_terminal_notification,
)
from notifications import _content_with_attachment_note


def configured_email():
    return EmailConfig(
        enabled=True, recipient=DEFAULT_RECIPIENT,
        sender="scout@example.test", smtp_host="smtp.example.test",
        smtp_port=587, smtp_username="user", smtp_password="top-secret",
        smtp_security="starttls",
    )


def completed_state(results=True):
    product = {
        "title": "A <B & C>", "gtin": "8809562191179", "asin": "B012345678",
        "scenarios": [{
            "scenario_id": "scenario-1", "supplier": "umma",
            "scenario_label": "U-Quick", "cost_gross_unit_eur": "8.06",
            "margin_percent": "24.50", "score": 81,
        }],
        "opportunity_combinations": [{
            "combination_id": "combination-1", "scenario_id": "scenario-1",
            "asin": "B012345678", "supplier": "umma", "scenario_label": "U-Quick",
            "cost_gross_unit_eur": "8.06", "price_reference": "22.90",
            "margin_percent": "24.50", "profit": "5.61", "score": 81,
        }],
        "recommended_combination": "combination-1",
        "best_purchase_scenario": "scenario-1",
        "scenario_roles": {"scenario_raccomandato": "scenario-1"},
        "combination_roles": {"recommended_combination": "combination-1"},
    }
    return {
        "job_id": "job-1", "status": "completed", "phase": "completed",
        "started_at": "2026-08-27T08:00:00Z",
        "completed_at": "2026-08-27T09:01:02Z",
        "selected_suppliers": ["abw", "umma", "qudo"],
        "sampled_identifier_count": 500,
        "results": [product] if results else [],
        "funnel": {
            "amazon_found": 90, "amazon_listings_found": 95,
            "beauty_listings": 50, "bsr_passed_listings": 20,
            "competition_passed_listings": 10, "fee_valid_listings": 9,
            "combinations_evaluated": 42,
        },
    }


class FakeTransport:
    def __init__(self, failures=0):
        self.failures = failures
        self.messages = []

    def send(self, message):
        self.messages.append(message)
        if len(self.messages) <= self.failures:
            raise TimeoutError("secret=must-not-be-persisted")
        return message["Message-ID"]


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "jobs.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def export_for(self, state, *, root=None):
        root = Path(root or self.temporary.name) / "exports"
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{state['job_id']}.xlsx"
        workbook = Workbook()
        workbook.active["A1"] = "Glow Up Scout"
        workbook.save(path)
        payload = path.read_bytes()
        state["export_state"] = {
            "status": "completed", "valid": True, "job_id": state["job_id"],
            "file_name": path.name, "file_size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
        runtime = {
            "job_id": state["job_id"], "status": "completed",
            "resumable": False, "export_path": str(path),
        }
        return path, runtime, root

    def test_completed_sends_one_multipart_email_to_recipient(self):
        transport = FakeTransport()
        row = send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=configured_email(), transport=transport, sleep_func=lambda _: None,
        )
        self.assertEqual(row["status"], "sent")
        self.assertEqual(row["attempt_count"], 1)
        self.assertEqual(row["recipient"], DEFAULT_RECIPIENT)
        self.assertEqual(len(transport.messages), 1)
        self.assertTrue(transport.messages[0].is_multipart())
        self.assertEqual(transport.messages[0]["To"], DEFAULT_RECIPIENT)

    def test_completed_with_valid_xlsx_attaches_final_job_export(self):
        state = completed_state()
        path, runtime, root = self.export_for(state)
        transport = FakeTransport()
        row = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=configured_email(), transport=transport,
            allowed_attachment_roots=[root],
        )
        attachments = list(transport.messages[0].iter_attachments())
        self.assertEqual(row["attachment_status"], "attached")
        self.assertEqual(row["attachment_size"], path.stat().st_size)
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].get_content_type(), (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ))
        self.assertEqual(
            attachments[0].get_filename(),
            "GlowUp-Scout-Discovery-2026-08-27-job-1.xlsx",
        )
        self.assertIn("Excel completo", transport.messages[0].get_body(preferencelist=("plain",)).get_content())

    def test_zero_results_with_valid_xlsx_is_attached(self):
        state = completed_state(False)
        _, runtime, root = self.export_for(state)
        transport = FakeTransport()
        row = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=configured_email(), transport=transport,
            allowed_attachment_roots=[root],
        )
        self.assertEqual(row["attachment_status"], "attached")
        self.assertEqual(len(list(transport.messages[0].iter_attachments())), 1)

    def test_failed_discovery_never_attaches_partial_workbook(self):
        state = completed_state(False)
        _, runtime, root = self.export_for(state)
        state.update({"status": "failed", "phase": "failed", "errors": [{"message": "boom"}]})
        runtime.update({"status": "failed", "resumable": False})
        transport = FakeTransport()
        row = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=configured_email(), transport=transport,
            allowed_attachment_roots=[root],
        )
        self.assertEqual(row["attachment_status"], "none")
        self.assertEqual(list(transport.messages[0].iter_attachments()), [])

    def test_missing_xlsx_sends_summary_with_unavailable_metadata(self):
        state = completed_state()
        root = Path(self.temporary.name) / "exports"
        runtime = {
            "job_id": state["job_id"], "status": "completed", "resumable": False,
            "export_path": str(root / f"{state['job_id']}.xlsx"),
        }
        state["export_state"] = {
            "status": "completed", "job_id": state["job_id"],
            "file_name": f"{state['job_id']}.xlsx",
        }
        transport = FakeTransport()
        row = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=configured_email(), transport=transport,
            allowed_attachment_roots=[root],
        )
        self.assertEqual((row["status"], row["attachment_status"]), ("sent", "unavailable"))
        self.assertEqual(list(transport.messages[0].iter_attachments()), [])

    def test_invalid_workbook_and_path_traversal_send_without_attachment(self):
        for index, outside in enumerate((False, True)):
            state = completed_state()
            state["job_id"] = f"invalid-{index}"
            allowed = Path(self.temporary.name) / f"allowed-{index}"
            actual = (Path(self.temporary.name) / "outside") if outside else allowed
            actual.mkdir(parents=True, exist_ok=True)
            path = actual / f"{state['job_id']}.xlsx"
            path.write_bytes(b"not-an-xlsx" if not outside else b"outside")
            state["export_state"] = {
                "status": "completed", "job_id": state["job_id"],
                "file_name": path.name,
            }
            runtime = {
                "job_id": state["job_id"], "status": "completed",
                "resumable": False, "export_path": str(path),
            }
            transport = FakeTransport()
            row = send_discovery_terminal_notification(
                state, database_path=self.database, runtime=runtime,
                config=configured_email(), transport=transport,
                allowed_attachment_roots=[allowed],
            )
            self.assertEqual((row["status"], row["attachment_status"]), ("sent", "invalid"))
            self.assertEqual(list(transport.messages[0].iter_attachments()), [])

    @unittest.skipUnless(hasattr(os, "symlink"), "symlink unavailable")
    def test_arbitrary_symlink_is_rejected(self):
        state = completed_state()
        target, runtime, root = self.export_for(state)
        link = root / "link.xlsx"
        link.symlink_to(target)
        runtime["export_path"] = str(link)
        state["export_state"]["file_name"] = link.name
        decision = prepare_discovery_attachment(
            state, runtime, allowed_roots=[root],
        )
        self.assertEqual(decision.status, "invalid")

    def test_attachment_over_limit_is_skipped_without_failing_mail(self):
        state = completed_state()
        path, runtime, root = self.export_for(state)
        config = replace(configured_email(), max_attachment_mb=0.000001)
        transport = FakeTransport()
        row = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=config, transport=transport, allowed_attachment_roots=[root],
        )
        self.assertEqual((row["status"], row["attachment_status"]), ("sent", "skipped_too_large"))
        self.assertEqual(row["attachment_size"], path.stat().st_size)
        body = transport.messages[0].get_body(preferencelist=("plain",)).get_content()
        self.assertIn("supera il limite", body)

    def test_attachment_retry_reuses_metadata_and_does_not_duplicate_notification(self):
        state = completed_state()
        _, runtime, root = self.export_for(state)
        transport = FakeTransport(failures=1)
        first = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=configured_email(), transport=transport,
            sleep_func=lambda _: None, allowed_attachment_roots=[root],
        )
        second = send_discovery_terminal_notification(
            state, database_path=self.database, runtime=runtime,
            config=configured_email(), transport=transport,
            allowed_attachment_roots=[root],
        )
        self.assertEqual((first["status"], first["attempt_count"]), ("sent", 2))
        self.assertEqual(first["attachment_status"], "attached")
        self.assertEqual(second["attempt_count"], 2)
        self.assertEqual(len(transport.messages), 2)

    def test_zero_results_has_distinct_event_and_subject(self):
        state = completed_state(results=False)
        content = render_discovery_notification(state)
        self.assertEqual(content.event_type, DISCOVERY_COMPLETED_ZERO_RESULTS)
        self.assertIn("senza opportunità", content.subject)
        self.assertIn("Opportunità finali: 0", content.text)

    def test_persisted_final_count_is_authoritative_without_results_collection(self):
        state = completed_state()
        state.pop("results")
        state.update({
            "final_opportunity_count": 143,
            "combination_count": 19_928,
            "fee_target_count": 843,
            "fee_valid_count": 833,
            "fee_unavailable_count": 10,
            "bsr_passed_count": 843,
            "best_opportunity": {
                "product": "Best product", "canonical_ean": "8809562191179",
                "asin": "B012345678", "supplier": "umma",
                "scenario": "U-Quick", "cost_gross_unit_eur": "8.06",
                "price_reference": "22.90", "margin_percent": "24.50",
                "profit": "5.61", "score": 81,
            },
        })
        content = render_discovery_notification(state)
        self.assertEqual(content.event_type, DISCOVERY_COMPLETED)
        self.assertIn("Opportunità finali: 143", content.text)
        self.assertIn("Combinazioni valutate: 19928", content.text)
        self.assertIn("Copertura Fee Amazon: 833/843", content.text)
        self.assertIn("BSR nel range: 843", content.text)
        self.assertIn("MIGLIORE OPPORTUNITÀ", content.text)
        self.assertIn("Best product", content.text)

    def test_explicit_zero_persisted_count_preserves_genuine_zero_result_event(self):
        state = completed_state()
        state["final_opportunity_count"] = 0
        self.assertEqual(
            discovery_terminal_event(state), DISCOVERY_COMPLETED_ZERO_RESULTS,
        )

    def test_notification_pending_runtime_accepts_valid_export_and_size_policy(self):
        state = completed_state()
        path, runtime, root = self.export_for(state)
        runtime["status"] = "notification_pending"
        attached = prepare_discovery_attachment(
            state, runtime, max_attachment_mb=15, allowed_roots=[root],
        )
        skipped = prepare_discovery_attachment(
            state, runtime, max_attachment_mb=0.000001, allowed_roots=[root],
        )
        self.assertEqual(attached.status, "attached")
        self.assertEqual(skipped.status, "skipped_too_large")
        self.assertEqual(skipped.size, path.stat().st_size)

    def test_preview_is_pure_and_reports_too_large_without_outbox(self):
        state = completed_state()
        state["final_opportunity_count"] = 1
        path, runtime, root = self.export_for(state)
        runtime["status"] = "notification_pending"
        preview = preview_discovery_terminal_notification(
            state, runtime=runtime,
            config=replace(configured_email(), max_attachment_mb=0.000001),
            allowed_attachment_roots=[root],
        )
        self.assertEqual(preview["event_type"], DISCOVERY_COMPLETED)
        self.assertEqual(preview["attachment_status"], "skipped_too_large")
        self.assertEqual(preview["attachment_size"], path.stat().st_size)
        self.assertIn("disponibile dalla UI Scout", preview["text"])
        self.assertIsNone(NotificationOutbox(self.database).get("job-1"))

    def test_large_workbook_message_reports_size_and_download_instead_of_attachment(self):
        content = render_discovery_notification(completed_state())
        rendered = _content_with_attachment_note(
            content,
            AttachmentDecision(
                "skipped_too_large", name="Discovery.xlsx", size=68_940_642,
                error="File Excel oltre il limite email configurato",
            ),
        )
        self.assertIn("Dimensione: 68,94 MB", rendered.text)
        self.assertIn("Non è stato allegato", rendered.text)
        self.assertIn("scaricarlo direttamente da Glow Up Scout", rendered.text)

    def test_completion_variants_share_one_terminal_notification_identity(self):
        transport = FakeTransport()
        zero = completed_state(False)
        first = send_discovery_terminal_notification(
            zero, database_path=self.database,
            config=configured_email(), transport=transport,
        )
        corrected = completed_state()
        corrected["final_opportunity_count"] = 1
        second = send_discovery_terminal_notification(
            corrected, database_path=self.database,
            config=configured_email(), transport=transport,
        )
        self.assertEqual(first["event_type"], DISCOVERY_COMPLETED_ZERO_RESULTS)
        self.assertEqual(second["event_type"], DISCOVERY_COMPLETED_ZERO_RESULTS)
        self.assertEqual(len(transport.messages), 1)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM notification_outbox WHERE entity_id='job-1'"
                ).fetchone()[0], 1,
            )

    def test_completed_partial_fee_coverage_warns_without_becoming_failure(self):
        state = completed_state()
        state.update({
            "fee_target_count": 843, "fee_valid_count": 842,
            "fee_unavailable_count": 1, "fee_coverage_partial": True,
        })
        content = render_discovery_notification(state)
        self.assertEqual(content.event_type, DISCOVERY_COMPLETED)
        self.assertIn("Copertura Fee Amazon: 842/843", content.text)
        self.assertIn("1 Fee Amazon non disponibili", content.text)
        self.assertIn("escluse dal ranking economico", content.text)

    def test_terminal_failed_sends_but_retryable_failure_does_not(self):
        state = completed_state(False)
        state.update({"status": "failed", "phase": "catalog", "errors": [{"message": "boom"}]})
        self.assertEqual(discovery_terminal_event(state, runtime={"resumable": False}), DISCOVERY_FAILED)
        self.assertIsNone(discovery_terminal_event(state, runtime={"resumable": True}))
        self.assertIsNone(send_discovery_terminal_notification(
            state, database_path=self.database, runtime={"resumable": True},
            config=configured_email(), transport=FakeTransport(),
        ))

    def test_waiting_retry_and_resume_do_not_notify(self):
        state = completed_state(False)
        state["status"] = "waiting_retry"
        self.assertIsNone(send_discovery_terminal_notification(
            state, database_path=self.database, runtime={"resumable": True},
            config=configured_email(), transport=FakeTransport(),
        ))

    def test_repeated_worker_or_browser_recreation_cannot_duplicate(self):
        transport = FakeTransport()
        first = send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=configured_email(), transport=transport,
        )
        second = send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=configured_email(), transport=transport,
        )
        self.assertEqual((first["status"], second["status"]), ("sent", "sent"))
        self.assertEqual(len(transport.messages), 1)

    def test_provider_failure_is_bounded_and_sanitized(self):
        transport = FakeTransport(failures=9)
        row = send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=configured_email(), transport=transport, max_attempts=3,
            sleep_func=lambda _: None,
        )
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["attempt_count"], 3)
        self.assertEqual(len(transport.messages), 3)
        self.assertNotIn("top-secret", row["failure_reason"])
        self.assertNotIn("must-not-be-persisted", row["failure_reason"])

    def test_transient_provider_failure_retries_then_succeeds(self):
        transport = FakeTransport(failures=1)
        sleeps = []
        row = send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=configured_email(), transport=transport,
            sleep_func=sleeps.append,
        )
        self.assertEqual(row["status"], "sent")
        self.assertEqual(row["attempt_count"], 2)
        self.assertEqual(sleeps, [1.0])

    def test_missing_configuration_is_persisted_without_sending(self):
        transport = FakeTransport()
        config = EmailConfig.from_env({})
        row = send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=config, transport=transport,
        )
        self.assertEqual(row["status"], "not_configured")
        self.assertEqual(transport.messages, [])

    def test_implicit_ssl_transport_authenticates_and_sends(self):
        config = EmailConfig(
            enabled=True, recipient="to@example.test", sender="from@example.test",
            smtp_host="smtp.example.test", smtp_port=465, smtp_username="user",
            smtp_password="secret", smtp_security="ssl",
        )
        client = unittest.mock.MagicMock()
        client.__enter__.return_value = client
        message = unittest.mock.MagicMock()
        message.__getitem__.return_value = "message-id"
        with (
            patch("notifications.smtplib.SMTP_SSL", return_value=client) as ssl_smtp,
            patch("notifications.smtplib.SMTP") as plain_smtp,
        ):
            self.assertEqual(SMTPEmailTransport(config).send(message), "message-id")
        ssl_smtp.assert_called_once()
        plain_smtp.assert_not_called()
        client.starttls.assert_not_called()
        client.login.assert_called_once_with("user", "secret")
        client.send_message.assert_called_once_with(message)

    def test_starttls_and_plain_transport_paths(self):
        for security, expected_starttls in (("starttls", True), ("none", False)):
            config = EmailConfig(
                enabled=True, recipient="to@example.test", sender="from@example.test",
                smtp_host="smtp.example.test", smtp_port=587, smtp_username=None,
                smtp_password=None, smtp_security=security,
            )
            client = unittest.mock.MagicMock()
            client.__enter__.return_value = client
            with patch("notifications.smtplib.SMTP", return_value=client):
                SMTPEmailTransport(config).send({"Message-ID": "message-id"})
            self.assertEqual(client.starttls.called, expected_starttls)
            client.login.assert_not_called()

    def test_explicit_security_and_legacy_tls_mapping(self):
        self.assertEqual(EmailConfig.from_env({}).smtp_security, "starttls")
        self.assertEqual(
            EmailConfig.from_env({"GLOWUP_SCOUT_SMTP_USE_TLS": "false"}).smtp_security,
            "none",
        )
        self.assertEqual(
            EmailConfig.from_env({
                "GLOWUP_SCOUT_SMTP_USE_TLS": "true",
                "GLOWUP_SCOUT_SMTP_SECURITY": "ssl",
            }).smtp_security,
            "ssl",
        )
        with self.assertRaises(ValueError):
            EmailConfig.from_env({"GLOWUP_SCOUT_SMTP_SECURITY": "invalid"})

    def test_launchd_safe_runtime_config_and_private_password(self):
        root = Path(self.temporary.name)
        config_path = root / "email.env"
        password_path = root / "secrets" / "smtp_password"
        config_path.write_text(
            "GLOWUP_SCOUT_EMAIL_ENABLED=true\n"
            "GLOWUP_SCOUT_EMAIL_TO=to@example.test\n"
            "GLOWUP_SCOUT_EMAIL_FROM=from@example.test\n"
            "GLOWUP_SCOUT_SMTP_HOST=authsmtp.securemail.pro\n"
            "GLOWUP_SCOUT_SMTP_PORT=465\n"
            "GLOWUP_SCOUT_SMTP_USERNAME=to@example.test\n"
            "GLOWUP_SCOUT_SMTP_SECURITY=ssl\n",
            encoding="utf-8",
        )
        password_path.parent.mkdir()
        password_path.write_text("file-secret\n", encoding="utf-8")
        password_path.chmod(0o600)
        config = EmailConfig.from_runtime(
            {}, config_path=config_path, password_path=password_path,
        )
        self.assertTrue(config.configured)
        self.assertEqual(config.smtp_security, "ssl")
        self.assertEqual(config.smtp_password, "file-secret")
        self.assertEqual(stat.S_IMODE(password_path.stat().st_mode), 0o600)

    def test_runtime_environment_overrides_file_and_secret_permissions_are_checked(self):
        root = Path(self.temporary.name)
        config_path = root / "email.env"
        password_path = root / "smtp_password"
        config_path.write_text("GLOWUP_SCOUT_SMTP_SECURITY=ssl\n", encoding="utf-8")
        password_path.write_text("secret\n", encoding="utf-8")
        password_path.chmod(0o644)
        with self.assertRaises(PermissionError):
            EmailConfig.from_runtime({}, config_path=config_path, password_path=password_path)
        password_path.chmod(0o600)
        config = EmailConfig.from_runtime(
            {"GLOWUP_SCOUT_SMTP_SECURITY": "none"},
            config_path=config_path, password_path=password_path,
        )
        self.assertEqual(config.smtp_security, "none")

    def test_runtime_config_ignores_unknown_and_password_keys(self):
        root = Path(self.temporary.name)
        config_path = root / "email.env"
        config_path.write_text(
            "GLOWUP_SCOUT_SMTP_PASSWORD=must-not-load\n"
            "UNRELATED_SECRET=must-not-load\n",
            encoding="utf-8",
        )
        config = EmailConfig.from_runtime(
            {}, config_path=config_path, password_path=root / "missing",
        )
        self.assertIsNone(config.smtp_password)

    def test_html_is_escaped_and_best_opportunity_rendered(self):
        content = render_discovery_notification(completed_state(), DISCOVERY_COMPLETED)
        self.assertNotIn("A <B & C>", content.html)
        self.assertIn("A &lt;B &amp; C&gt;", content.html)
        self.assertIn("Migliore opportunità", content.html)
        self.assertIn("UMMA", content.text)
        self.assertIn("€8,06", content.text)

    def test_timestamp_is_rendered_in_rome_timezone(self):
        content = render_discovery_notification(completed_state())
        self.assertIn("27/08/2026 11:01", content.text)
        self.assertIn("1 h 1 min", content.text)

    def test_password_is_never_persisted(self):
        send_discovery_terminal_notification(
            completed_state(), database_path=self.database,
            config=configured_email(), transport=FakeTransport(failures=9),
            sleep_func=lambda _: None,
        )
        with sqlite3.connect(self.database) as connection:
            dump = "\n".join(connection.iterdump())
        self.assertNotIn("top-secret", dump)

    def test_worker_notification_failure_does_not_change_completed_job(self):
        from discovery import DiscoveryCheckpointStore, default_filters
        from discovery_jobs import DiscoveryJobRegistry
        from discovery_worker import execute

        registry = DiscoveryJobRegistry(Path(self.temporary.name) / "runtime.sqlite3")
        checkpoints = DiscoveryCheckpointStore(Path(self.temporary.name) / "checkpoints")
        initial = checkpoints.create(default_filters())
        initial["selected_suppliers"] = ["abw"]
        initial["run_budget"] = 1
        checkpoints.save(initial)
        registry.register_checkpoint(initial)
        result = completed_state(False)
        result["job_id"] = initial["job_id"]
        worker_root = Path(self.temporary.name) / "worker-project"

        def write_worker_export(_state, output):
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            Workbook().save(output)

        with (
            patch("discovery_worker.run_discovery", return_value=result),
            patch(
                "discovery_worker.write_discovery_excel",
                side_effect=write_worker_export,
            ),
            patch("discovery_worker.PROJECT_ROOT", worker_root),
            patch(
                "discovery_worker.send_discovery_terminal_notification",
                side_effect=sqlite3.OperationalError("outbox unavailable"),
            ),
        ):
            returned = execute(
                initial["job_id"], registry=registry, checkpoint_store=checkpoints,
            )
        self.assertEqual(returned["status"], "completed")
        self.assertEqual(registry.get(initial["job_id"])["status"], "completed")
        export_state = checkpoints.load(initial["job_id"])["export_state"]
        self.assertEqual(export_state["status"], "completed")
        self.assertTrue(export_state["valid"])
        self.assertEqual(export_state["job_id"], initial["job_id"])
        self.assertGreater(export_state["file_size"], 0)
        self.assertEqual(len(export_state["sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
