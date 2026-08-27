import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from notifications import (
    DEFAULT_RECIPIENT,
    DISCOVERY_COMPLETED,
    DISCOVERY_COMPLETED_ZERO_RESULTS,
    DISCOVERY_FAILED,
    EmailConfig,
    NotificationOutbox,
    SMTPEmailTransport,
    discovery_terminal_event,
    render_discovery_notification,
    send_discovery_terminal_notification,
)


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

    def test_zero_results_has_distinct_event_and_subject(self):
        state = completed_state(results=False)
        content = render_discovery_notification(state)
        self.assertEqual(content.event_type, DISCOVERY_COMPLETED_ZERO_RESULTS)
        self.assertIn("senza opportunità", content.subject)
        self.assertIn("Opportunità finali: 0", content.text)

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
        with (
            patch("discovery_worker.run_discovery", return_value=result),
            patch("discovery_worker.write_discovery_excel"),
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


if __name__ == "__main__":
    unittest.main()
