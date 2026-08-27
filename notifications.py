"""Server-side notifications with a persistent, idempotent outbox."""

from __future__ import annotations

import html
import hashlib
import os
import re
import smtplib
import sqlite3
import ssl
import stat
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from purchase_scenarios import recommended_combination, recommended_scenario


DEFAULT_RECIPIENT = "nicola@glowupitalia.it"
DEFAULT_RUNTIME_DIRECTORY = Path.home() / "Library/Application Support/GlowUp-Scout"
DEFAULT_EMAIL_CONFIG_PATH = DEFAULT_RUNTIME_DIRECTORY / "email.env"
DEFAULT_EMAIL_PASSWORD_PATH = DEFAULT_RUNTIME_DIRECTORY / "secrets/smtp_password"
EMAIL_CONFIG_KEYS = {
    "GLOWUP_SCOUT_EMAIL_ENABLED",
    "GLOWUP_SCOUT_EMAIL_TO",
    "GLOWUP_SCOUT_EMAIL_FROM",
    "GLOWUP_SCOUT_SMTP_HOST",
    "GLOWUP_SCOUT_SMTP_PORT",
    "GLOWUP_SCOUT_SMTP_USERNAME",
    "GLOWUP_SCOUT_SMTP_SECURITY",
    "GLOWUP_SCOUT_SMTP_USE_TLS",
    "GLOWUP_SCOUT_EMAIL_MAX_ATTACHMENT_MB",
}
SMTP_SECURITY_MODES = {"ssl", "starttls", "none"}
DEFAULT_MAX_ATTACHMENT_MB = 15.0
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DEFAULT_ATTACHMENT_ROOT = Path(__file__).resolve().parent / "data/discovery_jobs"
ROME = ZoneInfo("Europe/Rome")
DISCOVERY_COMPLETED = "discovery_completed"
DISCOVERY_COMPLETED_ZERO_RESULTS = "discovery_completed_zero_results"
DISCOVERY_FAILED = "discovery_failed"
FUTURE_EVENT_TYPES = {
    "qogita_bootstrap_completed",
    "qogita_bootstrap_auto_stopped",
    "weekly_supplier_failed",
    "abw_source_required",
}
TERMINAL_FAILURE_STATUSES = {"failed", "error", "auto_stopped", "auto-stopped"}


OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_outbox (
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL,
    recipient TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    attempted_at TEXT,
    sent_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    provider_message_id TEXT,
    failure_reason TEXT,
    PRIMARY KEY (entity_id, event_type, channel)
);
CREATE INDEX IF NOT EXISTS idx_notification_outbox_updated
ON notification_outbox(updated_at DESC);
"""

OUTBOX_ATTACHMENT_COLUMNS = {
    "attachment_name": "TEXT",
    "attachment_size": "INTEGER",
    "attachment_status": "TEXT NOT NULL DEFAULT 'none'",
    "attachment_error": "TEXT",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _read_runtime_email_config(path: str | Path) -> dict[str, str]:
    """Read the allow-listed, non-secret email settings without shell evaluation."""
    source = Path(path).expanduser()
    if not source.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in source.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in EMAIL_CONFIG_KEYS:
            values[key] = value.strip()
    return values


def _read_password_file(path: str | Path) -> str | None:
    """Read a password only from a private regular file owned by the current user."""
    source = Path(path).expanduser()
    if not source.exists():
        return None
    details = source.stat()
    if not stat.S_ISREG(details.st_mode):
        raise ValueError("Il secret SMTP non è un file regolare")
    if details.st_uid != os.getuid() or stat.S_IMODE(details.st_mode) != 0o600:
        raise PermissionError("Il secret SMTP deve appartenere all'utente e avere mode 0600")
    password = source.read_text(encoding="utf-8").rstrip("\r\n")
    return password or None


def _smtp_security(values: Mapping[str, str]) -> str:
    explicit = (values.get("GLOWUP_SCOUT_SMTP_SECURITY") or "").strip().lower()
    if explicit:
        if explicit not in SMTP_SECURITY_MODES:
            raise ValueError("GLOWUP_SCOUT_SMTP_SECURITY deve essere ssl, starttls o none")
        return explicit
    return (
        "starttls"
        if _env_bool(values.get("GLOWUP_SCOUT_SMTP_USE_TLS"), default=True)
        else "none"
    )


@dataclass(frozen=True)
class EmailConfig:
    enabled: bool
    recipient: str
    sender: str | None
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_password: str | None
    smtp_security: str
    timeout_seconds: float = 20.0
    max_attachment_mb: float = DEFAULT_MAX_ATTACHMENT_MB

    @classmethod
    def from_env(cls, environment: dict[str, str] | None = None):
        values = environment if environment is not None else os.environ
        try:
            port = int(values.get("GLOWUP_SCOUT_SMTP_PORT", "587"))
        except (TypeError, ValueError):
            port = 587
        try:
            max_attachment_mb = max(
                0.0,
                float(values.get(
                    "GLOWUP_SCOUT_EMAIL_MAX_ATTACHMENT_MB",
                    str(DEFAULT_MAX_ATTACHMENT_MB),
                )),
            )
        except (TypeError, ValueError):
            max_attachment_mb = DEFAULT_MAX_ATTACHMENT_MB
        return cls(
            enabled=_env_bool(values.get("GLOWUP_SCOUT_EMAIL_ENABLED")),
            recipient=(
                values.get("GLOWUP_SCOUT_EMAIL_TO") or DEFAULT_RECIPIENT
            ).strip(),
            sender=(values.get("GLOWUP_SCOUT_EMAIL_FROM") or "").strip() or None,
            smtp_host=(values.get("GLOWUP_SCOUT_SMTP_HOST") or "").strip() or None,
            smtp_port=port,
            smtp_username=(
                values.get("GLOWUP_SCOUT_SMTP_USERNAME") or ""
            ).strip() or None,
            smtp_password=values.get("GLOWUP_SCOUT_SMTP_PASSWORD") or None,
            smtp_security=_smtp_security(values),
            max_attachment_mb=max_attachment_mb,
        )

    @classmethod
    def from_runtime(
        cls, environment: Mapping[str, str] | None = None, *,
        config_path: str | Path = DEFAULT_EMAIL_CONFIG_PATH,
        password_path: str | Path = DEFAULT_EMAIL_PASSWORD_PATH,
    ):
        """Load launchd-safe settings; process environment takes precedence."""
        values = _read_runtime_email_config(config_path)
        values.update(dict(os.environ if environment is None else environment))
        if not values.get("GLOWUP_SCOUT_SMTP_PASSWORD"):
            password = _read_password_file(password_path)
            if password:
                values["GLOWUP_SCOUT_SMTP_PASSWORD"] = password
        return cls.from_env(values)

    @property
    def smtp_use_tls(self) -> bool:
        """Backward-compatible view of the former STARTTLS boolean."""
        return self.smtp_security == "starttls"

    def missing_requirements(self) -> list[str]:
        if not self.enabled:
            return ["GLOWUP_SCOUT_EMAIL_ENABLED"]
        missing = []
        if not self.recipient:
            missing.append("GLOWUP_SCOUT_EMAIL_TO")
        if not self.sender:
            missing.append("GLOWUP_SCOUT_EMAIL_FROM")
        if not self.smtp_host:
            missing.append("GLOWUP_SCOUT_SMTP_HOST")
        if self.smtp_username and not self.smtp_password:
            missing.append("GLOWUP_SCOUT_SMTP_PASSWORD")
        if self.smtp_password and not self.smtp_username:
            missing.append("GLOWUP_SCOUT_SMTP_USERNAME")
        return missing

    @property
    def configured(self) -> bool:
        return not self.missing_requirements()


@dataclass(frozen=True)
class NotificationContent:
    event_type: str
    subject: str
    text: str
    html: str


@dataclass(frozen=True)
class EmailAttachment:
    path: Path
    name: str
    size: int
    mime_type: str = XLSX_MIME_TYPE


@dataclass(frozen=True)
class AttachmentDecision:
    status: str
    attachment: EmailAttachment | None = None
    name: str | None = None
    size: int | None = None
    error: str | None = None

    def metadata(self) -> dict[str, Any]:
        return {
            "attachment_name": self.name,
            "attachment_size": self.size,
            "attachment_status": self.status,
            "attachment_error": sanitize_text(self.error) if self.error else None,
        }


class NotificationOutbox:
    """SQLite outbox using at-most-once reservation for each logical event."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self):
        with self._connect() as connection:
            connection.executescript(OUTBOX_SCHEMA)
            existing = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(notification_outbox)"
                ).fetchall()
            }
            for name, declaration in OUTBOX_ATTACHMENT_COLUMNS.items():
                if name not in existing:
                    connection.execute(
                        f"ALTER TABLE notification_outbox ADD COLUMN {name} {declaration}"
                    )
            connection.commit()

    @staticmethod
    def _row(row):
        return dict(row) if row is not None else None

    def reserve(
        self, entity_id: str, event_type: str, recipient: str,
        attachment_metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        self.initialize()
        now = utc_now()
        metadata = dict(attachment_metadata or {})
        with self._connect() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO notification_outbox
                   (entity_id,event_type,channel,status,recipient,created_at,updated_at,
                    attachment_name,attachment_size,attachment_status,attachment_error)
                   VALUES (?,?, 'email','pending',?,?,?,?,?,?,?)""",
                (
                    entity_id, event_type, recipient, now, now,
                    metadata.get("attachment_name"),
                    metadata.get("attachment_size"),
                    metadata.get("attachment_status") or "none",
                    sanitize_text(metadata.get("attachment_error"))
                    if metadata.get("attachment_error") else None,
                ),
            )
            connection.commit()
            return cursor.rowcount == 1

    def begin_attempt(self, entity_id: str, event_type: str):
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """UPDATE notification_outbox SET status='sending',attempted_at=?,
                   attempt_count=attempt_count+1,updated_at=?
                   WHERE entity_id=? AND event_type=? AND channel='email'""",
                (now, now, entity_id, event_type),
            )
            connection.commit()

    def mark_sent(self, entity_id: str, event_type: str, message_id: str):
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                """UPDATE notification_outbox SET status='sent',sent_at=?,updated_at=?,
                   provider_message_id=?,failure_reason=NULL
                   WHERE entity_id=? AND event_type=? AND channel='email'""",
                (now, now, message_id, entity_id, event_type),
            )
            connection.commit()

    def mark_terminal(self, entity_id: str, event_type: str, status: str, reason: str):
        with self._connect() as connection:
            connection.execute(
                """UPDATE notification_outbox SET status=?,failure_reason=?,updated_at=?
                   WHERE entity_id=? AND event_type=? AND channel='email'""",
                (status, sanitize_text(reason), utc_now(), entity_id, event_type),
            )
            connection.commit()

    def update_attachment(
        self, entity_id: str, event_type: str, metadata: Mapping[str, Any],
    ):
        with self._connect() as connection:
            connection.execute(
                """UPDATE notification_outbox SET attachment_name=?,attachment_size=?,
                   attachment_status=?,attachment_error=?,updated_at=?
                   WHERE entity_id=? AND event_type=? AND channel='email'""",
                (
                    metadata.get("attachment_name"), metadata.get("attachment_size"),
                    metadata.get("attachment_status") or "none",
                    sanitize_text(metadata.get("attachment_error"))
                    if metadata.get("attachment_error") else None,
                    utc_now(), entity_id, event_type,
                ),
            )
            connection.commit()

    def get(self, entity_id: str, event_type: str | None = None):
        self.initialize()
        query = "SELECT * FROM notification_outbox WHERE entity_id=?"
        values: list[Any] = [entity_id]
        if event_type:
            query += " AND event_type=?"
            values.append(event_type)
        query += " ORDER BY updated_at DESC LIMIT 1"
        with self._connect() as connection:
            return self._row(connection.execute(query, values).fetchone())


def sanitize_text(value: object, limit: int = 500) -> str:
    text = str(value or "")
    text = re.sub(
        r"(?i)(authorization|password|secret|token|cookie)\s*[:=]\s*\S+",
        r"\1=[REDACTED]", text,
    )
    text = re.sub(r"https?://[^\s?]+\?\S+", "[URL REDACTED]", text)
    return text[:limit]


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_safe_xlsx(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as workbook:
            names = set(workbook.namelist())
            return (
                "[Content_Types].xml" in names
                and "xl/workbook.xml" in names
                and workbook.testzip() is None
            )
    except (OSError, zipfile.BadZipFile):
        return False


def prepare_discovery_attachment(
    state: Mapping[str, Any], runtime: Mapping[str, Any] | None, *,
    max_attachment_mb: float = DEFAULT_MAX_ATTACHMENT_MB,
    allowed_roots: Sequence[str | Path] | None = None,
) -> AttachmentDecision:
    """Validate the exact final export bound to this Discovery job."""
    event_type = discovery_terminal_event(dict(state), runtime=dict(runtime or {}))
    if event_type == DISCOVERY_FAILED:
        return AttachmentDecision("none")
    if event_type not in {DISCOVERY_COMPLETED, DISCOVERY_COMPLETED_ZERO_RESULTS}:
        return AttachmentDecision("none")

    job_id = str(state.get("job_id") or "")
    runtime = dict(runtime or {})
    export_state = dict(state.get("export_state") or {})
    if not job_id or str(runtime.get("job_id") or "") != job_id:
        return AttachmentDecision("invalid", error="Export non correlato al job")
    if str(runtime.get("status") or "") != "completed":
        return AttachmentDecision("invalid", error="Job runtime non completato")
    if export_state.get("job_id") and str(export_state["job_id"]) != job_id:
        return AttachmentDecision("invalid", error="Export state non coerente con job_id")
    if str(export_state.get("status") or "") not in {"completed", "valid", "generated"}:
        return AttachmentDecision("unavailable", error="Export finale non disponibile")
    raw_path = runtime.get("export_path")
    if not raw_path:
        return AttachmentDecision("unavailable", error="Export path assente")

    candidate = Path(str(raw_path)).expanduser()
    expected_name = str(export_state.get("file_name") or "")
    if candidate.is_symlink():
        return AttachmentDecision("invalid", error="Symlink non consentito")
    if candidate.suffix.lower() != ".xlsx" or candidate.name.endswith(".part"):
        return AttachmentDecision("invalid", error="Formato export non valido")
    if candidate.stem != job_id or (expected_name and candidate.name != expected_name):
        return AttachmentDecision("invalid", error="Export non coerente con job_id")
    try:
        resolved = candidate.resolve(strict=True)
        details = resolved.stat()
    except OSError:
        return AttachmentDecision("unavailable", error="File export non disponibile")
    if not stat.S_ISREG(details.st_mode) or details.st_size <= 0:
        return AttachmentDecision("invalid", size=max(0, details.st_size), error="File export non valido")

    roots = [Path(root).expanduser().resolve() for root in (allowed_roots or [DEFAULT_ATTACHMENT_ROOT])]
    if not any(resolved.is_relative_to(root) for root in roots):
        return AttachmentDecision("invalid", size=details.st_size, error="Export fuori directory consentita")
    if not _is_safe_xlsx(resolved):
        return AttachmentDecision("invalid", size=details.st_size, error="Workbook XLSX non valido")
    expected_size = export_state.get("file_size")
    if expected_size is not None and int(expected_size) != details.st_size:
        return AttachmentDecision("invalid", size=details.st_size, error="Dimensione export incoerente")
    expected_checksum = str(export_state.get("sha256") or "")
    if expected_checksum and _stream_sha256(resolved) != expected_checksum:
        return AttachmentDecision("invalid", size=details.st_size, error="Checksum export incoerente")

    completed_at = _parse_timestamp(state.get("completed_at") or state.get("updated_at"))
    date_part = (completed_at or datetime.now(timezone.utc)).astimezone(ROME).strftime("%Y-%m-%d")
    friendly_name = f"GlowUp-Scout-Discovery-{date_part}-{job_id[:8]}.xlsx"
    limit_bytes = int(max(0.0, float(max_attachment_mb)) * 1024 * 1024)
    if details.st_size > limit_bytes:
        return AttachmentDecision(
            "skipped_too_large", name=friendly_name, size=details.st_size,
            error="File Excel oltre il limite email configurato",
        )
    attachment = EmailAttachment(resolved, friendly_name, details.st_size)
    return AttachmentDecision("attached", attachment, friendly_name, details.st_size)


def _content_with_attachment_note(
    content: NotificationContent, decision: AttachmentDecision,
) -> NotificationContent:
    notes = {
        "attached": "Il file Excel completo della Discovery è allegato a questa email.",
        "skipped_too_large": "File Excel non allegato perché supera il limite email configurato.",
        "unavailable": "File Excel non allegato perché non disponibile.",
        "invalid": "File Excel non allegato perché non ha superato la validazione.",
    }
    note = notes.get(decision.status)
    if not note:
        return content
    return NotificationContent(
        content.event_type, content.subject,
        content.text.rstrip() + f"\n\n{note}\n",
        content.html.replace("</body>", f"<p>{html.escape(note)}</p></body>"),
    )


def _parse_timestamp(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _rome_timestamp(value: object) -> str:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return "—"
    return parsed.astimezone(ROME).strftime("%d/%m/%Y %H:%M")


def _duration(state: dict[str, Any]) -> str:
    started = _parse_timestamp(state.get("started_at"))
    completed = _parse_timestamp(state.get("completed_at") or state.get("updated_at"))
    seconds = (
        max(0, (completed - started).total_seconds())
        if started and completed else state.get("duration_seconds")
    )
    try:
        seconds = int(round(float(seconds)))
    except (TypeError, ValueError):
        return "—"
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours} h {minutes} min"
    if minutes:
        return f"{minutes} min {secs} s"
    return f"{secs} s"


def _money(value: object) -> str:
    try:
        rendered = f"{float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"€{rendered}"
    except (TypeError, ValueError):
        return "—"


def _percent(value: object) -> str:
    try:
        return f"{float(value):.2f}%".replace(".", ",")
    except (TypeError, ValueError):
        return "—"


def _pricing_valid(state: dict[str, Any]) -> int:
    listings = [
        listing
        for product in state.get("candidates") or []
        for listing in product.get("amazon_listings") or []
    ]
    valid = sum(
        (listing.get("pricing_status") in {"valid", "success"})
        or listing.get("reference_price") is not None
        for listing in listings
    )
    return int(valid)


def _summary_rows(state: dict[str, Any]) -> list[tuple[str, object]]:
    funnel = state.get("funnel") or {}
    analyzed = (
        state.get("sampled_identifier_count")
        or funnel.get("unique_supplier_eans")
        or len(state.get("candidates") or [])
    )
    return [
        ("Job ID", state.get("job_id") or "—"),
        ("Completata", _rome_timestamp(state.get("completed_at") or state.get("updated_at"))),
        ("Durata", _duration(state)),
        ("Supplier", " · ".join(str(x).upper() for x in state.get("selected_suppliers") or []) or "—"),
        ("EAN analizzati", int(analyzed or 0)),
        ("Prodotti trovati Amazon", int(funnel.get("amazon_found") or 0)),
        ("Listing Amazon", int(funnel.get("amazon_listings_found") or 0)),
        ("Beauty", int(funnel.get("beauty_listings") or funnel.get("beauty_valid") or 0)),
        ("BSR nel range", int(funnel.get("bsr_passed_listings") or funnel.get("bsr_passed") or 0)),
        ("Pricing validi", int(funnel.get("pricing_valid") or _pricing_valid(state))),
        ("Concorrenza valida", int(funnel.get("competition_passed_listings") or funnel.get("competition_passed") or 0)),
        ("Fee valide", int(funnel.get("fee_valid_listings") or funnel.get("fee_valid") or 0)),
        ("Combinazioni valutate", int(funnel.get("combinations_evaluated") or 0)),
        ("Opportunità finali", len(state.get("results") or [])),
    ]


def _best_opportunity(state: dict[str, Any]):
    product = next(iter(state.get("results") or []), None)
    if not product:
        return None
    combination = recommended_combination(product)
    if combination is None and isinstance(product.get("recommended_combination"), dict):
        combination = product["recommended_combination"]
    if combination is None:
        combination = next(iter(product.get("opportunity_combinations") or []), {})
    scenario = recommended_scenario(product)
    if scenario is None:
        scenario_id = combination.get("scenario_id") or product.get("best_purchase_scenario")
        scenario = next((
            row for row in product.get("scenarios") or []
            if row.get("scenario_id") == scenario_id
        ), {})
    return [
        ("Prodotto", product.get("amazon_title") or product.get("title") or "—"),
        ("EAN", product.get("canonical_ean") or product.get("gtin") or "—"),
        ("ASIN", combination.get("asin") or product.get("asin") or "—"),
        ("Supplier", str(combination.get("supplier") or scenario.get("supplier") or "—").upper()),
        ("Scenario", combination.get("scenario_label") or scenario.get("scenario_label") or "—"),
        ("Costo", _money(combination.get("cost_gross_unit_eur") or scenario.get("cost_gross_unit_eur"))),
        ("Prezzo Amazon", _money(combination.get("price_reference") or product.get("reference_price"))),
        ("Margine", _percent(combination.get("margin_percent") or scenario.get("margin_percent"))),
        ("Utile", _money(combination.get("profit") or product.get("profit"))),
        ("Score", combination.get("score") if combination.get("score") is not None else scenario.get("score", "—")),
    ]


def _failure_rows(state: dict[str, Any]) -> list[tuple[str, object]]:
    errors = state.get("errors") or []
    error = errors[-1] if errors else {}
    message = error.get("message") if isinstance(error, dict) else error
    return [
        ("Job ID", state.get("job_id") or "—"),
        ("Fase", state.get("phase") or "—"),
        ("Avviata", _rome_timestamp(state.get("started_at"))),
        ("Interrotta", _rome_timestamp(state.get("completed_at") or state.get("updated_at"))),
        ("Avanzamento", f"{int(state.get('progress_current') or 0)} / {int(state.get('progress_total') or 0)}"),
        ("Errore", sanitize_text(message or state.get("error") or "Errore non specificato")),
        ("Riprendibile", "sì" if state.get("resumable") else "no"),
    ]


def discovery_terminal_event(state: dict[str, Any], *, runtime: dict[str, Any] | None = None):
    status = str(state.get("status") or "").lower()
    runtime = runtime or {}
    if status == "completed":
        return (
            DISCOVERY_COMPLETED if state.get("results")
            else DISCOVERY_COMPLETED_ZERO_RESULTS
        )
    if status in TERMINAL_FAILURE_STATUSES and not runtime.get("resumable"):
        return DISCOVERY_FAILED
    return None


def render_discovery_notification(state: dict[str, Any], event_type: str | None = None):
    event_type = event_type or discovery_terminal_event(state)
    if event_type == DISCOVERY_COMPLETED:
        subject = "Glow Up Scout — Discovery completata"
        heading = "Discovery completata"
        rows = _summary_rows(state)
        best = _best_opportunity(state)
        closing = "Apri Glow Up Scout per consultare tutti i risultati e scaricare l’Excel."
    elif event_type == DISCOVERY_COMPLETED_ZERO_RESULTS:
        subject = "Glow Up Scout — Discovery completata senza opportunità"
        heading = "Discovery completata senza opportunità"
        rows = _summary_rows(state)
        best = None
        closing = "La Discovery è terminata correttamente, ma nessun prodotto ha superato tutti i criteri."
    elif event_type == DISCOVERY_FAILED:
        subject = "Glow Up Scout — Discovery interrotta"
        heading = "Discovery interrotta"
        rows = _failure_rows(state)
        best = None
        closing = "Apri Glow Up Scout per verificare lo stato ed eventualmente riprendere la Discovery."
    else:
        raise ValueError("Discovery state is not terminal or notification type is unsupported")

    text_rows = "\n".join(f"{label}: {value}" for label, value in rows)
    best_text = ""
    if best:
        best_text = "\n\nMIGLIORE OPPORTUNITÀ\n" + "\n".join(
            f"{label}: {value}" for label, value in best
        )
    plain = f"{heading}\n\n{text_rows}{best_text}\n\n{closing}\n"

    def table(values):
        return "".join(
            "<tr><th style='text-align:left;padding:6px 12px 6px 0;color:#52606d'>"
            f"{html.escape(str(label))}</th><td style='padding:6px 0'>"
            f"{html.escape(str(value))}</td></tr>"
            for label, value in values
        )

    best_html = ""
    if best:
        best_html = (
            "<h2 style='font-size:18px;margin-top:28px'>Migliore opportunità</h2>"
            f"<table role='presentation' style='border-collapse:collapse'>{table(best)}</table>"
        )
    rendered_html = (
        "<!doctype html><html><body style='margin:0;background:#f4f7fb'>"
        "<div style='max-width:620px;margin:auto;padding:24px'>"
        "<div style='background:white;border:1px solid #dfe7ef;border-radius:12px;padding:24px;"
        "font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#172b4d'>"
        f"<h1 style='font-size:22px;margin-top:0'>{html.escape(heading)}</h1>"
        f"<table role='presentation' style='border-collapse:collapse'>{table(rows)}</table>"
        f"{best_html}<p style='margin-top:28px;color:#52606d'>{html.escape(closing)}</p>"
        "</div></div></body></html>"
    )
    return NotificationContent(event_type, subject, plain, rendered_html)


class SMTPEmailTransport:
    def __init__(self, config: EmailConfig):
        self.config = config

    def send(self, message: EmailMessage):
        context = ssl.create_default_context()
        if self.config.smtp_security == "ssl":
            connection = smtplib.SMTP_SSL(
                self.config.smtp_host, self.config.smtp_port,
                timeout=self.config.timeout_seconds, context=context,
            )
        else:
            connection = smtplib.SMTP(
                self.config.smtp_host, self.config.smtp_port,
                timeout=self.config.timeout_seconds,
            )
        with connection as client:
            client.ehlo()
            if self.config.smtp_security == "starttls":
                client.starttls(context=context)
                client.ehlo()
            if self.config.smtp_username:
                client.login(self.config.smtp_username, self.config.smtp_password)
            client.send_message(message)
        return message["Message-ID"]


def build_email(
    content: NotificationContent, config: EmailConfig, entity_id: str,
    attachment: EmailAttachment | None = None,
):
    message = EmailMessage()
    message["Subject"] = content.subject
    message["From"] = config.sender
    message["To"] = config.recipient
    message["Date"] = formatdate(localtime=False)
    safe_entity = re.sub(r"[^A-Za-z0-9_.-]", "-", entity_id)
    message["Message-ID"] = make_msgid(idstring=f"{safe_entity}.{content.event_type}")
    message.set_content(content.text)
    message.add_alternative(content.html, subtype="html")
    if attachment:
        with attachment.path.open("rb") as source:
            payload = source.read(attachment.size + 1)
        if len(payload) != attachment.size:
            raise OSError("Attachment changed after validation")
        maintype, subtype = attachment.mime_type.split("/", 1)
        message.add_attachment(
            payload, maintype=maintype, subtype=subtype, filename=attachment.name,
        )
    return message


def send_discovery_terminal_notification(
    state: dict[str, Any], *, database_path: str | Path,
    runtime: dict[str, Any] | None = None, config: EmailConfig | None = None,
    transport=None, max_attempts: int = 3,
    sleep_func: Callable[[float], None] = time.sleep,
    allowed_attachment_roots: Sequence[str | Path] | None = None,
):
    """Attempt one terminal notification without ever changing job outcome."""
    event_type = discovery_terminal_event(state, runtime=runtime)
    if event_type is None:
        return None
    render_state = dict(state)
    if runtime and "resumable" in runtime:
        render_state["resumable"] = bool(runtime["resumable"])
    config = config or EmailConfig.from_runtime()
    decision = prepare_discovery_attachment(
        render_state, runtime, max_attachment_mb=config.max_attachment_mb,
        allowed_roots=allowed_attachment_roots,
    )
    content = _content_with_attachment_note(
        render_discovery_notification(render_state, event_type), decision,
    )
    return send_notification(
        content, entity_id=str(state.get("job_id") or ""),
        database_path=database_path, config=config, transport=transport,
        max_attempts=max_attempts, sleep_func=sleep_func,
        attachment=decision.attachment, attachment_metadata=decision.metadata(),
    )


def send_notification(
    content: NotificationContent, *, entity_id: str, database_path: str | Path,
    config: EmailConfig | None = None, transport=None, max_attempts: int = 3,
    sleep_func: Callable[[float], None] = time.sleep,
    attachment: EmailAttachment | None = None,
    attachment_metadata: Mapping[str, Any] | None = None,
):
    """Deliver any rendered email event through the shared persistent outbox."""
    config = config or EmailConfig.from_runtime()
    outbox = NotificationOutbox(database_path)
    if not entity_id:
        return None
    if not outbox.reserve(
        entity_id, content.event_type, config.recipient, attachment_metadata,
    ):
        return outbox.get(entity_id, content.event_type)
    missing = config.missing_requirements()
    if missing:
        outbox.mark_terminal(
            entity_id, content.event_type, "not_configured",
            "Configurazione email incompleta: " + ", ".join(missing),
        )
        return outbox.get(entity_id, content.event_type)

    try:
        message = build_email(content, config, entity_id, attachment)
    except Exception as exc:
        if attachment is None:
            outbox.mark_terminal(
                entity_id, content.event_type, "failed",
                f"Preparazione email fallita ({type(exc).__name__})",
            )
            return outbox.get(entity_id, content.event_type)
        attached_note = "Il file Excel completo della Discovery è allegato a questa email."
        invalid_note = "File Excel non allegato perché non è stato possibile leggerlo."
        fallback_content = NotificationContent(
            content.event_type, content.subject,
            content.text.replace(attached_note, invalid_note),
            content.html.replace(attached_note, invalid_note),
        )
        fallback_metadata = {
            "attachment_name": attachment.name,
            "attachment_size": attachment.size,
            "attachment_status": "invalid",
            "attachment_error": f"Lettura allegato fallita ({type(exc).__name__})",
        }
        outbox.update_attachment(entity_id, content.event_type, fallback_metadata)
        message = build_email(fallback_content, config, entity_id)
    sender = transport or SMTPEmailTransport(config)
    last_error = ""
    effective_attempts = max(1, min(int(max_attempts), 3))
    for attempt in range(effective_attempts):
        outbox.begin_attempt(entity_id, content.event_type)
        try:
            message_id = sender.send(message)
            outbox.mark_sent(
                entity_id, content.event_type,
                str(message_id or message["Message-ID"]),
            )
            return outbox.get(entity_id, content.event_type)
        except Exception as exc:  # notification failure must not affect Discovery
            last_error = f"Invio email fallito ({type(exc).__name__})"
            if attempt + 1 < effective_attempts:
                sleep_func(float(2 ** attempt))
    outbox.mark_terminal(
        entity_id, content.event_type, "failed", last_error,
    )
    return outbox.get(entity_id, content.event_type)
