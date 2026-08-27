#!/usr/bin/env python3
"""Safely configure Scout email runtime files outside the repository."""

from __future__ import annotations

import argparse
import getpass
import os
import tempfile
from pathlib import Path

from notifications import DEFAULT_EMAIL_CONFIG_PATH, DEFAULT_EMAIL_PASSWORD_PATH


REGISTER_CONFIG = """GLOWUP_SCOUT_EMAIL_ENABLED=true
GLOWUP_SCOUT_EMAIL_TO=nicola@glowupitalia.it
GLOWUP_SCOUT_EMAIL_FROM=nicola@glowupitalia.it
GLOWUP_SCOUT_SMTP_HOST=authsmtp.securemail.pro
GLOWUP_SCOUT_SMTP_PORT=465
GLOWUP_SCOUT_SMTP_USERNAME=nicola@glowupitalia.it
GLOWUP_SCOUT_SMTP_SECURITY=ssl
GLOWUP_SCOUT_EMAIL_MAX_ATTACHMENT_MB=15
"""


def _atomic_private_write(destination: Path, content: str):
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(destination.parent, 0o700)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_register_config(path: Path = DEFAULT_EMAIL_CONFIG_PATH):
    _atomic_private_write(path, REGISTER_CONFIG)
    print(f"Configurazione SMTP non sensibile salvata in {path}")


def set_password(path: Path = DEFAULT_EMAIL_PASSWORD_PATH):
    password = getpass.getpass("Password SMTP Register.it (input nascosto): ")
    confirmation = getpass.getpass("Ripeti password SMTP: ")
    if not password or password != confirmation:
        raise SystemExit("Password vuota o conferma non coincidente; nessun file scritto.")
    _atomic_private_write(path, password + "\n")
    print(f"Secret SMTP salvato con permessi 0600 in {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("write-register-config", "set-password"))
    args = parser.parse_args()
    if args.action == "write-register-config":
        write_register_config()
    else:
        set_password()


if __name__ == "__main__":
    main()
