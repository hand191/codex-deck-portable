#!/usr/bin/env python3
import argparse
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Create an online SQLite backup for Codex Deck."
    )
    parser.add_argument("database", type=Path)
    parser.add_argument("backup_directory", type=Path)
    args = parser.parse_args()

    database = args.database.resolve()
    if not database.is_file():
        print("No existing database; backup skipped.")
        return

    backup_directory = args.backup_directory.resolve()
    backup_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destination = backup_directory / f"codex-pre-deploy-{timestamp}.sqlite3"

    source_connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
        result = destination_connection.execute("PRAGMA integrity_check").fetchone()
        if not result or result[0] != "ok":
            raise RuntimeError(f"backup integrity check failed: {result!r}")
    finally:
        destination_connection.close()
        source_connection.close()

    os.chmod(destination, 0o600)
    print(destination)


if __name__ == "__main__":
    main()
