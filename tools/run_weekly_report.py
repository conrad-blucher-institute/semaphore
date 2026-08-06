#!/usr/bin/env python3
"""
run_weekly_report.py

Runs the existing parse_semaphore_logs.sh for the previous Fri-Thu
window (e.g. run on Fri 08/07 -> covers 07/31-08/06), then copies the
resulting CSV into sstephenson2.admin's home directory via sudo cp.

Meant to be run via cron on both sherlock-dev and sherlock-prod.
"""
import subprocess
import socket
import sys
from datetime import date, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PARSER = SCRIPT_DIR / "parse_semaphore_logs.sh"
DEST_DIR = "/home/sstephenson2.admin"


def get_date_range(today: date) -> tuple[date, date]:
    """Given today's date, return (start, end) for the prior 7-day window
    ending yesterday. Run on Fri 08/07/2026 -> (07/31/2026, 08/06/2026)."""
    end = today - timedelta(days=1)
    start = today - timedelta(days=7)
    return start, end


def fmt(d: date) -> str:
    return d.strftime("%m/%d/%y")


def env_label() -> str:
    host = socket.gethostname().lower()
    return "prod" if "prod" in host else "dev"


def main() -> None:
    today = date.today()
    start, end = get_date_range(today)
    start_str, end_str = fmt(start), fmt(end)

    print(f"Running weekly Semaphore report for {start_str} to {end_str}")

    if not PARSER.exists():
        print(f"ERROR: parser not found at {PARSER}", file=sys.stderr)
        sys.exit(1)

    result = subprocess.run(
        [str(PARSER), start_str, end_str],
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    # Pull the actual output path out of the bash script's own stdout
    csv_path = None
    for line in result.stdout.splitlines():
        if line.startswith("Output saved to:"):
            csv_path = Path(line.split("Output saved to:", 1)[1].strip())
            break

    if not csv_path or not csv_path.exists():
        print("ERROR: could not locate the generated CSV in script output", file=sys.stderr)
        sys.exit(1)

    cp_result = subprocess.run(
        ["sudo", "cp", str(csv_path), f"{DEST_DIR}/"],
        capture_output=True,
        text=True,
        check=False,
    )
    if cp_result.returncode != 0:
        print(f"ERROR: sudo cp failed: {cp_result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Copied {csv_path.name} to {DEST_DIR}/ ({env_label()})")


if __name__ == "__main__":
    main()