#!/usr/bin/env python
#parse_semaphore_logs.py
#-------------------------------
# Created By: Savannah Stephenson
# Created Date: 08/07/26
# Version 2.2
#-------------------------------
""" 
Walks Semaphore log files for a given date range, pulls out "success" and
"failed" model-run events, and writes them to a CSV. Prints a summary at
the end (success rate, top failure reasons, models with the most failures).

Usage:
    ./parse_semaphore_logs.py 02/07/26 02/18/26

Dates are MM/DD/YY, matching the timestamp format used in the log files
themselves (e.g. "08/07/26 15:31:26: ...").
""" 
#-------------------------------
# 
#

import argparse
import csv
import re
import socket
import sys
from collections import Counter, namedtuple
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional


# --- Regex patterns -------------------------------------------------------
# All patterns live inside one Patterns object instead of as bare
# module-level constants. main() builds it once (build_patterns) and
# passes it explicitly to every function that needs it -- nothing here
# is reachable except through that one parameter.
#
# Using collections.namedtuple rather than @dataclass(frozen=True) here:
# dataclasses were added in Python 3.7, and this needs to run on 3.6
# (sherlock-dev). namedtuple gives the same "small immutable bundle of
# named fields" behavior and has been available since Python 2.6.
Patterns = namedtuple(
    "Patterns",
    [
        "timestamp_line",
        "failure_block_start",
        "raw_traceback_start",
        "model_name",
        "missing_count",
        "time_diff",
        "data_source",
        "connection_failure_start",
        "db_conn_target",
        "http_error",
    ],
)


def build_patterns() -> Patterns:
    """Compile every regex the parser uses, once, up front. Compiling once
    (instead of inline in the loop) avoids recompiling the same pattern on
    every line of every log file -- a small but easy performance win.
    """
    return Patterns(
        # Matches a leading timestamp like "08/07/26 15:31:26: "
        timestamp_line=re.compile(r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}):"),

        # A line that starts a "failure block" we want to capture the
        # contents of. Only checked on *timestamped* lines -- Semaphore's
        # own log messages ("Full stack trace:", "DateRangeValidation:")
        # always carry a timestamp prefix. See raw_traceback_start for the
        # untimestamped case.
        failure_block_start=re.compile(r"DateRangeValidation:|Full stack trace:"),

        # A bare Python traceback header with NO Semaphore timestamp
        # prefix, e.g. when a library (requests, psycopg, etc.) raises and
        # Python's default exception printing kicks in rather than a
        # custom Semaphore log call. Checked only at the start of a line,
        # and only when we're not already inside some other block, so it
        # can open a failure block on its own.
        raw_traceback_start=re.compile(r"^Traceback \(most recent call last\):"),

        model_name=re.compile(r"Model (\S+)"),
        missing_count=re.compile(r"is missing (\d+)"),
        time_diff=re.compile(r"Time difference: ([^.]+)"),
        data_source=re.compile(r"source: ([^,]+)"),

        # Marks the start of a database-connection-outage block, e.g.:
        #   "Error:: Prediction failed due to Semaphore Exception"
        # Unlike failure_block_start above, blocks that start here have no
        # matching "FAILED - Null result inserted" line to close on -- the
        # model name isn't even mentioned in the block. These get closed
        # implicitly instead (see process_log_file), best-effort
        # attributed to whichever model most recently logged
        # "completed successfully".
        connection_failure_start=re.compile(r"Prediction failed due to \S+ Exception"),

        # Pulls the host/port out of a psycopg connection-refused message:
        #   connection to server at "172.17.0.1", port 5435 failed
        db_conn_target=re.compile(r'connection to server at\s+"([^"]+)",\s*port\s*(\d+)'),

        # Pulls status code + URL out of a requests-library HTTPError:
        #   404 Client Error: Not Found for url: https://.../42019.txt
        # Covers both "Client Error" (4xx) and "Server Error" (5xx), since
        # both come from the same HTTPError.raise_for_status() call.
        http_error=re.compile(r"(\d{3}) (?:Client|Server) Error.*?for url:\s*(\S+)"),
    )


# --- Date helpers -----------------------------------------------------------

def parse_mmddyy(date_str: str) -> date:
    """Parse an 'MM/DD/YY' string into a date object.
    """
    try:
        return datetime.strptime(date_str, "%m/%d/%y").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"'{date_str}' is not a valid MM/DD/YY date"
        ) from exc


def month_patterns(start: date, end: date) -> List[str]:
    """Generate 'YYYY_M' strings for every month between start and end
    (inclusive), matching the log filename convention (e.g. '2026_8').

    """
    patterns = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        patterns.append(f"{year}_{month}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return patterns


# --- Field extraction helpers ------------------------------------------------
# Each of these takes `patterns` as an explicit parameter rather than
# reaching out to a module-level constant -- so a function's signature
# alone tells you everything it depends on.

def extract_model_name(line: str, patterns: Patterns) -> Optional[str]:
    match = patterns.model_name.search(line)
    return match.group(1) if match else None


def extract_failure_reason(content: str, patterns: Patterns) -> str:
    """Classify a failure block's content into a short reason code."""
    if "is missing" in content and "values" in content:
        return "missing_data"
    if "is stale" in content:
        return "stale_data"
    if "gaps in the data that are larger than the interpolation limit" in content:
        return "interpolation_gap"
    if patterns.http_error.search(content):
        return "upstream_http_error"
    return "unknown"


def extract_data_source(content: str, patterns: Patterns) -> str:
    """Prefer the explicit 'source: X' field. Fall back to the domain of
    the failing URL for upstream HTTP errors, since those blocks don't
    have a 'source:' field at all -- the URL *is* the source.
    """
    match = patterns.data_source.search(content)
    if match:
        return match.group(1)

    http_match = patterns.http_error.search(content)
    if http_match:
        url = http_match.group(2)
        return re.sub(r"^https?://", "", url).split("/", 1)[0]

    return ""


def extract_missing_details(content: str, patterns: Patterns) -> str:
    """Build a semicolon/pipe-delimited summary string, same shape as the
    original bash version (missing_count / times / time_diff), so the CSV
    schema doesn't change for anyone downstream who consumes this file.

    Also appends HTTP status code + full URL for upstream_http_error
    blocks, reusing this column as a general "extra details" field rather
    than adding a whole new CSV column.
    """
    parts = []

    missing_count = patterns.missing_count.search(content)
    if missing_count:
        parts.append(f"missing_count:{missing_count.group(1)}")

    missing_times = [
        line.split("Missing time:", 1)[1].strip()
        for line in content.splitlines()
        if "Missing time:" in line
    ][:3]
    if missing_times:
        parts.append("times:" + ";".join(missing_times))

    time_diff = patterns.time_diff.search(content)
    if time_diff:
        parts.append(f"time_diff:{time_diff.group(1).strip()}")

    http_match = patterns.http_error.search(content)
    if http_match:
        parts.append(f"status_code:{http_match.group(1)}|url:{http_match.group(2)}")

    return "|".join(parts)


def extract_connection_failure_reason(content: str) -> str:
    """Classify a connection-failure block (separate from
    extract_failure_reason, since these blocks have a different shape --
    they're built around a database driver error, not a data-integrity
    check like DateRangeValidation). Plain substring checks, no regex
    needed, so no patterns object required here.
    """
    if "OperationalError" in content or "connection failed:" in content:
        return "db_connection_error"
    return "prediction_error_unknown"


def extract_connection_failure_details(content: str, patterns: Patterns) -> str:
    """Same idea as extract_missing_details, but for connection-failure
    blocks: pulls out the unreachable host/port, and flags whether the
    orchestrator's Discord alert *also* failed (meaning nobody got paged).
    """
    parts = []

    target = patterns.db_conn_target.search(content)
    if target:
        parts.append(f"host:{target.group(1)}|port:{target.group(2)}")

    if "discord notification" in content.lower():
        parts.append("discord_notification_failed:true")

    return "|".join(parts)


def write_connection_failure(writer, log_file: Path, timestamp: str,
                              model: Optional[str], content: str, start_line: int,
                              patterns: Patterns) -> None:
    """Write one connection-failure row to the CSV.

    Top-level (not nested inside process_log_file) so it's a standalone,
    testable unit -- process_log_file just calls it when a pending block
    needs to be flushed, rather than owning the write logic itself.
    """
    reason = extract_connection_failure_reason(content)
    details = extract_connection_failure_details(content, patterns)
    writer.writerow(
        [timestamp, model or "", "failed", reason, "", details,
         log_file.name, start_line]
    )


# --- Log file processing -----------------------------------------------------

def process_log_file(log_file: Path, start_int: int, end_int: int, writer,
                      patterns: Patterns) -> None:
    """Stream one log file line by line, writing success/failure rows to
    the CSV writer as they're found.

    Three kinds of failure block are tracked, because they open/close
    differently:

      1. "Explicitly closed, timestamped start" (in_failure_block /
         failure_content): DateRangeValidation / interpolation-limit
         failures. Opens on a *timestamped* line matching
         patterns.failure_block_start, closes cleanly on a matching
         "FAILED - Null result inserted" line, which also gives the model
         name.

      2. "Explicitly closed, untimestamped start" (also
         in_failure_block / failure_content -- same block, different
         opener): upstream HTTP errors (requests.exceptions.HTTPError,
         etc.). These come from a plain Python traceback with no Semaphore
         timestamp on the "Traceback (most recent call last):" header
         line, so they're opened by patterns.raw_traceback_start instead.
         They still close the same way, on "FAILED - Null result inserted".

      3. "Implicitly closed" (connection_failure_pending / ...):
         database-connection outages. These open on
         patterns.connection_failure_start but never produce a "FAILED"
         line -- the process crashes before it can log one. So there's
         nothing to match on to know the block is over; we only find out
         once the *next* recognized event appears (handled inline below,
         via write_connection_failure), or once the file ends (handled
         after the loop). The model name isn't in the block at all, so we
         fall back to whichever model most recently logged
         "completed successfully".

    That state genuinely needs to persist line-to-line, so (unlike the
    regex-based extract helpers) this isn't something we can simplify away
    with vectorized/whole-file operations.
    """
    in_failure_block = False
    failure_content = ""
    failure_start_line = 0

    connection_failure_pending = False
    connection_failure_content = ""
    connection_failure_start_line = 0
    connection_failure_timestamp = ""

    last_known_model: Optional[str] = None
    current_timestamp = ""

    with log_file.open(errors="replace") as fh:
        for line_num, line in enumerate(fh, start=1):
            line = line.rstrip("\n")

            ts_match = patterns.timestamp_line.match(line)

            # IMPORTANT: within a single failure event, Semaphore timestamps
            # *each* top-level log call separately -- e.g. both
            # "Error:: Prediction failed..." and the following
            # "Exception message: ..." line carry their own timestamp, even
            # though they're one logical event. So a timestamp alone does
            # NOT mean "a new event started" -- only one of the recognized
            # markers below (success / failure_block_start /
            # connection_failure_start / "FAILED - Null result inserted")
            # does. Anything else, timestamped or not, is a continuation of
            # whatever block is currently open.
            is_recognized_event = ts_match and (
                "completed successfully" in line
                or patterns.failure_block_start.search(line)
                or patterns.connection_failure_start.search(line)
                or "FAILED - Null result inserted" in line
            )

            if is_recognized_event:
                # A new recognized event closes out any previously-open
                # connection-failure block (it has no closing line of its
                # own, so this is the only place it gets flushed mid-file).
                if connection_failure_pending:
                    write_connection_failure(
                        writer, log_file, connection_failure_timestamp,
                        last_known_model, connection_failure_content,
                        connection_failure_start_line, patterns,
                    )
                    connection_failure_pending = False
                    connection_failure_content = ""

                current_timestamp = ts_match.group(1)
                date_part = current_timestamp.split(" ", 1)[0]

                # Reconstruct as YYMMDD (matches start_int/end_int format below)
                # so we can compare with plain integer ordering, same trick
                # the bash version used with its date_to_int() helper.
                mm, dd, yy = date_part.split("/")
                date_int = int(f"{yy}{mm}{dd}")

                if not (start_int <= date_int <= end_int):
                    in_failure_block = False
                    continue

                if "completed successfully" in line:
                    model = extract_model_name(line, patterns)
                    if model:
                        last_known_model = model
                        writer.writerow(
                            [current_timestamp, model, "success", "", "", "",
                             log_file.name, line_num]
                        )
                    in_failure_block = False

                elif patterns.failure_block_start.search(line):
                    in_failure_block = True
                    failure_content = line
                    failure_start_line = line_num

                elif patterns.connection_failure_start.search(line):
                    connection_failure_pending = True
                    connection_failure_content = line
                    connection_failure_start_line = line_num
                    connection_failure_timestamp = current_timestamp
                    in_failure_block = False

                elif "FAILED - Null result inserted" in line:
                    model = extract_model_name(line, patterns)
                    if in_failure_block and failure_content:
                        reason = extract_failure_reason(failure_content, patterns)
                        source = extract_data_source(failure_content, patterns)
                        details = extract_missing_details(failure_content, patterns)
                        writer.writerow(
                            [current_timestamp, model, "failed", reason, source,
                             details, log_file.name, failure_start_line]
                        )
                    in_failure_block = False
                    failure_content = ""

            elif in_failure_block:
                # A continuation line inside the current failure block --
                # keep appending it so extract_* helpers can scan the whole
                # block once the FAILED line is reached.
                failure_content += "\n" + line

            elif connection_failure_pending:
                # Continuation line (whether or not it has its own
                # timestamp) inside the current connection-failure block.
                connection_failure_content += "\n" + line

            elif patterns.raw_traceback_start.match(line):
                # A bare Python traceback started with no Semaphore
                # timestamp prefix and no other block currently open.
                # We don't know the date yet (no timestamp on this line),
                # but that's fine -- the block only gets written out once
                # we reach a *timestamped* "FAILED - Null result inserted"
                # line, and that line's own date-range check still applies.
                in_failure_block = True
                failure_content = line
                failure_start_line = line_num

    # The file may end while a connection-failure block is still "open"
    # (no later timestamped line arrived to trigger the flush above).
    if connection_failure_pending:
        write_connection_failure(
            writer, log_file, connection_failure_timestamp,
            last_known_model, connection_failure_content,
            connection_failure_start_line, patterns,
        )


# --- Main ---------------------------------------------------------------------
# main() is deliberately just five function calls, one per stage of the
# algorithm. Each stage has its own function below; main() itself is the
# one place you can read top-to-bottom to see the whole pipeline.


def parse_args() -> argparse.Namespace:
    """Stage 1: parse and validate the CLI date range.

    Returns the argparse Namespace (start_date/end_date as `date` objects)
    -- validation (start <= end) happens here too, via parser.error(),
    so a bad range exits with a clean usage message instead of a traceback.
    """
    parser = argparse.ArgumentParser(
        description="Parse Semaphore log files into a CSV of success/failure events.",
        epilog="Example: %(prog)s 02/07/26 02/18/26",
    )
    parser.add_argument("start_date", type=parse_mmddyy, help="Start date, MM/DD/YY")
    parser.add_argument("end_date", type=parse_mmddyy, help="End date, MM/DD/YY")
    args = parser.parse_args()

    if args.start_date > args.end_date:
        parser.error(
            f"Start date ({args.start_date:%m/%d/%y}) must be before or equal "
            f"to end date ({args.end_date:%m/%d/%y})"
        )

    return args


def resolve_output_csv_path(log_dir: Path) -> Path:
    """Stage 2 (part A): figure out where the CSV goes and what to name it.

    Mirrors the bash script's env-detection trick: the output filename
    embeds "dev" or "prod" based on the hostname, so runs from either
    server never overwrite each other's output.
    """
    output_dir = log_dir.parent.parent / "tools" / "parsed_semaphore_logs"
    output_dir.mkdir(parents=True, exist_ok=True)

    env_label = "prod" if "prod" in socket.gethostname() else "dev"
    return output_dir / f"semaphore_{env_label}_stats_{datetime.now():%Y%m%d_%H%M%S}.csv"


def find_log_files(log_dir: Path, start: date, end: date) -> List[Path]:
    """Stage 2 (part B): narrow down which log files could possibly contain
    rows in [start, end], without opening a single one yet.

    Log filenames embed their year/month (e.g. "2026_8_<model>.log"), so we
    can skip straight to the relevant files with a glob per month in range,
    rather than opening every file in the directory and checking dates
    line-by-line. This is the same optimization the bash version made with
    its `find -name ... -o -name ...` pattern list.

    Logs live one level down, in a per-model subdirectory
    (data/logs/<model>/2026_8_<model>.log), not flat in log_dir itself --
    so this has to search recursively. We use rglob (recursive glob)
    rather than glob for exactly that reason: the original bash script's
    `find "$LOG_DIR" -type f ...` recurses into subdirectories by default,
    and rglob is the direct pathlib equivalent of that behavior. A plain
    glob() here would only check log_dir's immediate contents and silently
    find nothing, regardless of date range -- which is exactly what
    happened on the first pass of this script.
    """
    candidate_patterns = month_patterns(start, end)
    return sorted(
        {
            f
            for pattern in candidate_patterns
            for f in log_dir.rglob(f"{pattern}_*.log")
            if f.is_file()
        }
    )


def write_csv(log_files: List[Path], start_int: int, end_int: int,
              output_csv: Path, patterns: Patterns) -> None:
    """Stage 3: the actual parse. Stream each candidate file line-by-line
    (process_log_file), classifying every "completed successfully" /
    "FAILED - Null result inserted" / connection-outage event we recognize,
    and write one CSV row per event as we go.

    We write incrementally (one open csv.writer for all files) rather than
    building a list of rows in memory first -- keeps memory flat regardless
    of how many months of logs are being parsed.
    """
    with output_csv.open("w", newline="") as out_fh:
        writer = csv.writer(out_fh)
        writer.writerow(
            ["timestamp", "model_name", "status", "failure_reason",
             "data_source", "missing_data_details", "log_file", "line_number"]
        )

        for i, log_file in enumerate(log_files, start=1):
            print(f"[{i}/{len(log_files)}] Processing: {log_file.name}")
            process_log_file(log_file, start_int, end_int, writer, patterns)


def print_summary(output_csv: Path) -> None:
    """Stage 4: re-read the CSV we just wrote and print aggregate stats.

    Re-reading (rather than tallying counters while writing in stage 3) is
    a deliberate simplification: it means write_csv only has one job
    (parse and write), and this function only has one job (summarize a
    finished CSV) -- and CSVs are cheap to re-read compared to the log
    files themselves. This is also where the Python rewrite pays off the
    most: instead of grep/cut/sort/uniq -c pipelines, one csv.DictReader
    pass + collections.Counter gets us everything.
    """
    with output_csv.open(newline="") as fh:
        rows = list(csv.DictReader(fh))

    total = len(rows)
    success_count = sum(1 for r in rows if r["status"] == "success")
    failed_rows = [r for r in rows if r["status"] == "failed"]
    failed_count = len(failed_rows)

    print("=== Summary Statistics ===")
    print(f"Total entries: {total}")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")
    if total:
        print(f"Success rate: {success_count / total * 100:.2f}%")

    print("\n=== Top Failure Reasons ===")
    for reason, count in Counter(r["failure_reason"] for r in failed_rows).most_common(5):
        print(f"{count:>6} {reason}")

    print("\n=== Models with Most Failures ===")
    for model, count in Counter(r["model_name"] for r in failed_rows).most_common(10):
        print(f"{count:>6} {model}")


def main() -> int:
    # 1. Parse & validate the CLI date range.
    args = parse_args()
    start_int = int(args.start_date.strftime("%y%m%d"))
    end_int = int(args.end_date.strftime("%y%m%d"))

    # All regexes get compiled once here and threaded through explicitly
    # from this point on -- nothing above this line exists as a global.
    patterns = build_patterns()

    # Path setup mirrors the bash script's relative-to-itself paths: logs
    # live in <semaphore_root>/data/logs, output in
    # <semaphore_root>/tools/parsed_semaphore_logs.
    script_dir = Path(__file__).resolve().parent
    log_dir = script_dir.parent / "data" / "logs"
    output_csv = resolve_output_csv_path(log_dir)

    print("Semaphore Log Parser - Starting...")
    print(f"Date range: {args.start_date:%m/%d/%y} to {args.end_date:%m/%d/%y}")
    print(f"Log directory: {log_dir}")
    print(f"Output directory: {output_csv.parent}")
    print(f"Output file: {output_csv.name}\n")

    # 2. Figure out which log files are even worth opening for this range.
    log_files = find_log_files(log_dir, args.start_date, args.end_date)
    if not log_files:
        print(f"ERROR: No matching log files found in {log_dir}")
        example_patterns = " ".join(
            f"{p}_<model>.log" for p in month_patterns(args.start_date, args.end_date)[:3]
        )
        print(f"Expected filenames like: {example_patterns}")
        return 1
    print(f"Found {len(log_files)} log file(s) to process\n")

    # 3. Parse those files and write one CSV row per recognized event.
    write_csv(log_files, start_int, end_int, output_csv, patterns)
    print("\nProcessing complete!")
    print(f"Output saved to: {output_csv}\n")

    # 4. Re-read the CSV and print success/failure stats.
    print_summary(output_csv)
    print(f"\nDone! Output: {output_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())