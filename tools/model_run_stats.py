#!/usr/bin/env python
# model_run_stats.py
# -------------------------------
# Created By: Savannah Stephenson and Claude AI
# -------------------------------
"""
Walks Semaphore log files for a given date range, pulls out "success" and
"failed" model-run events, and writes:
  1. a CSV of every event
  2. a single report.html with overall charts/tables, PLUS one section
     per model family with its own chart, a failure-reason table, and a
     table of individual failures with a blank column for investigator
     notes.

Usage:
    ./model_run_stats.py 02/07/26 02/18/26

Dates are MM/DD/YY, matching the timestamp format used in the log files
themselves (e.g. "08/07/26 15:31:26: ...").
"""
# -------------------------------
import argparse                        
import html                            
import re                              
import socket                          
import sys                             
from datetime import datetime          
from pathlib import Path               
import pandas as pd                    


# =============================================================================
# SECTION 1: Hardcoded reference data (MODEL_FAMILIES)
# =============================================================================
# This is a plain data structure that a team member edits by hand when a new
# model family is added to semaphore.
#
# A model belongs to a family if that family's name appears ANYWHERE in
# the model's file/directory name -- e.g. "magnolia_12" contains
# "magnolia", so it's part of the "magnolia" family.
#
# ORDER MATTERS: family_for_model() checks this list top to bottom and
# stops at the first match. If a model name could match more than one
# entry (e.g. "MRE_Bird-Island_Water-Temperature_102hr" contains BOTH
# "MRE" and "Bird-Island_Water-Temperature"), whichever is listed first
# wins. "MRE" is listed before "Bird-Island_Water-Temperature" here on
# purpose, so MRE-prefixed logs get their own family instead of being
# swallowed into "Bird-Island_Water-Temperature".
MODEL_FAMILIES = [
    "MRE",                              # must come before "Bird-Island_Water-Temperature" -- see note above
    "VirginiaKey_wl",
    "Bird-Island_Water-Temperature",
    "CRPS",
    "MLP-OP",
    "ThermalRefuge",
    "ar_inundation",
    "surge",
    "magnolia_transform",               # must come before "magnolia" -- see note above                         
    "magnolia",
]

# Fallback family name for any model whose name doesn't contain any of
# the strings in MODEL_FAMILIES above.
UNMAPPED_FAMILY = "unmapped"

# Fallback model name for a failure block that never sees a "Model X"
# line on its FAILED line (and has no filename to infer from, or the
# filename doesn't match the expected shape) -- an explicit sentinel
# instead of "", so it reads as an intentional "couldn't determine this"
# value in the CSV and report tables rather than a blank data gap.
MISSING_MODEL = "unknown_model"

# How many rows the per-family "investigator notes" table shows.
# If a family has more failures than this, the table shows the worst
# offenders (grouped by most-common reason, then most-recent) and a note
# says how many were left out -- the full list is always in the CSV.
NOTES_TABLE_CAP = 10

# How many families appear in the two *overall* (all-families-combined)
# charts. Kept small so those two legends stay readable; this cap does NOT
# apply to the per-family sections below them, which cover every family.
OVERALL_CHART_FAMILY_LIMIT = 10


def family_for_model(model_name):
    """Look up a model's family by checking whether any entry in
    MODEL_FAMILIES is a substring of the model's name. Returns
    UNMAPPED_FAMILY instead of raising an error if nothing matches -- a
    model Semaphore starts logging that isn't in MODEL_FAMILIES yet
    should still show up in the report (as "unmapped"), not crash the
    whole run.
    """
    for family in MODEL_FAMILIES:
        if family in model_name:
            return family
    return UNMAPPED_FAMILY


# =============================================================================
# SECTION 2: Regex patterns used while scanning log lines
# =============================================================================
# General-purpose patterns -- these pull specific pieces of text (a model
# name, a host/port, a URL) out of a block, regardless of what KIND of
# failure the block turns out to be. Failure-TYPE detection lives
# separately, in Section 3, so this dict only ever grows when there's a
# genuinely new piece of information to extract, not when a new failure
# type shows up.

def build_patterns():
    """Compile every regex the parser needs, once, and return them in a
    plain dict so a new pattern can be added later just by adding a new
    key -- no type definition to update anywhere else.
    """
    return {
        # Matches a leading timestamp like "08/07/26 15:31:26: "
        "timestamp_line": re.compile(r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}):"),

        # Marks the start of a "failure block" whose content we want to
        # capture. Only checked on timestamped lines.
        "failure_block_start": re.compile(r"DateRangeValidation:|Full stack trace:"),

        # A bare Python traceback header with NO Semaphore timestamp
        # prefix (e.g. a library raised and Python's default exception
        # printing kicked in instead of a Semaphore log call).
        "raw_traceback_start": re.compile(r"^Traceback \(most recent call last\):"),

        # Pulls the model name out of a line like "Model magnolia_12 ..."
        "model_name": re.compile(r"Model (\S+)"),

        # Pulls the missing-value count out of "... is missing 4 values"
        "missing_count": re.compile(r"is missing (\d+)"),

        # Pulls the free-text time gap out of "Time difference: 3:15:00 ..."
        "time_diff": re.compile(r"Time difference: ([^.]+)"),

        # Pulls the data source name out of "... source: NOAA_CO_OPS, ..."
        "data_source": re.compile(r"source: ([^,]+)"),

        # Pulls DataSeries + Location out of the "Failed results:" trailer
        # that Semaphore prints AFTER the "FAILED - Null result inserted"
        # line for some failure types (interpolation_gap in particular).
        # This is the only place those blocks carry any source info at all.
        "data_series_location": re.compile(r"DataSeries:\s*([^,]+),\s*Location:\s*([^,]+)"),

        # Marks the start of a database-connection-outage block.
        "connection_failure_start": re.compile(r"Prediction failed due to \S+ Exception"),

        # Pulls host/port out of a psycopg connection-refused message.
        "db_conn_target": re.compile(r'connection to server at\s+"([^"]+)",\s*port\s*(\d+)'),

        # Pulls HTTP status code + URL out of a requests-library HTTPError.
        "http_error": re.compile(r"(\d{3}) (?:Client|Server) Error.*?for url:\s*(\S+)"),
    }


# =============================================================================
# SECTION 3: Failure-type detection
# =============================================================================
# ONE flat dict, {failure_type_name: regex_to_detect_it}, covering 
# failure types. To add a brand-new failure type: add
# ONE line to this dict.
#
# ORDER MATTERS: dicts in Python keep the order you wrote them in, and
# classify_failure_reason() below checks them top to bottom, first match
# wins. So more specific patterns go above more general ones -- in
# particular, "stale_data" and "interpolation_gap" are checked BEFORE
# "missing_data", because "missing_data" also matches a generic
# "DateRangeValidation Failed in Data Gatherer!" message that can appear
# on stale/gap failures too. Checking the specific ones first means a
# stale/gap failure never gets misclassified as missing_data just
# because it also contains that generic wrapper phrase.
FAILURE_TYPE_PATTERNS = {
    "stale_data": re.compile(r"is stale"),
    "interpolation_gap": re.compile(r"gaps in the data that are larger than the interpolation limit"),
    # Matches either the specific "is missing N values" phrasing, OR the
    # generic "DateRangeValidation Failed in Data Gatherer!" message that
    # shows up with no more specific reason attached (just a
    # SeriesDescription block) -- both mean the same underlying thing:
    # some expected data wasn't there.
    "missing_data": re.compile(r"is missing \d+ values|DateRangeValidation Failed in Data Gatherer"),
    # The NOAA CO-OPS API library raising its own error (station not
    # found, no data for the requested product/time, etc). Shows up as a
    # bare Python traceback, same as upstream_http_error, but it's a
    # library-level error rather than a raw HTTP status code.
    "coopsapi_error": re.compile(r"COOPSAPIError"),
    "upstream_http_error": re.compile(r"\d{3} (?:Client|Server) Error"),
    "db_connection_error": re.compile(r"OperationalError|connection failed:"),
}


def classify_failure_reason(content, type_patterns):
    """Check each entry in a {reason: regex} dict against the block's
    text, in order, and return the name of the first one that matches.
    If nothing matches, return "unknown" -- NOT a silent guess -- so
    unrecognized failures are visible in the report instead of being
    mis-filed under some existing reason.
    """
    for reason, pattern in type_patterns.items():
        if pattern.search(content):
            return reason
    return "unknown"


def extract_series(content, patterns):
    """Pulls the DataSeries + Location trailer (e.g. 'pInundation
    (Aransas)') when present. It goes in its own 'series' column.
    """
    match = patterns["data_series_location"].search(content)
    if match:
        return f"{match.group(1).strip()} ({match.group(2).strip()})"
    return ""


# Reasons whose upstream system is always the same one, regardless of
# model or family -- these never need to be parsed out of the log text,
# they're just true by definition of the failure type itself.
FIXED_SOURCE_BY_REASON = {
    "upstream_http_error": "NDBC",
    "coopsapi_error": "NOAA",
    "db_connection_error": "DATABASE",
}

# Model families whose failures pull from an internal system rather than
# an external data provider, when the log doesn't state a source
# explicitly. Currently just magnolia_transform -> SEMAPHORE (it
# consumes Semaphore's own prior predictions, not raw external data).
FAMILY_SOURCE_FALLBACK = {
    "magnolia_transform": "SEMAPHORE",
}


def resolve_source(reason, content, patterns, model_family):
    """Figure out what to show in the 'source' column, in priority
    order:
      1. A reason with an always-the-same upstream system (see
         FIXED_SOURCE_BY_REASON) -- these win outright, no need to look
         at the log text at all.
      2. interpolation_gap NEVER has a source in the raw log message --
         always blank, no family exceptions. (this should lowkey be fixed in logging)
      3. The log's own explicit 'source: X' field, when present (this is
         the case that already works correctly -- DateRangeValidation
         blocks that print a SeriesDescription).
      4. A family-level fallback for models that pull from an internal
         system instead of an external provider (see
         FAMILY_SOURCE_FALLBACK) -- currently just magnolia_transform.
      5. Otherwise, blank. We don't guess.
    """
    if reason in FIXED_SOURCE_BY_REASON:
        return FIXED_SOURCE_BY_REASON[reason]

    if reason == "interpolation_gap":
        return ""

    match = patterns["data_source"].search(content)
    if match:
        return match.group(1).strip()

    return FAMILY_SOURCE_FALLBACK.get(model_family, "")


def extract_missing_details(content, patterns):
    """Build a '|'-delimited details string: missing count, up to 3
    missing timestamps, the time-gap phrase, and (for HTTP errors) the
    status code and URL -- one flexible text column instead of many
    mostly-empty CSV columns. Used for every reason EXCEPT
    db_connection_error and coopsapi_error, which have their own
    extractors below (those blocks have a completely different shape --
    host/port or a library error message instead of missing-data fields).

    If none of the structured fields above are present -- some failure
    messages genuinely don't include a missing-count or a source line --
    this falls back to the raw block content rather than leaving the
    details column blank, so there's always something to read.
    """
    parts = []

    missing_count = patterns["missing_count"].search(content)
    if missing_count:
        parts.append(f"missing_count:{missing_count.group(1)}")

    missing_times = [
        line.split("Missing time:", 1)[1].strip()
        for line in content.splitlines()
        if "Missing time:" in line
    ][:3]
    if missing_times:
        parts.append("times:" + ";".join(missing_times))

    time_diff = patterns["time_diff"].search(content)
    if time_diff:
        parts.append(f"time_diff:{time_diff.group(1).strip()}")

    http_match = patterns["http_error"].search(content)
    if http_match:
        parts.append(f"status_code:{http_match.group(1)}|url:{http_match.group(2)}")

    if not parts:
        return content

    return "|".join(parts)


def extract_connection_failure_details(content, patterns):
    """Same idea as extract_missing_details, but for db_connection_error
    blocks specifically: pulls the unreachable host/port and flags a
    failed Discord alert (meaning nobody got paged). Falls back to the
    raw block content if neither field is found, same reasoning as
    extract_missing_details above.
    """
    parts = []
    target = patterns["db_conn_target"].search(content)
    if target:
        parts.append(f"host:{target.group(1)}|port:{target.group(2)}")
    if "discord notification" in content.lower():
        parts.append("discord_notification_failed:true")

    if not parts:
        return content

    return "|".join(parts)


def extract_coopsapi_details(content, patterns):
    """For coopsapi_error blocks: pulls the actual error message the
    noaa_coops library raised (e.g. "CO-OPS API returned an error: No
    data was found...") off the same line as the COOPSAPIError, and
    flags a failed Discord alert the same way the connection-failure
    extractor does. Falls back to the raw block content if the message
    couldn't be pulled out for some reason.
    """
    parts = []
    message = re.search(r"COOPSAPIError:\s*(.+)", content)
    if message:
        parts.append(f"message:{message.group(1).strip()}")
    if "discord notification" in content.lower():
        parts.append("discord_notification_failed:true")

    if not parts:
        return content

    return "|".join(parts)


def extract_failure_details(reason, content, patterns, model_family):
    """Now that we know WHICH failure type this block is (from
    classify_failure_reason) and which family it belongs to, pull out
    the (source, details, series) fields for it:
      - "unknown" keeps the whole raw block and a blank source instead
        of guessing at a shape it doesn't recognize, so a person can
        read it and decide whether it deserves a new entry in
        FAILURE_TYPE_PATTERNS.
      - "db_connection_error" uses the host/port extractor.
      - "coopsapi_error" uses its own extractor for the library's error
        message.
      - everything else uses the standard missing-data-style extractor.
    `source` always comes from resolve_source() (see its docstring for
    the priority order); `series` is pulled independently of reason,
    whenever the log happens to include a DataSeries/Location trailer.
    """
    series = extract_series(content, patterns)

    if reason == "unknown":
        return "", content, series
    if reason == "db_connection_error":
        return resolve_source(reason, content, patterns, model_family), extract_connection_failure_details(content, patterns), series
    if reason == "coopsapi_error":
        return resolve_source(reason, content, patterns, model_family), extract_coopsapi_details(content, patterns), series

    source = resolve_source(reason, content, patterns, model_family)
    details = extract_missing_details(content, patterns)
    return source, details, series


# =============================================================================
# SECTION 4: CLI args and date helpers
# =============================================================================

def parse_mmddyy(date_str):
    """Parse an 'MM/DD/YY' string into a date object, or raise the
    argparse-friendly error type so a bad date exits with a clean usage
    message instead of a raw traceback.
    """
    try:
        return datetime.strptime(date_str, "%m/%d/%y").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"'{date_str}' is not a valid MM/DD/YY date") from exc


def parse_args():
    """Parse and validate the CLI date range. Returns start_date/end_date
    as `date` objects; also checks start <= end here so main() doesn't
    have to.
    """
    parser = argparse.ArgumentParser(
        description="Parse Semaphore log files into a CSV + HTML report.",
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


def month_patterns(start, end):
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


# =============================================================================
# SECTION 5: Locating input files and the output folder
# =============================================================================

def find_log_files(log_dir, start, end):
    """Narrow down which log files could possibly contain rows in
    [start, end] without opening a single one yet, by globbing for
    filenames that embed the right year/month. Logs live one level down
    in per-model subfolders, so this searches recursively (rglob).
    """
    candidate_patterns = month_patterns(start, end)
    found = set()
    for pattern in candidate_patterns:
        for f in log_dir.rglob(f"{pattern}_*.log"):
            if f.is_file():
                found.add(f)
    return sorted(found)


def resolve_output_dir(semaphore_root):
    """Build (and create) this run's output folder. Everything the run
    produces -- CSV and report.html -- lands in one folder together,
    named with dev/prod (detected from hostname) and a timestamp so runs
    never collide.
    """
    tools_dir = semaphore_root / "tools" / "parsed_semaphore_logs"
    env_label = "prod" if "prod" in socket.gethostname().lower() else "dev"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = tools_dir / f"semaphore_{env_label}_report_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


# =============================================================================
# SECTION 6: Parsing log files into event dicts
# =============================================================================

def extract_model_name(line, patterns):
    """Pull the model name out of a line like 'Model magnolia_12 ...'."""
    match = patterns["model_name"].search(line)
    return match.group(1) if match else None


def infer_model_name_from_filename(log_file):
    """Best-effort fallback for connection-failure blocks that happen
    before any 'completed successfully' line has appeared in the file --
    there's no prior model name to fall back on in that case (see
    last_known_model in parse_log_file below), so this pulls it from the
    filename instead. Log filenames look like '2026_8_magnolia_12.log';
    stripping the leading 'YYYY_M_' and trailing '.log' leaves the model
    name. Returns None (not a guess) if the filename doesn't match that
    shape, so callers still fall back to "" rather than a wrong name.
    """
    match = re.match(r"^\d{4}_\d{1,2}_(.+)\.log$", log_file.name)
    return match.group(1) if match else None


def make_event(timestamp, model, status, reason, source, details, series, log_file, line_number):
    """Build one event dict with a consistent set of keys. Centralizing
    this in one function means every event -- success, failure, or
    connection-failure -- has exactly the same shape, which is what lets
    pd.DataFrame(events) turn the whole list into a clean table with no
    missing columns.
    """
    model_name = model or MISSING_MODEL
    return {
        "timestamp": timestamp,
        "model_name": model_name,
        # Every event gets a family looked up right here, so nothing
        # downstream (charts, tables) ever has to think about the
        # model->family mapping again.
        "model_family": family_for_model(model_name),
        "status": status,
        "failure_reason": reason,
        "data_source": source,
        # The data SERIES (e.g. "pInundation (Aransas)") is a separate
        # concept from data_source -- a series name, not an upstream
        # provider -- so it gets its own column rather than being
        # crammed into data_source.
        "series": series,
        "details": details,
        "log_file": log_file.name,
        "line_number": line_number,
    }


def parse_date_int(timestamp_str):
    """Parse a 'MM/DD/YY HH:MM:SS' timestamp into a YYMMDD int for range
    comparison. Returns None if the timestamp is blank or malformed --
    callers treat that as "can't verify this belongs in range, so don't
    write it" rather than crashing or guessing.
    """
    if not timestamp_str:
        return None
    date_part = timestamp_str.split(" ", 1)[0]
    try:
        mm, dd, yy = date_part.split("/")
        return int(f"{yy}{mm}{dd}")
    except ValueError:
        return None


def parse_log_file(log_file, start_int, end_int, patterns):
    """Stream one log file line by line and return a list of finished
    event dicts (no CSV writing happens in here -- that split is
    deliberate, see write_events_csv below).

    Two kinds of block need to stay open past their own apparent
    "closing" line, because useful trailing detail (or nothing at all)
    follows it:
      1. Failure blocks: open on a timestamped "DateRangeValidation:" /
         "Full stack trace:" line, or on a bare Python traceback with no
         Semaphore timestamp. The "FAILED - Null result inserted" line
         LOOKS like a close signal, but Semaphore prints a "Failed
         results:" trailer with the model's DataSeries/Location right
         AFTER it -- so we keep collecting past that line too, and only
         finalize once the NEXT recognized event shows up (or the file
         ends). That trailer is the only place some failure types (like
         interpolation_gap) carry any source info at all.

         A "bare traceback" open (no Semaphore timestamp) can also be
         triggered by things that are NOT model failures at all -- e.g.
         a Discord-notification-send error logged right after a model
         completed successfully. Those never produce a "FAILED - Null
         result inserted" line, so we only write a failure event for a
         block that actually reached that line (see
         pending_failure_reached_failed_line below); anything else is
         silently discarded.
      2. Connection-failure blocks: open on "Prediction failed due to
         ... Exception", but the process crashes before logging any
         closing line at all -- same implicit-close handling, just with
         nothing useful ever following it. Every path into this state
         happens through an already date-range-checked event, so it
         doesn't need the same re-validation failure blocks do.

    Both kinds are flushed the same way: check for a pending one at the
    top of every new recognized event, and once more at end-of-file.
    """
    events = []

    # --- state for a "failure block" (implicit close, see docstring) ---
    pending_failure = False
    pending_failure_content = ""
    pending_failure_start_line = 0
    pending_failure_timestamp = ""
    pending_failure_model = None
    # True only once we've actually seen "FAILED - Null result inserted"
    # for this block -- see the docstring above.
    pending_failure_reached_failed_line = False

    # --- state for a "connection failure block" (implicit close) ---
    connection_failure_pending = False
    connection_failure_content = ""
    connection_failure_start_line = 0
    connection_failure_timestamp = ""

    last_known_model = None
    current_timestamp = ""

    with log_file.open(errors="replace") as fh:
        for line_num, line in enumerate(fh, start=1):
            line = line.rstrip("\n")
            ts_match = patterns["timestamp_line"].match(line)

            # A timestamp alone does NOT mean "a new event started" --
            # Semaphore stamps every top-level log call, even ones that
            # are really a continuation of the same logical event.
            #
            # "FAILED - Null result inserted" is handled SEPARATELY below
            # (not lumped in with the other three markers) precisely
            # because it must NOT trigger a flush of the block it
            # belongs to -- it's a continuation marker (it tells us the
            # model name), not a boundary. Flushing still happens later,
            # once the block's real next event arrives.
            starts_new_block_or_success = ts_match and (
                "completed successfully" in line
                or patterns["failure_block_start"].search(line)
                or patterns["connection_failure_start"].search(line)
            )
            is_failed_line = ts_match and "FAILED - Null result inserted" in line

            if starts_new_block_or_success:
                # A new recognized event closes out any previously-open
                # pending block -- neither failure nor connection-failure
                # blocks have a reliable single closing line, so this is
                # where they actually get flushed.
                if pending_failure:
                    # Only write this as an event if the block actually
                    # reached "FAILED - Null result inserted" (otherwise
                    # it's something else entirely, like an orphaned
                    # Discord-notification traceback -- see docstring)
                    # AND its timestamp is verifiably inside the
                    # requested range. A block opened via
                    # raw_traceback_start never went through the normal
                    # date check at open time, so it's re-checked here.
                    if pending_failure_reached_failed_line:
                        date_int = parse_date_int(pending_failure_timestamp)
                        if date_int is not None and start_int <= date_int <= end_int:
                            reason = classify_failure_reason(pending_failure_content, FAILURE_TYPE_PATTERNS)
                            model_family = family_for_model(pending_failure_model) if pending_failure_model else UNMAPPED_FAMILY
                            source, details, series = extract_failure_details(
                                reason, pending_failure_content, patterns, model_family
                            )
                            events.append(make_event(
                                pending_failure_timestamp, pending_failure_model, "failed",
                                reason, source, details, series, log_file, pending_failure_start_line,
                            ))
                    pending_failure = False
                    pending_failure_content = ""
                    pending_failure_model = None
                    pending_failure_reached_failed_line = False

                if connection_failure_pending:
                    reason = classify_failure_reason(connection_failure_content, FAILURE_TYPE_PATTERNS)
                    # If no "completed successfully" line has been seen
                    # yet in this file, last_known_model is still None --
                    # fall back to the model name embedded in the log
                    # filename itself rather than leaving it blank.
                    model_for_event = last_known_model or infer_model_name_from_filename(log_file)
                    model_family = family_for_model(model_for_event) if model_for_event else UNMAPPED_FAMILY
                    source, details, series = extract_failure_details(
                        reason, connection_failure_content, patterns, model_family
                    )
                    events.append(make_event(
                        connection_failure_timestamp, model_for_event, "failed",
                        reason, source, details, series, log_file, connection_failure_start_line,
                    ))
                    connection_failure_pending = False
                    connection_failure_content = ""

                current_timestamp = ts_match.group(1)
                date_part = current_timestamp.split(" ", 1)[0]

                # Reconstruct as YYMMDD so it can be compared with plain
                # integer ordering against start_int/end_int.
                mm, dd, yy = date_part.split("/")
                date_int = int(f"{yy}{mm}{dd}")

                if date_int > end_int:
                    # Past the requested range. Any pending block was
                    # already flushed above, so there's nothing left to
                    # do with this line -- and since Semaphore logs are
                    # written chronologically within a file, every line
                    # after this one will also be past end_int, so stop
                    # reading the rest of the file instead of scanning to
                    # EOF. (This assumes strict chronological order; a
                    # log with an out-of-order line after this point --
                    # e.g. a delayed flush -- would have that line
                    # silently skipped rather than read.)
                    break

                if date_int < start_int:
                    # Before the requested range -- ignore this event
                    # entirely and stop tracking any block that might
                    # have been open (it belongs to a date we don't want
                    # anyway).
                    pending_failure = False
                    continue

                if "completed successfully" in line:
                    model = extract_model_name(line, patterns)
                    last_known_model = model or last_known_model
                    events.append(make_event(
                        current_timestamp, model, "success", "", "", "", "",
                        log_file, line_num,
                    ))

                elif patterns["failure_block_start"].search(line):
                    # Open a new failure block and start accumulating.
                    pending_failure = True
                    pending_failure_content = line
                    pending_failure_start_line = line_num
                    pending_failure_timestamp = current_timestamp
                    pending_failure_model = None
                    pending_failure_reached_failed_line = False

                elif patterns["connection_failure_start"].search(line):
                    # Open a new connection-failure block.
                    connection_failure_pending = True
                    connection_failure_content = line
                    connection_failure_start_line = line_num
                    connection_failure_timestamp = current_timestamp

            elif is_failed_line:
                # This LOOKS like a "close" line, but it isn't one -- we
                # keep the block open (see docstring) so the "Failed
                # results:" trailer that follows still gets captured.
                # Just record the model name off this line and keep
                # collecting; the block gets flushed later, whenever the
                # next real event (or EOF) arrives.
                if pending_failure:
                    pending_failure_reached_failed_line = True
                    if not pending_failure_timestamp:
                        # A bare traceback (raw_traceback_start) can open
                        # a block with no timestamp at all, if it's the
                        # very first thing in the file before any
                        # recognized event has set current_timestamp.
                        # This FAILED line always has one, though (it
                        # matched is_failed_line only because ts_match
                        # succeeded) -- use it so the event isn't written
                        # with a blank timestamp later.
                        pending_failure_timestamp = ts_match.group(1)
                    model = extract_model_name(line, patterns)
                    if model:
                        pending_failure_model = model
                    pending_failure_content += "\n" + line
                elif connection_failure_pending:
                    # Not expected in practice -- a connection-failure
                    # block ends in a crash, not a "FAILED - Null result
                    # inserted" line -- but if it ever does happen, treat
                    # it as a continuation line of the block that's
                    # actually open instead of silently dropping it. This
                    # file's philosophy is "never silently guess or drop,
                    # always show it as unknown," so an unexpected line
                    # still has to land somewhere.
                    connection_failure_content += "\n" + line

            elif pending_failure:
                # Continuation line -- could be inside the original
                # block, or in the "Failed results:" trailer after the
                # FAILED line. Either way, keep collecting until the
                # next recognized event flushes it.
                pending_failure_content += "\n" + line

            elif connection_failure_pending:
                # Continuation line inside the currently-open
                # connection-failure block.
                connection_failure_content += "\n" + line

            elif patterns["raw_traceback_start"].match(line):
                # A bare Python traceback with no Semaphore timestamp and
                # no other block open. We don't know the date from this
                # line, but that's fine -- the block only gets flushed
                # once we reach the next recognized event, and that
                # event's own date-range check still applies.
                pending_failure = True
                pending_failure_content = line
                pending_failure_start_line = line_num
                pending_failure_timestamp = current_timestamp
                pending_failure_model = None
                pending_failure_reached_failed_line = False

    # The file may end while a block is still open (no later event
    # arrived to trigger the flush above). Same gating as the mid-stream
    # flush: only write it if it actually reached "FAILED", and only if
    # its timestamp lands inside the requested range.
    if pending_failure and pending_failure_reached_failed_line:
        date_int = parse_date_int(pending_failure_timestamp)
        if date_int is not None and start_int <= date_int <= end_int:
            reason = classify_failure_reason(pending_failure_content, FAILURE_TYPE_PATTERNS)
            model_family = family_for_model(pending_failure_model) if pending_failure_model else UNMAPPED_FAMILY
            source, details, series = extract_failure_details(
                reason, pending_failure_content, patterns, model_family
            )
            events.append(make_event(
                pending_failure_timestamp, pending_failure_model, "failed",
                reason, source, details, series, log_file, pending_failure_start_line,
            ))
    if connection_failure_pending:
        reason = classify_failure_reason(connection_failure_content, FAILURE_TYPE_PATTERNS)
        model_for_event = last_known_model or infer_model_name_from_filename(log_file)
        model_family = family_for_model(model_for_event) if model_for_event else UNMAPPED_FAMILY
        source, details, series = extract_failure_details(
            reason, connection_failure_content, patterns, model_family
        )
        events.append(make_event(
            connection_failure_timestamp, model_for_event, "failed",
            reason, source, details, series, log_file, connection_failure_start_line,
        ))

    return events


def parse_all_logs(log_files, start_int, end_int, patterns):
    """Parse every candidate log file and return ONE DataFrame built from
    ONE big list of event dicts. We deliberately grow a plain Python list
    across the loop (cheap) and only build the DataFrame once at the end
    -- pd.DataFrame() is not meant to be rebuilt row by row in a loop.
    """
    all_events = []
    for i, log_file in enumerate(log_files, start=1):
        print(f"[{i}/{len(log_files)}] Processing: {log_file.name}")
        all_events.extend(parse_log_file(log_file, start_int, end_int, patterns))

    df = pd.DataFrame(all_events)

    if df.empty:
        # No events fell in the requested range. pd.DataFrame([]) has NO
        # columns at all (not even "timestamp"), so touching df["timestamp"]
        # below would raise a KeyError before main() ever gets a chance to
        # print its friendly "No events found" message. Bail out here with
        # the same empty-but-columnless DataFrame; callers already check
        # df.empty before doing anything else with it.
        return df

    # Convert the timestamp column from a raw "MM/DD/YY HH:MM:SS" string
    # into an actual pandas Timestamp -- everything downstream (sorting,
    # grouping by day, plotting on a time axis) depends on this being a
    # real datetime type, not text.
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%y %H:%M:%S")

    # A plain `date` (no time-of-day) column, used for every "group by
    # day" chart below -- keeps that grouping logic in one place instead
    # of re-deriving it in every chart function.
    df["date"] = df["timestamp"].dt.date

    return df


# =============================================================================
# SECTION 7: Small stats helpers (pure pandas, no state)
# =============================================================================

def compute_overall_stats(df):
    """Total / success / failed counts and success rate for the whole
    DataFrame. Kept as its own function so both the report and (if ever
    needed) a quick console printout can call the same thing.
    """
    total = len(df)
    success_count = int((df["status"] == "success").sum())
    failed_count = int((df["status"] == "failed").sum())
    success_rate = (success_count / total * 100) if total else 0.0
    return {
        "total": total,
        "success_count": success_count,
        "failed_count": failed_count,
        "success_rate": success_rate,
    }


def top_failure_reasons(df, n=5):
    """Top N failure reasons, overall, most common first."""
    failed = df[df["status"] == "failed"]
    return failed["failure_reason"].value_counts().head(n)


def top_failing_models(df, n=10):
    """Top N individual models by failure count, overall."""
    failed = df[df["status"] == "failed"]
    return failed["model_name"].value_counts().head(n)


def families_with_failures_worst_first(df):
    """Every family that has at least one failure in range, ordered
    worst (most failures) first -- this is the order the per-family
    report sections get written in.
    """
    failed = df[df["status"] == "failed"]
    return failed["model_family"].value_counts().index.tolist()


def build_notes_table(df, family, cap=NOTES_TABLE_CAP):
    """Build the per-family 'investigator notes' table: individual
    failure rows, capped at `cap`, sorted so the most COMMON failure
    reasons in this family come first (and within a reason, most recent
    first) -- this surfaces the failure modes costing the most, not just
    whatever happened most recently. Adds a blank Notes column for
    hand-written investigation notes.
    """
    family_failures = df[(df["model_family"] == family) & (df["status"] == "failed")].copy()

    # Count how common each reason is *within this family*, then use
    # that count as a sort key so the most-common reason's rows are
    # grouped together at the top.
    reason_counts = family_failures["failure_reason"].value_counts()
    family_failures["_reason_rank"] = family_failures["failure_reason"].map(reason_counts)

    family_failures = family_failures.sort_values(
        by=["_reason_rank", "timestamp"],
        ascending=[False, False],  # most-common reason first, then most-recent first
    )

    capped = family_failures.head(cap).drop(columns=["_reason_rank"])
    capped = capped[["timestamp", "model_name", "failure_reason", "data_source", "series", "details"]].copy()
    capped["Notes"] = ""  # blank column, filled in by hand later
    return capped, len(family_failures)  # also return the true total, for the "showing N of M" note


# =============================================================================
# SECTION 8: SVG chart-drawing functions
# =============================================================================
# No charting library involved -- an SVG is just XML text, so these
# functions build that text by hand with plain string formatting and
# return it as a string. build_report() below embeds that string
# directly into report.html with an f-string; there are no separate
# image files to manage.

# A small fixed color palette used across every chart, so a given
# failure reason or family gets a consistent color across the report
# (matched up by the order names appear in each chart's own data, since
# there's no shared legend across charts).
PALETTE = [
    "#4472A8", "#C1554B", "#4C9F70", "#D9A441", "#8B5FA8",
    "#4AA3A3", "#B85C8A", "#7A9B3F", "#C77B3E", "#5B6B8C",
]


def svg_bar_chart(labels, values, colors=None, title=""):
    """A simple vertical bar chart -- one bar per label. Used for the
    success-vs-failure chart.
    """
    width, height = 420, 300
    margin_left, margin_right, margin_top, margin_bottom = 50, 20, 40, 50
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom
    max_val = max(values) if values and max(values) > 0 else 1
    colors = colors or [PALETTE[i % len(PALETTE)] for i in range(len(values))]

    n = len(values)
    slot_w = plot_w / n
    bar_w = slot_w * 0.6

    bars = []
    for i, (label, val) in enumerate(zip(labels, values)):
        bar_h = (val / max_val) * plot_h
        x = margin_left + i * slot_w + (slot_w - bar_w) / 2
        y = margin_top + plot_h - bar_h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="{colors[i]}" />')
        bars.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 6:.1f}" font-size="12" text-anchor="middle" fill="#222">{val}</text>')
        bars.append(f'<text x="{x + bar_w / 2:.1f}" y="{margin_top + plot_h + 18:.1f}" font-size="12" text-anchor="middle" fill="#444">{label}</text>')

    axis = (
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#999" />'
        f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" stroke="#999" />'
    )
    title_el = f'<text x="{width / 2:.1f}" y="20" font-size="14" font-weight="bold" text-anchor="middle" fill="#111">{title}</text>' if title else ""

    return (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" font-family="sans-serif">'
        f'{title_el}{axis}{"".join(bars)}</svg>'
    )


def svg_y_axis_labels(max_val, margin_left, margin_top, plot_w, plot_h, count=4):
    """Build gridline + label elements for `count + 1` evenly-spaced
    y-axis ticks from 0 up to max_val, instead of just showing 0 and the
    top value. Shared by every chart type that has a numeric y-axis.
    """
    if max_val <= 0:
        max_val = 1
    elements = []
    for i in range(count + 1):
        v = round(max_val * i / count)
        y = margin_top + plot_h - (v / max_val) * plot_h
        elements.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{margin_left + plot_w}" y2="{y:.1f}" stroke="#eee" />')
        elements.append(f'<text x="{margin_left - 8:.1f}" y="{y + 4:.1f}" font-size="11" text-anchor="end" fill="#666">{v}</text>')
    return elements


def svg_multi_line_chart(x_labels, series, title=""):
    """A line chart with one line per entry in `series` (a dict of
    {name: [values aligned to x_labels]}). Used for
    failures-by-family-over-time and each single-family over-time chart
    (which just passes a `series` dict with one entry).
    """
    width, height = 640, 380
    margin_left, margin_right, margin_top, margin_bottom = 55, 20, 40, 70
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    all_values = [v for values in series.values() for v in values]
    max_val = max(all_values) if all_values and max(all_values) > 0 else 1

    n = len(x_labels)
    x_step = plot_w / (n - 1) if n > 1 else 0

    def x_pos(i):
        return margin_left + i * x_step if n > 1 else margin_left + plot_w / 2

    def y_pos(v):
        return margin_top + plot_h - (v / max_val) * plot_h

    elements = svg_y_axis_labels(max_val, margin_left, margin_top, plot_w, plot_h)
    elements.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#999" />')
    elements.append(f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" stroke="#999" />')

    # x-axis labels -- thin out if there are many, so they don't overlap
    label_every = max(1, n // 8)
    for i, label in enumerate(x_labels):
        if i % label_every == 0 or i == n - 1:
            lx, ly = x_pos(i), margin_top + plot_h + 15
            elements.append(
                f'<text x="{lx:.1f}" y="{ly:.1f}" font-size="10" text-anchor="middle" fill="#444" '
                f'transform="rotate(35 {lx:.1f} {ly:.1f})">{label}</text>'
            )

    legend_items = []
    for idx, (name, values) in enumerate(series.items()):
        color = PALETTE[idx % len(PALETTE)]
        points = " ".join(f"{x_pos(i):.1f},{y_pos(v):.1f}" for i, v in enumerate(values))
        elements.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2" />')
        for i, v in enumerate(values):
            elements.append(f'<circle cx="{x_pos(i):.1f}" cy="{y_pos(v):.1f}" r="3" fill="{color}" />')
        legend_items.append((name, color))

    # legend, wrapped across rows if there are many series
    legend_els = []
    per_row = 4
    for i, (name, color) in enumerate(legend_items):
        row, col = i // per_row, i % per_row
        lx = margin_left + col * (plot_w / per_row)
        ly = margin_top + plot_h + 35 + row * 16
        legend_els.append(
            f'<rect x="{lx:.1f}" y="{ly:.1f}" width="10" height="10" fill="{color}" />'
            f'<text x="{lx + 14:.1f}" y="{ly + 9:.1f}" font-size="11" fill="#333">{name}</text>'
        )
    legend_rows = (len(legend_items) + per_row - 1) // per_row
    total_height = height + legend_rows * 16

    title_el = f'<text x="{width / 2:.1f}" y="20" font-size="14" font-weight="bold" text-anchor="middle" fill="#111">{title}</text>' if title else ""

    return (
        f'<svg viewBox="0 0 {width} {total_height}" xmlns="http://www.w3.org/2000/svg" font-family="sans-serif">'
        f'{title_el}{"".join(elements)}{"".join(legend_els)}</svg>'
    )


def svg_stacked_bar_chart(categories, series, title=""):
    """Stacked bar chart -- one bar per category, segments from `series`
    (a dict of {segment_name: [values aligned to categories]}). Used for
    the failure-reasons-by-family chart.
    """
    width, height = 640, 380
    margin_left, margin_right, margin_top, margin_bottom = 55, 20, 40, 90
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    totals = [sum(series[name][i] for name in series) for i in range(len(categories))]
    max_total = max(totals) if totals and max(totals) > 0 else 1

    n = len(categories)
    slot_w = plot_w / n
    bar_w = slot_w * 0.6

    elements = svg_y_axis_labels(max_total, margin_left, margin_top, plot_w, plot_h)
    elements.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#999" />')
    elements.append(f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" stroke="#999" />')

    for i, category in enumerate(categories):
        x = margin_left + i * slot_w + (slot_w - bar_w) / 2
        y_cursor = margin_top + plot_h
        for idx, name in enumerate(series):
            val = series[name][i]
            if val <= 0:
                continue
            seg_h = (val / max_total) * plot_h
            y_cursor -= seg_h
            color = PALETTE[idx % len(PALETTE)]
            elements.append(f'<rect x="{x:.1f}" y="{y_cursor:.1f}" width="{bar_w:.1f}" height="{seg_h:.1f}" fill="{color}" />')
        lx, ly = x + bar_w / 2, margin_top + plot_h + 15
        elements.append(
            f'<text x="{lx:.1f}" y="{ly:.1f}" font-size="10" text-anchor="middle" fill="#444" '
            f'transform="rotate(35 {lx:.1f} {ly:.1f})">{category}</text>'
        )

    legend_els = []
    per_row = 4
    for idx, name in enumerate(series):
        color = PALETTE[idx % len(PALETTE)]
        row, col = idx // per_row, idx % per_row
        lx = margin_left + col * (plot_w / per_row)
        ly = margin_top + plot_h + 55 + row * 16
        legend_els.append(
            f'<rect x="{lx:.1f}" y="{ly:.1f}" width="10" height="10" fill="{color}" />'
            f'<text x="{lx + 14:.1f}" y="{ly + 9:.1f}" font-size="11" fill="#333">{name}</text>'
        )

    title_el = f'<text x="{width / 2:.1f}" y="20" font-size="14" font-weight="bold" text-anchor="middle" fill="#111">{title}</text>' if title else ""

    return (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" font-family="sans-serif">'
        f'{title_el}{"".join(elements)}{"".join(legend_els)}</svg>'
    )


def svg_grouped_bar_chart(x_labels, series, title="", colors=None):
    """Grouped bar chart -- one slot per x_label (e.g. a date), with one
    bar per entry in `series` (a dict of {name: [values aligned to
    x_labels]}) side by side within that slot. Used for the
    successes-vs-failures-per-day chart, where a line chart wasn't a
    great fit since each day is really two independent counts, not a
    continuous trend.
    """
    width, height = 640, 380
    margin_left, margin_right, margin_top, margin_bottom = 55, 20, 40, 90
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    all_values = [v for values in series.values() for v in values]
    max_val = max(all_values) if all_values and max(all_values) > 0 else 1

    n = len(x_labels)
    k = len(series)
    slot_w = plot_w / n
    group_w = slot_w * 0.7
    bar_w = group_w / k if k else group_w

    elements = svg_y_axis_labels(max_val, margin_left, margin_top, plot_w, plot_h)
    elements.append(f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_h}" stroke="#999" />')
    elements.append(f'<line x1="{margin_left}" y1="{margin_top + plot_h}" x2="{margin_left + plot_w}" y2="{margin_top + plot_h}" stroke="#999" />')

    colors = colors or [PALETTE[i % len(PALETTE)] for i in range(k)]

    # thin out x-axis labels if there are many days, same idea as the line chart
    label_every = max(1, n // 12)
    for i, label in enumerate(x_labels):
        slot_x = margin_left + i * slot_w + (slot_w - group_w) / 2
        for j, (name, values) in enumerate(series.items()):
            val = values[i]
            bar_h = (val / max_val) * plot_h
            x = slot_x + j * bar_w
            y = margin_top + plot_h - bar_h
            elements.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w * 0.9:.1f}" height="{bar_h:.1f}" fill="{colors[j]}" />')
        if i % label_every == 0 or i == n - 1:
            lx, ly = slot_x + group_w / 2, margin_top + plot_h + 15
            elements.append(
                f'<text x="{lx:.1f}" y="{ly:.1f}" font-size="10" text-anchor="middle" fill="#444" '
                f'transform="rotate(35 {lx:.1f} {ly:.1f})">{label}</text>'
            )

    legend_els = []
    per_row = 4
    for idx, name in enumerate(series):
        row, col = idx // per_row, idx % per_row
        lx = margin_left + col * (plot_w / per_row)
        ly = margin_top + plot_h + 55 + row * 16
        legend_els.append(
            f'<rect x="{lx:.1f}" y="{ly:.1f}" width="10" height="10" fill="{colors[idx]}" />'
            f'<text x="{lx + 14:.1f}" y="{ly + 9:.1f}" font-size="11" fill="#333">{name}</text>'
        )

    title_el = f'<text x="{width / 2:.1f}" y="20" font-size="14" font-weight="bold" text-anchor="middle" fill="#111">{title}</text>' if title else ""

    return (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" font-family="sans-serif">'
        f'{title_el}{"".join(elements)}{"".join(legend_els)}</svg>'
    )


def chart_success_vs_failure(stats):
    return svg_bar_chart(
        ["Success", "Failed"], [stats["success_count"], stats["failed_count"]],
        colors=["#4C9F70", "#C1554B"], title="Success vs. Failure Count",
    )


def chart_runs_over_time(df):
    """Bar chart (success vs. failed side by side, per day) instead of a
    line chart -- each day is really two independent counts, not a
    continuous trend, so bars read more directly than lines here.
    """
    daily = df.groupby(["date", "status"]).size().unstack(fill_value=0)
    x_labels = [d.strftime("%m/%d") for d in daily.index]
    series = {
        "Success": daily.get("success", pd.Series(0, index=daily.index)).tolist(),
        "Failed": daily.get("failed", pd.Series(0, index=daily.index)).tolist(),
    }
    return svg_grouped_bar_chart(x_labels, series, title="Runs Over Time (Success vs. Failed by Day)",
                                  colors=["#4C9F70", "#C1554B"])


def full_date_index(df):
    """Every calendar date from the overall dataset's min to max date,
    inclusive -- used to reindex "over time" charts so days with zero
    events still show up on the x-axis instead of being skipped, which
    would otherwise make the chart look like a continuous trend across
    what were actually long silent gaps.
    """
    return pd.date_range(df["date"].min(), df["date"].max(), freq="D").date


def chart_failures_by_family_over_time(df, top_n=OVERALL_CHART_FAMILY_LIMIT):
    """One line per family, capped to the top_n worst families so the
    legend stays readable even with many families defined.
    """
    failed = df[df["status"] == "failed"]
    top_families = failed["model_family"].value_counts().head(top_n).index
    daily_by_family = failed.groupby(["date", "model_family"]).size().unstack(fill_value=0)
    daily_by_family = daily_by_family.reindex(columns=top_families, fill_value=0)
    daily_by_family = daily_by_family.reindex(index=full_date_index(df), fill_value=0)
    x_labels = [d.strftime("%m/%d") for d in daily_by_family.index]
    series = {fam: daily_by_family[fam].tolist() for fam in daily_by_family.columns}
    return svg_multi_line_chart(x_labels, series, title=f"Failures by Model Family Over Time (Top {top_n} Families)")


def chart_failure_reasons_by_family(df, top_n=OVERALL_CHART_FAMILY_LIMIT):
    """Stacked bar: for the top_n worst families, how many failures of
    each reason they had.
    """
    failed = df[df["status"] == "failed"]
    top_families = list(failed["model_family"].value_counts().head(top_n).index)
    by_reason = failed.groupby(["model_family", "failure_reason"]).size().unstack(fill_value=0)
    by_reason = by_reason.reindex(index=top_families, fill_value=0)
    series = {reason: by_reason[reason].tolist() for reason in by_reason.columns}
    return svg_stacked_bar_chart(top_families, series, title=f"Failure Reasons by Family (Top {top_n} Families)")


def chart_family_over_time(df, family):
    """A single family's own failures-over-time line -- used inside that
    family's report section.
    """
    family_failed = df[(df["model_family"] == family) & (df["status"] == "failed")]
    daily = family_failed.groupby("date").size().reindex(full_date_index(df), fill_value=0)
    x_labels = [d.strftime("%m/%d") for d in daily.index]
    return svg_multi_line_chart(x_labels, {family: daily.tolist()}, title=f"{family}: Failures Over Time")


# =============================================================================
# SECTION 9: HTML table + section rendering
# =============================================================================

def series_to_html_table(series, col_names):
    """Turn a pandas Series (e.g. value_counts() output) into a small
    HTML table."""
    rows = "".join(
        f"<tr><td>{html.escape(str(index))}</td><td>{html.escape(str(value))}</td></tr>"
        for index, value in series.items()
    )
    return (
        f"<table><thead><tr><th>{html.escape(col_names[0])}</th><th>{html.escape(col_names[1])}</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def dataframe_to_html_table(df):
    """Same idea as series_to_html_table, but for a full DataFrame --
    used for the top-failing-models table and the investigator-notes
    tables.
    """
    header = "".join(f"<th>{html.escape(str(col))}</th>" for col in df.columns)
    rows = ""
    for _, row in df.iterrows():
        cells = "".join(
            f"<td>{'' if pd.isna(v) else html.escape(str(v))}</td>" for v in row
        )
        rows += f"<tr>{cells}</tr>"
    return f"<table><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>"


def render_overall_section(stats, charts, top_reasons, top_models):
    """Build the HTML for the report's overall (all-families) section:
    summary numbers, the four whole-dataset charts, and the two overall
    top-N tables.
    """
    html_str = "<h2>Summary</h2>"
    html_str += (
        "<table><tbody>"
        f"<tr><td>Total runs</td><td>{stats['total']}</td></tr>"
        f"<tr><td>Successful</td><td>{stats['success_count']}</td></tr>"
        f"<tr><td>Failed</td><td>{stats['failed_count']}</td></tr>"
        f"<tr><td>Success rate</td><td>{stats['success_rate']:.2f}%</td></tr>"
        "</tbody></table>"
    )
    html_str += f'<div class="chart">{charts["success_vs_failure"]}</div>'

    html_str += "<h2>Runs Over Time</h2>"
    html_str += f'<div class="chart">{charts["runs_over_time"]}</div>'

    html_str += "<h2>Failures by Model Family Over Time</h2>"
    html_str += (
        f"<p class='note'>Top {OVERALL_CHART_FAMILY_LIMIT} families by total failure count. "
        "Family grouping comes from the hardcoded <code>MODEL_FAMILIES</code> list at the "
        "top of this script.</p>"
    )
    html_str += f'<div class="chart">{charts["failures_by_family_over_time"]}</div>'

    html_str += "<h2>Failure Reasons by Family</h2>"
    html_str += f'<div class="chart">{charts["failure_reasons_by_family"]}</div>'

    html_str += "<h2>Top Failure Reasons (Overall)</h2>"
    html_str += series_to_html_table(top_reasons, ("Reason", "Count"))

    html_str += "<h2>Models with Most Failures (Overall)</h2>"
    html_str += dataframe_to_html_table(top_models)

    return html_str


def render_family_section(df, family):
    """Build the HTML for ONE family's section: a subheading, a mini
    summary specific to that family, that family's own over-time chart,
    a failure-reason breakdown table, and the capped investigator-notes
    table with a blank Notes column.
    """
    family_df = df[df["model_family"] == family]
    family_stats = compute_overall_stats(family_df)

    chart_svg = chart_family_over_time(df, family)

    reason_counts = family_df[family_df["status"] == "failed"]["failure_reason"].value_counts()
    notes_table, true_total = build_notes_table(df, family)

    html_str = f"<h2>Family: {html.escape(str(family))}</h2>"
    html_str += (
        "<p class='family-stats'>"
        f"Runs: {family_stats['total']} &nbsp;|&nbsp; "
        f"Failures: {family_stats['failed_count']} &nbsp;|&nbsp; "
        f"Success rate: {family_stats['success_rate']:.2f}%"
        "</p>"
    )
    html_str += f'<div class="chart">{chart_svg}</div>'

    html_str += "<h3>Failure reasons in this family</h3>"
    html_str += series_to_html_table(reason_counts, ("Reason", "Count"))

    html_str += "<h3>Investigator notes</h3>"
    if true_total > NOTES_TABLE_CAP:
        html_str += (
            f"<p class='note'>Showing top {NOTES_TABLE_CAP} of {true_total} failures, "
            "grouped by most-common reason then most recent. "
            "See the CSV for the full list.</p>"
        )
    html_str += dataframe_to_html_table(notes_table)

    return html_str


# =============================================================================
# SECTION 10: Top-level orchestration
# =============================================================================

# Minimal CSS, inlined into the report so it's one self-contained file
# with no external stylesheet to lose track of.
REPORT_CSS = """
body { font-family: sans-serif; max-width: 900px; margin: 30px auto; padding: 0 20px; color: #222; }
h1 { border-bottom: 2px solid #333; padding-bottom: 8px; }
h2 { margin-top: 40px; border-bottom: 1px solid #ccc; padding-bottom: 4px; }
h3 { margin-top: 24px; }
table { border-collapse: collapse; margin: 10px 0 20px 0; width: 100%; }
th, td { border: 1px solid #ddd; padding: 6px 10px; text-align: left; font-size: 14px; }
th { background: #f4f4f4; }
.chart { margin: 10px 0 30px 0; }
.chart svg { max-width: 100%; height: auto; }
.note { color: #666; font-size: 13px; font-style: italic; }
.family-stats { font-size: 14px; }
"""


def write_events_csv(df, output_dir):
    """Save the full event DataFrame to CSV. This is the same data the
    report is built from -- the CSV is the "everything" export, the
    report is the "here's what to look at" summary."""
    csv_path = output_dir / "model_run_events.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def build_report(df, output_dir):
    """Build every chart, assemble the full HTML string (overall
    section, then one section per family with failures, worst-first),
    and write report.html into output_dir. Everything -- charts, tables,
    styling -- lives in this one file; there are no separate PNGs or
    stylesheets to keep track of.
    """
    stats = compute_overall_stats(df)

    charts = {
        "success_vs_failure": chart_success_vs_failure(stats),
        "runs_over_time": chart_runs_over_time(df),
        "failures_by_family_over_time": chart_failures_by_family_over_time(df),
        "failure_reasons_by_family": chart_failure_reasons_by_family(df),
    }

    top_reasons = top_failure_reasons(df)
    top_models_series = top_failing_models(df)
    top_models_df = top_models_series.rename("Failure Count").reset_index()
    top_models_df.columns = ["Model", "Failure Count"]
    top_models_df["Family"] = top_models_df["Model"].map(family_for_model)
    top_models_df = top_models_df[["Model", "Family", "Failure Count"]]

    body = f"<h1>Semaphore Model Run Report</h1>"
    body += (
        f"<p><strong>Date range:</strong> {df['date'].min():%m/%d/%y} to {df['date'].max():%m/%d/%y}<br>"
        f"<strong>Generated:</strong> {datetime.now():%m/%d/%y %H:%M:%S}<br>"
        f"<strong>Source CSV:</strong> <code>model_run_events.csv</code></p>"
    )

    body += render_overall_section(stats, charts, top_reasons, top_models_df)

    # One section per family that had at least one failure, worst-first.
    for family in families_with_failures_worst_first(df):
        body += render_family_section(df, family)

    html_doc = f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>Semaphore Model Run Report</title><style>{REPORT_CSS}</style></head><body>{body}</body></html>"

    report_path = output_dir / "report.html"
    report_path.write_text(html_doc)
    return report_path


def main():
    # 1. Parse & validate the CLI date range.
    args = parse_args()
    start_int = int(args.start_date.strftime("%y%m%d"))
    end_int = int(args.end_date.strftime("%y%m%d"))

    # Compile every regex once, up front, and thread it through
    # explicitly from here on -- nothing above this line is a global.
    patterns = build_patterns()

    # Path setup mirrors Semaphore's own layout: logs live in
    # <semaphore_root>/data/logs, output goes in
    # <semaphore_root>/tools/parsed_semaphore_logs/<run folder>.
    script_dir = Path(__file__).resolve().parent
    semaphore_root = script_dir.parent
    log_dir = semaphore_root / "data" / "logs"
    output_dir = resolve_output_dir(semaphore_root)

    print("Semaphore Model Run Stats - Starting...")
    print(f"Date range: {args.start_date:%m/%d/%y} to {args.end_date:%m/%d/%y}")
    print(f"Log directory: {log_dir}")
    print(f"Output directory: {output_dir}\n")

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

    # 3. Parse every file into ONE DataFrame.
    df = parse_all_logs(log_files, start_int, end_int, patterns)
    if df.empty:
        print("No events found in the given date range.")
        return 1

    # 4. Save the raw event table.
    csv_path = write_events_csv(df, output_dir)
    print(f"\nSaved event CSV: {csv_path}")

    # 5. Build the self-contained HTML report (charts + tables inline).
    report_path = build_report(df, output_dir)
    print(f"Saved report: {report_path}")

    print(f"\nDone! See: {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())