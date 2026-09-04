#!/bin/bash
# run_weekly_model_report.sh
# -------------------------------
# Wrapper for model_run_stats.py, meant to be called from cron every
# Friday morning. Cron doesn't handle the "%" characters in date format
# strings well (it treats a bare % as a newline unless escaped), so this
# computes the dates in plain bash instead of trying to do it inline in
# the crontab entry.
#
# Reports on the last 7 days, ending today.
# -------------------------------

# Resolve the directory this script lives in, so it works no matter what
# working directory cron happens to invoke it from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# "MM/DD/YY" to match what model_run_stats.py expects on the command line.
START_DATE=$(date -d '7 days ago' +%m/%d/%y)
END_DATE=$(date +%m/%d/%y)

cd "$SCRIPT_DIR" || exit 1
python3 model_run_stats.py "$START_DATE" "$END_DATE"