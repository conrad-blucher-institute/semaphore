#!/bin/bash

# Semaphore Log Parser
# Usage: ./parse_semaphore_logs.sh <start_date> <end_date>
# Dates in MM/DD/YY format, e.g.: ./parse_semaphore_logs.sh 02/07/26 02/18/26

# --- Validate CLI arguments ---
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <start_date> <end_date>"
    echo "  Dates must be in MM/DD/YY format"
    echo "  Example: $0 02/07/26 02/18/26"
    exit 1
fi

START_DATE="$1"
END_DATE="$2"

# --- Path setup (relative to script location) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEMAPHORE_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${SEMAPHORE_ROOT}/data/logs"
OUTPUT_DIR="${SEMAPHORE_ROOT}/tools/parsed_semaphore_logs"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

OUTPUT_CSV="${OUTPUT_DIR}/semaphore_dev_stats_$(date +%Y%m%d_%H%M%S).csv"

# --- Date utilities ---

# Convert MM/DD/YY to YYMMDD integer for comparison
date_to_int() {
    local date_str="$1"
    local month=$(echo "$date_str" | cut -d'/' -f1)
    local day=$(echo "$date_str" | cut -d'/' -f2)
    local year=$(echo "$date_str" | cut -d'/' -f3)
    echo "${year}${month}${day}"
}

START_INT=$(date_to_int "$START_DATE")
END_INT=$(date_to_int "$END_DATE")

# Validate that start <= end
if [ "$START_INT" -gt "$END_INT" ]; then
    echo "ERROR: Start date ($START_DATE) must be before or equal to end date ($END_DATE)"
    exit 1
fi

is_date_valid() {
    local timestamp="$1"
    local date_part=$(echo "$timestamp" | cut -d' ' -f1)
    local date_int=$(date_to_int "$date_part")

    if [ "$date_int" -ge "$START_INT" ] && [ "$date_int" -le "$END_INT" ]; then
        return 0  # valid
    else
        return 1  # out of range
    fi
}

# --- Generate year/month patterns within date range ---

generate_month_patterns() {
    local start_month=$(echo "$START_DATE" | cut -d'/' -f1 | sed 's/^0//')
    local start_year="20$(echo "$START_DATE" | cut -d'/' -f3)"
    local end_month=$(echo "$END_DATE" | cut -d'/' -f1 | sed 's/^0//')
    local end_year="20$(echo "$END_DATE" | cut -d'/' -f3)"

    local year=$start_year
    local month=$start_month

    while [ "$year" -lt "$end_year" ] || { [ "$year" -eq "$end_year" ] && [ "$month" -le "$end_month" ]; }; do
        echo "${year}_${month}"
        ((month++))
        if [ "$month" -gt 12 ]; then
            month=1
            ((year++))
        fi
    done
}

build_log_file_list() {
    local patterns
    patterns=$(generate_month_patterns)

    local find_args=()
    local first=true

    while IFS= read -r pattern; do
        if [ "$first" = true ]; then
            find_args+=( -name "${pattern}_*.log" )
            first=false
        else
            find_args+=( -o -name "${pattern}_*.log" )
        fi
    done <<< "$patterns"

    find "$LOG_DIR" -type f \( "${find_args[@]}" \) 2>/dev/null | sort
}

# --- Field extraction helpers ---

extract_model_name() {
    local line="$1"
    echo "$line" | grep -oP 'Model \K[^\s]+' | head -1
}

extract_failure_reason() {
    local content="$1"
    if echo "$content" | grep -q "is missing.*values"; then
        echo "missing_data"
    elif echo "$content" | grep -q "is stale"; then
        echo "stale_data"
    else
        echo "unknown"
    fi
}

extract_data_source() {
    local content="$1"
    echo "$content" | grep -oP 'source: \K[^,]+' | head -1
}

extract_missing_details() {
    local content="$1"
    local details=""

    local missing_count=$(echo "$content" | grep -oP 'is missing \K\d+')
    if [ -n "$missing_count" ]; then
        details="missing_count:${missing_count}"
    fi

    local missing_times=$(echo "$content" | grep "Missing time:" | head -3 | sed 's/.*Missing time: //' | tr '\n' ';' | sed 's/;$//')
    if [ -n "$missing_times" ]; then
        if [ -n "$details" ]; then
            details="${details}|times:${missing_times}"
        else
            details="times:${missing_times}"
        fi
    fi

    local time_diff=$(echo "$content" | grep -oP 'Time difference: \K[^.]+')
    if [ -n "$time_diff" ]; then
        details="${details}|time_diff:${time_diff}"
    fi

    echo "$details"
}

# --- Log file processor ---

process_log_file() {
    local log_file="$1"
    local filename=$(basename "$log_file")

    local line_num=0
    local current_timestamp=""
    local current_model=""
    local in_failure_block=false
    local failure_content=""
    local failure_start_line=0

    while IFS= read -r line; do
        ((line_num++))

        if [[ $line =~ ^([0-9]{2}/[0-9]{2}/[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2}): ]]; then
            current_timestamp="${BASH_REMATCH[1]}"

            if ! is_date_valid "$current_timestamp"; then
                in_failure_block=false
                continue
            fi

            if echo "$line" | grep -q "completed successfully"; then
                current_model=$(extract_model_name "$line")
                if [ -n "$current_model" ]; then
                    echo "${current_timestamp},${current_model},success,,,,$filename,$line_num" >> "$OUTPUT_CSV"
                fi
                in_failure_block=false

            elif echo "$line" | grep -q "DateRangeValidation:"; then
                in_failure_block=true
                failure_content="$line"
                failure_start_line=$line_num
                current_model=""

            elif echo "$line" | grep -q "FAILED - Null result inserted"; then
                current_model=$(extract_model_name "$line")

                if [ "$in_failure_block" = true ] && [ -n "$failure_content" ]; then
                    local failure_reason=$(extract_failure_reason "$failure_content")
                    local data_source=$(extract_data_source "$failure_content")
                    local missing_details=$(extract_missing_details "$failure_content")

                    missing_details=$(echo "$missing_details" | sed 's/,/;/g')

                    echo "${current_timestamp},${current_model},failed,${failure_reason},${data_source},\"${missing_details}\",$filename,$failure_start_line" >> "$OUTPUT_CSV"
                fi

                in_failure_block=false
                failure_content=""
            fi
        fi

        if [ "$in_failure_block" = true ]; then
            failure_content="${failure_content}"$'\n'"${line}"
        fi

    done < "$log_file"
}

# --- Main ---

echo "Semaphore Log Parser - Starting..."
echo "Date range: $START_DATE to $END_DATE"
echo "Log directory: $LOG_DIR"
echo "Output directory: $OUTPUT_DIR"
echo "Output file: $(basename "$OUTPUT_CSV")"
echo ""

# Write CSV header
echo "timestamp,model_name,status,failure_reason,data_source,missing_data_details,log_file,line_number" > "$OUTPUT_CSV"

# Find only log files matching the year/month patterns in our date range
log_files=$(build_log_file_list)

if [ -z "$log_files" ]; then
    echo "ERROR: No matching log files found in $LOG_DIR"
    echo "Expected filenames like: $(generate_month_patterns | head -3 | sed 's/$/_<model>.log/' | tr '\n' ' ')"
    exit 1
fi

total_files=$(echo "$log_files" | wc -l)
current_file=0

echo "Found $total_files log file(s) to process"
echo ""

while IFS= read -r log_file; do
    ((current_file++))
    echo "[$current_file/$total_files] Processing: $(basename "$log_file")"
    process_log_file "$log_file"
done <<< "$log_files"

echo ""
echo "Processing complete!"
echo "Output saved to: $OUTPUT_CSV"
echo ""

# Summary statistics
echo "=== Summary Statistics ==="
total_lines=$(tail -n +2 "$OUTPUT_CSV" | wc -l)
success_count=$(tail -n +2 "$OUTPUT_CSV" | grep -c ",success,")
failed_count=$(tail -n +2 "$OUTPUT_CSV" | grep -c ",failed,")

echo "Total entries: $total_lines"
echo "Successful: $success_count"
echo "Failed: $failed_count"

if [ $total_lines -gt 0 ]; then
    success_rate=$(awk "BEGIN {printf \"%.2f\", ($success_count/$total_lines)*100}")
    echo "Success rate: ${success_rate}%"
fi

echo ""
echo "=== Top Failure Reasons ==="
tail -n +2 "$OUTPUT_CSV" | grep ",failed," | cut -d',' -f4 | sort | uniq -c | sort -rn | head -5

echo ""
echo "=== Models with Most Failures ==="
tail -n +2 "$OUTPUT_CSV" | grep ",failed," | cut -d',' -f2 | sort | uniq -c | sort -rn | head -10

echo ""
echo "Done! Output: $OUTPUT_CSV"