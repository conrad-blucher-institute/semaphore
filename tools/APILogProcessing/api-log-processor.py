from __future__ import annotations
import re
import json
import argparse
from typing import Optional
import pandas as pd

#!/usr/bin/env python3
"""
This script processes API log files retrieved via docker logs commands and generates a report 
with various statistics about the API usage. It extracts relevant information from log lines, 
parses them into structured data, and provides insights such as request counts, processing times, 
and endpoint statistics.

Arguments:
- logfile (str): Path to the log file to process. Defaults to 'api.log'.
- --raw_out, -ro (str): Optional. Path to write the raw JSON output of the processed log data. (Better if you want to do further processing)
- --out, -o (str): Optional. Path to write the generated report. If not provided, the report is printed to stdout.
import re
"""

LINE_RE = re.compile(
    r'^\[INFO\]\s+Access Time:\s+(?P<access_time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+),\s+'
    r'Process Time:\s+(?P<process_time>[\d.]+)s,\s+'
    r'uvicorn\.access:\s+(?P<client>[^ ]+)\s+-\s+'
    r'"(?P<method>\S+)\s+(?P<path>[^"]+)\s+HTTP/\d\.\d"\s+'
    r'(?P<status>\d+)\b'
)

def parse_line(line: str) -> Optional[dict]:
    m = LINE_RE.match(line)
    if not m:
        return None
    d = m.groupdict()
    # type conversions
    try:
        d['process_time'] = float(d['process_time'])
    except Exception:
        d['process_time'] = None
    try:
        d['status'] = int(d['status'])
    except Exception:
        d['status'] = None
    return d

def extract_from_file(path: str) -> list[dict]:
    out = []
    with open(path, 'r', encoding='utf-8', errors='replace') as fh:
        for line in fh:
            parsed = parse_line(line.rstrip('\n'))
            if parsed:
                out.append(parsed)
    return out

def create_report(df: pd.DataFrame) -> str:
    report = []

    start_time = pd.to_datetime(df['access_time'], format='%Y-%m-%d %H:%M:%S,%f').min()
    end_time = pd.to_datetime(df['access_time'], format='%Y-%m-%d %H:%M:%S,%f').max()
    time_covered = end_time - start_time

    report.append(f'Start Time: {start_time}')
    report.append(f'End Time: {end_time}')
    report.append(f'Time Covered: {time_covered}')

    total_requests = len(df)
    avg_process_time = df['process_time'].mean()
    max_process_time = df['process_time'].max()
    min_process_time = df['process_time'].min()

    report.append(f'Total Requests: {total_requests}')
    report.append(f'Average Process Time: {avg_process_time:.4f} seconds')
    report.append(f'Max Process Time: {max_process_time:.4f} seconds')
    report.append(f'Min Process Time: {min_process_time:.4f} seconds')

    status_counts = df['status'].value_counts().sort_index()
    report.append('\nStatus Code Counts:')
    for status, count in status_counts.items():
        report.append(f'  {status}: {count}')


    df['access_minute'] = pd.to_datetime(df['access_time'], format='%Y-%m-%d %H:%M:%S,%f').dt.floor('T')

    max_access_times = df['access_minute'].value_counts().max()
    min_access_times = df['access_minute'].value_counts().min()
    mean_access_times = df['access_minute'].value_counts().mean()

    report.append(f'\nMax Access Times in a Minute: {max_access_times}')
    report.append(f'Min Access Times in a Minute: {min_access_times}')
    report.append(f'Mean Access Times in a Minute: {mean_access_times:.4f}')

    endpoint_counts = df['endpoint'].value_counts().sort_values(ascending=False)
    report.append('\nRequest Counts by Endpoint:')
    for endpoint, count in endpoint_counts.items():
        report.append(f'{count} requests |  {endpoint}')

    endpoint_averages = df.groupby('endpoint')['process_time'].mean().sort_values(ascending=False)
    report.append('\nAverage Process Time by Endpoint:')
    for endpoint, avg_time in endpoint_averages.items():
        report.append(f'{avg_time:.4f} seconds |  {endpoint}')

    endpoint_max = df.groupby('endpoint')['process_time'].max().sort_values(ascending=False)
    report.append('\nMax Process Time by Endpoint:')
    for endpoint, max_time in endpoint_max.items():
        report.append(f'{max_time:.4f} seconds |  {endpoint}')

    endpoint_min = df.groupby('endpoint')['process_time'].min().sort_values(ascending=False)
    report.append('\nMin Process Time by Endpoint:')
    for endpoint, min_time in endpoint_min.items():
        report.append(f'{min_time:.4f} seconds |  {endpoint}')

    path_averages = df.groupby('path')['process_time'].mean().sort_values(ascending=False)
    path_counts = df['path'].value_counts()

    report.append('\nAverage Process Time by Full Path (with Counts):')
    for path, avg_time in path_averages.items():
        count = path_counts.get(path, 0)
        report.append(f'{avg_time:.4f} seconds | {count} requests | {path}')

    return '\n'.join(report)

def main():
    p = argparse.ArgumentParser(description='Extract uvicorn access lines from a log file.')
    p.add_argument('logfile', nargs='?', default='api.log', help='Path to log file (default: api.log)')
    p.add_argument('--raw_out', '-ro', help='Write JSON output to file instead of stdout')
    p.add_argument('--out', '-o', help='Write report out to file instead of stdout')
    args = p.parse_args()

    records = extract_from_file(args.logfile)

    
    df = pd.DataFrame(records)
    df['finished_time'] = pd.to_datetime(df['access_time'], format='%Y-%m-%d %H:%M:%S,%f') + pd.to_timedelta(df['process_time'], unit='s')
    df['finished_time'] = df['finished_time'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    df['endpoint'] = df['path'].str.extract(r'/([^/]+(?:/[^/]+)?)')

    records = df.to_dict(orient='records')
    data = json.dumps(records, indent=2, ensure_ascii=False)

    report = create_report(df=df)

    if args.raw_out:
        with open(args.raw_out, 'w', encoding='utf-8') as fh:
            fh.write(data)
        
    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            fh.write(report)
    else:
        print(data)

if __name__ == '__main__':
    main()