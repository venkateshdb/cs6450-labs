#!/usr/bin/env python3

# Extracts throughputs and summarizes them from a set of kvs server logs.
# Mostly thrown together with Claude from a draft awk script.

import os
import glob
import statistics

LOG_DIR = "./logs/latest"

total_commit_throughput = 0.0
total_abort_throughput = 0.0

# Find all matching log files
log_files = sorted(glob.glob(os.path.join(LOG_DIR, "kvsserver-*.log")))

if not log_files:
    print("No matching log files found.")
    exit(0)

for log_path in log_files:
    node = os.path.basename(log_path).removeprefix("kvsserver-").removesuffix(".log")
    commit_throughput = []
    abort_throughput = []
    with open(log_path) as f:
        for line in f:
            if "commit/s" in line:
                parts = line.strip().split()
                try:
                    # Throughput value is the 2nd column in original awk ($2)
                    commit_throughput.append(float(parts[1]))
                except (IndexError, ValueError):
                    pass
            elif "abort/s" in line:
                parts = line.strip().split()
                try:
                    abort_throughput.append(float(parts[1]))
                except (IndexError, ValueError):
                    pass
                    
    if commit_throughput:
        median_commit = statistics.median(commit_throughput)
        total_commit_throughput += median_commit
        print(f"{node} median {median_commit:.0f} commit/s", end="")

        if abort_throughput:
            median_abort = statistics.median(abort_throughput)
            total_abort_throughput += median_abort
            print(f" {median_abort:.0f} abort/s")
        else:
            print()
    else:
        print(f"{node} no commit/s data")

print(f"\nTotal: {total_commit_throughput:.0f} commit/s {total_abort_throughput:.0f} abort/s")
