"""Item 2 report: the distinct sequence of results the reader saw, in order.

Usage: item2_report.py READER_FILE WRITER_FILE

The writer's log gives the counts the source table had: seed, then every
committed count up to the last one (the table only grows). A count is acceptable
when it lies in [seed, last committed]; an error is acceptable when its message
says the table is being rebuilt, halted or unavailable. Anything else is flagged.
"""
import sys
import time

reader_path, writer_path = sys.argv[1], sys.argv[2]

seed = None
last = None
for line in open(writer_path):
    parts = line.split()
    if len(parts) >= 2 and parts[-1] == "seed":
        seed = int(parts[1])
    elif len(parts) == 2:
        last = int(parts[1])

rows = []
for line in open(reader_path):
    parts = line.rstrip("\n").split(" ", 2)
    if len(parts) < 2:
        continue
    t = float(parts[0])
    if parts[1] == "ERROR":
        rows.append((t, "ERROR", parts[2] if len(parts) > 2 else ""))
    else:
        rows.append((t, int(parts[1]), ""))

t0 = rows[0][0] if rows else time.time()
runs = []
for t, val, msg in rows:
    key = (val, msg)
    if runs and runs[-1]["key"] == key:
        runs[-1]["n"] += 1
        runs[-1]["last"] = t
    else:
        runs.append({"key": key, "first": t, "last": t, "n": 1})


def verdict(val, msg):
    if val == "ERROR":
        low = msg.lower()
        if any(w in low for w in ("rebuil", "unavailable", "halt", "re-snapshot", "resnapshot")):
            return "acceptable (explicit error: table being rebuilt/unavailable)"
        return "NOT ACCEPTABLE (unexplained error)"
    if seed is not None and last is not None and seed <= val <= last:
        return "acceptable (a count the source had)"
    return "NOT ACCEPTABLE (a count the source never had)"


print(f"seed={seed} last_committed={last} reads={len(rows)}")
bad = 0
for r in runs:
    val, msg = r["key"]
    label = str(val) if val != "ERROR" else "ERROR " + msg
    v = verdict(val, msg)
    if v.startswith("NOT"):
        bad += 1
    print(f"+{r['first'] - t0:7.2f}s to +{r['last'] - t0:7.2f}s  x{r['n']:<5} {label[:200]}  -> {v}")
print("unacceptable runs:", bad)
