"""Apply-loop latency from the sidecar's own log.

  ticks.py MIRROR_LOG [FROM_LINE]

sidecar.sh sets QS_SLOW_TICK=1ms, so every tick of the apply loop that took a
millisecond or more logs its total and its phases. A tick with an apply_ms
phase applied something; those are the ones reported, as the whole tick (which
is what a reader waits for) and as the apply phase alone.
"""
import re
import statistics
import sys

start = int(sys.argv[2]) if len(sys.argv) > 2 else 0
total, apply = [], []
for i, line in enumerate(open(sys.argv[1], errors="replace")):
    if i < start or "slow tick" not in line:
        continue
    kv = dict(re.findall(r"(\w+_ms)=(\d+)", line))
    if "apply_ms" in kv:
        total.append(int(kv["total_ms"]))
        apply.append(int(kv["apply_ms"]))


def q(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))]


if not total:
    sys.exit("no apply ticks in the log")
print(f"apply ticks: {len(total)}")
print(f"  whole tick  median {statistics.median(total):.0f} ms  p95 {q(total, .95)} ms  max {max(total)} ms")
print(f"  apply phase median {statistics.median(apply):.0f} ms  p95 {q(apply, .95)} ms  max {max(apply)} ms")
