"""The real reader, for the Go tests: DuckDB with the ducklake and sqlite
extensions, attached read-only to the catalog the Go publisher writes.

One JSON object per line on stdin, one per line on stdout.

  {"attach": "<catalog.sqlite>"}        first line; answers {"version": ...}
  {"sql": "...", "fresh": false}        run on the long-lived connection, or on
                                        a new one that attaches from nothing
  {"loop": {...}}                       see loop(): query in a loop for a while

Every value comes back as text, so that the test compares what a reader sees
and not how Python renders it.
"""
import json
import sys
import time

import duckdb

catalog = None


def connect():
    c = duckdb.connect()
    c.execute("LOAD ducklake")
    c.execute("LOAD sqlite")
    c.execute(f"ATTACH 'ducklake:sqlite:{catalog}' AS lake (READ_ONLY)")
    return c


def run(c, sql):
    rows = c.execute(sql).fetchall()
    return [[None if v is None else str(v) for v in r] for r in rows]


def loop(c, spec):
    """Run spec["sql"] for spec["seconds"], and report every DISTINCT answer
    with the wall-clock span it was seen in, and every error. The publisher
    writes its own account of what it published; the test matches the two."""
    seen, errors, n = {}, [], 0
    end = time.time() + spec["seconds"]
    while time.time() < end:
        try:
            t0 = time.time()
            rows = run(c, spec["sql"])
            key = json.dumps(rows)
            if key not in seen:
                seen[key] = [t0, time.time()]
            else:
                seen[key][1] = time.time()
            n += 1
        except Exception as e:  # noqa: BLE001 - every error is a finding
            errors.append(str(e))
            if len(errors) > 20:
                break
    return {"queries": n, "answers": [[json.loads(k), v[0], v[1]] for k, v in seen.items()], "errors": errors}


def main():
    global catalog
    con = None
    for line in sys.stdin:
        req = json.loads(line)
        try:
            if "attach" in req:
                catalog = req["attach"]
                boot = duckdb.connect()
                boot.execute("INSTALL ducklake")
                boot.execute("INSTALL sqlite")
                boot.close()
                con = connect()
                out = {"version": duckdb.__version__}
            elif "loop" in req:
                out = loop(connect() if req.get("fresh") else con, req["loop"])
            else:
                c = connect() if req.get("fresh") else con
                out = {"rows": run(c, req["sql"])}
                if req.get("fresh"):
                    c.close()
        except Exception as e:  # noqa: BLE001
            out = {"error": str(e)}
        sys.stdout.write(json.dumps(out) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
