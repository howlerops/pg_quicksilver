"""Checks shared by the demonstrations: the lake against the heap, without text formatting.

lake_equals_heap(table) compares, for one table, on PostgreSQL and on the DuckLake
table read by DuckDB:
  count(*), and per column: count(col), sum(col) for numeric and integer columns,
  sum(length(col)) for text, min/max (timestamps as epoch microseconds, text
  under the C collation, which is byte order as DuckDB sorts).
Values are compared as Python values (int, Decimal, UUID, str), never as the text
a client would print. It returns True, or a list of (measure, heap, lake) that differ.

wait_published(table, lsn) waits until the LSN the lake stands at, as the catalog
records it (qs_mirror_table, coalesced with qs_mirror_watermark exactly as the
catalog's own freshness query does), is at or past lsn.
"""
import os
import sqlite3
import subprocess
import time
from decimal import Decimal

import duckdb
import psycopg

PG_DSN = os.environ.get("QS_PG_DSN", "postgresql://postgres@localhost:5660/app?sslmode=disable")
BASE = os.environ.get("BASE", "/var/lib/postgresql/qslake2")
QS_VERIFY = os.environ.get("QS_VERIFY_BIN", "")


def lsn_num(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def catalog_path(mirror):
    return os.path.join(mirror, "_ducklake", "catalog.sqlite")


def published_lsn(mirror, table):
    """The LSN the lake stands at for table, or '' if the catalog has no row yet."""
    c = sqlite3.connect(f"file:{catalog_path(mirror)}?mode=ro", uri=True, timeout=30)
    try:
        row = c.execute(
            "SELECT coalesce(w.applied_lsn, t.applied_lsn) FROM qs_mirror_table t "
            "LEFT JOIN qs_mirror_watermark w ON w.table_name = t.table_name "
            "AND w.manifest_lsn = t.applied_lsn WHERE t.table_name = ?",
            (table,)).fetchone()
        return row[0] if row and row[0] else ""
    finally:
        c.close()


def wait_published(mirror, table, lsn, timeout=180):
    deadline = time.time() + timeout
    at = ""
    while time.time() < deadline:
        at = published_lsn(mirror, table)
        if at and lsn_num(at) >= lsn_num(lsn):
            return at
        time.sleep(0.2)
    raise TimeoutError(f"{table}: published {at or 'none'} < {lsn} after {timeout}s")


def current_wal_lsn(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        return cur.fetchone()[0]


def columns(conn, table):
    schema, name = table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, name))
        return cur.fetchall()


def _measures(cols, dialect, table=""):
    """One SELECT list, for PostgreSQL or DuckDB, over the columns in cols.

    PostgreSQL has no min/max over uuid, so there the uuid extremes are ordered
    scalar subqueries over the table (the btree order, the same order DuckDB uses).
    """
    out = ["count(*)"]
    names = []
    for i, (col, typ) in enumerate(cols):
        q = f'"{col}"'
        out.append(f"count({q})")
        if typ in ("smallint", "integer", "bigint"):
            out.append(f"sum({q})")
            out.append(f"min({q})")
            out.append(f"max({q})")
        elif typ == "numeric":
            out.append(f"sum({q})")
            out.append(f"min({q})")
            out.append(f"max({q})")
        elif typ == "uuid":
            if dialect == "pg":
                out.append(f"(SELECT {q} FROM {table} WHERE {q} IS NOT NULL ORDER BY {q} LIMIT 1)")
                out.append(f"(SELECT {q} FROM {table} WHERE {q} IS NOT NULL ORDER BY {q} DESC LIMIT 1)")
            else:
                out.append(f"min({q})")
                out.append(f"max({q})")
        elif typ.startswith("timestamp"):
            if dialect == "pg":
                e = f"(extract(epoch from {q}) * 1000000)::bigint"
            else:
                e = f"epoch_us({q})"
            out.append(f"min({e})")
            out.append(f"max({e})")
        elif typ == "text" or typ.startswith("character"):
            if dialect == "pg":
                out.append(f'sum(length({q}))')
                out.append(f'min({q} COLLATE "C")')
                out.append(f'max({q} COLLATE "C")')
            else:
                out.append(f"sum(length({q}))")
                out.append(f"min({q})")
                out.append(f"max({q})")
        else:
            raise ValueError(f"no measure for column type {typ} ({col})")
        names.append(col)
    return out, names


def heap_measures(conn, table, cols):
    exprs, _ = _measures(cols, "pg", table)
    with conn.cursor() as cur:
        cur.execute("SELECT " + ", ".join(exprs) + f" FROM {table}")
        return cur.fetchone()


def lake_measures(con, table, cols):
    exprs, _ = _measures(cols, "duckdb")
    schema, name = table.split(".", 1)
    return con.sql("SELECT " + ", ".join(exprs) + f' FROM lake."{schema}"."{name}"').fetchall()[0]


def _labels(cols):
    labs = ["count(*)"]
    for col, typ in cols:
        labs.append(f"count({col})")
        if typ in ("smallint", "integer", "bigint", "numeric"):
            labs += [f"sum({col})", f"min({col})", f"max({col})"]
        elif typ == "uuid":
            labs += [f"min({col})", f"max({col})"]
        elif typ.startswith("timestamp"):
            labs += [f"min_us({col})", f"max_us({col})"]
        else:
            labs += [f"sum(length({col}))", f"min({col})", f"max({col})"]
    return labs


def lake_equals_heap(table, mirror, conn=None):
    """True if the lake table equals the heap table on every measure, else a list of diffs.

    Call only with writes quiescent and the lake at the heap's LSN (see settle_and_check).
    """
    own = conn is None
    if own:
        conn = psycopg.connect(PG_DSN, autocommit=True)
    try:
        cols = columns(conn, table)
        heap = heap_measures(conn, table, cols)
    finally:
        if own:
            conn.close()
    con = duckdb.connect()
    try:
        con.sql("LOAD ducklake")
        con.sql(f"ATTACH 'ducklake:sqlite:{catalog_path(mirror)}' AS lake (READ_ONLY)")
        lake = lake_measures(con, table, cols)
    finally:
        con.close()
    labs = _labels(cols)
    diffs = [(labs[i], heap[i], lake[i]) for i in range(len(heap))
             if _norm(heap[i]) != _norm(lake[i])]
    return True if not diffs else diffs


def _norm(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, Decimal):
        return v.normalize()
    if isinstance(v, float):
        return Decimal(str(v))
    return v


def qs_verify(table, mirror, dsn=PG_DSN):
    """Runs qs-verify and returns MATCH, DIVERGED, or the raw output if neither was printed."""
    out = subprocess.run([QS_VERIFY, "-dsn", dsn, "-mirror", mirror, "-table", table],
                         capture_output=True, text=True, timeout=900)
    text = (out.stdout + out.stderr).strip()
    for word in ("MATCH", "DIVERGED"):
        if word in text.split():
            return word, text
    return "UNKNOWN", text


def rss_mb(pid):
    """Current and peak resident set of a process, in MB, from /proc/<pid>/status."""
    cur = peak = 0
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    cur = int(line.split()[1]) / 1024
                elif line.startswith("VmHWM:"):
                    peak = int(line.split()[1]) / 1024
    except FileNotFoundError:
        return None, None
    return round(cur, 1), round(peak, 1)
