import os, re, json, glob, decimal, datetime
import duckdb

D = "/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/dvexp"
M = f"{D}/mirror/public.orders"
DATA = f"{D}/data"
RAW = f"{D}/raw"
COLS = '"id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note"'

QUERIES = [
    ("count_star", "SELECT count(*) FROM orders"),
    ("count_by_status", "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status"),
    ("sum_amount", "SELECT sum(amount) FROM orders"),
    ("month_filter_agg",
     "SELECT count(*), sum(amount) FROM orders WHERE created_at >= timestamptz '2025-06-01 00:00:00+00' "
     "AND created_at < timestamptz '2025-07-01 00:00:00+00'"),
    ("revenue_by_month_join",
     "SELECT date_trunc('month', o.created_at) AS month, date_trunc('year', c.created_at) AS signup_year, "
     "sum(o.amount) AS revenue FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY 1, 2 ORDER BY 1, 2"),
]


def norm(v):
    if isinstance(v, decimal.Decimal):
        return str(v.normalize())
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep=" ")
    return v


def canon(rows):
    return [[norm(v) for v in r] for r in rows]


def connect(threads):
    c = duckdb.connect()
    c.execute(f"SET threads = {threads}")
    c.execute("SET TimeZone = 'UTC'")
    c.execute("SET default_null_order = 'NULLS_LAST_ON_ASC_FIRST_ON_DESC'")
    return c


def base_view():
    s = open(f"{RAW}/view_orders.sql").read()
    return "\n".join(l for l in s.splitlines() if not l.startswith("--"))


def customers_view():
    s = open(f"{RAW}/view_customers.sql").read()
    return "\n".join(l for l in s.splitlines() if not l.startswith("--"))


BR = re.compile(r"FROM read_parquet\('([^']+)', file_row_number = true\) WHERE file_row_number NOT IN "
                r"\(SELECT p FROM read_parquet\('([^']+)'\)\)")


def rewrite(fn):
    """Apply fn(datafile, dvfile, idx) -> replacement FROM-clause to every DV'd branch."""
    i = [0]

    def r(m):
        i[0] += 1
        return fn(m.group(1), m.group(2), i[0])
    out = BR.sub(r, base_view())
    assert i[0] == 12, i[0]
    return out


def state():
    return json.load(open(f"{M}/state.json"))


# ---------------------------------------------------------------- variants
def v0(c):
    c.execute("CREATE VIEW orders AS " + base_view())


def v0_cast(c):
    s = base_view().replace('"amount", "created_at", "payload"',
                            '"amount", CAST("created_at" AS TIMESTAMP WITH TIME ZONE) AS "created_at", "payload"')
    assert "CAST(" in s
    c.execute("CREATE VIEW orders AS " + s)


def v1_not_exists(c):
    c.execute("CREATE VIEW orders AS " + rewrite(lambda f, d, i:
        f"FROM read_parquet('{f}', file_row_number = true) b WHERE NOT EXISTS "
        f"(SELECT 1 FROM read_parquet('{d}') d WHERE d.p = b.file_row_number)"))


def v1_anti(c):
    c.execute("CREATE VIEW orders AS " + rewrite(lambda f, d, i:
        f"FROM read_parquet('{f}', file_row_number = true) b ANTI JOIN read_parquet('{d}') d "
        f"ON b.file_row_number = d.p"))


def v1_left(c):
    c.execute("CREATE VIEW orders AS " + rewrite(lambda f, d, i:
        f"FROM read_parquet('{f}', file_row_number = true) b LEFT JOIN read_parquet('{d}') d "
        f"ON b.file_row_number = d.p WHERE d.p IS NULL"))


def v1_temp(c):
    def fn(f, d, i):
        c.execute(f"CREATE TEMP TABLE dv{i} AS SELECT p FROM read_parquet('{d}') ORDER BY p")
        return (f"FROM read_parquet('{f}', file_row_number = true) b ANTI JOIN dv{i} d "
                f"ON b.file_row_number = d.p")
    c.execute("CREATE VIEW orders AS " + rewrite(fn))


def v1_temp_notin(c):
    def fn(f, d, i):
        c.execute(f"CREATE TEMP TABLE dv{i} AS SELECT p FROM read_parquet('{d}') ORDER BY p")
        return (f"FROM read_parquet('{f}', file_row_number = true) b WHERE file_row_number NOT IN "
                f"(SELECT p FROM dv{i})")
    c.execute("CREATE VIEW orders AS " + rewrite(fn))


def v1_temp_pk(c):
    """temp table with a PRIMARY KEY (ART index) - lets DuckDB know p is unique / non-null."""
    def fn(f, d, i):
        c.execute(f"CREATE TEMP TABLE dv{i}(p BIGINT PRIMARY KEY)")
        c.execute(f"INSERT INTO dv{i} SELECT p FROM read_parquet('{d}') ORDER BY p")
        return (f"FROM read_parquet('{f}', file_row_number = true) b ANTI JOIN dv{i} d "
                f"ON b.file_row_number = d.p")
    c.execute("CREATE VIEW orders AS " + rewrite(fn))


def v1_list(c):
    """sorted list held in a variable; membership by list_contains."""
    def fn(f, d, i):
        c.execute(f"SET VARIABLE dv{i} = (SELECT list(p ORDER BY p) FROM read_parquet('{d}'))")
        return (f"FROM read_parquet('{f}', file_row_number = true) b WHERE NOT "
                f"list_contains(getvariable('dv{i}'), file_row_number)")
    c.execute("CREATE VIEW orders AS " + rewrite(fn))


def v2(c):
    c.execute(f"CREATE VIEW orders AS SELECT {COLS} FROM read_parquet('{DATA}/compact/orders.parquet')")


def v2_sorted(c):
    c.execute(f"CREATE VIEW orders AS SELECT {COLS} FROM read_parquet('{DATA}/compact/orders_sorted.parquet')")


def _delta_branches():
    """the non-base branches of the baseline view, unchanged"""
    s = base_view()
    parts = s.split("WITH qs_whole AS (", 1)[1].rsplit("\n)\nSELECT", 1)[0].split("\n  UNION ALL\n")
    assert len(parts) == 14, len(parts)
    return [p.strip() for p in parts[1:]]


def _part_view(c, n, name="orders"):
    br = []
    for k in range(n):
        f = f"{DATA}/part{n}/base_{k:02d}.parquet"
        d = f"{DATA}/part{n}/dv_{k:02d}.parquet"
        if os.path.exists(d):
            br.append(f"SELECT {COLS} FROM read_parquet('{f}', file_row_number = true) WHERE file_row_number "
                      f"NOT IN (SELECT p FROM read_parquet('{d}'))")
        else:
            br.append(f"SELECT {COLS} FROM read_parquet('{f}')")
    br += _delta_branches()
    c.execute(f"CREATE VIEW {name} AS " + "\n UNION ALL\n".join(br))


def v5_16(c):
    _part_view(c, 16)


def v5_64(c):
    _part_view(c, 64)


def v5_skew16(c):
    """hypothetical: same 700k dead rows but all inside 1 of 16 files (answers differ; timing only)"""
    br = []
    for k in range(16):
        f = f"{DATA}/part16/base_{k:02d}.parquet"
        if k == 15:
            br.append(f"SELECT {COLS} FROM read_parquet('{f}', file_row_number = true) WHERE file_row_number "
                      f"NOT IN (SELECT p FROM read_parquet('{DATA}/part16/skew_dv_15.parquet'))")
        else:
            br.append(f"SELECT {COLS} FROM read_parquet('{f}')")
    br += _delta_branches()
    c.execute("CREATE VIEW orders AS " + "\n UNION ALL\n".join(br))


def v6_mask(c):
    """per-file boolean 'live' sidecar, same row count as the data file, joined positionally"""
    def fn(f, d, i):
        m = f"{DATA}/mask/" + os.path.basename(f).replace(".parquet", ".live.parquet")
        return (f"FROM (SELECT * FROM read_parquet('{f}') POSITIONAL JOIN read_parquet('{m}')) WHERE live")
    c.execute("CREATE VIEW orders AS " + rewrite(fn))


def v3_ducklake(tag):
    def f(c):
        c.execute("LOAD ducklake")
        c.execute(f"ATTACH 'ducklake:{DATA}/ducklake_{tag}/meta.ducklake' AS lake (READ_ONLY, DATA_PATH '{DATA}/ducklake_{tag}/files/', OVERRIDE_DATA_PATH true)")
        c.execute("CREATE VIEW orders AS SELECT * FROM lake.orders")
    return f


def v4_iceberg(ver):
    def f(c):
        c.execute("LOAD iceberg")
        cur = open(f"{DATA}/iceberg/wh/default/orders_v{ver}/metadata/current.txt").read().strip()
        c.execute(f"CREATE VIEW orders AS SELECT {COLS} FROM iceberg_scan('{cur}')")
    return f


def v0_pad(c):
    """a constant first column in every branch, so count(*) over the UNION keeps it instead of "id" """
    s = base_view().replace('SELECT "id", "tenant_id"', 'SELECT 0::TINYINT AS qs_pad, "id", "tenant_id"')
    assert s.count("qs_pad") == 14
    c.execute("CREATE VIEW orders AS " + s)


def v0_pad_temp(c):
    def fn(f, d, i):
        c.execute(f"CREATE TEMP TABLE dv{i} AS SELECT p FROM read_parquet('{d}') ORDER BY p")
        return (f"FROM read_parquet('{f}', file_row_number = true) b ANTI JOIN dv{i} d "
                f"ON b.file_row_number = d.p")
    s = rewrite(fn).replace('SELECT "id", "tenant_id"', 'SELECT 0::TINYINT AS qs_pad, "id", "tenant_id"')
    c.execute("CREATE VIEW orders AS " + s)


VARIANTS = {
    "0_baseline_padfirst": v0_pad,
    "1_temp_table_anti_padfirst": v0_pad_temp,
    "0_baseline": v0,
    "0_baseline_cast": v0_cast,
    "1_not_exists": v1_not_exists,
    "1_anti_join": v1_anti,
    "1_left_join_null": v1_left,
    "1_temp_table_anti": v1_temp,
    "1_temp_table_notin": v1_temp_notin,
    "1_temp_table_pk": v1_temp_pk,
    "1_sorted_list": v1_list,
    "2_compacted": v2,
    "2_compacted_sorted": v2_sorted,
    "3_ducklake_deletes": v3_ducklake("a"),
    "3_ducklake_merged": v3_ducklake("b"),
    "3_ducklake_rewritten": v3_ducklake("c"),
    "4_iceberg_v2_posdeletes": v4_iceberg(2),
    "4_iceberg_v3_dv": v4_iceberg(3),
    "5_part16": v5_16,
    "5_part64": v5_64,
    "5_part16_skew_hypothetical": v5_skew16,
    "6_live_mask_positional": v6_mask,
}


def setup(c, variant):
    c.execute("CREATE VIEW customers AS " + customers_view())
    VARIANTS[variant](c)
