#!/usr/bin/env python3
"""Writes corpus_seed.sql: one or more statements for every entry of the seed
allowlist, over public.qs_types (validate/types.sql).

The seed is only as good as the statements that exercised it, so the corpus is
generated from the same lists the seed is: every comparison operator on every
pair of types it is listed for, column against column and column against
literal; every arithmetic operator; every aggregate; ORDER BY, GROUP BY and
min/max on every ordered type. validate/coverage.py then checks that no entry
was left unexercised.

    gen_seed_corpus.py > corpus_seed.sql
"""
T = "public.qs_types"
CMP = ["=", "<>", "<", "<=", ">", ">="]
INTS = {"smallint": "si", "integer": "i", "bigint": "bi"}
out = []


def add(sql):
    out.append(sql)


# comparisons across the integer widths, column to column and to a literal
for op in CMP:
    for a in INTS.values():
        for b in INTS.values():
            if a == b:
                add(f"SELECT id FROM {T} WHERE {a} {op} id - 10000 ORDER BY id" if a == "i" else
                    f"SELECT x.id FROM {T} x JOIN {T} y ON y.id = x.id + 1 WHERE x.{a} {op} y.{a} ORDER BY x.id")
            else:
                add(f"SELECT id FROM {T} WHERE {a} {op} {b} ORDER BY id")
    add(f"SELECT id FROM {T} WHERE i {op} 7 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE bi {op} -3 AND si {op} 12 ORDER BY id")

# + - * on integer and bigint (the extremes overflow on both sides: left out)
for op in ["+", "-", "*"]:
    for a in ("i", "bi"):
        for b in ("i", "bi"):
            add(f"SELECT id, {a} {op} {b} FROM {T} WHERE id < 100000 ORDER BY id")
    add(f"SELECT id, i {op} 3, bi {op} 5, (i {op} bi) {op} id FROM {T} WHERE id < 100000 ORDER BY id")

# numeric: comparison, + - *, against literals with a decimal point, and
# against integers under a comparison
for op in CMP:
    add(f"SELECT id FROM {T} WHERE n {op} n2 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE n {op} 12.50 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE n {op} 7 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE n {op} i ORDER BY id")
    add(f"SELECT id FROM {T} WHERE n {op} si ORDER BY id")
    add(f"SELECT id FROM {T} WHERE n4 {op} n ORDER BY id")
for op in ["+", "-", "*"]:
    add(f"SELECT id, n {op} n2, n {op} n4, n4 {op} n4 FROM {T} WHERE id < 100000 ORDER BY id")
    add(f"SELECT id, n {op} 1.5, n4 {op} 0.25, (n {op} n2) {op} 2.0 FROM {T} WHERE id < 100000 ORDER BY id")
    add(f"SELECT sum(n {op} n2), sum(n {op} 1.5), min(n {op} n4), max(n4 {op} 0.5) FROM {T} WHERE id < 100000")

# date, timestamp, timestamptz: comparison
for col, lit in (("d", "'2024-06-15'"), ("ts", "'2024-01-20 12:00:00.000003'"), ("tz", "'2024-03-10 03:00:00-04'")):
    for op in CMP:
        add(f"SELECT id FROM {T} WHERE {col} {op} {col}2 ORDER BY id")
        add(f"SELECT id FROM {T} WHERE {col} {op} {lit} ORDER BY id")

# text and uuid and boolean: equality
for op in ("=", "<>"):
    add(f"SELECT id FROM {T} WHERE t {op} t2 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE t {op} 'Éclair' ORDER BY id")
    add(f"SELECT id FROM {T} WHERE tc {op} tc2 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE u {op} u2 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE u {op} 'c4ca4238-a0b9-2382-0dcc-509a6f75849b' ORDER BY id")
    add(f"SELECT id FROM {T} WHERE b {op} b2 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE b {op} true ORDER BY id")

# text ordering: only the "C" columns
for op in ("<", "<=", ">", ">="):
    add(f"SELECT id FROM {T} WHERE tc {op} tc2 ORDER BY id")
    add(f"SELECT id FROM {T} WHERE tc {op} 'b' ORDER BY id")
add(f"SELECT id, tc FROM {T} ORDER BY tc, id")
add(f"SELECT id, tc FROM {T} ORDER BY tc DESC, id LIMIT 300")
add(f"SELECT id, tc FROM {T} ORDER BY tc NULLS FIRST, tc2 DESC NULLS LAST, id LIMIT 300")
add(f"SELECT min(tc), max(tc), min(tc2), max(tc2) FROM {T}")
add(f"SELECT b, min(tc), max(tc) FROM {T} GROUP BY b")

# ORDER BY, GROUP BY, min/max, count on every type
ORDERED = ["si", "i", "bi", "n", "n4", "d", "ts", "tz"]
for c in ORDERED:
    add(f"SELECT id, {c} FROM {T} ORDER BY {c}, id LIMIT 400")
    add(f"SELECT id, {c} FROM {T} ORDER BY {c} DESC, id LIMIT 400")
    add(f"SELECT id, {c} FROM {T} ORDER BY {c} NULLS FIRST, id LIMIT 400")
    add(f"SELECT id, {c} FROM {T} ORDER BY {c} DESC NULLS LAST, id LIMIT 400")
    add(f"SELECT min({c}), max({c}), count({c}) FROM {T}")
    add(f"SELECT b, min({c}), max({c}), count({c}) FROM {T} GROUP BY b")
    add(f"SELECT {c}, count(*) FROM {T} GROUP BY {c} ORDER BY {c}")
for c in ("b", "t", "tc", "u"):
    add(f"SELECT {c}, count(*), count({c}) FROM {T} GROUP BY {c}")
    add(f"SELECT id, {c} FROM {T} WHERE id % 97 = 0 ORDER BY id" if False else f"SELECT id, {c} FROM {T} WHERE id > 19000 ORDER BY id")
add(f"SELECT count(*), sum(i), sum(bi), sum(n), sum(n4), sum(id) FROM {T} WHERE id < 100000")
add(f"SELECT b, count(*), sum(i), sum(bi), sum(n), sum(n4) FROM {T} WHERE id < 100000 GROUP BY b")
add(f"SELECT sum(bi), sum(i) FROM {T}")
add(f"SELECT t, sum(n), min(d), max(tz) FROM {T} GROUP BY t")
add(f"SELECT d, count(*) FROM {T} GROUP BY d HAVING count(*) > 50 ORDER BY d")
add(f"SELECT si, i, count(*) FROM {T} WHERE id < 3000 GROUP BY si, i ORDER BY si, i")

# implicit widening
add(f"SELECT id, coalesce(i, si), coalesce(bi, i), coalesce(bi, si) FROM {T} ORDER BY id")
add(f"SELECT id, CASE WHEN b THEN i ELSE si END, CASE WHEN b2 THEN bi ELSE i END, CASE WHEN b THEN bi ELSE si END FROM {T} ORDER BY id")
add(f"SELECT id FROM {T} WHERE i IN (si, 5, 7) OR bi IN (i, si, 100) ORDER BY id")

# AND / OR / NOT, IS NULL, boolean tests, CASE, COALESCE
add(f"SELECT id FROM {T} WHERE (i > 0 AND bi < 0) OR NOT (n >= 10.00 OR b) ORDER BY id")
add(f"SELECT id, i IS NULL, n IS NOT NULL, u IS NULL, d IS NOT NULL, t IS NULL, ts IS NULL, tz IS NOT NULL FROM {T} ORDER BY id")
add(f"SELECT id, b IS TRUE, b IS NOT TRUE, b IS FALSE, b IS NOT FALSE, b IS UNKNOWN, b IS NOT UNKNOWN FROM {T} ORDER BY id")
add(f"SELECT id, b AND b2, b OR b2, NOT b FROM {T} ORDER BY id")
add(f"SELECT id, CASE WHEN i > 0 THEN 'pos' WHEN i < 0 THEN 'neg' ELSE t END FROM {T} ORDER BY id")
add(f"SELECT id, CASE i WHEN 1 THEN bi WHEN 2 THEN id ELSE NULL END, CASE WHEN n > n2 THEN n ELSE n2 END FROM {T} ORDER BY id")
add(f"SELECT id, coalesce(t, t2, 'none'), coalesce(n, n2), coalesce(d, d2), coalesce(ts, ts2), coalesce(tz, tz2), coalesce(u, u2), coalesce(b, b2) FROM {T} ORDER BY id")
add(f"SELECT id, CASE WHEN b THEN d ELSE d2 END, CASE WHEN b THEN ts END, CASE WHEN b2 THEN tz ELSE tz2 END, CASE WHEN b THEN u ELSE u2 END FROM {T} ORDER BY id")

# joins on equality of each key type, inner and left
for c in ("i", "bi", "si", "n", "t", "tc", "u", "d", "ts", "tz", "b"):
    add(f"SELECT x.id, y.id FROM {T} x JOIN {T} y ON y.{c} = x.{c}2 AND y.id = x.id + 3 ORDER BY x.id" if c in ("n", "t", "tc", "u", "d", "ts", "tz", "b") else
        f"SELECT x.id, y.id FROM {T} x JOIN {T} y ON y.{c} = x.{c} AND y.id = x.id + 201 ORDER BY x.id")
add(f"SELECT x.id, y.id, y.n FROM {T} x LEFT JOIN {T} y ON y.id = x.i ORDER BY x.id")
add(f"SELECT x.id, count(y.id), sum(y.n) FROM {T} x LEFT JOIN {T} y ON y.i = x.id AND y.b GROUP BY x.id ORDER BY x.id LIMIT 500")
add(f"SELECT x.b, count(*), sum(y.bi) FROM {T} x JOIN {T} y ON y.id = x.bi JOIN {T} z ON z.id = y.id GROUP BY x.b")

# subqueries in FROM, HAVING, IN lists, LIMIT with ORDER BY
add(f"SELECT q.i, q.c FROM (SELECT i, count(*) AS c, sum(n) AS s FROM {T} GROUP BY i) q WHERE q.c > 90 AND q.s > 0.00 ORDER BY q.i")
add(f"SELECT q.id, q.n FROM (SELECT id, n FROM {T} WHERE n > 50.00) q WHERE q.id < 5000 ORDER BY q.n DESC, q.id LIMIT 100")
add(f"SELECT i, sum(n) FROM {T} GROUP BY i HAVING sum(n) > 100.00 AND count(*) > 5 ORDER BY i")
add(f"SELECT id FROM {T} WHERE i IN (1, 2, 3) AND bi IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ORDER BY id")
add(f"SELECT id FROM {T} WHERE t IN ('apple', 'zebra', '') OR u IN ('c4ca4238-a0b9-2382-0dcc-509a6f75849b') ORDER BY id")
add(f"SELECT id FROM {T} WHERE d IN ('2024-02-29', '2024-03-01') OR n IN (0.01, 12.50) ORDER BY id")
add(f"SELECT * FROM {T} WHERE id >= 100000 ORDER BY id")
add(f"SELECT * FROM {T} ORDER BY id LIMIT 500")
add(f"SELECT id, si, i, bi, n, n2, n4, b, b2, t, t2, tc, tc2, u, u2, d, d2, ts, ts2, tz, tz2 FROM {T} ORDER BY id DESC LIMIT 700")

print("-- Generated by gen_seed_corpus.py; do not edit. One or more statements per")
print("-- entry of the seed allowlist, over public.qs_types.")
for k, sql in enumerate(out, 1):
    print(f"\n-- @id seed-{k:03d}\n{sql};")
