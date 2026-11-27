"""Canonical rendering and comparison of one statement's two results.

Both sides are reduced to the same shape before anything is compared:

    names   column names as the engine reported them
    types   one canonical token per column ("int4", "numeric(12,2)", "text", ...)
    rows    tuples of str-or-None, each value in PostgreSQL's TEXT OUTPUT form

PostgreSQL's side needs no rendering: every value is fetched as the text the
server sent, with no client-side type adaptation at all. DuckDB's side is read
through Arrow (so an interval keeps its months, an infinite timestamp keeps its
sentinel, a DECIMAL keeps its scale) and rendered here the way PostgreSQL would
have printed the same value. Rendering DuckDB *towards* PostgreSQL, rather than
both towards some third form, means a rendering bug shows up as a false
DIFFERENCE and gets looked at, instead of as a false match that never does.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import re
import struct
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import pyarrow as pa

# Two floats that agree to this relative tolerance are the same answer. Engines
# sum in different orders; the last digit of a float8 is not a semantic claim.
FLOAT_RTOL = 1e-12

EXACT = "exact"
DIFF_TYPE = "same_values_diff_type"
DIFF_NAME = "same_values_diff_name"
DIFF_VALUE = "different_value"
ERR_MIRROR = "error_mirror_only"
ERR_SOURCE = "error_source_only"
ERR_BOTH = "error_both"
CLASSES = [EXACT, DIFF_NAME, DIFF_TYPE, DIFF_VALUE, ERR_MIRROR, ERR_SOURCE, ERR_BOTH]
CLASS_LABEL = {
    EXACT: "exact",
    DIFF_NAME: "same values, different column name",
    DIFF_TYPE: "same values, different type",
    DIFF_VALUE: "different value",
    ERR_MIRROR: "error on mirror only",
    ERR_SOURCE: "error on source only",
    ERR_BOTH: "error on both",
}


@dataclass
class Result:
    names: list = field(default_factory=list)
    types: list = field(default_factory=list)       # canonical tokens
    raw_types: list = field(default_factory=list)   # as the engine names them
    rows: list = field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------- type tokens

_PG_TOKENS = {
    "smallint": "int2", "integer": "int4", "bigint": "int8",
    "real": "float4", "double precision": "float8",
    "text": "text", "character varying": "text", "name": "text", '"char"': "text",
    "unknown": "text", "character": "char", "bpchar": "char",
    "boolean": "bool", "date": "date", "interval": "interval", "uuid": "uuid",
    "time without time zone": "time", "time with time zone": "timetz",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "json": "json", "jsonb": "json", "bytea": "bytea", "inet": "inet", "cidr": "inet",
}


def pg_token(fmt: str, typtype: str = "b") -> str:
    """format_type() output -> canonical token."""
    if fmt.endswith("[]"):
        return "array<" + pg_token(fmt[:-2], typtype) + ">"
    if typtype == "e":
        return "enum"
    if fmt.startswith("numeric"):
        return fmt.replace(" ", "")
    base = re.sub(r"\(\d+\)", "", fmt).strip()        # varchar(40), timestamp(3) ...
    return _PG_TOKENS.get(base, base)


_DUCK_TOKENS = {
    "TINYINT": "int1", "SMALLINT": "int2", "INTEGER": "int4", "BIGINT": "int8",
    "HUGEINT": "int16", "UTINYINT": "uint", "USMALLINT": "uint", "UINTEGER": "uint",
    "UBIGINT": "uint", "UHUGEINT": "uint", "FLOAT": "float4", "DOUBLE": "float8",
    "VARCHAR": "text", "BOOLEAN": "bool", "DATE": "date", "TIME": "time",
    "TIME WITH TIME ZONE": "timetz", "TIMESTAMP": "timestamp",
    "TIMESTAMP_S": "timestamp", "TIMESTAMP_MS": "timestamp", "TIMESTAMP_NS": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz", "INTERVAL": "interval", "UUID": "uuid",
    "JSON": "json", "BLOB": "bytea", '"NULL"': "null",
}


def duck_token(t: str) -> str:
    t = str(t)
    if t.endswith("[]"):
        return "array<" + duck_token(t[:-2]) + ">"
    if t.startswith("ENUM("):
        return "enum"
    m = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", t)
    if m:
        return f"numeric({m.group(1)},{m.group(2)})"
    return _DUCK_TOKENS.get(t, t.lower())


_INTS = {"int1", "int2", "int4", "int8", "int16", "uint"}
_FLOATS = {"float4", "float8"}


def _is_num(tok: str) -> bool:
    return tok in _INTS or tok in _FLOATS or tok.startswith("numeric")


def types_equal(a: str, b: str) -> bool:
    if a == b:
        return True
    # An unconstrained PostgreSQL numeric has no (p,s) to compare; the rendered
    # digits carry the scale, and the value comparison sees those.
    if a == "numeric" and b.startswith("numeric"):
        return True
    if a.startswith("array<") and b.startswith("array<"):
        return types_equal(a[6:-1], b[6:-1])
    return False


def type_label(a: str, b: str) -> str:
    """Why two tokens differ, in terms someone can act on. a = source, b = mirror."""
    if a in _INTS and b in _INTS:
        return "integer width"
    if a.startswith("numeric") and b.startswith("numeric"):
        return "numeric precision/scale"
    if _is_num(a) and _is_num(b):
        if a in _FLOATS and b in _FLOATS:
            return "float width"
        if a in _INTS or b in _INTS:
            return "integer vs fractional type"
        return "numeric vs float"
    if b == "text" and a in ("uuid", "json", "enum", "inet", "bytea", "interval", "timetz") \
            or b == "text" and a.startswith("array<"):
        return f"{a.split('<')[0]} read as VARCHAR"
    if a == "char" and b == "text":
        return "char(n) read as VARCHAR"
    if {a, b} <= {"date", "timestamp", "timestamptz"}:
        return "date/timestamp/timestamptz family"
    return f"{a} vs {b}"


# ------------------------------------------------- DuckDB -> PostgreSQL text

def _civil(days: int):
    """Days since 1970-01-01 -> (y, m, d), proleptic Gregorian, any range."""
    z = days + 719468
    era = (z if z >= 0 else z - 146096) // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + 1 if m <= 2 else y), m, d


def _fmt_ymd(days: int):
    y, m, d = _civil(days)
    if y <= 0:
        return f"{1 - y:04d}-{m:02d}-{d:02d}", " BC"
    return f"{y:04d}-{m:02d}-{d:02d}", ""


def fmt_date(days: int) -> str:
    # Exactly DuckDB's two sentinels and nothing near them. The mirror writes
    # INT32_MIN for -infinity; DuckDB reads that as an ordinary date 5.8 million
    # years BC, and a renderer that rounded it to "-infinity" would hide that.
    if days == 2147483647:
        return "infinity"
    if days == -2147483647:
        return "-infinity"
    s, bc = _fmt_ymd(days)
    return s + bc


def _fmt_hms(us: int) -> str:
    s, frac = divmod(us, 1_000_000)
    out = f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"
    if frac:
        out += (".%06d" % frac).rstrip("0")
    return out


def fmt_time(us: int) -> str:
    return _fmt_hms(us)


_I64_MAX = 9223372036854775807


def fmt_ts(us: int, tz: ZoneInfo | None = None) -> str:
    if us == _I64_MAX:
        return "infinity"
    if us == -_I64_MAX:
        return "-infinity"
    off = ""
    if tz is not None:
        try:
            utc = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(microseconds=us)
            secs = int(utc.astimezone(tz).utcoffset().total_seconds())
        except OverflowError:
            secs = 0
        us += secs * 1_000_000
        sign = "-" if secs < 0 else "+"
        a = abs(secs)
        off = f"{sign}{a // 3600:02d}"
        if a % 3600:
            off += f":{a % 3600 // 60:02d}"
            if a % 60:
                off += f":{a % 60:02d}"
    days, rem = divmod(us, 86_400_000_000)
    ymd, bc = _fmt_ymd(days)
    return f"{ymd} {_fmt_hms(rem)}{off}{bc}"


def fmt_interval(months: int, days: int, us: int) -> str:
    """IntervalStyle = postgres."""
    years = int(months / 12)            # truncates toward zero, as PostgreSQL does
    mons = months - years * 12
    out, zero, before = "", True, False
    for val, unit in ((years, "year"), (mons, "mon"), (days, "day")):
        if not val:
            continue
        out += ("" if zero else " ") + ("+" if before and val > 0 else "") \
            + f"{val} {unit}" + ("" if val == 1 else "s")
        before, zero = val < 0, False
    if zero or us:
        sign = "-" if us < 0 else ("+" if before else "")
        out += ("" if zero else " ") + sign + _fmt_hms(abs(us))
    return out


def _f32(x: float) -> float:
    return struct.unpack("f", struct.pack("f", x))[0]


def fmt_float(x: float, single: bool = False) -> str:
    """float8out / float4out: shortest digits that round-trip, PostgreSQL's layout."""
    if x != x:
        return "NaN"
    if x in (math.inf, -math.inf):
        return "Infinity" if x > 0 else "-Infinity"
    if x == 0:
        return "-0" if math.copysign(1, x) < 0 else "0"
    if single:
        for p in range(0, 9):
            s = "%.*e" % (p, x)
            try:
                if _f32(float(s)) == x:
                    break
            except OverflowError:       # rounds up past FLT_MAX at this precision
                continue
    else:
        s = "%.16e" % x
        for p in range(0, 17):
            c = "%.*e" % (p, x)
            if float(c) == x:
                s = c
                break
    mant, exp = s.split("e")
    exp = int(exp)
    neg = mant.startswith("-")
    digits = mant.lstrip("-").replace(".", "").rstrip("0") or "0"
    if -4 <= exp < (6 if single else 15):
        if exp < 0:
            body = "0." + "0" * (-exp - 1) + digits
        elif len(digits) <= exp + 1:
            body = digits + "0" * (exp + 1 - len(digits))
        else:
            body = digits[:exp + 1] + "." + digits[exp + 1:]
    else:
        body = digits[0] + ("." + digits[1:] if len(digits) > 1 else "")
        body += "e%s%02d" % ("-" if exp < 0 else "+", abs(exp))
    return ("-" if neg else "") + body


def _quote_elem(s: str) -> str:
    if s == "" or s.lower() == "null" or re.search(r'[{}",\\\s]', s):
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


def _render(v, t: pa.DataType, tz) -> str | None:
    """One Arrow value -> PostgreSQL text. Temporal values arrive as integers."""
    if v is None:
        return None
    if pa.types.is_boolean(t):
        return "t" if v else "f"
    if pa.types.is_integer(t):
        return str(v)
    if pa.types.is_decimal(t):
        return format(v, "f")
    if pa.types.is_float32(t):
        return fmt_float(v, single=True)
    if pa.types.is_floating(t):
        return fmt_float(v)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return v
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "\\x" + bytes(v).hex()
    if pa.types.is_date(t):
        return fmt_date(v if isinstance(v, int) else (v - dt.date(1970, 1, 1)).days)
    if pa.types.is_timestamp(t):
        if not isinstance(v, int):      # nested inside a list: a Python datetime
            v = (v.replace(tzinfo=None) - dt.datetime(1970, 1, 1)) // dt.timedelta(microseconds=1) \
                if v.tzinfo is None else \
                (v - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)) // dt.timedelta(microseconds=1)
        else:
            v = {"s": v * 1_000_000, "ms": v * 1000, "us": v, "ns": v // 1000}[t.unit]
        return fmt_ts(v, tz if t.tz else None)
    if pa.types.is_time(t):
        if not isinstance(v, int):
            v = ((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond
        return fmt_time(v)
    if pa.types.is_interval(t):
        return fmt_interval(v.months, v.days, v.nanoseconds // 1000)
    if pa.types.is_duration(t):
        return fmt_interval(0, 0, v // dt.timedelta(microseconds=1) if not isinstance(v, int) else v)
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        return _render_list(v, t.value_type, tz)
    if pa.types.is_struct(t) or pa.types.is_map(t):
        return json.dumps(v, default=str, ensure_ascii=False, sort_keys=True)
    return str(v)


def _render_list(v, elem: pa.DataType, tz) -> str:
    nested = pa.types.is_list(elem) or pa.types.is_large_list(elem)
    parts = []
    for x in v:
        if x is None:
            parts.append("NULL")
        elif nested:
            parts.append(_render_list(x, elem.value_type, tz))
        else:
            parts.append(_quote_elem(_render(x, elem, tz)))
    return "{" + ",".join(parts) + "}"


def render_arrow(tbl: pa.Table, tzname: str) -> list:
    """An Arrow table from DuckDB -> rows of PostgreSQL text."""
    tz = ZoneInfo(tzname)
    cols = []
    for i in range(tbl.num_columns):
        col, t = tbl.column(i), tbl.schema.field(i).type
        if pa.types.is_timestamp(t) or pa.types.is_time64(t):
            vals = col.cast(pa.int64()).to_pylist()
        elif pa.types.is_date32(t) or pa.types.is_time32(t):
            vals = col.cast(pa.int32()).to_pylist()
        else:
            vals = col.to_pylist()
        cols.append([_render(v, t, tz) for v in vals])
    return list(zip(*cols)) if cols else []


# ------------------------------------------------------------- comparison

_SPECIAL = {"nan": "nan", "infinity": "inf", "-infinity": "-inf", "inf": "inf", "-inf": "-inf"}
_TEMPORAL = {"date", "timestamp", "timestamptz"}
_OFFSET = re.compile(r"[+-]\d\d(:\d\d){0,2}$")

STRICT, NOISE, LOOSE, DIFF = 0, 1, 2, 3


def _dec(s: str):
    k = _SPECIAL.get(s.lower())
    if k:
        return k
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _temporal_norm(s: str, tok: str) -> str:
    if tok == "timestamptz":
        s = _OFFSET.sub("", s)
    return s[:-9] if s.endswith(" 00:00:00") else s


def _json_norm(s: str):
    try:
        return json.dumps(json.loads(s), sort_keys=True, ensure_ascii=False)
    except (ValueError, RecursionError):
        return None


def cell(a, b, ta: str, tb: str) -> int:
    """How two rendered cells relate: STRICT (same text), NOISE (floats within
    FLOAT_RTOL), LOOSE (same value, rendered or typed differently), DIFF."""
    if a is None or b is None:
        return STRICT if a is b else DIFF
    if a == b:
        return STRICT
    if _is_num(ta) and _is_num(tb):
        x, y = _dec(a), _dec(b)
        if x is None or y is None or isinstance(x, str) or isinstance(y, str):
            return STRICT if x == y and x is not None else DIFF
        both_float = ta in _FLOATS and tb in _FLOATS
        if ta in _FLOATS or tb in _FLOATS:
            if x == y:
                return STRICT if both_float else LOOSE
            if math.isclose(float(x), float(y), rel_tol=FLOAT_RTOL, abs_tol=0.0):
                return NOISE if both_float else LOOSE
            return DIFF
        return LOOSE if x == y else DIFF
    if "json" in (ta, tb):
        x, y = _json_norm(a), _json_norm(b)
        return LOOSE if x is not None and x == y else DIFF
    if ta in _TEMPORAL and tb in _TEMPORAL and ta != tb:
        return LOOSE if _temporal_norm(a, ta) == _temporal_norm(b, tb) else DIFF
    return DIFF


def _sort_key(v, ta: str, tb: str):
    """A key under which loosely-equal cells sort together."""
    if v is None:
        return (1, "")                      # NULLs last; any consistent place will do
    if _is_num(ta) and _is_num(tb):
        x = _dec(v)
        if isinstance(x, str):
            return (0, {"-inf": -math.inf, "inf": math.inf, "nan": math.inf}[x], x)
        if x is not None:
            f = float(x)
            return (0, float("%.10e" % f) if f == f and abs(f) != math.inf else f, "")
    if "json" in (ta, tb):
        return (0, 0.0, _json_norm(v) or v)
    if ta in _TEMPORAL and tb in _TEMPORAL and ta != tb:
        return (0, 0.0, _temporal_norm(_OFFSET.sub("", v), "x"))
    return (0, 0.0, v)


def _walk(pg_rows, dk_rows, tp, td):
    """Worst cell relation over aligned rows, the count of differing rows, and
    the first differing cell."""
    worst, bad, first = STRICT, 0, None
    for r, (pr, dr) in enumerate(zip(pg_rows, dk_rows)):
        row_bad = False
        for c, (a, b) in enumerate(zip(pr, dr)):
            k = cell(a, b, tp[c], td[c])
            if k > worst:
                worst = k
            if k == DIFF:
                row_bad = True
                if first is None:
                    first = {"row": r, "col": c, "source": a, "mirror": b}
        bad += row_bad
    return worst, bad, first


def compare(pg: Result, dk: Result, ordered: bool) -> dict:
    """Classify one statement. `pg` is the reference."""
    out = {"class": None, "sub": None, "ordered": ordered, "names_differ": False,
           "type_diffs": [], "float_noise": False}
    if pg.error and dk.error:
        out["class"] = ERR_BOTH
        return out
    if dk.error:
        out["class"] = ERR_MIRROR
        return out
    if pg.error:
        out["class"] = ERR_SOURCE
        return out
    out["rows"] = [len(pg.rows), len(dk.rows)]
    if len(pg.names) != len(dk.names):
        out.update({"class": DIFF_VALUE, "sub": "column count",
                    "example": {"source": len(pg.names), "mirror": len(dk.names)}})
        return out
    out["names_differ"] = pg.names != dk.names
    if out["names_differ"]:
        out["names"] = [[a, b] for a, b in zip(pg.names, dk.names) if a != b]
    for n, a, b in zip(pg.names, pg.types, dk.types):
        if not types_equal(a, b):
            out["type_diffs"].append({"col": n, "source": a, "mirror": b, "label": type_label(a, b)})
    if len(pg.rows) != len(dk.rows):
        out.update({"class": DIFF_VALUE, "sub": "row count",
                    "example": {"source": len(pg.rows), "mirror": len(dk.rows)}})
        return out

    tp, td = pg.types, dk.types

    def sorted_rows(rows):
        return sorted(rows, key=lambda r: tuple(_sort_key(v, tp[c], td[c]) for c, v in enumerate(r)))

    if ordered:
        worst, bad, first = _walk(pg.rows, dk.rows, tp, td)
        if worst == DIFF:
            w2, _, _ = _walk(sorted_rows(pg.rows), sorted_rows(dk.rows), tp, td)
            out["sub"] = "row order" if w2 != DIFF else "cell values"
    else:
        worst, bad, first = _walk(sorted_rows(pg.rows), sorted_rows(dk.rows), tp, td)
        if worst == DIFF:
            out["sub"] = "cell values"
    if worst == DIFF:
        first["col"] = pg.names[first["col"]]
        out.update({"class": DIFF_VALUE, "rows_differing": bad, "example": first})
        return out
    out["float_noise"] = worst == NOISE
    if worst == LOOSE or out["type_diffs"]:
        out["class"] = DIFF_TYPE
        if worst == LOOSE:
            out["sub"] = "rendering differs"
    elif out["names_differ"]:
        out["class"] = DIFF_NAME
    else:
        out["class"] = EXACT
    return out
