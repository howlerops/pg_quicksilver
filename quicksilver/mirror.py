"""The columnar mirror: writer, deletion vectors, compaction, verification.

Implements the storage design in docs/04 *as corrected by measurement*:
deletes and superseded rows are resolved by **deletion vectors** computed at
write/compaction time, not by a query-time `_lsn` window function. The original
design measured 33x even at zero deltas (docs/11 Result 5); deletion vectors
measured 0.8-1.1x.

On-disk layout per mirrored table:

    <root>/<schema.table>/
      base/000001.parquet          compacted data
      delta/000007.parquet         recent micro-batches, append-only
      dv/000001.dv.json            deleted row POSITIONS in the matching base file
      index/000001.idx.json        key -> row position, needed to build a dv
      state.json                   applied_lsn + file manifest

The key->position index is the part that needs real engineering at scale; here
it is a dict on disk, which is honest for a prototype and would not survive
production volumes.

NOTE: prototype. Production belongs in Go/Rust.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .changestream import Change, Transaction, lsn_to_int


def arrow_schema(columns: dict[str, str]) -> pa.Schema:
    """Map declared Postgres types to a FIXED Arrow schema.

    Two bugs this prevents, both found by running the slice:
      * inferring per batch makes an all-NULL column `null`-typed, so schemas
        drift between delta files and the UNION ALL read path breaks;
      * `numeric` must be decimal, not float — wal2json emits it as a JSON
        number unless asked otherwise, which silently rounds money.
    """
    fields = []
    for name, pgtype in columns.items():
        t = pgtype.lower().strip()
        if t.startswith("numeric") or t.startswith("decimal"):
            if "(" in t:
                prec, scale = (int(x) for x in t[t.index("(") + 1:t.index(")")].split(","))
            else:
                prec, scale = 38, 9
            at = pa.decimal128(prec, scale)
        elif t in ("smallint", "int", "integer", "bigint", "int2", "int4", "int8"):
            at = pa.int64()
        elif t in ("bool", "boolean"):
            at = pa.bool_()
        elif t.startswith("double") or t == "real":
            at = pa.float64()
        elif t.startswith("timestamp"):
            at = pa.timestamp("us", tz="UTC") if "with time zone" in t or t.endswith("tz") \
                 else pa.timestamp("us")
        else:
            at = pa.string()
        fields.append(pa.field(name, at))
    return pa.schema(fields)


def _coerce(value, field: pa.Field):
    if value is None:
        return None
    if pa.types.is_decimal(field.type):
        return Decimal(str(value))
    if pa.types.is_integer(field.type):
        return int(value)
    if pa.types.is_boolean(field.type):
        return value if isinstance(value, bool) else str(value).lower() in ("t", "true", "1")
    if pa.types.is_string(field.type):
        return str(value)
    return value


@dataclass
class TableState:
    applied_lsn: str = "0/0"
    base_files: list[str] = field(default_factory=list)
    delta_files: list[str] = field(default_factory=list)
    seq: int = 0


class TableMirror:
    """One mirrored table."""

    def __init__(self, root: Path, schema: str, table: str, key: str, columns: dict[str, str]):
        self.qualified = f"{schema}.{table}"
        self.key = key
        self.columns = columns                      # name -> postgres type
        self.dir = Path(root) / self.qualified
        for sub in ("base", "delta", "dv", "index"):
            (self.dir / sub).mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    # ---- state ---------------------------------------------------------
    def _state_path(self) -> Path:
        return self.dir / "state.json"

    def _load_state(self) -> TableState:
        p = self._state_path()
        if p.exists():
            return TableState(**json.loads(p.read_text()))
        return TableState()

    def _save_state(self) -> None:
        """Durable watermark. Written AFTER the data files are fsynced, so a
        crash replays the last batch rather than skipping it."""
        tmp = self._state_path().with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state.__dict__, indent=2))
        fd = os.open(tmp, os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)
        tmp.replace(self._state_path())

    # ---- write path ----------------------------------------------------
    def apply(self, txns: list[Transaction]) -> dict:
        """Apply a run of whole transactions atomically.

        Every change in every transaction lands, then applied_lsn advances.
        There is no state in which half a transaction is visible.
        """
        upserts: dict = {}          # key -> row (last write wins within the batch)
        deletes: set = set()
        last_lsn = self.state.applied_lsn

        for txn in txns:
            for c in txn.changes:
                if c.qualified != self.qualified:
                    continue
                if c.op == "insert" or c.op == "update":
                    k = c.row[self.key]
                    upserts[k] = c.row
                    deletes.discard(k)
                elif c.op == "delete":
                    k = (c.key or {})[self.key]
                    deletes.add(k)
                    upserts.pop(k, None)
                elif c.op == "truncate":
                    self._truncate()
                    upserts.clear()
                    deletes.clear()
            last_lsn = txn.commit_lsn

        # An update supersedes the old row: tombstone it in base, re-insert in delta.
        superseded = set(upserts) | deletes

        if superseded:
            self._extend_deletion_vectors(superseded)
        if upserts:
            self._write_delta(list(upserts.values()))

        self.state.applied_lsn = last_lsn
        self._save_state()
        return {"upserts": len(upserts), "deletes": len(deletes), "applied_lsn": last_lsn}

    def _write_delta(self, rows: list[dict]) -> None:
        self.state.seq += 1
        name = f"{self.state.seq:06d}.parquet"
        schema = arrow_schema(self.columns)
        table = pa.Table.from_pylist(
            [{f.name: _coerce(r.get(f.name), f) for f in schema} for r in rows],
            schema=schema)
        pq.write_table(table, self.dir / "delta" / name, compression="zstd")
        self.state.delta_files.append(name)

    def _extend_deletion_vectors(self, keys: set) -> None:
        """Mark row positions dead in each base file. This is the work that
        query time does NOT have to do — the whole point of the correction."""
        for base in self.state.base_files:
            idx_path = self.dir / "index" / f"{Path(base).stem}.idx.json"
            if not idx_path.exists():
                continue
            index = json.loads(idx_path.read_text())
            hits = [index[str(k)] for k in keys if str(k) in index]
            if not hits:
                continue
            dv_path = self.dir / "dv" / f"{Path(base).stem}.dv.json"
            existing = set(json.loads(dv_path.read_text())) if dv_path.exists() else set()
            dv_path.write_text(json.dumps(sorted(existing | set(hits))))

        # deltas are small and newest-wins within a batch; drop superseded rows
        # from pending deltas by rewriting them (cheap at micro-batch size)
        for dfile in list(self.state.delta_files):
            p = self.dir / "delta" / dfile
            t = pq.read_table(p)
            if t.num_rows == 0:
                continue
            col = t.column(self.key).to_pylist()
            keep = [i for i, k in enumerate(col) if k not in keys]
            if len(keep) == t.num_rows:
                continue
            if not keep:
                # every row superseded — drop the file rather than take([]),
                # which infers null-typed indices and raises
                p.unlink()
                self.state.delta_files.remove(dfile)
                continue
            pq.write_table(t.take(pa.array(keep, type=pa.int64())), p, compression="zstd")

    def _truncate(self) -> None:
        for sub in ("base", "delta", "dv", "index"):
            for f in (self.dir / sub).iterdir():
                f.unlink()
        self.state.base_files.clear()
        self.state.delta_files.clear()

    # ---- compaction ----------------------------------------------------
    def compact(self, con: duckdb.DuckDBPyConnection) -> dict:
        """Fold base + deltas into one base file, applying deletion vectors,
        and rebuild the key->position index. Query cost is a function of how
        far behind this runs, so compaction lag is a first-class metric."""
        if not (self.state.base_files or self.state.delta_files):
            return {"rows": 0, "base_file": None}      # nothing to compact yet
        rows = con.execute(f"SELECT * FROM {self._live_sql()}").fetch_arrow_table()
        for sub in ("base", "delta", "dv", "index"):
            for f in (self.dir / sub).iterdir():
                f.unlink()
        self.state.seq += 1
        name = f"{self.state.seq:06d}.parquet"
        pq.write_table(rows, self.dir / "base" / name, compression="zstd")
        self.state.base_files = [name]
        self.state.delta_files = []

        keys = rows.column(self.key).to_pylist()
        (self.dir / "index" / f"{Path(name).stem}.idx.json").write_text(
            json.dumps({str(k): i for i, k in enumerate(keys)}))
        self._save_state()
        return {"rows": rows.num_rows, "base_file": name}

    # ---- read path -----------------------------------------------------
    def _live_sql(self) -> str:
        """SQL for the current logical contents: base files minus their
        deletion vectors, union the deltas. No window function, no global
        dedup — that is the docs/11 Result 5 correction."""
        parts = []
        for base in self.state.base_files:
            p = self.dir / "base" / base
            dv_path = self.dir / "dv" / f"{Path(base).stem}.dv.json"
            cols = ", ".join(f'"{c}"' for c in self.columns)
            if dv_path.exists():
                dead = json.loads(dv_path.read_text())
                if dead:
                    lst = ",".join(str(d) for d in dead)
                    parts.append(
                        f"(SELECT {cols} FROM (SELECT *, (row_number() OVER ())-1 AS _pos "
                        f"FROM read_parquet('{p}')) WHERE _pos NOT IN ({lst}))")
                    continue
            parts.append(f"(SELECT {cols} FROM read_parquet('{p}'))")
        for d in self.state.delta_files:
            p = self.dir / "delta" / d
            cols = ", ".join(f'"{c}"' for c in self.columns)
            parts.append(f"(SELECT {cols} FROM read_parquet('{p}'))")
        if not parts:
            typed = ", ".join(f'NULL::VARCHAR AS "{c}"' for c in self.columns)
            return f"(SELECT {typed} WHERE false)"
        return "(" + " UNION ALL ".join(parts) + ")"

    def live_view_sql(self, name: str) -> str:
        return f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {self._live_sql()}"

    # ---- verification (goal G7) ----------------------------------------
    def checksum(self, con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
        """(row_count, order-independent checksum) over the mirror."""
        cols = " || '|' || ".join(f'COALESCE("{c}"::VARCHAR,\'\')' for c in self.columns)
        r = con.execute(
            f"SELECT count(*), COALESCE(sum(hash({cols})::HUGEINT),0)::VARCHAR "
            f"FROM {self._live_sql()}").fetchone()
        return int(r[0]), int(r[1])


def source_checksum(pgcur, schema: str, table: str, columns: dict[str, str],
                    snapshot_lsn: str | None = None) -> tuple[int, int]:
    """Same checksum computed on the source. Equality is the divergence alarm.

    DuckDB's hash() and Postgres's hashtext() differ, so both sides are hashed
    by DuckDB over identical text encodings — the mirror is compared against a
    freshly-read copy of the source rather than against a Postgres-side hash.
    """
    cols = ", ".join(f'"{c}"' for c in columns)
    pgcur.execute(f"SELECT {cols} FROM {schema}.{table}")
    rows = pgcur.fetchall()
    con = duckdb.connect()
    arrow = pa.Table.from_pylist([dict(zip(columns, r)) for r in rows])
    con.register("src", arrow)
    expr = " || '|' || ".join(f'COALESCE("{c}"::VARCHAR,\'\')' for c in columns)
    r = con.execute(
        f"SELECT count(*), COALESCE(sum(hash({expr})::HUGEINT),0)::VARCHAR FROM src").fetchone()
    return int(r[0]), int(r[1])
