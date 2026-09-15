"""The internal change-stream interface.

This is the load-bearing design decision from docs/03: every ingest path —
logical replication now, physical-WAL-on-standby in phase 3, the archive tee
for backfill — produces the same `Transaction` objects, so the downstream
(batching, columnar write, compaction, verification) is written once.

Two invariants the rest of the system depends on:

  1. **Transactions are whole.** A Transaction is yielded only when its COMMIT
     has been seen. docs/06 section 2: if the mirror applies half a transaction
     that moved a row between tables, a join across them sees it twice or not
     at all. That bug class is unreproducible and appears weekly; it is designed
     out here rather than patched later.

  2. **Every transaction carries its commit LSN.** That is the durable
     watermark (`applied_lsn`), the ordering key, and the provenance handle for
     verification.

NOTE: prototype. The production implementation belongs in Go (CNPG-I is Go) or
Rust; this exists to prove the pipeline and pin the interface.
"""
from __future__ import annotations

import json
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Iterator, Literal, Sequence

Op = Literal["insert", "update", "delete", "truncate"]


def lsn_to_int(lsn: str) -> int:
    """'2/F9D4C6A8' -> comparable integer."""
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


@dataclass(frozen=True)
class Change:
    op: Op
    schema: str
    table: str
    lsn: str
    row: dict | None = None          # new tuple (insert/update)
    key: dict | None = None          # identity (update/delete)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass
class Transaction:
    commit_lsn: str
    changes: list[Change] = field(default_factory=list)

    @property
    def tables(self) -> set[str]:
        return {c.qualified for c in self.changes}

    def __len__(self) -> int:
        return len(self.changes)


class ChangeStream:
    """Front-end interface. Phase 3 swaps the implementation, not the caller."""

    def transactions(self) -> Iterator[Transaction]:
        raise NotImplementedError

    def confirm(self, lsn: str) -> None:
        """Tell the source everything through `lsn` is durably applied, so it
        may release WAL. Only call after the mirror write has been fsynced —
        confirming early is how you lose data on a crash."""
        raise NotImplementedError


class LogicalChangeStream(ChangeStream):
    """Postgres logical replication via wal2json (Path 2 in docs/03).

    Uses pg_logical_slot_get_changes() rather than the streaming replication
    protocol. That is a deliberate prototype simplification: it is pull-based
    and SQL-callable, so it needs no protocol implementation. It also caps
    latency at the poll interval, which is why the production version must use
    the streaming protocol.
    """

    def __init__(self, conn, slot: str, tables: Sequence[str], batch_bytes: int = 64 * 1024 * 1024):
        self.conn = conn
        self.slot = slot
        self.tables = set(tables)
        self.batch_bytes = batch_bytes
        self._pending_confirm: str | None = None

    def ensure_slot(self) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.slot,))
        if not cur.fetchone():
            cur.execute("SELECT pg_create_logical_replication_slot(%s,'wal2json')", (self.slot,))

    def drop_slot(self) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", (self.slot,))
        if cur.fetchone():
            cur.execute("SELECT pg_drop_replication_slot(%s)", (self.slot,))

    def slot_lag_bytes(self) -> int:
        cur = self.conn.cursor()
        cur.execute("""SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
                       FROM pg_replication_slots WHERE slot_name=%s""", (self.slot,))
        r = cur.fetchone()
        return int(r[0]) if r and r[0] is not None else 0

    def transactions(self) -> Iterator[Transaction]:
        """Yield whole transactions. `peek` rather than `get` so nothing is
        consumed until confirm() — a crash mid-write replays rather than loses."""
        cur = self.conn.cursor()
        cur.execute(
            """SELECT lsn, data FROM pg_logical_slot_peek_changes(
                   %s, NULL, NULL,
                   'format-version','2','include-lsn','true','include-transaction','true')""",
            (self.slot,),
        )
        txn: Transaction | None = None
        for _lsn, data in cur.fetchall():
            # parse_float=Decimal: wal2json emits numerics as JSON numbers, and
            # routing money through float64 silently rounds it. This keeps the
            # exact decimal text the plugin wrote. (wal2json 2.5 has no
            # numeric-as-string option, and this is better anyway — it needs no
            # plugin support.)
            msg = json.loads(data, parse_float=Decimal)
            action = msg["action"]
            if action == "B":
                txn = Transaction(commit_lsn=msg.get("lsn", ""))
            elif action == "C":
                if txn is not None:
                    txn.commit_lsn = msg.get("lsn", txn.commit_lsn)
                    self._pending_confirm = txn.commit_lsn
                    yield txn                      # only now — the COMMIT is seen
                    txn = None
            elif action in ("I", "U", "D", "T") and txn is not None:
                qualified = f"{msg.get('schema')}.{msg.get('table')}"
                if qualified not in self.tables:
                    continue
                txn.changes.append(_to_change(action, msg))

    def confirm(self, lsn: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """SELECT 1 FROM pg_logical_slot_get_changes(%s, %s, NULL,
                   'format-version','2','include-lsn','true','include-transaction','true')""",
            (self.slot, lsn),
        )
        cur.fetchall()


def _to_change(action: str, msg: dict) -> Change:
    cols = {c["name"]: c["value"] for c in msg.get("columns", [])}
    ident = {c["name"]: c["value"] for c in msg.get("identity", [])}
    op: Op = {"I": "insert", "U": "update", "D": "delete", "T": "truncate"}[action]
    return Change(
        op=op,
        schema=msg.get("schema", ""),
        table=msg.get("table", ""),
        lsn=msg.get("lsn", ""),
        row=cols or None,
        key=ident or None,
    )
