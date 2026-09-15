"""DDL handling — the part every logical-CDC pipeline leaks on.

`pgoutput`/`wal2json` do not replicate DDL. Every CDC product therefore hand-rolls
a schema-drift mechanism, and that is where they all fail: the pipeline keeps
running against a stale schema and silently writes wrong data.

The approach here is walshadow's catalog/data split (docs/02), adapted to logical
replication: an **event trigger** on the source records every DDL statement into
`quicksilver.ddl_log`, and that table is itself mirrored. So DDL arrives **inside
the change stream, in commit order**, and acts as a **barrier** — the ingest
drains everything before it, evolves the mirror schema, then continues. No
polling, no race between "schema changed" and "rows in the new shape arrived".

Policy, per docs/06: DDL we can apply faithfully is applied. Anything else stops
mirroring that table **loudly** rather than corrupting it quietly.
"""
from __future__ import annotations

from dataclasses import dataclass

# DDL we can apply to a columnar mirror without rewriting history.
#   ADD COLUMN    — old files lack it; the read path NULL-fills (see mirror._file_sql)
#   DROP COLUMN   — stop projecting it
#   RENAME COLUMN — project under the new name
SUPPORTED_TAGS = {"ALTER TABLE", "CREATE TABLE"}

SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS quicksilver;

CREATE TABLE IF NOT EXISTS quicksilver.ddl_log (
    id        bigserial PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT now(),
    tag       text NOT NULL,
    object    text,
    statement text
);

CREATE OR REPLACE FUNCTION quicksilver.on_ddl() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        -- never log our own bookkeeping, or the log recurses
        IF r.schema_name IS DISTINCT FROM 'quicksilver' THEN
            INSERT INTO quicksilver.ddl_log(tag, object, statement)
            VALUES (r.command_tag, r.object_identity, current_query());
        END IF;
    END LOOP;
END $$;

DROP EVENT TRIGGER IF EXISTS quicksilver_ddl;
CREATE EVENT TRIGGER quicksilver_ddl ON ddl_command_end EXECUTE FUNCTION quicksilver.on_ddl();
"""

TEARDOWN_SQL = """
DROP EVENT TRIGGER IF EXISTS quicksilver_ddl;
DROP SCHEMA IF EXISTS quicksilver CASCADE;
"""


@dataclass
class SchemaDiff:
    added: dict[str, str]
    dropped: list[str]
    retyped: dict[str, tuple[str, str]]

    @property
    def empty(self) -> bool:
        return not (self.added or self.dropped or self.retyped)

    @property
    def safe(self) -> bool:
        """A retype can silently change values (numeric -> text, widening a
        decimal). Adds and drops cannot."""
        return not self.retyped

    def describe(self) -> str:
        bits = []
        if self.added:
            bits.append("added " + ", ".join(f"{k} {v}" for k, v in self.added.items()))
        if self.dropped:
            bits.append("dropped " + ", ".join(self.dropped))
        if self.retyped:
            bits.append("RETYPED " + ", ".join(
                f"{k} {a}->{b}" for k, (a, b) in self.retyped.items()))
        return "; ".join(bits) or "no change"


def setup(cur) -> None:
    cur.execute(SETUP_SQL)


def teardown(cur) -> None:
    cur.execute(TEARDOWN_SQL)


def live_columns(cur, schema: str, table: str) -> dict[str, str]:
    """The source's current column list, in ordinal order — the authority."""
    cur.execute(
        """SELECT a.attname, format_type(a.atttypid, a.atttypmod)
           FROM pg_attribute a
           JOIN pg_class c ON c.oid = a.attrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = %s AND c.relname = %s
             AND a.attnum > 0 AND NOT a.attisdropped
           ORDER BY a.attnum""",
        (schema, table),
    )
    return {r[0]: r[1] for r in cur.fetchall()}


def diff(old: dict[str, str], new: dict[str, str]) -> SchemaDiff:
    return SchemaDiff(
        added={k: v for k, v in new.items() if k not in old},
        dropped=[k for k in old if k not in new],
        retyped={k: (old[k], new[k]) for k in old if k in new and old[k] != new[k]},
    )
