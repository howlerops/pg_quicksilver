"""Publish a quicksilver mirror table as a DuckLake table, without rewriting it.

One call to Publisher.publish() brings the DuckLake table to the state the
mirror's manifest (state.json) names, as ONE DuckLake snapshot. Data files are
hard-linked into the lake's data directory, never copied: the link is what lets
time travel survive the mirror removing its own name for a file after a merge
or a rewrite.

Two ways of getting there, because the first one turned out not to be enough:

mode="sql"      DuckLake's public SQL only, in one transaction.
                  new file      CALL ducklake_add_data_files(...)
                  vector        DELETE ... WHERE filename = F AND
                                file_row_number IN (SELECT p FROM vector)
                  dropped file  DELETE ... WHERE filename = F
                Correct at every snapshot, but a dropped file that already had
                deletes is NOT retired: DuckLake records the rest of its rows
                as more deletes and keeps listing the file. Every merge leaves
                such files behind for readers to open.

mode="catalog"  the catalog tables written directly, per the specification, in
                one transaction on the catalog database.
                  new file      ducklake_data_file + ducklake_file_column_stats
                                rows, taken from a scratch in-memory lake that
                                ducklake_add_data_files was run against, so the
                                statistics are DuckLake's own
                  vector        converted to DuckLake's delete-file layout
                                (file_path, pos) and registered in
                                ducklake_delete_file; the previous one is ended
                  dropped file  end_snapshot on the file and its delete file

What both refuse: a manifest that names column-partial patch files, and a
halted mirror. See README.md.
"""
import json
import os
import time

import duckdb

MD = "__ducklake_metadata_lake"
SMD = "__ducklake_metadata_scratch"


class Unpublishable(Exception):
    pass


def duck_type(pg):
    s = pg.lower().strip()
    if s.endswith("[]"):
        return "VARCHAR"
    if s.startswith(("numeric", "decimal")):
        return "DECIMAL" + (s[s.index("("):] if "(" in s else "(38,9)")
    if s in ("smallint", "int", "integer", "bigint", "int2", "int4", "int8"):
        return "BIGINT"
    if s in ("bool", "boolean"):
        return "BOOLEAN"
    if s.startswith("double") or s == "real":
        return "DOUBLE"
    if s in ("timestamptz", "timestamp with time zone"):
        return "TIMESTAMPTZ"
    if s in ("timestamp", "timestamp without time zone"):
        return "TIMESTAMP"
    if s == "date":
        return "DATE"
    return "VARCHAR"  # uuid, text, json: the mirror stores these as strings


def lit(s):
    return "'" + str(s).replace("'", "''") + "'"


def ident(s):
    return '"' + s.replace('"', '""') + '"'


class Publisher:
    def __init__(self, mirror_dir, catalog, data_path, table, columns, mode="catalog"):
        """columns: [(name, postgres type)] in order — the sidecar knows these
        from the source catalog; the manifest does not carry them."""
        self.dir = mirror_dir
        self.table = table
        self.columns = columns
        self.mode = mode
        self.data_path = os.path.abspath(data_path)
        os.makedirs(self.data_path, exist_ok=True)
        self.con = c = duckdb.connect()
        c.execute("LOAD ducklake")
        c.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH {lit(self.data_path + '/')})")
        if not self._lake_cols("lake"):
            cols = ", ".join(f"{ident(n)} {duck_type(t)}" for n, t in columns)
            c.execute(f"CREATE TABLE lake.{ident(table)} ({cols})")
        self.tid = self._tid(MD)
        if mode == "catalog":
            self._scratch()

    def _lake_cols(self, db):
        return [r[0] for r in self.con.execute(
            "select column_name from information_schema.columns where table_catalog=? and table_name=? "
            "order by ordinal_position", [db, self.table]).fetchall()]

    def _tid(self, md):
        return self.con.execute(f"select table_id from {md}.ducklake_table where table_name=? "
                                "and end_snapshot is null", [self.table]).fetchone()[0]

    def _scratch(self):
        """A throwaway in-memory lake with the same table. Its only job is to
        let DuckLake itself read a Parquet footer and say what the catalog rows
        for that file are."""
        c = self.con
        c.execute(f"ATTACH 'ducklake::memory:' AS scratch (DATA_PATH {lit(self.data_path + '/')})")
        cols = c.execute("select column_name, data_type from information_schema.columns where table_catalog='lake' "
                         "and table_name=? order by ordinal_position", [self.table]).fetchall()
        c.execute(f"CREATE TABLE scratch.{ident(self.table)} ({', '.join(f'{ident(n)} {t}' for n, t in cols)})")
        self.stid = self._tid(SMD)
        ids = "select column_name, column_id from {}.ducklake_column where table_id = {} and end_snapshot is null order by 1"
        if c.execute(ids.format(MD, self.tid)).fetchall() != c.execute(ids.format(SMD, self.stid)).fetchall():
            raise RuntimeError("scratch lake assigned different column ids; statistics cannot be carried over")
        self.sfile = {}   # lake path -> scratch data_file_id
        self.mapping = {}  # scratch mapping_id -> catalog mapping_id
        # The table-level min/max the catalog carries must cover every live
        # file, so a restarted publisher shows the scratch lake all of them.
        for (p,) in c.execute(f"select path from {MD}.ducklake_data_file where table_id = {self.tid} "
                              "and end_snapshot is null").fetchall():
            self._scratch_add(p)

    def _scratch_add(self, path):
        c = self.con
        c.execute(f"CALL ducklake_add_data_files('scratch', {lit(self.table)}, {lit(path)}, allow_missing => true)")
        self.sfile[path] = c.execute(f"select max(data_file_id) from {SMD}.ducklake_data_file").fetchone()[0]

    def _state(self):
        for _ in range(50):
            try:
                with open(os.path.join(self.dir, "state.json")) as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                time.sleep(0.01)
        raise RuntimeError("no readable manifest")

    def _dv_path(self, rel, gen):
        stem = os.path.basename(rel)[: -len(".parquet")]
        p = os.path.join(self.dir, "dv", f"{stem}.{gen:09d}.dv.parquet")
        if not os.path.exists(p):
            raise FileNotFoundError(p)  # stale manifest, or a legacy .dv.json
        return p

    def _lake_path(self, rel):
        return os.path.join(self.data_path, rel.replace("/", "-"))

    def _link(self, rel):
        dst = self._lake_path(rel)
        if os.path.exists(dst):
            os.remove(dst)
        os.link(os.path.join(self.dir, rel), dst)  # ENOENT = stale manifest; the caller retries
        return dst

    def publish(self):
        t = {}
        t0 = time.perf_counter()
        st = self._state()
        if st.get("halted"):
            raise Unpublishable("mirror is halted: " + st["halted"])
        if st.get("partial_cols"):
            raise Unpublishable(
                "manifest names column-partial patch files %s; a DuckLake data file is whole rows"
                % sorted(st["partial_cols"]))
        c = self.con
        want = {}
        for d, key in (("base", "base_files"), ("delta", "delta_files")):
            for f in st.get(key) or []:
                want[f"{d}/{f}"] = (st.get("dv_gen") or {}).get(f[: -len(".parquet")], 0)

        # What is published is read back from the lake: the manifest recorded
        # in the last snapshot's commit_extra_info. A side table in the catalog
        # database would be simpler and, in sql mode, cannot be used — DuckDB
        # lets one transaction write to the lake or to its metadata database,
        # not both, so it could not commit with the snapshot it describes.
        last = c.execute(
            f"select commit_extra_info from {MD}.ducklake_snapshot_changes "
            "where author = 'qs-mirror' order by snapshot_id desc limit 1").fetchone()
        have = json.loads(last[0])["dv_gen"] if last else {}

        add = [r for r in want if r not in have]
        drop = [r for r in have if r not in want]
        dvs = [r for r in want if want[r] and want[r] != have.get(r, 0)]
        lake_cols = self._lake_cols("lake")
        new_cols = [(n, ty) for n, ty in self.columns if n not in lake_cols]
        res = {"seq": st["seq"], "added": len(add), "dropped": len(drop), "dv_changed": len(dvs),
               "files": len(want), "snapshot": None}
        if not (add or drop or dvs or new_cols):
            res["ms"] = (time.perf_counter() - t0) * 1000
            return res
        t["plan"] = time.perf_counter() - t0
        extra = json.dumps({"seq": st["seq"], "applied_lsn": st["applied_lsn"], "dv_gen": want})
        msg = f"manifest seq {st['seq']}"

        # ADD COLUMN ... DEFAULT records the default as the column's
        # initial_default, which is what DuckLake serves for a file that
        # predates the column: attmissingval, by another name.
        alters = []
        for n, ty in new_cols:
            mv = (st.get("missing_vals") or {}).get(n)
            dflt = f" DEFAULT CAST({lit(mv)} AS {duck_type(ty)})" if mv is not None else ""
            alters.append(f"ADD COLUMN {ident(n)} {duck_type(ty)}{dflt}")

        if self.mode == "sql":
            self._publish_sql(want, add, drop, dvs, alters, msg, extra, t)
        else:
            # A schema change is its own snapshot here. It is harmless to see
            # alone: every existing row reads the new column as its default.
            for a in alters:
                c.execute(f"ALTER TABLE lake.{ident(self.table)} {a}")
                c.execute(f"ALTER TABLE scratch.{ident(self.table)} {a}")
            if add or drop or dvs:
                self._publish_catalog(want, add, drop, dvs, msg, extra, t)
        res["snapshot"] = c.execute(f"select max(snapshot_id) from {MD}.ducklake_snapshot").fetchone()[0]
        res["ms"] = (time.perf_counter() - t0) * 1000
        res.update({k + "_ms": v * 1000 for k, v in t.items()})
        return res

    def _publish_sql(self, want, add, drop, dvs, alters, msg, extra, t):
        c = self.con
        tbl = f"lake.{ident(self.table)}"
        c.execute("BEGIN")
        try:
            for a in alters:
                c.execute(f"ALTER TABLE {tbl} {a}")
            t1 = time.perf_counter()
            for rel in add:
                c.execute(f"CALL ducklake_add_data_files('lake', {lit(self.table)}, {lit(self._link(rel))}, "
                          "allow_missing => true)")
            t["add"] = time.perf_counter() - t1
            t1 = time.perf_counter()
            for rel in dvs:
                # Positions already deleted are not there to match, so the
                # whole vector is handed over every time.
                c.execute(f"DELETE FROM {tbl} WHERE filename = {lit(self._lake_path(rel))} AND file_row_number IN "
                          f"(SELECT p FROM read_parquet({lit(self._dv_path(rel, want[rel]))}))")
            t["delete"] = time.perf_counter() - t1
            t1 = time.perf_counter()
            for rel in drop:
                c.execute(f"DELETE FROM {tbl} WHERE filename = {lit(self._lake_path(rel))}")
            t["drop"] = time.perf_counter() - t1
            t1 = time.perf_counter()
            c.execute("CALL lake.set_commit_message('qs-mirror', ?, extra_info => ?)", [msg, extra])
            c.execute("COMMIT")
            t["commit"] = time.perf_counter() - t1
        except Exception:
            c.execute("ROLLBACK")
            raise

    def _publish_catalog(self, want, add, drop, dvs, msg, extra, t):
        c = self.con
        tid = self.tid
        t1 = time.perf_counter()
        for rel in add:
            self._scratch_add(self._link(rel))
        t["add"] = time.perf_counter() - t1

        # Delete files are written before the transaction that names them, as
        # the data files were: a file nobody names is invisible.
        t1 = time.perf_counter()
        dfiles = {}
        for rel in dvs:
            out = self._lake_path(rel)[: -len(".parquet")] + f".g{want[rel]}-delete.parquet"
            c.execute(f"COPY (SELECT {lit(self._lake_path(rel))} AS file_path, p AS pos FROM "
                      f"read_parquet({lit(self._dv_path(rel, want[rel]))})) TO {lit(out)} (FORMAT parquet)")
            n, groups, footer = c.execute(
                f"select num_rows, num_row_groups, footer_size from parquet_file_metadata({lit(out)})").fetchone()
            dfiles[rel] = (out, n, os.path.getsize(out), footer, groups)
        t["delete"] = time.perf_counter() - t1

        t1 = time.perf_counter()
        c.execute("BEGIN")
        try:
            snap, sv, ncat, nfile = c.execute(
                f"select snapshot_id + 1, schema_version, next_catalog_id, next_file_id from {MD}.ducklake_snapshot "
                "order by snapshot_id desc limit 1").fetchone()
            ids = {p: i for p, i in c.execute(
                f"select path, data_file_id from {MD}.ducklake_data_file where table_id = {tid} "
                "and end_snapshot is null").fetchall()}
            stats = c.execute(f"select next_row_id from {MD}.ducklake_table_stats where table_id = {tid}").fetchone()
            if stats is None:
                c.execute(f"insert into {MD}.ducklake_table_stats values ({tid}, 0, 0, 0)")
            next_row = stats[0] if stats else 0
            rows = size = 0
            for rel in add:
                path = self._lake_path(rel)
                sid = self.sfile[path]
                rc, sz, smap = c.execute(f"select record_count, file_size_bytes, mapping_id from "
                                         f"{SMD}.ducklake_data_file where data_file_id = {sid}").fetchone()
                if smap is not None and smap not in self.mapping:
                    # A mapping id is drawn from the same counter as file ids.
                    # DuckLake only looks for mappings newer than the last
                    # next_file_id it saw; one numbered any other way reads as
                    # "Unknown name map id".
                    m, nfile = nfile, nfile + 1
                    c.execute(f"insert into {MD}.ducklake_column_mapping select * replace ({m} as mapping_id, "
                              f"{tid} as table_id) from {SMD}.ducklake_column_mapping where mapping_id = {smap}")
                    c.execute(f"insert into {MD}.ducklake_name_mapping select * replace ({m} as mapping_id) "
                              f"from {SMD}.ducklake_name_mapping where mapping_id = {smap}")
                    self.mapping[smap] = m
                c.execute(
                    f"insert into {MD}.ducklake_data_file select * replace ({nfile} as data_file_id, {tid} as table_id, "
                    f"{snap} as begin_snapshot, {next_row} as row_id_start, "
                    f"{self.mapping.get(smap, 'NULL')} as mapping_id) "
                    f"from {SMD}.ducklake_data_file where data_file_id = {sid}")
                c.execute(
                    f"insert into {MD}.ducklake_file_column_stats select * replace ({nfile} as data_file_id, "
                    f"{tid} as table_id) from {SMD}.ducklake_file_column_stats where data_file_id = {sid}")
                ids[path] = nfile
                nfile += 1
                next_row += rc
                rows += rc
                size += sz
            for rel in dvs:
                out, n, sz, footer, groups = dfiles[rel]
                did = ids[self._lake_path(rel)]
                c.execute(f"update {MD}.ducklake_delete_file set end_snapshot = {snap} "
                          f"where data_file_id = {did} and end_snapshot is null")
                c.execute(f"insert into {MD}.ducklake_delete_file (delete_file_id, table_id, begin_snapshot, "
                          "data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, "
                          f"row_group_count) values ({nfile}, {tid}, {snap}, {did}, {lit(out)}, false, 'parquet', "
                          f"{n}, {sz}, {footer}, {groups})")
                nfile += 1
            if drop:
                gone = ", ".join(str(ids[self._lake_path(rel)]) for rel in drop)
                c.execute(f"update {MD}.ducklake_data_file set end_snapshot = {snap} where data_file_id in ({gone})")
                c.execute(f"update {MD}.ducklake_delete_file set end_snapshot = {snap} "
                          f"where data_file_id in ({gone}) and end_snapshot is null")
            if add:
                c.execute(f"update {MD}.ducklake_table_stats set record_count = record_count + {rows}, "
                          f"next_row_id = {next_row}, file_size_bytes = file_size_bytes + {size} where table_id = {tid}")
                # Bounds over every file the scratch lake has seen: a superset
                # of the live ones, so never too narrow.
                c.execute(f"delete from {MD}.ducklake_table_column_stats where table_id = {tid}")
                c.execute(f"insert into {MD}.ducklake_table_column_stats select * replace ({tid} as table_id) "
                          f"from {SMD}.ducklake_table_column_stats where table_id = {self.stid}")
            changes = ",".join(x for x, on in ((f"inserted_into_table:{tid}", add),
                                               (f"deleted_from_table:{tid}", dvs or drop)) if on)
            c.execute(f"insert into {MD}.ducklake_snapshot values ({snap}, now(), {sv}, {ncat}, {nfile})")
            c.execute(f"insert into {MD}.ducklake_snapshot_changes (snapshot_id, changes_made, author, "
                      "commit_message, commit_extra_info) values (?, ?, 'qs-mirror', ?, ?)",
                      [snap, changes, msg, extra])
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        t["commit"] = time.perf_counter() - t1
