package ducklake

// The applied LSN of a table that has been quiet.
//
// qs_mirror_table.applied_lsn is the applied_lsn of the MANIFEST a lake table
// was last published from. That used to be how far the table had applied,
// because every Apply rewrote every manifest. It no longer is: an Apply that
// changes nothing in a table writes nothing for it, and the mirror root's
// watermark.json says instead "the tables named here, each with its manifest
// still at the LSN noted, have applied through W" (internal/mirror,
// watermark.go). A manifest that does not change is not submitted, so the
// catalog row of a quiet table stands at its last CHANGE, and a reader gating
// on it would see a table that is in fact current as stale by however long it
// has been quiet.
//
// Publishing a DuckLake snapshot for every watermark would put back exactly
// the per-tick, per-table write the watermark removed, and grow the snapshot
// list by one per tick for nothing a query can see. So the watermark is
// carried BESIDE the snapshots:
//
//	qs_mirror_watermark(table_name, manifest_lsn, applied_lsn, written_at)
//
// one row per table the watermark currently speaks for and takes further than
// its manifest, replaced as a whole in ONE small SQLite transaction of its
// own. No ducklake_* table is touched: no snapshot, no schema version, nothing
// DuckLake reads. The worker does it after its publishes and on its one-second
// tick, and only when watermark.json has changed, so the cost is at most one
// short transaction a second while the source is writing to OTHER tables and
// nothing at all while it is idle.
//
// THE RULE A READER APPLIES is the mirror's own (effectiveLSN), with the lake
// in the place of the manifest:
//
//	effective = w.applied_lsn  if w.manifest_lsn = qs_mirror_table.applied_lsn
//	            else qs_mirror_table.applied_lsn
//
// The equality is what makes it safe whichever of the two is behind. The lake
// lags the mirror (a publish is queued): then the lake table stands at an
// older manifest than the one the watermark saw, the LSNs differ, and the row
// is ignored. The watermark lags the lake (the table changed and was
// published since): the same. Only when the lake holds exactly the contents
// the watermark vouched for does its LSN apply. FreshnessSQL and ReadTable
// both do this, so `qs-query -ducklake -lsn` and anything running the printed
// freshness query see the effective LSN; a reader that selects applied_lsn
// from qs_mirror_table directly still sees the published manifest's, which is
// never too high.

import (
	"context"
	"database/sql"
	"maps"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const watermarkDDL = `CREATE TABLE IF NOT EXISTS qs_mirror_watermark(table_name VARCHAR PRIMARY KEY, manifest_lsn VARCHAR, applied_lsn VARCHAR, written_at VARCHAR)`

// SyncWatermark copies the mirror root's watermark into the catalog if it has
// changed since the last copy. The worker calls it; it is exported for a
// caller that wants the catalog current now (a test, a shutdown).
func (p *Publisher) SyncWatermark(ctx context.Context) error {
	p.work.Lock()
	defer p.work.Unlock()
	w, err := mirror.ReadWatermark(p.opt.MirrorRoot)
	if err != nil {
		return err
	}
	// Only what takes a table further than its own manifest is worth a row.
	rows := map[string]string{}
	lsn := ""
	if w != nil {
		lsn = w.AppliedLSN
		for q, manifestLSN := range w.Tables {
			if parseLSN(w.AppliedLSN) > parseLSN(manifestLSN) {
				rows[q] = manifestLSN
			}
		}
	}
	if p.wmSynced && lsn == p.wmLSN && maps.Equal(rows, p.wmRows) {
		return nil
	}
	if len(rows) == 0 && p.wmSynced && len(p.wmRows) == 0 {
		// Nothing was there and nothing is to be: an LSN alone is not a row.
		p.wmLSN = lsn
		return nil
	}
	tx, err := p.cat.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM qs_mirror_watermark`); err != nil {
		return err
	}
	at := p.opt.Now().UTC().Format(timeLayout)
	for q, manifestLSN := range rows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO qs_mirror_watermark VALUES (?, ?, ?, ?)`,
			q, manifestLSN, lsn, at); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	p.wmSynced, p.wmLSN, p.wmRows = true, lsn, rows
	p.mu.Lock()
	p.global.WatermarkSyncs++
	p.mu.Unlock()
	return nil
}

// effectiveLSN reads the watermark row that speaks for a table standing at
// manifestLSN, if there is one. A catalog from before the table existed, which
// only a publisher adds, has none.
func effectiveLSN(ctx context.Context, db *sql.DB, qualified, manifestLSN string) string {
	var lsn sql.NullString
	err := db.QueryRowContext(ctx, `SELECT applied_lsn FROM qs_mirror_watermark
		WHERE table_name = ? AND manifest_lsn = ?`, qualified, manifestLSN).Scan(&lsn)
	if err != nil || !lsn.Valid || parseLSN(lsn.String) <= parseLSN(manifestLSN) {
		return manifestLSN
	}
	return lsn.String
}
