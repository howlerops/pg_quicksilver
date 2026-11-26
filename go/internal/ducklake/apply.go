package ducklake

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// apply writes the rows of one snapshot that takes the lake table to the
// manifest. It returns the snapshot's change list, empty if there is nothing
// for a reader to see.
func (p *Publisher) apply(ctx context.Context, tx *sql.Tx, snap *head, work *lakeTable,
	m *mirror.LakeManifest, plan *publishPlan, res *Result, made *[]string) ([]string, error) {
	var changes []string
	schemaChanged := false
	q := m.Qualified()

	if work.schemaID < 0 {
		work.schemaID = snap.nextCatalogID
		snap.nextCatalogID++
		if err := insertSchema(ctx, tx, work.schemaID, snap.snapshot, work.schema); err != nil {
			return nil, err
		}
		changes = append(changes, "created_schema:"+quoteIdent(work.schema))
		schemaChanged = true
	}
	if work.hasView {
		// The table is back in service: the refusal comes down with it.
		if err := endView(ctx, tx, work.viewID, snap.snapshot); err != nil {
			return nil, err
		}
		changes = append(changes, fmt.Sprintf("dropped_view:%d", work.viewID))
		work.hasView, schemaChanged = false, true
	}
	created := false
	if !work.exists {
		work.id = snap.nextCatalogID
		snap.nextCatalogID++
		if err := insertTable(ctx, tx, work.id, snap.snapshot, work.schemaID, work.name); err != nil {
			return nil, err
		}
		work.exists, created = true, true
		work.cols, work.files, work.mappings = nil, map[string]*lakeFile{}, map[string]int64{}
		work.nextRowID = 0
		changes = append(changes, "created_table:"+quoteIdent(work.schema)+"."+quoteIdent(work.name))
		schemaChanged = true
		if err := os.MkdirAll(work.dir(p.dataDir), 0o755); err != nil {
			return nil, err
		}
		if err := syncDir(filepath.Dir(work.dir(p.dataDir))); err != nil {
			return nil, err
		}
	}

	// Columns, by name. A column DuckLake has and the manifest does not is
	// dropped; one the manifest has and DuckLake does not is added, with the
	// value older files read back as (attmissingval) as its initial default;
	// one whose type no longer covers its files is ended and begun again
	// under the same id, which is how DuckLake itself records ALTER TYPE.
	altered := false
	have := map[string]int{}
	for i, c := range work.cols {
		have[c.name] = i
	}
	want := map[string]bool{}
	var maxOrder int64
	for _, c := range work.cols {
		maxOrder = max(maxOrder, c.order)
	}
	var cols []lakeColumn
	for _, mc := range m.Columns {
		want[mc.Name] = true
		typ := plan.types[mc.Name]
		if i, ok := have[mc.Name]; ok {
			c := work.cols[i]
			if c.typ != typ {
				if err := endColumn(ctx, tx, work.id, c.id, snap.snapshot); err != nil {
					return nil, err
				}
				c.typ = typ
				if err := insertColumn(ctx, tx, work.id, snap.snapshot, c); err != nil {
					return nil, err
				}
				altered = true
			}
			cols = append(cols, c)
			continue
		}
		maxOrder++
		c := lakeColumn{id: work.nextColumnID, order: maxOrder, name: mc.Name, typ: typ, initial: mc.Missing}
		work.nextColumnID++
		if err := insertColumn(ctx, tx, work.id, snap.snapshot, c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
		altered = true
	}
	for _, c := range work.cols {
		if !want[c.name] {
			if err := endColumn(ctx, tx, work.id, c.id, snap.snapshot); err != nil {
				return nil, err
			}
			altered = true
		}
	}
	sort.SliceStable(cols, func(i, j int) bool { return cols[i].order < cols[j].order })
	work.cols = cols
	if altered {
		schemaChanged = true
		if !created {
			changes = append(changes, fmt.Sprintf("altered_table:%d", work.id))
		}
	}
	if schemaChanged {
		snap.schemaVersion++
		if err := insertSchemaVersion(ctx, tx, snap.snapshot, snap.schemaVersion, work.id); err != nil {
			return nil, err
		}
	}
	colID := map[string]int64{}
	for _, c := range work.cols {
		colID[c.name] = c.id
	}

	dir := work.dir(p.dataDir)
	inserted, deleted := false, false

	for _, lf := range plan.drops {
		if err := endDataFile(ctx, tx, lf.id, snap.snapshot); err != nil {
			return nil, err
		}
		delete(work.files, lf.rel)
		deleted = true
		res.Dropped++
	}

	type withDead struct {
		mf *mirror.LakeFile
		lf *lakeFile
	}
	var vectors []withDead
	for _, f := range plan.keeps {
		lf := work.files[f.mf.Rel]
		if f.mf.DVGen != lf.dvGen {
			vectors = append(vectors, withDead{f.mf, lf})
		}
	}
	for _, f := range plan.adds {
		lf := &lakeFile{id: snap.nextFileID, rel: f.mf.Rel, rows: f.info.rows, size: f.info.size,
			types: f.info.types(), legacyInf: f.info.legacyInfinity()}
		snap.nextFileID++
		lf.name = fmt.Sprintf("%s-%d.parquet",
			strings.ReplaceAll(strings.TrimSuffix(f.mf.Rel, ".parquet"), "/", "-"), lf.id)
		dst := filepath.Join(dir, lf.name)
		_ = os.Remove(dst) // only ever a leftover of a publish that did not commit
		if err := os.Link(f.src, dst); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("%w: %s", errStale, f.mf.Rel)
			}
			return nil, err
		}
		*made = append(*made, dst)

		// The file has no field ids, so it is mapped to columns by name. A
		// file column with no live column of that name (dropped since the
		// file was written) is left out, and is then not read at all.
		var names []string
		var fields []int64
		var stats []columnStat
		var sig strings.Builder
		for _, fc := range f.info.cols {
			id, ok := colID[fc.name]
			if !ok {
				continue
			}
			names, fields = append(names, fc.name), append(fields, id)
			fmt.Fprintf(&sig, "%q=%d;", fc.name, id)
			s := fc.columnStat
			s.columnID = id
			stats = append(stats, s)
		}
		mapping, ok := work.mappings[sig.String()]
		if !ok {
			// Drawn from the file-id counter: DuckLake looks for mappings it
			// has not seen by comparing with the last next_file_id it read,
			// and one numbered any other way is "Unknown name map id".
			mapping = snap.nextFileID
			snap.nextFileID++
			if err := insertMapping(ctx, tx, mapping, work.id, names, fields); err != nil {
				return nil, err
			}
			work.mappings[sig.String()] = mapping
		}
		if err := insertDataFile(ctx, tx, work.id, snap.snapshot, lf, f.info, work.nextRowID, mapping, stats); err != nil {
			return nil, err
		}
		work.nextRowID += f.info.rows
		work.files[lf.rel] = lf
		inserted = true
		res.Added++
		if f.mf.DVGen != 0 {
			vectors = append(vectors, withDead{f.mf, lf})
		}
	}

	for _, v := range vectors {
		if v.lf.hasDelete {
			if err := endDeleteFile(ctx, tx, v.lf.id, snap.snapshot); err != nil {
				return nil, err
			}
			v.lf.hasDelete = false
		}
		v.lf.dvGen = v.mf.DVGen
		deleted = true
		res.Deletes++
		if v.mf.DVGen == 0 {
			continue
		}
		id := snap.nextFileID
		snap.nextFileID++
		name := fmt.Sprintf("%s.g%d-%d%s", strings.TrimSuffix(v.lf.name, ".parquet"), v.mf.DVGen, id, deleteSuffix)
		dst := filepath.Join(dir, name)
		info, err := writeDeleteFile(dst, filepath.Join(dir, v.lf.name), v.mf.Dead, v.mf.DeadCount)
		if err != nil {
			return nil, err
		}
		*made = append(*made, dst)
		if err := insertDeleteFile(ctx, tx, id, work.id, snap.snapshot, v.lf.id, name, info, v.mf.DVGen); err != nil {
			return nil, err
		}
		v.lf.hasDelete = true
	}

	if inserted || len(plan.drops) > 0 || created {
		if err := setTableStats(ctx, tx, work.id, work.nextRowID); err != nil {
			return nil, err
		}
	}
	// The table's column statistics, restated from the files now live: a
	// publish that changed nothing about them writes the same rows again.
	if err := setTableColumnStats(ctx, tx, work); err != nil {
		return nil, err
	}
	if inserted {
		changes = append(changes, fmt.Sprintf("inserted_into_table:%d", work.id))
	}
	if deleted {
		changes = append(changes, fmt.Sprintf("deleted_from_table:%d", work.id))
	}
	_ = q
	return changes, nil
}

// refuse takes a table out of service in the lake: the table is ended, and a
// view of the same name whose only effect is to fail with the reason takes its
// place. A reader holding a name gets an error; it does not get the table as
// it stood when publishing stopped, which is the failure a halt exists for.
func (p *Publisher) refuse(ctx context.Context, tx *sql.Tx, snap *head, work *lakeTable, q, reason string) ([]string, error) {
	var changes []string
	text := refusedSQL(q, reason)
	if work.exists {
		if err := endTable(ctx, tx, work.id, snap.snapshot); err != nil {
			return nil, err
		}
		changes = append(changes, fmt.Sprintf("dropped_table:%d", work.id))
		work.exists, work.cols, work.files = false, nil, map[string]*lakeFile{}
	}
	if work.hasView && work.viewSQL != text {
		if err := endView(ctx, tx, work.viewID, snap.snapshot); err != nil {
			return nil, err
		}
		changes = append(changes, fmt.Sprintf("dropped_view:%d", work.viewID))
		work.hasView = false
	}
	if !work.hasView {
		if work.schemaID < 0 {
			work.schemaID = snap.nextCatalogID
			snap.nextCatalogID++
			if err := insertSchema(ctx, tx, work.schemaID, snap.snapshot, work.schema); err != nil {
				return nil, err
			}
			changes = append(changes, "created_schema:"+quoteIdent(work.schema))
		}
		work.viewID = snap.nextCatalogID
		snap.nextCatalogID++
		if err := insertView(ctx, tx, work.viewID, snap.snapshot, work.schemaID, work.name, text); err != nil {
			return nil, err
		}
		work.hasView, work.viewSQL = true, text
		changes = append(changes, "created_view:"+quoteIdent(work.schema)+"."+quoteIdent(work.name))
	}
	if len(changes) > 0 {
		snap.schemaVersion++
	}
	return changes, nil
}

const timeLayout = "2006-01-02 15:04:05.000000+00"

// stamp is the time of the next snapshot. It never goes backwards, whatever
// the clock does: expiry orders snapshots by it.
func (p *Publisher) stamp() string {
	at := p.opt.Now().UTC().Format(timeLayout)
	if at < p.lastTime {
		at = p.lastTime
	}
	return at
}

// Expire removes snapshots superseded for longer than the retention window,
// then the files only they could see. The worker calls it after publishing.
func (p *Publisher) Expire(ctx context.Context) error {
	p.work.Lock()
	defer p.work.Unlock()
	now := p.opt.Now()
	if !p.lastExp.IsZero() && now.Sub(p.lastExp) < p.opt.Retention/20 && now.Sub(p.lastExp) >= 0 {
		return nil
	}
	p.lastExp = now
	cutoff := now.Add(-p.opt.Retention).UTC().Format(timeLayout)
	tx, err := p.cat.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	keep, ok, err := expirySnapshot(ctx, tx, cutoff)
	if err != nil || !ok {
		return err
	}
	n, err := expireBefore(ctx, tx, keep, now.UTC().Format(timeLayout))
	if err != nil {
		return err
	}
	// Committed and durable (synchronous=FULL) BEFORE anything is unlinked:
	// a catalog that names a file must find it.
	if err := tx.Commit(); err != nil {
		return err
	}
	p.mu.Lock()
	p.global.ExpiredSnapshots += n
	p.mu.Unlock()
	if err := p.unlinkScheduled(ctx); err != nil {
		return err
	}
	// A mapping no file uses went with its files; forget it here too.
	live, err := liveMappings(ctx, p.cat.db)
	if err != nil {
		return err
	}
	for _, lt := range p.tables {
		for sig, id := range lt.mappings {
			if !live[id] {
				delete(lt.mappings, sig)
			}
		}
	}
	return nil
}

// unlinkScheduled pays the unlinks a committed expiry owes. It is safe to
// repeat, which is what a restart does.
func (p *Publisher) unlinkScheduled(ctx context.Context) error {
	paths, err := scheduledFiles(ctx, p.cat.db)
	if err != nil || len(paths) == 0 {
		return err
	}
	var bytes int64
	for _, rel := range paths {
		path := filepath.Join(p.dataDir, filepath.FromSlash(rel))
		if fi, err := os.Lstat(path); err == nil {
			bytes += fi.Size()
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := clearScheduled(ctx, p.cat.db); err != nil {
		return err
	}
	p.mu.Lock()
	p.global.UnlinkedFiles += int64(len(paths))
	p.global.UnlinkedBytes += bytes
	p.mu.Unlock()
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ---- what an operator sees ----------------------------------------------------

// TableStats is where one table's lake stands.
type TableStats struct {
	SubmittedSeq, PublishedSeq int64
	SubmittedLSN, PublishedLSN string
	PublishedAt                time.Time
	Publishes, Coalesced       int64
	Errors                     int64
	LastError                  string
	LastSeconds, TotalSeconds  float64
	// Refused is why the lake table is not served, when it is not.
	Refused string
	// NeedsRewrite is set when only a base rewrite makes the table
	// publishable: it holds patch files, or files of types that do not mix.
	NeedsRewrite bool
	// LagSeconds is how long the oldest unpublished manifest has waited.
	LagSeconds float64
}

// GlobalStats is the lake as a whole.
type GlobalStats struct {
	Snapshot                     int64
	ExpiredSnapshots             int64
	UnlinkedFiles, UnlinkedBytes int64
	CatalogBytes, WALBytes       int64
	P50Seconds, P95Seconds       float64
}

func (p *Publisher) statsFor(q string) *TableStats {
	st := p.stats[q]
	if st == nil {
		st = &TableStats{}
		p.stats[q] = st
	}
	return st
}

// Stats reports every table submitted so far, and the lake.
func (p *Publisher) Stats() (map[string]TableStats, GlobalStats) {
	now := p.opt.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]TableStats, len(p.stats))
	for q, st := range p.stats {
		s := *st
		pm := p.pending[q]
		if pm == nil {
			pm = p.retry[q]
		}
		if pm != nil {
			s.LagSeconds = now.Sub(pm.since).Seconds()
		}
		out[q] = s
	}
	g := p.global
	if fi, err := os.Stat(filepath.Join(p.dir, CatalogFile)); err == nil {
		g.CatalogBytes = fi.Size()
	}
	if fi, err := os.Stat(filepath.Join(p.dir, CatalogFile+"-wal")); err == nil {
		g.WALBytes = fi.Size()
	}
	n := min(p.recentN, len(p.recent))
	if n > 0 {
		d := append([]float64(nil), p.recent[:n]...)
		sort.Float64s(d)
		g.P50Seconds, g.P95Seconds = d[n/2], d[min(n-1, n*95/100)]
	}
	return out, g
}

// rewriteAsk is a rewrite NeedsRewrite has asked the sidecar for.
type rewriteAsk struct {
	why  string
	n    int
	next time.Time // not asked again for the same reason before this
}

const (
	rewriteBackoff    = 15 * time.Second
	rewriteBackoffMax = 15 * time.Minute
)

// NeedsRewrite reports whether a table is waiting on a base rewrite to become
// publishable. The sidecar asks on every maintenance tick and starts one each
// time the answer is yes, so a yes is a REQUEST, and each is a rewrite of the
// whole table. It is yes only when
//
//   - the manifest the lake stands at needs one, and the lake stands at the
//     mirror's newest: nothing is waiting to be published, being published,
//     or waiting to be published AGAIN after failing. A rewrite's own
//     manifest whose publish failed is the last case. The verdict in hand is
//     then about the manifest before it, and answering from that started a
//     full rewrite on every tick for as long as the catalog could not be
//     written;
//   - and no rewrite has been asked for, for the same reason, less than a
//     backoff ago. The reason of a published manifest replaces the one asked
//     for, so a rewrite whose result is published and needs another for a
//     DIFFERENT reason gets it at once, and one that left the same reason
//     behind (or was never started, or failed) is asked for again after 15
//     seconds, then 30, doubling to 15 minutes.
func (p *Publisher) NeedsRewrite(qualified string) bool {
	now := p.opt.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.stats[qualified]
	if st == nil || !st.NeedsRewrite || p.busy ||
		p.pending[qualified] != nil || p.retry[qualified] != nil {
		return false
	}
	a := p.asks[qualified]
	if a == nil || a.why != p.why[qualified] {
		a = &rewriteAsk{why: p.why[qualified]}
		p.asks[qualified] = a
	}
	// A clock that went backwards past the last ask does not hold one up.
	if a.n > 0 && now.Before(a.next) && now.After(a.next.Add(-rewriteBackoffMax)) {
		return false
	}
	wait := rewriteBackoffMax
	if a.n < 7 {
		wait = min(rewriteBackoff<<a.n, rewriteBackoffMax)
	}
	a.n++
	a.next = now.Add(wait)
	return true
}

// Counts reads how many rows of each kind the catalog holds.
func (p *Publisher) Counts(ctx context.Context) (Counts, error) {
	p.work.Lock()
	defer p.work.Unlock()
	return readCounts(ctx, p.cat.db)
}

// Metrics is the Prometheus text for the lake, appended to the sidecar's.
func (p *Publisher) Metrics() string {
	tables, g := p.Stats()
	names := make([]string, 0, len(tables))
	for q := range tables {
		names = append(names, q)
	}
	sort.Strings(names)
	var b strings.Builder
	gauge := func(name, help string, value func(TableStats) string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
		for _, q := range names {
			fmt.Fprintf(&b, "%s{table=\"%s\"} %s\n", name, labelEscape(q), value(tables[q]))
		}
	}
	gauge("quicksilver_ducklake_published_lsn", "applied_lsn of the manifest the lake table stands at, as a WAL position.",
		func(s TableStats) string { return fmt.Sprint(parseLSN(s.PublishedLSN)) })
	gauge("quicksilver_ducklake_published_manifest_seq", "Sequence of the manifest the lake table stands at.",
		func(s TableStats) string { return fmt.Sprint(s.PublishedSeq) })
	gauge("quicksilver_ducklake_submitted_manifest_seq", "Sequence of the newest manifest the mirror has committed.",
		func(s TableStats) string { return fmt.Sprint(s.SubmittedSeq) })
	gauge("quicksilver_ducklake_lag_seconds", "How long the oldest unpublished manifest has waited; 0 when the lake is at the manifest.",
		func(s TableStats) string { return fmt.Sprintf("%.3f", s.LagSeconds) })
	gauge("quicksilver_ducklake_last_publish_seconds", "Duration of the last publish that wrote a snapshot.",
		func(s TableStats) string { return fmt.Sprintf("%.6f", s.LastSeconds) })
	gauge("quicksilver_ducklake_publishes_total", "Snapshots written for the table.",
		func(s TableStats) string { return fmt.Sprint(s.Publishes) })
	gauge("quicksilver_ducklake_publish_seconds_total", "Time spent writing them.",
		func(s TableStats) string { return fmt.Sprintf("%.6f", s.TotalSeconds) })
	gauge("quicksilver_ducklake_coalesced_total", "Manifests never published because a newer one arrived first.",
		func(s TableStats) string { return fmt.Sprint(s.Coalesced) })
	gauge("quicksilver_ducklake_publish_errors_total", "Publishes that failed.",
		func(s TableStats) string { return fmt.Sprint(s.Errors) })
	gauge("quicksilver_ducklake_refused", "1 while the lake table is replaced by a view that fails (halted, or waiting on a base rewrite).",
		func(s TableStats) string {
			if s.Refused != "" {
				return "1"
			}
			return "0"
		})
	fmt.Fprintf(&b, `# HELP quicksilver_ducklake_publish_seconds Publish duration over the last 4096 publishes.
# TYPE quicksilver_ducklake_publish_seconds summary
quicksilver_ducklake_publish_seconds{quantile="0.5"} %.6f
quicksilver_ducklake_publish_seconds{quantile="0.95"} %.6f
# HELP quicksilver_ducklake_snapshot Newest DuckLake snapshot written by this process.
# TYPE quicksilver_ducklake_snapshot gauge
quicksilver_ducklake_snapshot %d
# HELP quicksilver_ducklake_expired_snapshots_total Snapshots expired by this process.
# TYPE quicksilver_ducklake_expired_snapshots_total counter
quicksilver_ducklake_expired_snapshots_total %d
# HELP quicksilver_ducklake_unlinked_files_total Lake files unlinked after expiry.
# TYPE quicksilver_ducklake_unlinked_files_total counter
quicksilver_ducklake_unlinked_files_total %d
# HELP quicksilver_ducklake_catalog_bytes Size of the SQLite catalog, and of its write-ahead log.
# TYPE quicksilver_ducklake_catalog_bytes gauge
quicksilver_ducklake_catalog_bytes{file="catalog"} %d
quicksilver_ducklake_catalog_bytes{file="wal"} %d
`, g.P50Seconds, g.P95Seconds, g.Snapshot, g.ExpiredSnapshots, g.UnlinkedFiles, g.CatalogBytes, g.WALBytes)
	return b.String()
}

func labelEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// parseLSN turns "16/B374D848" into the 64-bit position, 0 if it is not one.
func parseLSN(s string) uint64 {
	var hi, lo uint64
	if _, err := fmt.Sscanf(s, "%X/%X", &hi, &lo); err != nil {
		return 0
	}
	return hi<<32 | lo
}
