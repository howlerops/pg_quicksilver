// Package ducklake publishes the mirror as a DuckLake: the mirror's own
// Parquet files, hard-linked, behind a DuckLake catalog in SQLite that this
// package writes directly. No row is rewritten and DuckDB is not involved in
// writing anything; DuckDB (in a PostgreSQL backend through pg_duckdb, or
// anywhere else) is the reader:
//
//	ATTACH 'ducklake:sqlite:<mirror>/_ducklake/catalog.sqlite' AS lake (READ_ONLY);
//	SELECT ... FROM lake.public.orders;
//
// go/ducklake/ holds the Python prototype this was built from, and the record
// of what did not work (README there, probe.py).
//
// # One manifest, one snapshot
//
// The mirror hands over a description of each manifest it commits
// (mirror.LakeManifest). Publishing one brings the lake table to exactly that
// manifest, in ONE SQLite transaction that is one DuckLake snapshot:
//
//	a data file the manifest names and the lake lacks   linked, registered with
//	                                                    statistics from its footer
//	a file whose dead set changed                       a new delete file written
//	                                                    from the bitmap; the old ended
//	a file the manifest no longer names                 ended (merge, rewrite, truncate)
//	a column added, dropped or of another type          a column row begun or ended
//	a halted or unpublishable table                     the table ended, and a view of
//	                                                    the same name that fails with
//	                                                    the reason put in its place
//
// A reader therefore sees a state the mirror was in, or the one before it,
// and never anything between; a crash at any point leaves the previous
// snapshot, because nothing in the catalog changed. Files made for a publish
// that did not commit are unnamed, and are swept when the lake is next opened.
//
// # Off the apply path
//
// Submit stores the manifest and returns. A worker publishes the newest
// manifest of each table; if two arrived while it was busy, the older is
// never published, which is fine — the newer one is also a state the mirror
// was in. The cost is that the lake holds its own link to every data file,
// so the mirror removing its name for one (QS_RETIRE_GRACE_SECONDS) takes
// nothing from a lake reader. That is also the constraint: hard links do not
// cross filesystems, and Open refuses a lake that is not on the mirror's.
//
// # Expiry
//
// A snapshot is expired once it has been superseded for longer than the
// retention window. Its catalog rows go in one transaction; only after that
// has committed are the files nothing retained can see unlinked. A reader on
// a retained snapshot never had those files in its plan, and a reader still
// on an expired one keeps whatever it has already opened.
package ducklake

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const (
	// DirName is the lake's directory under the mirror root.
	DirName = "_ducklake"
	// CatalogFile is the SQLite catalog inside it.
	CatalogFile  = "catalog.sqlite"
	dataDirName  = "data"
	deleteSuffix = "-delete.parquet"

	// DefaultRetention is how long a superseded snapshot stays readable.
	DefaultRetention = 5 * time.Minute
)

// Options configures a Publisher.
type Options struct {
	// MirrorRoot is the mirror's directory (QS_MIRROR_PATH).
	MirrorRoot string
	// Dir is the lake directory; MirrorRoot/_ducklake when empty.
	Dir string
	// Retention is how long a snapshot outlives being superseded.
	Retention time.Duration
	Log       *slog.Logger
	// Now is the clock; tests move it.
	Now func() time.Time
}

// CatalogPath is where the catalog of a mirror's lake is.
func CatalogPath(mirrorRoot string) string {
	return filepath.Join(mirrorRoot, DirName, CatalogFile)
}

// CatalogPathIn is CatalogPath for a lake directory that may have been
// configured (Options.Dir); the absolute path a reader attaches.
func CatalogPathIn(mirrorRoot, dir string) string {
	if dir == "" {
		dir = filepath.Join(mirrorRoot, DirName)
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs
	}
	return filepath.Join(dir, CatalogFile)
}

// Current reports whether the lake table stands at the newest manifest that
// was submitted for it, and the LSN it stands at. The manifest sequence
// number cannot answer that: it counts files, and two manifests that differ
// only in which rows are dead, or only in their applied LSN, share one.
func (p *Publisher) Current(qualified string) (lsn string, current bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pub := p.published[qualified]
	if pub == nil {
		return "", false
	}
	return pub.AppliedLSN, pub == p.submitted[qualified] && pub.Refusal == ""
}

// Publisher keeps one lake in step with the mirror tables submitted to it.
type Publisher struct {
	opt     Options
	dir     string
	dataDir string
	cat     *catalog
	lock    *os.File

	// work serialises everything that writes the lake.
	work     sync.Mutex
	tables   map[string]*lakeTable
	lastTime string
	lastExp  time.Time

	mu      sync.Mutex
	pending map[string]*pendingManifest
	retry   map[string]*pendingManifest
	busy    bool
	why     map[string]string      // why the published manifest needs a rewrite, "" if it does not
	asks    map[string]*rewriteAsk // rewrites asked for and not yet seen to have helped
	stats   map[string]*TableStats
	// The newest manifest submitted for a table, and the one the lake stands
	// at: see Current.
	submitted map[string]*mirror.LakeManifest
	published map[string]*mirror.LakeManifest
	global    GlobalStats
	recent    []float64 // seconds, a ring of the last publishes
	recentN   int

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	// Tests only: beforeCommit fails a publish at the last moment, with its
	// files already in the lake.
	beforeCommit func() error
}

type pendingManifest struct {
	m     *mirror.LakeManifest
	since time.Time
}

type lakeTable struct {
	schema, name string
	schemaID     int64 // -1: the schema does not exist yet
	known        bool  // has a qs_mirror_table row
	id           int64
	exists       bool
	viewID       int64
	hasView      bool
	viewSQL      string
	cols         []lakeColumn
	files        map[string]*lakeFile // by mirror-relative path
	mappings     map[string]int64     // file-column signature -> mapping id
	nextColumnID int64
	nextRowID    int64
	seq          int64
	lsn, refusal string
}

type lakeColumn struct {
	id, order int64
	name, typ string
	initial   *string
}

type lakeFile struct {
	id        int64
	name, rel string
	rows      int64
	size      int64
	dvGen     int
	hasDelete bool
	types     map[string]string // column name -> DuckLake type, from the footer
	legacyInf []string          // columns that can hold the old -infinity, from the footer
}

func (lt *lakeTable) clone() *lakeTable {
	c := *lt
	c.cols = append([]lakeColumn(nil), lt.cols...)
	c.files = make(map[string]*lakeFile, len(lt.files))
	for k, f := range lt.files {
		ff := *f
		c.files[k] = &ff
	}
	c.mappings = make(map[string]int64, len(lt.mappings))
	for k, v := range lt.mappings {
		c.mappings[k] = v
	}
	return &c
}

func (lt *lakeTable) dir(dataDir string) string { return filepath.Join(dataDir, lt.schema, lt.name) }

// errStale is a manifest whose files the mirror has already removed: the
// publisher fell further behind than the mirror keeps a retired file. A newer
// manifest exists by then, and that one is published instead.
var errStale = errors.New("manifest names a file the mirror has since removed")

var errDeadCount = errors.New("dead set holds fewer positions than its count")

// Open opens, or creates, the lake of a mirror and starts its worker.
func Open(ctx context.Context, opt Options) (*Publisher, error) {
	if opt.Dir == "" {
		opt.Dir = filepath.Join(opt.MirrorRoot, DirName)
	}
	if opt.Retention <= 0 {
		opt.Retention = DefaultRetention
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	if opt.Now == nil {
		opt.Now = time.Now
	}
	dir, err := filepath.Abs(opt.Dir)
	if err != nil {
		return nil, err
	}
	p := &Publisher{
		opt: opt, dir: dir, dataDir: filepath.Join(dir, dataDirName),
		tables:  map[string]*lakeTable{},
		pending: map[string]*pendingManifest{}, retry: map[string]*pendingManifest{},
		why: map[string]string{}, asks: map[string]*rewriteAsk{},
		stats: map[string]*TableStats{}, recent: make([]float64, 4096),
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	if err := os.MkdirAll(p.dataDir, 0o755); err != nil {
		return nil, err
	}
	if err := sameFilesystem(opt.MirrorRoot, p.dataDir); err != nil {
		return nil, err
	}
	if p.lock, err = lockDir(dir); err != nil {
		return nil, err
	}
	if p.cat, err = openCatalog(ctx, filepath.Join(dir, CatalogFile), p.dataDir+"/"); err != nil {
		unlockDir(p.lock)
		return nil, err
	}
	if err := p.recover(ctx); err != nil {
		p.cat.close()
		unlockDir(p.lock)
		return nil, err
	}
	go p.run()
	return p, nil
}

// sameFilesystem proves a hard link from the mirror into the lake works, by
// making one. Comparing device numbers is not enough: two bind mounts of one
// filesystem share a device and still refuse a link across them.
func sameFilesystem(mirrorRoot, dataDir string) error {
	src, err := os.CreateTemp(mirrorRoot, ".qs-lake-probe-*")
	if err != nil {
		return fmt.Errorf("ducklake: cannot write to the mirror directory %s: %w", mirrorRoot, err)
	}
	src.Close()
	defer os.Remove(src.Name())
	dst := filepath.Join(dataDir, filepath.Base(src.Name()))
	defer os.Remove(dst)
	if err := os.Link(src.Name(), dst); err != nil {
		return fmt.Errorf("ducklake: the lake directory %s must be on the same filesystem as the mirror %s: "+
			"the lake holds hard links to the mirror's data files rather than copies, and a link from one "+
			"to the other failed (%w). Put the lake under the mirror path (the default) or turn QS_DUCKLAKE off",
			dataDir, mirrorRoot, err)
	}
	return nil
}

// recover is the part of a restart that needs no manifest: finish an expiry
// whose unlinks were owed, and remove files no catalog row names — what a
// publish that never committed left behind.
func (p *Publisher) recover(ctx context.Context) error {
	if err := p.cat.db.QueryRowContext(ctx, `SELECT max(snapshot_time) FROM ducklake_snapshot`).Scan(&p.lastTime); err != nil {
		return err
	}
	if err := p.unlinkScheduled(ctx); err != nil {
		return err
	}
	named, err := referencedFiles(ctx, p.cat.db)
	if err != nil {
		return err
	}
	return filepath.WalkDir(p.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(p.dataDir, path)
		if !named[filepath.ToSlash(rel)] {
			p.opt.Log.Info("ducklake: removing a file no snapshot names", "file", rel)
			_ = os.Remove(path)
		}
		return nil
	})
}

// Close stops the worker. What was submitted and not yet published is left
// for the reconciliation at the next start.
func (p *Publisher) Close() {
	select {
	case <-p.stop:
		return
	default:
	}
	close(p.stop)
	<-p.done
	p.work.Lock()
	defer p.work.Unlock()
	_ = p.cat.close()
	unlockDir(p.lock)
}

// Submit hands over a manifest. It never blocks on the lake: it is called on
// the apply goroutine, between one apply and the next.
func (p *Publisher) Submit(m *mirror.LakeManifest) {
	q := m.Qualified()
	now := p.opt.Now()
	p.mu.Lock()
	since := now
	st := p.statsFor(q)
	if prev := p.pending[q]; prev != nil {
		since = prev.since
		st.Coalesced++
	}
	p.pending[q] = &pendingManifest{m: m, since: since}
	delete(p.retry, q)
	if p.submitted == nil {
		p.submitted = map[string]*mirror.LakeManifest{}
	}
	p.submitted[q] = m
	st.SubmittedSeq = int64(m.Seq)
	st.SubmittedLSN = m.AppliedLSN
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Flush waits until everything submitted has been published or has failed.
func (p *Publisher) Flush(ctx context.Context) error {
	for {
		p.mu.Lock()
		idle := len(p.pending) == 0 && !p.busy
		p.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (p *Publisher) run() {
	defer close(p.done)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	ctx := context.Background()
	for {
		select {
		case <-p.stop:
			return
		case <-p.wake:
		case <-tick.C:
			// A publish that failed for a reason that may pass (a full disk)
			// is tried again, unless a newer manifest has replaced it.
			p.mu.Lock()
			for q, pm := range p.retry {
				if p.pending[q] == nil {
					p.pending[q] = pm
				}
				delete(p.retry, q)
			}
			p.mu.Unlock()
		}
		for {
			p.mu.Lock()
			var q string
			var pm *pendingManifest
			for k, v := range p.pending {
				if pm == nil || v.since.Before(pm.since) {
					q, pm = k, v
				}
			}
			if pm != nil {
				delete(p.pending, q)
				p.busy = true
			}
			p.mu.Unlock()
			if pm == nil {
				break
			}
			_, err := p.Publish(ctx, pm.m)
			p.mu.Lock()
			p.busy = false
			if err != nil && !errors.Is(err, errStale) && p.pending[q] == nil {
				p.retry[q] = pm
			}
			p.mu.Unlock()
			if err != nil {
				p.opt.Log.Warn("ducklake: publish failed", "table", q, "manifest_seq", pm.m.Seq, "err", err)
			}
			select {
			case <-p.stop:
				return
			default:
			}
		}
		if err := p.Expire(ctx); err != nil {
			p.opt.Log.Warn("ducklake: expiry failed", "err", err)
		}
	}
}

// Result is what one publish did.
type Result struct {
	Snapshot int64 // the DuckLake snapshot written, 0 if the lake already stood there
	Added    int
	Dropped  int
	Deletes  int
	Refused  string
	Duration time.Duration
}

// Publish brings the lake table to m, synchronously. The worker calls it;
// tests call it directly to publish at a known point.
func (p *Publisher) Publish(ctx context.Context, m *mirror.LakeManifest) (res Result, err error) {
	p.work.Lock()
	defer p.work.Unlock()
	start := time.Now()
	q := m.Qualified()
	var rewrite string
	res, rewrite, err = p.publish(ctx, m)
	res.Duration = time.Since(start)

	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.statsFor(q)
	if err != nil {
		delete(p.tables, q) // whatever was half-built in memory is not the catalog
		st.Errors++
		st.LastError = err.Error()
		return res, err
	}
	st.LastError = ""
	if p.published == nil {
		p.published = map[string]*mirror.LakeManifest{}
	}
	p.published[q] = m
	st.PublishedSeq, st.PublishedLSN = int64(m.Seq), m.AppliedLSN
	st.Refused, st.NeedsRewrite = res.Refused, rewrite != ""
	// What was asked for a different reason, or for one that has gone, is
	// not what is asked now: see NeedsRewrite.
	if a := p.asks[q]; a != nil && a.why != rewrite {
		delete(p.asks, q)
	}
	p.why[q] = rewrite
	st.PublishedAt = p.opt.Now()
	if res.Snapshot != 0 {
		st.Publishes++
		st.LastSeconds = res.Duration.Seconds()
		st.TotalSeconds += st.LastSeconds
		p.recent[p.recentN%len(p.recent)] = st.LastSeconds
		p.recentN++
		p.global.Snapshot = res.Snapshot
	}
	return res, nil
}

// publishPlan is what a manifest needs of the lake's files, worked out before
// anything is written.
type publishPlan struct {
	adds  []planFile
	keeps []planFile
	drops []*lakeFile
	types map[string]string
}

type planFile struct {
	mf   *mirror.LakeFile
	src  string
	info *fileInfo
	lf   *lakeFile
}

func (p *Publisher) publish(ctx context.Context, m *mirror.LakeManifest) (res Result, rewrite string, err error) {
	if m.Err != nil {
		return res, "", fmt.Errorf("manifest could not be described: %w", m.Err)
	}
	q := m.Qualified()
	reason := m.Refusal
	if reason == "" {
		var partial []string
		for i := range m.Files {
			if m.Files[i].Partial != nil {
				partial = append(partial, m.Files[i].Rel)
			}
		}
		if len(partial) > 0 {
			// The files are the reason: patches written after a rewrite has
			// folded these in are another one.
			rewrite = "patch files " + strings.Join(partial, ",")
			reason = fmt.Sprintf("it holds column-partial patch files (%s), which are not whole rows and cannot "+
				"be DuckLake data files; a base rewrite is scheduled and the table is published when it completes",
				strings.Join(partial, ", "))
		}
	}

	tx, err := p.cat.db.BeginTx(ctx, nil)
	if err != nil {
		return res, rewrite, err
	}
	defer tx.Rollback()
	var made []string // files created for this publish, removed if it fails
	defer func() {
		if err != nil {
			for _, f := range made {
				_ = os.Remove(f)
			}
		}
	}()

	h, err := readHead(ctx, tx)
	if err != nil {
		return res, rewrite, err
	}
	lt := p.tables[q]
	if lt == nil {
		if lt, err = loadTable(ctx, tx, m.Schema, m.Table); err != nil {
			return res, rewrite, err
		}
	}
	work := lt.clone()

	var plan *publishPlan
	if reason == "" {
		var mix, why string
		if plan, mix, why, err = p.plan(m, work); err != nil {
			return res, rewrite, err
		}
		if mix != "" {
			reason, rewrite = mix, why
		}
	}

	snap := h
	snap.snapshot++
	var changes []string
	if reason != "" {
		changes, err = p.refuse(ctx, tx, &snap, work, q, reason)
		res.Refused = reason
	} else {
		changes, err = p.apply(ctx, tx, &snap, work, m, plan, &res, &made)
	}
	if err != nil {
		return res, rewrite, err
	}
	work.seq, work.lsn, work.refusal = int64(m.Seq), m.AppliedLSN, reason

	at := p.stamp()
	switch {
	case len(changes) > 0:
		msg := fmt.Sprintf("%s manifest seq %d at %s", q, m.Seq, m.AppliedLSN)
		extra := fmt.Sprintf(`{"table":%q,"applied_lsn":%q,"manifest_seq":%d}`, q, m.AppliedLSN, m.Seq)
		if err = insertSnapshot(ctx, tx, snap, at, changes, msg, extra); err != nil {
			return res, rewrite, err
		}
		if err = saveMirrorTable(ctx, tx, work, snap.snapshot, at); err != nil {
			return res, rewrite, err
		}
		res.Snapshot = snap.snapshot
	case !lt.known || lt.seq != work.seq || lt.lsn != work.lsn:
		// Nothing a reader can see changed; only where the mirror stands.
		if err = saveMirrorTable(ctx, tx, work, h.snapshot, at); err != nil {
			return res, rewrite, err
		}
	default:
		return res, rewrite, nil
	}
	work.known = true
	// The links and delete files this snapshot names have to be in their
	// directory for good before the snapshot is.
	if len(made) > 0 {
		var written []string
		for _, f := range made {
			if strings.HasSuffix(f, deleteSuffix) { // a link is to a file the mirror already synced
				written = append(written, f)
			}
		}
		if err = syncFiles(written); err != nil {
			return res, rewrite, err
		}
		if err = syncDir(work.dir(p.dataDir)); err != nil {
			return res, rewrite, err
		}
	}
	if p.beforeCommit != nil {
		if err = p.beforeCommit(); err != nil {
			return res, rewrite, err
		}
	}
	if err = tx.Commit(); err != nil {
		return res, rewrite, err
	}
	p.lastTime = at
	p.tables[q] = work
	return res, rewrite, nil
}

// plan compares the manifest's files with the lake's and settles every
// column's type. mix is non-empty when the files cannot be one table, and
// why then names the cause without naming the files: a rewrite that leaves
// the same cause behind is asked for again only after a wait (NeedsRewrite).
func (p *Publisher) plan(m *mirror.LakeManifest, work *lakeTable) (plan *publishPlan, mix, why string, err error) {
	plan = &publishPlan{types: map[string]string{}}
	kept := map[string]bool{}
	dir := work.dir(p.dataDir)
	for i := range m.Files {
		mf := &m.Files[i]
		src := filepath.Join(m.Dir, mf.Rel)
		st, err := os.Stat(src)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, "", "", fmt.Errorf("%w: %s", errStale, mf.Rel)
			}
			return nil, "", "", err
		}
		if lf := work.files[mf.Rel]; lf != nil && work.exists {
			// The same NAME is not the same file: a rebuilt table numbers its
			// files from wherever its manifest says, and a lake that outlived
			// it would otherwise keep serving the old one under the new name.
			if lst, err := os.Stat(filepath.Join(dir, lf.name)); err == nil && os.SameFile(st, lst) {
				if lf.types == nil {
					info, err := readFileInfo(filepath.Join(dir, lf.name))
					if err != nil {
						return nil, "", "", err
					}
					lf.types, lf.legacyInf = info.types(), info.legacyInfinity()
				}
				plan.keeps = append(plan.keeps, planFile{mf: mf, src: src, lf: lf})
				kept[mf.Rel] = true
				continue
			}
		}
		info, err := readFileInfo(src)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, "", "", fmt.Errorf("%w: %s", errStale, mf.Rel)
			}
			return nil, "", "", err
		}
		plan.adds = append(plan.adds, planFile{mf: mf, src: src, info: info})
	}
	if work.exists {
		for rel, lf := range work.files {
			if !kept[rel] {
				plan.drops = append(plan.drops, lf)
			}
		}
		sort.Slice(plan.drops, func(i, j int) bool { return plan.drops[i].id < plan.drops[j].id })
	}
	for _, c := range m.Columns {
		// Every file is held against what the mirror writes NOW, not only
		// against the other files: a table whose files are all of the
		// previous release is no mix, and is as wrong. Its `real` is a DOUBLE
		// holding 0.1 where the source value, and the mirror's view, is the
		// float4; its unconstrained numeric is DECIMAL(38,9), 12.500000000
		// for the 12.5 the view gives. The mirror reads a file by the
		// declared type and converts; the lake can only refuse.
		typ, from := typeOfArrow(c.Type), "what the mirror writes now"
		see := func(rel string, types map[string]string) {
			t, ok := types[c.Name]
			if !ok || mix != "" {
				return
			}
			w, ok := widen(typ, t)
			if !ok {
				why = fmt.Sprintf("two types in column %q", c.Name)
				mix = fmt.Sprintf("it holds column %q as two types DuckLake cannot read as one column "+
					"(%s in %s, %s in %s); a base rewrite, which writes every row in the current type, is "+
					"scheduled and the table is published when it completes", c.Name, typ, from, t, rel)
				return
			}
			if w != typ {
				typ, from = w, rel
			}
		}
		// A file of the previous release whose -infinity DuckDB would read
		// as a finite date (stats.go, legacyInfinity). A file the lake already
		// holds is looked at too: a lake written before this check has them.
		old := func(rel string, cols []string) {
			if mix != "" {
				return
			}
			for _, name := range cols {
				if name == c.Name {
					why = fmt.Sprintf("the old -infinity in column %q", c.Name)
					mix = fmt.Sprintf("its file %s may hold -infinity in column %q as the previous release "+
						"stored it, which DuckDB reads as a finite date in 290309 BC (5877642 BC for a date) "+
						"and the mirror's own view corrects per file; a base rewrite, which writes every "+
						"row in the current form, is scheduled and the table is published when it completes",
						rel, c.Name)
				}
			}
		}
		for _, f := range plan.keeps {
			see(f.mf.Rel, f.lf.types)
			old(f.mf.Rel, f.lf.legacyInf)
		}
		for _, f := range plan.adds {
			see(f.mf.Rel, f.info.types())
			old(f.mf.Rel, f.info.legacyInfinity())
		}
		if mix != "" {
			return nil, mix, why, nil
		}
		plan.types[c.Name] = typ
	}
	return plan, "", "", nil
}
