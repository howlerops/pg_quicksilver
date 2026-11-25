package mirror

// A snapshot a reader can pick up, and the grace that lets it finish.
//
// ViewSQL (serve.go) bakes file paths in, and until now only the one-shot CLI
// generated it. A view made earlier then failed in three ways, measured:
//
//   - if it named a deletion vector it errored with "No files found" about two
//     applies later, when markDead retired generation N-2;
//   - if it named none it kept answering, from an arbitrarily old manifest,
//     with nothing to say so;
//   - after a delta merge or a base rewrite it errored.
//
// So nothing gave a reader a CURRENT snapshot, and nothing let a query that
// started on one snapshot finish. Two mechanisms, both here:
//
// PUBLISHING. Every manifest write ends by publishing snapshot.json next to
// state.json: the view SQL for that manifest, the LSN it stands at, a sequence
// number that only goes up, and the files it names. It is ONE file, so the SQL
// and its descriptor cannot be read from two different snapshots, and it is
// replaced by temp-fsync-rename-syncdir exactly as the manifest is.
//
// The hook is the END OF saveState, and that is the whole coverage argument:
// saveState is the only function that writes state.json, so apply, delta
// merge, compaction swap, truncate, evolve, snapshot and halt are covered by
// construction rather than by remembering each one — and so is whatever path
// is added next. It runs after the manifest's own directory sync has
// succeeded, so a published snapshot is never ahead of the manifest it
// describes: a crash or a failed fsync anywhere before that leaves the OLDER
// snapshot in place, which describes files that are all still on disk.
//
// A publish that fails is returned from saveState. Swallowing it would leave
// readers on a snapshot that, for an insert-only table, goes on answering from
// an old manifest for ever — the silent case this file exists to remove.
//
// RETENTION GRACE. A file dropped from the manifest is not unlinked; it is
// retired, and unlinked RetireGraceSeconds after the snapshot that stopped
// naming it was published. A query that starts on the then-current snapshot
// therefore has at least the grace period to finish, whatever lands meanwhile.
// The clock starts at SUPERSESSION, not at the call: a file retired while the
// published snapshot still names it (Compact removes before it writes) waits,
// unstamped, for the publish that replaces that snapshot.
//
// Nothing about a deferred deletion is durable, deliberately. The stamp is the
// file's mtime, and a restart collects from the directory: see sweep.
//
// DISK. What this holds is exactly the files retired within the last grace
// period: per window, one superseded vector generation per data file per apply
// that touched it, the inputs of every delta merge, and the old base and the
// deltas folded with it for each swap — that last being the 2x a rewrite
// already costs while it runs, held one grace period longer rather than made
// higher. RetireMaxBytes is the hard ceiling on top of that proportional
// bound: past it the OLDEST retirements go early, which turns a slow reader's
// query into "file not found, re-read the snapshot" rather than the volume
// into a full one. A full volume stops the mirror; a re-read does not.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// RetireGraceSeconds is how long a file outlives the snapshot that named it.
// Zero restores unlink-on-retire.
var RetireGraceSeconds = envFloat("QS_RETIRE_GRACE_SECONDS", 60)

// RetireMaxBytes caps, per table, the bytes held only for the grace period.
// Zero means no cap. A base file larger than this is unlinked as soon as it is
// superseded, so a mirror whose readers need the grace across a rewrite sets
// it above the size of its base.
var RetireMaxBytes = int64(envFloat("QS_RETIRE_MAX_BYTES", 1<<30))

// PublishedFile is the name of the published snapshot, beside state.json.
const PublishedFile = "snapshot.json"

// Published is what a reader picks up: one manifest, as SQL, with enough
// around it to decide whether it is fresh enough and whether it has changed.
type Published struct {
	Table string `json:"table"`
	// Seq goes up by one on every publish and never down: comparing it with
	// the one a reader holds is how a reader detects change without diffing
	// SQL. It is carried in this file and nowhere else.
	Seq         uint64    `json:"snapshot_seq"`
	AppliedLSN  string    `json:"applied_lsn"`
	PublishedAt time.Time `json:"published_at"`
	// GraceSeconds is how long the files below outlive this snapshot once a
	// newer one replaces it — the budget of a query that starts on it now.
	GraceSeconds float64 `json:"retire_grace_seconds"`
	// Halted is why this mirror must not be served. SQL then raises it.
	Halted string `json:"halted,omitempty"`
	// Files are relative to the table directory. SQL names them by the
	// absolute path the WRITER sees, as ViewSQL always has.
	Files []string `json:"files"`
	SQL   string   `json:"sql"`
}

// ReadPublished reads a table's published snapshot. It needs no column list
// and no manifest, which is the point: the writer already did that work.
func ReadPublished(root, schema, table string) (*Published, error) {
	return readPublished(filepath.Join(root, schema+"."+table))
}

func readPublished(dir string) (*Published, error) {
	b, err := os.ReadFile(filepath.Join(dir, PublishedFile))
	if err != nil {
		return nil, err
	}
	var p Published
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("%s: %w", PublishedFile, err)
	}
	return &p, nil
}

// retired is one file waiting out its grace. A zero deadline means the
// published snapshot still names it, so its clock has not started.
type retired struct {
	size     int64
	deadline time.Time
}

// publisher is the per-table state behind publishing and retirement. Its lock
// exists for the timer that unlinks expired files, which is the only thing
// here that runs off the apply goroutine; it touches paths and nothing else.
type publisher struct {
	mu      sync.Mutex
	loaded  bool
	swept   bool
	seq     uint64
	named   map[string]bool // absolute paths the PUBLISHED snapshot names
	pending map[string]*retired
	bytes   int64
	timer   *time.Timer
	// cols caches Parquet footers by path. Data files are immutable and their
	// names are never reused, and without this publishing would reopen every
	// file in the manifest on every apply.
	cols map[string]map[string]bool

	grace    time.Duration
	maxBytes int64
	now      func() time.Time
	manual   bool // tests: no timer, reap only when told
}

func (t *Table) publisher() *publisher {
	if t.pub == nil {
		t.pub = &publisher{
			named:    map[string]bool{},
			pending:  map[string]*retired{},
			cols:     map[string]map[string]bool{},
			grace:    time.Duration(RetireGraceSeconds * float64(time.Second)),
			maxBytes: RetireMaxBytes,
			now:      time.Now,
		}
	}
	return t.pub
}

// load picks up the sequence and the file set of the snapshot a previous
// process published, so that the sequence continues and a file that snapshot
// names is known to be in a reader's hands.
func (p *publisher) load(dir string) {
	if p.loaded {
		return
	}
	p.loaded = true
	prev, err := readPublished(dir)
	if err != nil {
		return
	}
	p.seq = prev.Seq
	for _, f := range prev.Files {
		p.named[filepath.Join(dir, f)] = true
	}
}

// manifestFiles lists what the manifest in State names, relative to Dir.
func (t *Table) manifestFiles() []string {
	var out []string
	add := func(rel string) {
		out = append(out, rel)
		if gen := t.State.DVGen[dvStem(rel)]; gen != 0 {
			if dv := t.dvPathGenAny(rel, gen); dv != "" {
				out = append(out, filepath.Join("dv", filepath.Base(dv)))
			}
		}
	}
	for _, b := range t.State.BaseFiles {
		add(filepath.Join("base", b))
	}
	for _, d := range t.State.DeltaFiles {
		add(filepath.Join("delta", d))
	}
	return out
}

// haltedSQL is the snapshot of a mirror that must not be served: a statement
// that fails with the reason, rather than the absence of a statement, because
// a reader that finds nothing new keeps running the last thing it found.
//
// It fails by naming a relation that does not exist, and the name is the
// message. An expression that raises — error('...') — is the obvious way and
// does not work: wrapped in a query that wants column n it fails to BIND with
// `Referenced column "n" not found`, which sends the reader looking at their
// own SQL, and wrapped in count(*) the engine is entitled to never evaluate
// it. A missing relation fails every query, in every engine, with its name.
func haltedSQL(qualified, reason string) string {
	return "SELECT * FROM " + sqlIdent("quicksilver mirror "+qualified+
		" is halted and must not be served: "+reason)
}

// publish writes the snapshot of the manifest saveState has just made durable.
// Only saveState calls it; see the top of the file for why that is enough.
func (t *Table) publish() error {
	p := t.publisher()
	p.load(t.Dir)

	pub := Published{
		Table:        t.Qualified,
		Seq:          p.seq + 1,
		AppliedLSN:   t.State.AppliedLSN,
		PublishedAt:  p.now().UTC(),
		GraceSeconds: p.grace.Seconds(),
		Files:        []string{},
	}
	if h := t.Halted(); h != "" {
		pub.Halted = h
		pub.SQL = haltedSQL(t.Qualified, h)
	} else {
		sql, err := t.ViewSQL()
		if err != nil {
			return fmt.Errorf("publish snapshot: %w", err)
		}
		pub.SQL = sql
		pub.Files = append(pub.Files, t.manifestFiles()...)
	}
	b, err := json.MarshalIndent(pub, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(t.Dir, PublishedFile), b); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	if err := syncDir(t.Dir); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}

	named := make(map[string]bool, len(pub.Files))
	for _, f := range pub.Files {
		named[filepath.Join(t.Dir, f)] = true
	}
	inflight := ""
	if t.compacting != nil {
		inflight = filepath.Join(t.Dir, "base", t.compacting.name)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq = pub.Seq
	p.supersede(named)
	if !p.swept {
		p.swept = true
		p.sweep(t.Dir, inflight)
	}
	p.reap()
	p.enforceCap()
	p.arm()
	return nil
}

// supersede installs the file set of a newly published snapshot, and starts
// the clock of every retired file the previous one was still naming.
func (p *publisher) supersede(named map[string]bool) {
	now := p.now()
	for path, r := range p.pending {
		if named[path] {
			// Retired and then named again: the manifest wins.
			p.bytes -= r.size
			delete(p.pending, path)
			continue
		}
		if r.deadline.IsZero() {
			p.stamp(path, r, now)
		}
	}
	p.named = named
	for path := range p.cols {
		if !named[path] {
			delete(p.cols, path)
		}
	}
}

// stamp starts a file's grace, and writes the moment down as its mtime: that
// is the only record a restart has of when a file stopped being named.
func (p *publisher) stamp(path string, r *retired, now time.Time) {
	r.deadline = now.Add(p.grace)
	_ = os.Chtimes(path, now, now)
}

// retire is how a file leaves the mirror. Every removal of something a
// published snapshot may name goes through here; nothing else unlinks one.
func (t *Table) retire(path string) {
	p := t.publisher()
	p.load(t.Dir)
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, dup := p.pending[path]; dup {
		return
	}
	fi, err := os.Lstat(path)
	if err != nil {
		return
	}
	named := p.named[path]
	if p.grace <= 0 && !named {
		_ = os.Remove(path)
		return
	}
	r := &retired{size: fi.Size()}
	if !named {
		// Already superseded, at some moment no later than this one.
		p.stamp(path, r, p.now())
	}
	p.pending[path] = r
	p.bytes += r.size
	p.enforceCap()
	p.arm()
}

// reap unlinks every retired file whose grace has run out. Called with mu held.
func (p *publisher) reap() {
	now := p.now()
	for path, r := range p.pending {
		if r.deadline.IsZero() || r.deadline.After(now) {
			continue
		}
		p.drop(path, r)
	}
}

func (p *publisher) drop(path string, r *retired) {
	if !p.named[path] {
		_ = os.Remove(path)
	}
	p.bytes -= r.size
	delete(p.pending, path)
}

// enforceCap gives up the oldest retirements when the grace is holding more
// than RetireMaxBytes. Files the published snapshot still names are not
// candidates: their readers are current, not slow.
func (p *publisher) enforceCap() {
	if p.maxBytes <= 0 || p.bytes <= p.maxBytes {
		return
	}
	var paths []string
	for path, r := range p.pending {
		if !r.deadline.IsZero() {
			paths = append(paths, path)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		return p.pending[paths[i]].deadline.Before(p.pending[paths[j]].deadline)
	})
	for _, path := range paths {
		if p.bytes <= p.maxBytes {
			return
		}
		p.drop(path, p.pending[path])
	}
}

// arm makes sure something will come back for the earliest deadline, so that
// an idle table still gives its disk back. Called with mu held.
func (p *publisher) arm() {
	if p.manual || p.timer != nil {
		return
	}
	var first time.Time
	for _, r := range p.pending {
		if !r.deadline.IsZero() && (first.IsZero() || r.deadline.Before(first)) {
			first = r.deadline
		}
	}
	if first.IsZero() {
		return
	}
	p.timer = time.AfterFunc(first.Sub(p.now())+time.Millisecond, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.timer = nil
		p.reap()
		p.arm()
	})
}

// sweep is the restart half of retirement: it collects what a previous process
// retired and did not live to unlink, and what a crash left half-made.
//
// A file is garbage when no manifest names it and it is older than the grace
// period; one that is younger is adopted and unlinked when it reaches that
// age. Age is mtime, which stamp set to the moment the file was superseded.
//
// Two kinds of file are unnamed and NOT garbage, and this is how each is told
// apart:
//
//   - A file an Apply has written and not yet committed. There is none: this
//     runs on the apply goroutine, from publish, immediately after a manifest
//     was made durable, so everything that goroutine has written is in the
//     manifest being compared against. It is never run from New, which a
//     reader calls too, because a reader cannot know what a writer is in the
//     middle of.
//   - The output of a rewrite in flight, which the manifest will not name
//     until the swap. That is identified by NAME — the writer knows which file
//     its own rewrite is producing — and not by age, because a rewrite of a
//     large table runs for longer than any sensible grace period.
//
// An ABORTED rewrite's output is unnamed and unprotected, and may be unlinked
// while still being written. That is what AbortCompaction does with it anyway.
//
// It runs once per process. Files orphaned later by a failed Apply are left
// for the next restart, which a failed Apply normally is.
func (p *publisher) sweep(dir, inflight string) {
	now := p.now()
	for _, sub := range []string{"base", "delta", "dv"} {
		entries, _ := os.ReadDir(filepath.Join(dir, sub))
		for _, e := range entries {
			path := filepath.Join(dir, sub, e.Name())
			if p.named[path] || p.pending[path] != nil {
				continue
			}
			if inflight != "" && strings.HasPrefix(path, inflight) {
				continue
			}
			info, err := e.Info()
			if err != nil || info.IsDir() {
				continue
			}
			deadline := info.ModTime().Add(p.grace)
			if !deadline.After(now) {
				_ = os.Remove(path)
				continue
			}
			p.pending[path] = &retired{size: info.Size(), deadline: deadline}
			p.bytes += info.Size()
		}
	}
}

// columnsOf is fileColumns through the footer cache.
func (t *Table) columnsOf(path string) (map[string]bool, error) {
	p := t.publisher()
	p.mu.Lock()
	have, ok := p.cols[path]
	p.mu.Unlock()
	if ok {
		return have, nil
	}
	have, err := fileColumns(path)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.cols[path] = have
	p.mu.Unlock()
	return have, nil
}
