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
//
// THE INVARIANT a deferred unlink has to keep, stated once:
//
//	A deadline belongs to a FILE, never to a path. When it fires it may
//	unlink only the very file it was set for, and only if no manifest or
//	published snapshot names that path. A path that has been written again,
//	or named again, is out of the publisher's hands for good.
//
// It matters because paths ARE reused: a data file is named by a sequence
// number that advances only when a manifest is written, so whatever a crash
// left above the manifest's sequence (the chunks of a half-applied
// transaction, the output of a rewrite that never swapped, a vector
// generation whose manifest never landed) is written again, name for name, by
// the replay. A deadline held against the path then deleted the replay's
// file: the durable manifest named files that were gone, with applied_lsn
// already past them.
//
// Three things hold it, none dependent on the order in which a restart
// happens to publish and apply:
//
//  1. No deadline is ever taken on a file no manifest can have named
//     (neverNamed): sweep and discard unlink those on the spot. Nothing can
//     be reading one, and they are exactly the paths that get reused. This
//     removes the source; it is not what the invariant rests on.
//  2. Every creation of a data file or vector goes through Table.claim, which
//     cancels whatever deadline stands against the path.
//  3. drop re-checks at the moment of the unlink: the path is not named, and
//     the file there is still the one the deadline was taken on (same inode,
//     same mtime, same size as recorded when its clock started).
//
// TestRandomInterleavingsAgainstAModel passes with any ONE of the three and
// nothing else, and fails with none. 2 and 3 are the two that do not depend
// on having recognised the file for what it is.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	// Halted is why this mirror must not be served. SQL then raises it. It is
	// the manifest's halt, or — for the length of a re-snapshot whose previous
	// contents could not be kept — rebuildingReason.
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
	// id is the file the deadline was taken on, as it was when its clock
	// started (after stamp set its mtime). drop unlinks nothing else.
	id os.FileInfo
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
	closed   bool // Table.Close: no timer is armed again

	// lake is the hand-off to a DuckLake publisher, if one asked. See lake.go.
	lake lakeHook
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
func (t *Table) manifestFiles() []string { return t.filesOf(&t.State) }

// filesOf lists what a manifest names, relative to Dir.
func (t *Table) filesOf(st *State) []string {
	var out []string
	add := func(rel string) {
		out = append(out, rel)
		if gen := st.DVGen[dvStem(rel)]; gen != 0 {
			if dv := t.dvPathGenAny(rel, gen); dv != "" {
				out = append(out, filepath.Join("dv", filepath.Base(dv)))
			}
		}
	}
	for _, b := range st.BaseFiles {
		add(filepath.Join("base", b))
	}
	for _, d := range st.DeltaFiles {
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

// rebuildingReason is what a reader is told while a re-snapshot runs whose
// previous contents could not be kept on disk beside the new ones. It is
// published in Halted — "why this must not be served" — so that every reader
// that honours a halt honours this, and it never reaches the manifest.
const rebuildingReason = "the table is being rebuilt from a snapshot and its previous " +
	"contents were too large to keep meanwhile (QS_RETIRE_MAX_BYTES); retry when it completes"

// publish writes the snapshot of the manifest saveState has just made durable.
// Only saveState and Halt's on-disk patch call it; see the top of the file for
// why that is enough.
func (t *Table) publish() error {
	refusal := t.State.Halted
	if refusal == "" {
		refusal = t.rebuilding
	}
	return t.publishManifest(&t.State, refusal)
}

// publishManifest publishes st, which is the manifest now on disk. refusal,
// when set, is why it must not be served.
//
// A refusing snapshot names no files and PROTECTS all of them: what the
// manifest names stays exactly where it is. A halted table's files are the
// evidence an operator comes to look at, and the basis of every reader still
// inside the grace of the snapshot the halt replaced; a halt says "do not
// serve this", not "this may be deleted". For the same reason a refusing
// publish never runs the restart sweep — as a process's first publish it
// would otherwise find every file unnamed and unlink all that are older than
// the grace.
func (t *Table) publishManifest(st *State, refusal string) error {
	p := t.publisher()
	p.load(t.Dir)

	pub := Published{
		Table:        t.Qualified,
		Seq:          p.seq + 1,
		AppliedLSN:   st.AppliedLSN,
		PublishedAt:  p.now().UTC(),
		GraceSeconds: p.grace.Seconds(),
		Files:        []string{},
	}
	protect := t.filesOf(st)
	if refusal != "" {
		pub.Halted = refusal
		pub.SQL = haltedSQL(t.Qualified, refusal)
	} else {
		sql, err := t.ViewSQL()
		if err != nil {
			return fmt.Errorf("publish snapshot: %w", err)
		}
		pub.SQL = sql
		pub.Files = append(pub.Files, protect...)
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
	t.notifyLake(st, refusal)

	named := make(map[string]bool, len(protect))
	for _, f := range protect {
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
	if !p.swept && refusal == "" {
		p.swept = true
		p.sweep(t.Dir, inflight, t.neverNamed)
		bases := append(append([]string{}, st.BaseFiles...), t.State.BaseFiles...)
		sweepIndex(t.Dir, bases, inflight)
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
	// Every OTHER file the replaced snapshot named and this one does not has
	// stopped being named at this moment too, retired or not, and its mtime
	// has to say so: that is the only thing a restart's sweep can judge it
	// by. Not all of them are retired here and now. A superseded vector
	// generation is kept for one further write; the files a swap or a merge
	// replaces are retired a few lines after the manifest that drops them; and
	// the first publish of a restarted process drops files that process never
	// held at all. Left with the mtime of the day they were written, a restart
	// inside their grace unlinked them on the spot — the files of the snapshot
	// every reader was on.
	for path := range p.named {
		if !named[path] && p.pending[path] == nil {
			_ = os.Chtimes(path, now, now)
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
	// The mtime is part of the file's identity, so it is read back.
	if fi, err := os.Lstat(path); err == nil {
		r.id = fi
	}
}

// claim takes a path back from the publisher because a file is about to be
// written there. Whatever deadline stood against the path was for a file that
// is being replaced, and must not fall on the new one. Every creation of a
// data file or a vector calls it; see the invariant at the top of this file.
func (t *Table) claim(path string) {
	p := t.publisher()
	p.mu.Lock()
	defer p.mu.Unlock()
	if r := p.pending[path]; r != nil {
		p.bytes -= r.size
		delete(p.pending, path)
	}
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
	r := &retired{size: fi.Size(), id: fi}
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

// drop ends a retirement, and unlinks the file only if the invariant at the
// top of this file allows it: the path is not named, and what is there now is
// the file the deadline was taken on. Anything else at that path was written
// after the deadline was set and belongs to whoever wrote it.
func (p *publisher) drop(path string, r *retired) {
	if !p.named[path] && sameFileAs(path, r.id) {
		_ = os.Remove(path)
	}
	p.bytes -= r.size
	delete(p.pending, path)
}

// enforceCap gives up the oldest retirements when the grace is holding more
// than RetireMaxBytes. Files the published snapshot still names are not
// candidates: their readers are current, not slow.
//
// Neither is a file with another name, which is what a DuckLake's hard link
// to it is (internal/ducklake). The cap is about disk, and unlinking such a
// file gives none back: counting it evicted the files of snapshot.json's
// readers early, to free bytes the lake went on holding. It is looked at
// only when the total is over the cap, and each time, because the lake drops
// its link when its own retention says so.
func (p *publisher) enforceCap() {
	if p.maxBytes <= 0 || p.bytes <= p.maxBytes {
		return
	}
	var paths []string
	var linked int64
	for path, r := range p.pending {
		if fi, err := os.Lstat(path); err == nil && linkedElsewhere(fi) {
			linked += r.size
			continue
		}
		if !r.deadline.IsZero() {
			paths = append(paths, path)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		return p.pending[paths[i]].deadline.Before(p.pending[paths[j]].deadline)
	})
	for _, path := range paths {
		if p.bytes-linked <= p.maxBytes {
			return
		}
		p.drop(path, p.pending[path])
	}
}

// arm makes sure something will come back for the earliest deadline, so that
// an idle table still gives its disk back. Called with mu held.
func (p *publisher) arm() {
	if p.manual || p.closed || p.timer != nil {
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
		if p.closed {
			return
		}
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
// That is for files a manifest ONCE named. A file none ever can have
// (neverNamed) is what a crash left half-made: no reader holds it, so it has
// no grace to wait out, and its name is the next one this process will write.
// It is unlinked here and never adopted — see the invariant at the top.
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
func (p *publisher) sweep(dir, inflight string, neverNamed func(sub, name string) bool) {
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
			if !deadline.After(now) || neverNamed(sub, e.Name()) {
				_ = os.Remove(path)
				continue
			}
			p.pending[path] = &retired{size: info.Size(), deadline: deadline, id: info}
			p.bytes += info.Size()
		}
	}
}

// sweepIndex removes what a crash left under index/: the buckets of a rewrite
// that never finished, the runs and temporaries of an index build that never
// did, and the index of a base file the manifest no longer names. None of it
// has a reader (an index is this process's own), so nothing waits out the
// grace.
//
// It runs where sweep does: once, at the writer's first manifest write, so
// that qs-query and qs-verify, which open the same directory and write no
// manifest, never remove anything. What is kept is the index of each base
// file the manifest names, everything of the rewrite in flight (inflight is
// its base file's path), and any bucket whose rewrite is registered as
// running (compact_cluster.go), which is what makes the last safe without
// trusting this function's idea of what is in flight.
func sweepIndex(dir string, bases []string, inflight string) {
	keep := map[string]bool{}
	for _, b := range bases {
		keep[trimParquet(filepath.Base(b))+".kidx"] = true
	}
	flying := ""
	if inflight != "" {
		flying = trimParquet(filepath.Base(inflight)) + ".kidx"
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "index"))
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(dir, "index", name)
		if e.IsDir() || keep[name] || scratchLive(path) {
			continue
		}
		// The index being built for the new base: the file, its sorted runs
		// and the temporary the merge writes (kidx.go). Nothing else.
		if rest, ok := strings.CutPrefix(name, flying); ok && flying != "" &&
			(rest == "" || strings.HasPrefix(rest, ".run") || strings.HasPrefix(rest, ".tmp")) {
			continue
		}
		_ = os.Remove(path)
	}
}

// sameFileAs reports whether the file at path is still the one id describes.
// The inode alone is not enough — a filesystem hands a freed inode number
// straight back out — so the mtime and size recorded with it must match too.
func sameFileAs(path string, id os.FileInfo) bool {
	if id == nil {
		return false
	}
	fi, err := os.Lstat(path)
	return err == nil && os.SameFile(fi, id) &&
		fi.ModTime().Equal(id.ModTime()) && fi.Size() == id.Size()
}

// neverNamed reports whether a file under sub ("base", "delta" or "dv") is one
// no manifest of this table can ever have named, judged against the manifest
// in State — which, where sweep calls this, is the one just made durable.
//
// Names are sequence numbers and the manifest holds the highest it has
// committed, so a data file numbered above it was written by something that
// did not reach its manifest. A vector is the same one level down: its
// generation is above the one the manifest names for its data file, or its
// data file is itself above the sequence.
func (t *Table) neverNamed(sub, name string) bool {
	switch sub {
	case "base", "delta":
		n, err := strconv.Atoi(trimParquet(name))
		return err == nil && n > t.State.Seq
	case "dv":
		gen := dvGenOf(name)
		i := strings.Index(name, ".")
		if gen < 0 || i < 0 {
			return false
		}
		stem := name[:i]
		if n, err := strconv.Atoi(stem); err == nil && n > t.State.Seq {
			return true
		}
		live := contains(t.State.BaseFiles, stem+".parquet") ||
			contains(t.State.DeltaFiles, stem+".parquet")
		return live && gen > t.State.DVGen[stem]
	}
	return false
}

// stop ends the publisher's background work. Files still waiting out their
// grace are left for the next process's sweep, which is what they would have
// been left for had this one died.
func (p *publisher) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
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
