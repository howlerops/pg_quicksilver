package mirror

// The on-disk base index against the map it replaced.
//
// kidx.go's claim is that a sorted file plus one bit per base position gives
// "exactly the answers the one big map gave". That is the kind of sentence this
// package has learned not to leave as a sentence, so the map is kept here, as
// it was, and the two are driven through the same history — inserts, updates,
// deletes, re-inserts of a deleted key, digests learned and forgotten, delta
// merges, a rewrite racing all of those and its swap, truncates, and restarts
// with and without the index file — and compared after every step.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// oracleIndex is keyIndex as it stood before the base moved to disk.
type oracleIndex struct {
	nums  map[int64]loc
	uuids map[[2]uint64]loc
	strs  map[string]loc
}

func newOracle() *oracleIndex {
	return &oracleIndex{nums: map[int64]loc{}, uuids: map[[2]uint64]loc{}, strs: map[string]loc{}}
}

func (x *oracleIndex) get(k rowKey) (loc, bool) {
	switch {
	case k.isNum:
		l, ok := x.nums[k.num]
		return l, ok
	case k.isUUID:
		l, ok := x.uuids[[2]uint64{uint64(k.num), k.lo}]
		return l, ok
	}
	l, ok := x.strs[k.str]
	return l, ok
}

func (x *oracleIndex) set(k rowKey, l loc) {
	switch {
	case k.isNum:
		x.nums[k.num] = l
	case k.isUUID:
		x.uuids[[2]uint64{uint64(k.num), k.lo}] = l
	default:
		x.strs[k.str] = l
	}
}

func (x *oracleIndex) del(k rowKey) {
	switch {
	case k.isNum:
		delete(x.nums, k.num)
	case k.isUUID:
		delete(x.uuids, [2]uint64{uint64(k.num), k.lo})
	default:
		delete(x.strs, k.str)
	}
}

func (x *oracleIndex) len() int { return len(x.nums) + len(x.uuids) + len(x.strs) }

func (x *oracleIndex) each(fn func(rowKey, loc)) {
	for n, l := range x.nums {
		fn(rowKey{num: n, isNum: true}, l)
	}
	for u, l := range x.uuids {
		fn(rowKey{num: int64(u[0]), lo: u[1], isUUID: true}, l)
	}
	for s, l := range x.strs {
		fn(rowKey{str: s}, l)
	}
}

func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func testUUID(i int) string { return formatUUID(mix64(uint64(i)*2+1), mix64(uint64(i)*2+2)) }

// modelKeyText is the i-th key of the model's universe, as PostgreSQL prints it.
// Integers are spread over the whole signed range, so the sign flip that makes
// byte order numeric order is exercised, with a run of small ones for density.
func modelKeyText(uuid bool, i int) string {
	if uuid {
		return testUUID(i)
	}
	if i%3 == 0 {
		return fmt.Sprint(i - 500)
	}
	return fmt.Sprint(int64(mix64(uint64(i))))
}

func kidxTable(t testing.TB, uuid bool) *Table {
	t.Helper()
	typ := "bigint"
	if uuid {
		typ = "uuid"
	}
	return kidxTableOf(t, typ)
}

// modelTextKey is the i-th key of a text-keyed model: every shape of byte
// string a key column can hand over, chosen to be hard on an index that finds
// keys by a hash of them. All distinct.
func modelTextKey(i int) string {
	n := fmt.Sprint(i)
	switch i % 8 {
	case 0:
		if i == 0 {
			return "" // the empty key is a key
		}
		return "k" + n
	case 1:
		// Two hundred bytes in common, different only at the very end.
		return strings.Repeat("p", 200) + n
	case 2:
		// Arbitrary bytes, NUL and 0xff among them, of every short length.
		b := make([]byte, 1+i%40)
		for j := range b {
			b[j] = byte(mix64(uint64(i)*131 + uint64(j)))
		}
		return string(b) + "\x00" + n
	case 3:
		return testUUID(i) // canonical: held as 128 bits in memory, as text on disk
	case 4:
		return strings.ToUpper(testUUID(i)) // not canonical: a different key from its lower case
	case 5:
		// What an integer column would have parsed, and what it would not.
		return []string{"", "0", "-", "+", "00"}[i/8%5] + n + []string{"", ".0", " ", "e0"}[i/40%4]
	case 6:
		// Each a prefix of the next.
		return "q" + n + strings.Repeat("\x00", i/8%7)
	}
	if i < 64 {
		return strings.Repeat(n+"/", 20_000) // a hundred kilobytes and more
	}
	return strings.Repeat("z", 300) + n
}

// weakKidxHash makes collisions ordinary: fourteen bits of hash, so a universe
// of sixty thousand keys puts about four on every sort key.
func weakKidxHash(s string) kkey {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return kkey{hi: mix64(h) >> 50 << 50, lo: uint64(len(s) & 1)}
}

func withKidxHash(t testing.TB, h func(string) kkey) {
	old := kidxHash
	kidxHash = h
	t.Cleanup(func() { kidxHash = old })
}

func kidxTableOf(t testing.TB, typ string) *Table {
	t.Helper()
	cols := map[string]string{"id": typ, "v": "text", "n": "bigint"}
	tbl, err := New(t.TempDir(), "public", "r", "id", cols, []string{"id", "v", "n"})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

type kidxModel struct {
	t      *testing.T
	tbl    *Table
	rng    *rand.Rand
	uuid   bool
	keys   []rowKey
	texts  []string
	x      *keyIndex
	o      *oracleIndex
	base   string   // current base file name, "" if none
	dead   *deadSet // what markDead would hold for it
	seq    int
	delta  fileID
	dpos   int32
	recent []int // keys lately written to a delta
	// touched is what Table.Apply reports to a rewrite in flight.
	touched map[rowKey]bool
	step    int
}

func (m *kidxModel) check(i int) {
	k := m.keys[i]
	got, gok := m.x.get(k)
	want, wok := m.o.get(k)
	if gok != wok || got != want {
		m.t.Fatalf("step %d key %s: index says %+v,%v and the map says %+v,%v",
			m.step, k, got, gok, want, wok)
	}
}

func (m *kidxModel) checkAll(when string) {
	if m.x.len() != m.o.len() {
		m.t.Fatalf("step %d (%s): len %d, the map has %d", m.step, when, m.x.len(), m.o.len())
	}
	seen := make(map[rowKey]bool, m.o.len())
	m.x.each(func(k rowKey, l loc) {
		if seen[k] {
			m.t.Fatalf("step %d (%s): each gave %s twice", m.step, when, k)
		}
		seen[k] = true
		if want, ok := m.o.get(k); !ok || want != l {
			m.t.Fatalf("step %d (%s): each gave %s at %+v, the map says %+v,%v", m.step, when, k, l, want, ok)
		}
	})
	if len(seen) != m.o.len() {
		m.t.Fatalf("step %d (%s): each gave %d keys, the map has %d", m.step, when, len(seen), m.o.len())
	}
	for i := range m.keys {
		m.check(i)
	}
}

func (m *kidxModel) newDelta() {
	m.seq++
	m.delta, m.dpos = makeFileID(kindDelta, m.seq), 0
}

// supersede is what Apply does to a key's current row before it writes or
// deletes: mark the position dead. Only the base file's set is modelled.
func (m *kidxModel) supersede(k rowKey) {
	l, ok := m.o.get(k)
	if ok && m.base != "" && l.File == baseID(m.base) {
		m.dead.add(int(l.Pos))
	}
	if m.touched != nil {
		m.touched[k] = true
	}
}

func (m *kidxModel) upsert(i int) {
	k := m.keys[i]
	m.check(i)
	m.supersede(k)
	l := loc{File: m.delta, Pos: m.dpos}
	if m.rng.Intn(4) == 0 {
		l.Hash = m.rng.Uint64() | 1
	}
	m.dpos++
	m.x.set(k, l)
	m.o.set(k, l)
	m.recent = append(m.recent, i)
	m.check(i)
}

func (m *kidxModel) delete(i int) {
	k := m.keys[i]
	m.check(i)
	m.supersede(k)
	m.x.del(k)
	m.o.del(k)
	m.check(i)
}

// noteHash is noteHashes: the same row, with its digest learned or cleared.
func (m *kidxModel) noteHash(i int) {
	k := m.keys[i]
	l, ok := m.o.get(k)
	if !ok {
		return
	}
	l.Hash = 0
	if m.rng.Intn(2) == 0 {
		l.Hash = m.rng.Uint64() | 1
	}
	m.x.set(k, l)
	m.o.set(k, l)
	m.check(i)
}

// merge is mergeGroup: keys that live in deltas move to a new delta.
func (m *kidxModel) merge() {
	m.newDelta()
	n := min(len(m.recent), 300)
	for _, i := range m.recent[len(m.recent)-n:] {
		k := m.keys[i]
		l, ok := m.o.get(k)
		if !ok || l.File.kind() != kindDelta {
			continue
		}
		nl := loc{File: m.delta, Pos: m.dpos, Hash: l.Hash}
		m.dpos++
		m.x.set(k, nl)
		m.o.set(k, nl)
		if m.touched != nil {
			m.touched[k] = true
		}
		m.check(i)
	}
	m.recent = m.recent[:0]
	m.newDelta()
}

func (m *kidxModel) randomOp() {
	m.step++
	i := m.rng.Intn(len(m.keys))
	switch r := m.rng.Intn(100); {
	case r < 45:
		m.upsert(i)
	case r < 70:
		m.delete(i)
	case r < 80:
		m.noteHash(i)
	case r < 99:
		m.check(i)
	default:
		if m.rng.Intn(20) == 0 {
			m.merge()
		} else if m.rng.Intn(8) == 0 {
			m.newDelta()
		}
	}
	if m.step%50_000 == 0 && m.x.len() != m.o.len() {
		m.t.Fatalf("step %d: len %d, the map has %d", m.step, m.x.len(), m.o.len())
	}
}

// rewrite is BeginCompaction, some Applies while it runs, and FinishCompaction.
func (m *kidxModel) rewrite(during int) {
	// The snapshot: every live key, and for a few of them a stale copy ahead of
	// it, which is what a rewrite reading between "mark dead" and "write new"
	// folds in.
	type row struct {
		k    rowKey
		text string
		hash uint64
	}
	textOf := map[rowKey]string{}
	for i, k := range m.keys {
		textOf[k] = m.texts[i]
	}
	var rows []row
	m.o.each(func(k rowKey, _ loc) {
		r := row{k: k, text: textOf[k]}
		if m.rng.Intn(3) == 0 {
			r.hash = m.rng.Uint64() | 1
		}
		rows = append(rows, r)
	})
	m.rng.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	for i := 0; i < len(rows)/100; i++ {
		// A LATER duplicate is the newer one; make that explicit by giving the
		// earlier copy a different digest.
		stale := rows[m.rng.Intn(len(rows))]
		stale.hash = 7
		rows = append([]row{stale}, rows...)
	}

	m.seq++
	name := fmt.Sprintf("%06d.parquet", m.seq)
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		out[i] = map[string]any{"id": r.text, "v": "x", "n": "1"}
	}
	if err := m.tbl.writeParquet(filepath.Join(m.tbl.Dir, "base", name), out); err != nil {
		m.t.Fatal(err)
	}
	kb := m.tbl.newKidxBuilder(name, len(rows), true)
	newO := newOracle()
	var wantDups []int
	for i, r := range rows {
		if prev, ok := newO.get(r.k); ok {
			wantDups = append(wantDups, int(prev.Pos))
		}
		newO.set(r.k, loc{File: baseID(name), Pos: int32(i), Hash: r.hash})
		if err := kb.add(r.k, int32(i), r.hash, false); err != nil {
			m.t.Fatal(err)
		}
	}
	newX, recDups, extraDups, err := kb.finish()
	if err != nil {
		m.t.Fatal(err)
	}
	dups := append(recDups, extraDups...)
	if len(dups) != len(wantDups) {
		m.t.Fatalf("rewrite found %d superseded copies, the map found %d", len(dups), len(wantDups))
	}
	ds := map[int]bool{}
	for _, p := range dups {
		ds[p] = true
	}
	for _, p := range wantDups {
		if !ds[p] {
			m.t.Fatalf("rewrite did not report position %d as superseded", p)
		}
	}
	newX.markGone(recDups)

	// The apply loop carries on against the OLD index while the rewrite runs.
	m.touched = map[rowKey]bool{}
	m.newDelta()
	for i := 0; i < during; i++ {
		m.randomOp()
	}

	// The swap, as FinishCompaction does it, on both.
	newDead := &deadSet{}
	for _, p := range dups {
		newDead.add(p)
	}
	for k := range m.touched {
		inNew, wasFolded := newX.get(k)
		oNew, oFolded := newO.get(k)
		if wasFolded != oFolded || inNew != oNew {
			m.t.Fatalf("swap: rewrite index says %+v,%v for %s, the map says %+v,%v", inNew, wasFolded, k, oNew, oFolded)
		}
		cur, stillLive := m.x.get(k)
		if wasFolded && (!stillLive || !cur.sameRow(inNew)) {
			newDead.add(int(inNew.Pos))
		}
		if stillLive {
			newX.set(k, cur)
			newO.set(k, cur)
		} else {
			newX.del(k)
			newO.del(k)
		}
	}
	m.touched = nil
	old := m.base
	m.x.close()
	m.x, m.o, m.base, m.dead = newX, newO, name, newDead
	if old != "" {
		_ = os.Remove(filepath.Join(m.tbl.Dir, "base", old))
		m.tbl.dropKidx(old)
	}
	m.checkAll("after swap")
}

// restart drops the index and loads it the way ensureIndex does: the base from
// its file (or, with the file gone, from the key column), the deltas replayed
// on top. Digests do not survive a restart in either implementation.
func (m *kidxModel) restart(loseFile bool) {
	m.x.close()
	strip := newOracle()
	m.o.each(func(k rowKey, l loc) {
		l.Hash = 0
		strip.set(k, l)
	})
	m.o = strip
	if m.base == "" {
		m.x = newKeyIndex(0)
	} else {
		if loseFile {
			m.tbl.dropKidx(m.base)
		}
		before := kidxRebuilds.Load()
		m.tbl.State.BaseFiles = []string{m.base}
		m.tbl.dead = map[string]*deadSet{dvStem(m.base): m.dead}
		x, err := m.tbl.baseIndex(m.base)
		if err != nil {
			m.t.Fatal(err)
		}
		if rebuilt := kidxRebuilds.Load() != before; rebuilt != loseFile {
			m.t.Fatalf("step %d: restart rebuilt=%v with the file lost=%v", m.step, rebuilt, loseFile)
		}
		m.x = x
	}
	m.o.each(func(k rowKey, l loc) {
		if l.File.kind() == kindDelta {
			m.x.set(k, l)
		}
	})
	m.checkAll("after restart")
}

func (m *kidxModel) truncate() {
	m.x.close()
	if m.base != "" {
		_ = os.Remove(filepath.Join(m.tbl.Dir, "base", m.base))
		m.tbl.dropKidx(m.base)
	}
	m.x, m.o, m.base, m.dead = newKeyIndex(0), newOracle(), "", nil
	m.recent = m.recent[:0]
	m.newDelta()
}

func TestKidxModel(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy; run without -short")
	}
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	// Small runs, so that every rewrite of a few tens of thousands of keys goes
	// through the spill-and-merge path as well as the in-memory tail.
	defer func(n int) { kidxRunRecords = n }(kidxRunRecords)
	kidxRunRecords = 7_000

	universe, rounds, perRound := 60_000, 24, 50_000
	if testing.Short() {
		rounds = 4
	}
	for _, v := range []struct {
		name, typ string
		kind      int
		key       func(int) string
		hash      func(string) kkey
	}{
		{"int64", "bigint", kidxInt, func(i int) string { return modelKeyText(false, i) }, nil},
		{"uuid", "uuid", kidxUUID, func(i int) string { return modelKeyText(true, i) }, nil},
		// Keys of any length: every byte string, found by hash and confirmed
		// by comparison. Once with the real hash and once with one so weak
		// that every lookup meets other keys' records on the way.
		{"text", "text", kidxBytes, modelTextKey, nil},
		{"text, colliding", "text", kidxBytes, modelTextKey, weakKidxHash},
		{"varchar of uuids", "character varying", kidxBytes, func(i int) string { return modelKeyText(true, i) }, nil},
	} {
		uuid := v.typ == "uuid"
		t.Run(v.name, func(t *testing.T) {
			if v.hash != nil {
				withKidxHash(t, v.hash)
			}
			m := &kidxModel{
				t: t, tbl: kidxTableOf(t, v.typ), uuid: uuid,
				rng: rand.New(rand.NewSource(20261125)), x: newKeyIndex(0), o: newOracle(),
			}
			seen := map[string]bool{}
			for i := 0; i < universe; i++ {
				s := v.key(i)
				if seen[s] {
					t.Fatalf("the universe holds %q twice", s)
				}
				seen[s] = true
				m.texts = append(m.texts, s)
				m.keys = append(m.keys, m.tbl.keyOfString(s))
			}
			if got := m.tbl.kidxKind(); got != v.kind {
				t.Fatalf("a %s key is indexed as kind %d, want %d", v.typ, got, v.kind)
			}
			if _, _, ok := kidxKeyOf(v.kind, m.keys[1]); !ok {
				t.Fatalf("key %q is not one its own table's index holds", m.keys[1])
			}
			m.newDelta()
			for r := 0; r < rounds; r++ {
				for i := 0; i < perRound; i++ {
					m.randomOp()
				}
				switch r % 6 {
				case 0, 2, 4:
					m.rewrite(perRound / 5)
				case 1:
					m.restart(false)
				case 3:
					m.restart(true)
					m.restart(false) // the rebuild wrote a file; it must load
				case 5:
					m.checkAll("before truncate")
					m.truncate()
				}
			}
			// The last round truncated. Finish on a table with a base file, a
			// vector over it and deltas on top, and restart that.
			for i := 0; i < perRound; i++ {
				m.randomOp()
			}
			m.rewrite(perRound / 5)
			for i := 0; i < perRound; i++ {
				m.randomOp()
			}
			m.restart(false)
			m.checkAll("at the end")
			if m.x.disk == nil || m.x.live == 0 || m.dead.n == 0 {
				t.Fatal("the model never exercised the on-disk index")
			}
			if n := len(m.x.strs) + len(m.x.uuids) + len(m.x.nums); n+m.x.live != m.x.len() || m.x.disk.kind != v.kind {
				t.Fatalf("%d keys in memory and %d in the file do not make %d", n, m.x.live, m.x.len())
			}
			t.Logf("%d steps, %d keys live, %d in the base file", m.step, m.x.len(), m.x.live)
			m.x.close()
		})
	}
}

// kidxApply runs one batch through the real apply path.
func kidxApply(t testing.TB, tbl *Table, lsn *int, changes []changestream.Change) {
	t.Helper()
	*lsn++
	pos := fmt.Sprintf("0/%X", *lsn)
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: changes}}); err != nil {
		t.Fatal(err)
	}
}

func kidxInsert(uuid bool, i int, v string) changestream.Change {
	r := map[string]any{"id": modelKeyText(uuid, i), "v": v, "n": fmt.Sprint(i)}
	return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r}
}

func kidxDelete(uuid bool, i int) changestream.Change {
	k := map[string]any{"id": modelKeyText(uuid, i)}
	return changestream.Change{Op: changestream.OpDelete, Schema: "public", Table: "r", Key: k}
}

func indexDump(t testing.TB, tbl *Table) map[string]string {
	t.Helper()
	if _, err := tbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	tbl.index.each(func(k rowKey, l loc) { out[k.String()] = fmt.Sprintf("%s#%d", l.File.name(), l.Pos) })
	if len(out) != tbl.index.len() {
		t.Fatalf("each gave %d keys, len says %d", len(out), tbl.index.len())
	}
	return out
}

// TestKidxNeverTrusted damages or misplaces the index file in every way that
// was thought of and requires each to be DETECTED — the index rebuilt from the
// base file, the answers those of an undamaged one — and never used.
func TestKidxNeverTrusted(t *testing.T) {
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	for _, name := range []string{"int64", "uuid", "text"} {
		uuid := name != "int64"
		t.Run(name, func(t *testing.T) {
			tbl := kidxTable(t, uuid)
			if name == "text" {
				// The same keys, in a column that holds them as bytes: the
				// file has a heap, and the heap is checked like the records.
				tbl = kidxTableOf(t, "text")
			}
			root := filepath.Dir(tbl.Dir)
			reopen := func() *Table {
				n, err := New(root, "public", "r", "id", tbl.Columns, tbl.Order)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			lsn := 0
			var ch []changestream.Change
			for i := 0; i < 3000; i++ {
				ch = append(ch, kidxInsert(uuid, i, "a"))
			}
			kidxApply(t, tbl, &lsn, ch)
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}
			// The index of THIS base file, kept to be planted on the next one.
			previous, err := os.ReadFile(tbl.kidxPath(tbl.State.BaseFiles[0]))
			if err != nil {
				t.Fatalf("Compact wrote no index file: %v", err)
			}
			ch = nil
			for i := 0; i < 3000; i += 3 {
				ch = append(ch, kidxDelete(uuid, i))
			}
			for i := 3000; i < 4000; i++ {
				ch = append(ch, kidxInsert(uuid, i, "b"))
			}
			kidxApply(t, tbl, &lsn, ch)
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}
			// And something on top of the base: deletes in its vector, rows in a
			// delta, so a wrong index has every chance to give a wrong answer.
			ch = nil
			for i := 1; i < 3000; i += 7 {
				ch = append(ch, kidxDelete(uuid, i))
			}
			for i := 2; i < 3000; i += 11 {
				ch = append(ch, kidxInsert(uuid, i, "c"))
			}
			kidxApply(t, tbl, &lsn, ch)
			want := indexDump(t, tbl)
			wantRows, err := tbl.Live()
			if err != nil {
				t.Fatal(err)
			}
			base := tbl.State.BaseFiles[0]
			path := tbl.kidxPath(base)
			good, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			tbl.index.close()

			same := func(when string, n *Table) {
				t.Helper()
				got := indexDump(t, n)
				if len(got) != len(want) {
					t.Fatalf("%s: %d keys, want %d", when, len(got), len(want))
				}
				for k, v := range want {
					if got[k] != v {
						t.Fatalf("%s: key %s at %q, want %q", when, k, got[k], v)
					}
				}
				rows, err := n.Live()
				if err != nil {
					t.Fatal(err)
				}
				if c1, s1 := Checksum(rows, n.Order); true {
					if c2, s2 := Checksum(wantRows, n.Order); c1 != c2 || s1 != s2 {
						t.Fatalf("%s: live rows differ", when)
					}
				}
				n.index.close()
			}

			// An intact file is used as it is.
			before := kidxRebuilds.Load()
			same("intact", reopen())
			if kidxRebuilds.Load() != before {
				t.Fatal("an intact index file was rebuilt")
			}

			flip := func(off int) func() {
				return func() {
					b := append([]byte(nil), good...)
					b[off] ^= 0x40
					if err := os.WriteFile(path, b, 0o644); err != nil {
						t.Fatal(err)
					}
				}
			}
			write := func(b []byte) func() {
				return func() {
					if err := os.WriteFile(path, b, 0o644); err != nil {
						t.Fatal(err)
					}
				}
			}
			rec := len(good) - kidxHeader
			v1 := append([]byte(nil), good...)
			copy(v1, kidxMagicV1)
			cases := []struct {
				name string
				do   func()
			}{
				{"missing", func() { _ = os.Remove(path) }},
				{"empty", write(nil)},
				{"truncated mid-record", write(good[:len(good)-5])},
				{"truncated by whole records", write(good[:len(good)-2*(rec/4000)])},
				{"truncated to the header", write(good[:kidxHeader])},
				{"truncated inside the header", write(good[:40])},
				{"one extra record", write(append(append([]byte(nil), good...), good[len(good)-rec/4000:]...))},
				{"flipped byte in a key", flip(kidxHeader + 3)},
				{"flipped byte in a position", flip(len(good) - 10)},
				{"flipped byte in the middle", flip(kidxHeader + rec/2)},
				{"flipped byte in the record count", flip(16)},
				{"flipped byte in the base size", flip(48)},
				{"flipped byte in the checksum", flip(64)},
				{"flipped magic", flip(0)},
				{"flipped byte in the key kind", flip(8)},
				{"flipped byte in the heap length", flip(72)},
				{"flipped byte in the heap checksum", flip(68)},
				{"flipped last byte", flip(len(good) - 1)},
				{"the previous format's magic", write(v1)},
				{"a file of the previous format", write(kidxV1File(t, tbl, base))},
				{"the previous base file's index", write(previous)},
				{"garbage", write([]byte("this is not an index file, but it is at least as long as a header should be...."))},
				{"base file replaced under the same name", func() {
					// A rewrite that died before its manifest reuses the name.
					// Same rows, written again: only the file's identity moved.
					write(good)()
					p := filepath.Join(tbl.Dir, "base", base)
					b, err := os.ReadFile(p)
					if err != nil {
						t.Fatal(err)
					}
					st, _ := os.Stat(p)
					if err := os.WriteFile(p, b, 0o644); err != nil {
						t.Fatal(err)
					}
					_ = os.Chtimes(p, st.ModTime().Add(1), st.ModTime().Add(1))
				}},
			}
			for _, c := range cases {
				c.do()
				before := kidxRebuilds.Load()
				same(c.name, reopen())
				if kidxRebuilds.Load() != before+1 {
					t.Fatalf("%s: the index file was trusted", c.name)
				}
				// What the rebuild left must itself be a file that loads.
				before = kidxRebuilds.Load()
				same(c.name+", second start", reopen())
				if kidxRebuilds.Load() != before {
					t.Fatalf("%s: the rebuilt index file did not load", c.name)
				}
				if c.name != "base file replaced under the same name" {
					now, err := os.ReadFile(path)
					if err != nil || string(now) != string(good) {
						t.Fatalf("%s: the rebuilt file is not the one the base file determines", c.name)
					}
				} else if good, err = os.ReadFile(path); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// kidxV1File is the index of a base file exactly as the previous release wrote
// it: QSKIDX01, a 72-byte header, records of key | position | digest, every
// checksum right. What this binary must do with one is rebuild it.
func kidxV1File(t testing.TB, tbl *Table, base string) []byte {
	t.Helper()
	path := filepath.Join(tbl.Dir, "base", base)
	var recs []krec
	err := tbl.forEachRowGroup(path, nil, []string{tbl.Key}, func(rows []map[string]any, first int) error {
		for i, r := range rows {
			k := tbl.keyOf(r[tbl.Key])
			if kidxWidth(k) == 0 {
				k = rowKey{num: int64(mix64(uint64(first + i))), lo: 1, isUUID: true}
			}
			recs = append(recs, krec{k: toKKey(k), pos: uint32(first + i)})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(recs, krecCmp)
	width, kind := 16, kidxUUID
	if tbl.numericKey {
		width, kind = 8, kidxInt
	}
	bind, err := kidxBindFor(path, baseID(base), uint64(len(recs)))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 72)
	var buf [kidxMaxRec]byte
	for _, r := range recs {
		putRec(buf[:], kind, r)
		out = append(out, buf[:width+12]...)
	}
	le := binary.LittleEndian
	copy(out, kidxMagicV1)
	le.PutUint32(out[8:], uint32(width))
	le.PutUint64(out[16:], uint64(len(recs)))
	le.PutUint64(out[24:], bind.rows)
	le.PutUint64(out[40:], bind.seq)
	le.PutUint64(out[48:], bind.size)
	le.PutUint64(out[56:], uint64(bind.mtime))
	le.PutUint32(out[64:], crc32.Checksum(out[72:], crcTable))
	le.PutUint32(out[68:], crc32.Checksum(out[:68], crcTable))
	return out
}

// TestKidxTextKeys: a key that is neither an integer nor a uuid used to have no
// index file at all, and a table that mixed UUID-shaped text with other text
// rebuilt at every start. Both are in the file now, as their bytes, and the
// file is reused.
func TestKidxTextKeys(t *testing.T) {
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	cols := map[string]string{"id": "text", "v": "text"}
	root := t.TempDir()
	open := func() *Table {
		tbl, err := New(root, "public", "r", "id", cols, []string{"id", "v"})
		if err != nil {
			t.Fatal(err)
		}
		return tbl
	}
	ins := func(id string) changestream.Change {
		r := map[string]any{"id": id, "v": "x"}
		return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r}
	}
	tbl := open()
	lsn := 0
	var ch []changestream.Change
	for i := 0; i < 500; i++ {
		ch = append(ch, ins(fmt.Sprintf("key-%d", i)))
	}
	kidxApply(t, tbl, &lsn, ch)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if d := tbl.index.disk; d == nil || d.kind != kidxBytes || d.n != 500 || len(tbl.index.strs) != 0 {
		t.Fatalf("a text-keyed table: disk=%+v, %d keys in memory", d, len(tbl.index.strs))
	}

	// Now UUID-shaped text as well, the empty key, and a long one.
	long := strings.Repeat("long/", 50_000)
	ch = []changestream.Change{ins(""), ins(long), ins(long + "x")}
	for i := 0; i < 500; i++ {
		ch = append(ch, ins(testUUID(i)))
	}
	kidxApply(t, tbl, &lsn, ch)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	del := map[string]any{"id": "key-7"}
	kidxApply(t, tbl, &lsn, []changestream.Change{
		{Op: changestream.OpDelete, Schema: "public", Table: "r", Key: del},
		{Op: changestream.OpDelete, Schema: "public", Table: "r", Key: map[string]any{"id": testUUID(9)}},
		{Op: changestream.OpDelete, Schema: "public", Table: "r", Key: map[string]any{"id": long}},
	})
	want := indexDump(t, tbl)
	if n := len(tbl.index.strs) + len(tbl.index.uuids); len(want) != 1000 || tbl.index.disk == nil || n != 0 {
		t.Fatalf("mixed table: %d keys, disk=%v, %d in memory", len(want), tbl.index.disk != nil, n)
	}
	for _, k := range []string{"", long + "x", "key-8", testUUID(8)} {
		if _, ok := want[k]; !ok {
			t.Fatalf("key %.20q is not in the index", k)
		}
	}
	for _, k := range []string{long, long[:len(long)-1], "key-7", testUUID(9), strings.ToUpper(testUUID(8)), "key-8\x00", " "} {
		if _, ok := tbl.index.get(tbl.keyOfString(k)); ok {
			t.Fatalf("key %.20q is in the index", k)
		}
	}
	tbl.index.close()
	before := kidxRebuilds.Load()
	again := open()
	got := indexDump(t, again)
	if kidxRebuilds.Load() != before {
		t.Fatal("the index of a text-keyed table was rebuilt at start")
	}
	if len(got) != len(want) {
		t.Fatalf("after restart %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("after restart key %.20q at %q, want %q", k, got[k], v)
		}
	}
	again.index.close()
}

// TestKidxOneHashForEveryKey: a hash that says the same thing about every key
// leaves the bytes to do all the work. The index must still give each key its
// own row, across fence boundaries, through a rewrite and after a restart.
func TestKidxOneHashForEveryKey(t *testing.T) {
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	withKidxHash(t, func(string) kkey { return kkey{hi: 7, lo: 7} })
	defer func(n int) { kidxRunRecords = n }(kidxRunRecords)
	kidxRunRecords = 100
	tbl := kidxTableOf(t, "text")
	base := "000001.parquet"
	keys := make([]string, 700)
	rows := make([]map[string]any, 0, len(keys)+50)
	for i := range keys {
		keys[i] = modelTextKey(i + 64)
		rows = append(rows, map[string]any{"id": keys[i], "v": "x", "n": "1"})
	}
	// Fifty of them twice: the later copy is the live one.
	for i := 0; i < 50; i++ {
		rows = append(rows, map[string]any{"id": keys[i*7], "v": "y", "n": "2"})
	}
	if err := tbl.writeParquet(filepath.Join(tbl.Dir, "base", base), rows); err != nil {
		t.Fatal(err)
	}
	build := func() (*keyIndex, []int) {
		kb := tbl.newKidxBuilder(base, len(rows), true)
		for i, r := range rows {
			if err := kb.add(tbl.keyOfString(r["id"].(string)), int32(i), 0, false); err != nil {
				t.Fatal(err)
			}
		}
		x, dups, extra, err := kb.finish()
		if err != nil || len(extra) != 0 {
			t.Fatal(err, extra)
		}
		return x, dups
	}
	x, dups := build()
	slices.Sort(dups)
	if len(dups) != 50 {
		t.Fatalf("%d superseded copies found, want 50", len(dups))
	}
	for i, p := range dups {
		if p != i*7 {
			t.Fatalf("superseded copy %d is position %d, want %d", i, p, i*7)
		}
	}
	x.markGone(dups)
	check := func(x *keyIndex) {
		t.Helper()
		if x.disk == nil || x.disk.n != len(rows) || x.len() != len(keys) {
			t.Fatalf("index holds %d keys, want %d", x.len(), len(keys))
		}
		for i, k := range keys {
			want := int32(i)
			if i%7 == 0 && i/7 < 50 {
				want = int32(len(keys) + i/7)
			}
			if l, ok := x.get(tbl.keyOfString(k)); !ok || l.Pos != want {
				t.Fatalf("key %d: %+v,%v, want position %d", i, l, ok, want)
			}
			if _, ok := x.get(tbl.keyOfString(k + "?")); ok {
				t.Fatalf("a key that was never written was found")
			}
		}
		n := 0
		x.each(func(k rowKey, l loc) { n++ })
		if n != len(keys) {
			t.Fatalf("each gave %d keys, want %d", n, len(keys))
		}
	}
	check(x)
	x.close()
	// And from the file, as a start loads it.
	tbl.State.BaseFiles = []string{base}
	dead := &deadSet{}
	for _, p := range dups {
		dead.add(p)
	}
	tbl.dead = map[string]*deadSet{dvStem(base): dead}
	before := kidxRebuilds.Load()
	y, err := tbl.baseIndex(base)
	if err != nil || kidxRebuilds.Load() != before {
		t.Fatalf("the file did not load: %v", err)
	}
	check(y)
	y.close()
}

// TestKidxDigestsDoNotSurviveRestart: a digest in the file was true when the
// file was written. What was learned since lived in memory, so a later process
// may not believe any of them (kidx.go, noHash).
func TestKidxDigestsDoNotSurviveRestart(t *testing.T) {
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	tbl := kidxTable(t, false)
	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{"id": fmt.Sprint(i), "v": "x", "n": "1"}
	}
	name := "000001.parquet"
	if err := tbl.writeParquet(filepath.Join(tbl.Dir, "base", name), rows); err != nil {
		t.Fatal(err)
	}
	kb := tbl.newKidxBuilder(name, 100, true)
	for i := range rows {
		if err := kb.add(tbl.keyOfString(fmt.Sprint(i)), int32(i), uint64(1000+i), false); err != nil {
			t.Fatal(err)
		}
	}
	x, _, _, err := kb.finish()
	if err != nil {
		t.Fatal(err)
	}
	k := tbl.keyOfString("42")
	if l, ok := x.get(k); !ok || l.Hash != 1042 || l.Pos != 42 {
		t.Fatalf("in the process that built it: %+v %v", l, ok)
	}
	x.close()
	tbl.State.BaseFiles = []string{name}
	before := kidxRebuilds.Load()
	y, err := tbl.baseIndex(name)
	if err != nil {
		t.Fatal(err)
	}
	if kidxRebuilds.Load() != before {
		t.Fatal("the file did not load")
	}
	if l, ok := y.get(k); !ok || l.Hash != 0 || l.Pos != 42 {
		t.Fatalf("after a restart: %+v %v, want position 42 and no digest", l, ok)
	}
	y.close()
}

// TestKidxSearch checks the interpolating search against the plain one on key
// distributions chosen to make a guess from the value as wrong as it can be:
// clusters, powers of two, one top half shared by every key, duplicates across
// a fence boundary.
func TestKidxSearch(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	shapes := map[string]func(i int) rowKey{
		"serial":      func(i int) rowKey { return rowKey{num: int64(i), isNum: true} },
		"negative":    func(i int) rowKey { return rowKey{num: int64(i) - 2500, isNum: true} },
		"powers":      func(i int) rowKey { return rowKey{num: int64(1)<<uint(i%63) + int64(i/63), isNum: true} },
		"two cluster": func(i int) rowKey { return rowKey{num: int64(i%2)*(1<<62) + int64(i), isNum: true} },
		"extremes": func(i int) rowKey {
			if i%2 == 0 {
				return rowKey{num: -1 << 63, isNum: true}
			}
			return rowKey{num: 1<<63 - 1 - int64(i%7), isNum: true}
		},
		"uuid random": func(i int) rowKey {
			return rowKey{num: int64(mix64(uint64(i))), lo: mix64(uint64(i) + 99), isUUID: true}
		},
		"uuid one top":  func(i int) rowKey { return rowKey{num: 42, lo: mix64(uint64(i)), isUUID: true} },
		"uuid few tops": func(i int) rowKey { return rowKey{num: int64(i % 5), lo: uint64(i), isUUID: true} },
		"uuid v7-like": func(i int) rowKey {
			return rowKey{num: int64(0x0190<<48 + uint64(i/3)<<16), lo: mix64(uint64(i)), isUUID: true}
		},
		"uuid repeated": func(i int) rowKey { return rowKey{num: int64(i / 300), lo: 5, isUUID: true} },
	}
	for name, gen := range shapes {
		for _, n := range []int{1, 7, 64, 65, 5000} {
			keys := make([]rowKey, n)
			for i := range keys {
				keys[i] = gen(i)
			}
			// The column's type decides what the file holds: these are the two
			// fixed-width kinds, whose record is the key.
			tbl := kidxTable(t, keys[0].isUUID)
			base := "000001.parquet"
			if err := os.WriteFile(filepath.Join(tbl.Dir, "base", base), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			kb := tbl.newKidxBuilder(base, n, true)
			for i, k := range keys {
				if err := kb.add(k, int32(i), 0, false); err != nil {
					t.Fatal(err)
				}
			}
			x, _, _, err := kb.finish()
			if err != nil {
				t.Fatal(err)
			}
			d := x.disk
			if d == nil || d.n != n {
				t.Fatalf("%s/%d: no index", name, n)
			}
			plain := func(k kkey) int {
				a, b := 0, d.n
				for a < b {
					m := (a + b) / 2
					if d.keyAt(m).less(k) {
						a = m + 1
					} else {
						b = m
					}
				}
				return a
			}
			try := func(k kkey) {
				if got, want := d.search(k), plain(k); got != want {
					t.Fatalf("%s/%d: search(%x,%x) = %d, want %d", name, n, k.hi, k.lo, got, want)
				}
				if got, want := d.lowerBound(k), plain(k); got != want {
					t.Fatalf("%s/%d: lowerBound(%x,%x) = %d, want %d", name, n, k.hi, k.lo, got, want)
				}
			}
			for _, k := range keys {
				kk := toKKey(k)
				try(kk)
				try(kkey{kk.hi, kk.lo + 1})
				try(kkey{kk.hi, kk.lo - 1})
				try(kkey{kk.hi + 1, kk.lo})
				try(kkey{kk.hi - 1, kk.lo})
				try(kkey{kk.hi + 1, 0})
			}
			for i := 0; i < 2000; i++ {
				try(kkey{rng.Uint64(), rng.Uint64()})
			}
			try(kkey{})
			try(kkey{^uint64(0), ^uint64(0)})
			// And the live record of a repeated key is its highest position.
			last := map[rowKey]int32{}
			for i, k := range keys {
				last[k] = int32(i)
			}
			for k, p := range last {
				if l, ok := x.get(k); !ok || l.Pos != p {
					t.Fatalf("%s/%d: get(%s) = %+v,%v, want position %d", name, n, k, l, ok, p)
				}
			}
			x.close()
		}
	}
}
