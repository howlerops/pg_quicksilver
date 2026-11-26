package mirror

// What a DuckLake publisher (internal/ducklake) needs to know about a
// manifest, and nothing it could use to change one.
//
// The publisher runs on its own goroutine, behind the apply loop, and may get
// to a manifest after the Table has moved on from it. So it is not handed the
// Table. It is handed a LakeManifest: a copy of the manifest saveState has
// just made durable, with the dead set of every file as that manifest names
// it. Nothing in one is written again after it leaves here.
//
// The dead sets are the reason this is an accessor and not "read state.json
// and the vectors it names": the apply goroutine already holds every dead set
// in memory (dvcache.go), and a vector generation is retired two writes after
// it is superseded, so a publisher that is three manifests behind would find
// the file gone. A bitmap is copied only when its generation has changed
// since the last manifest handed over, which for one apply is the files that
// apply touched.

import (
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
)

// LakeManifest is one committed manifest, described for a publisher.
type LakeManifest struct {
	Schema, Table string
	// Dir is the table's directory in the mirror; Files are relative to it.
	Dir        string
	Seq        int
	AppliedLSN string
	// Refusal, when set, is why this manifest must not be served: the table
	// is halted, or is being rebuilt with nothing kept to read meanwhile.
	// Columns and Files are then empty.
	Refusal string
	Columns []LakeColumn
	Files   []LakeFile
	// Err is set when the manifest could not be described (a vector the
	// manifest names could not be read). The publisher must not publish it.
	Err error
}

// LakeColumn is a column as the mirror declares it now.
type LakeColumn struct {
	Name   string
	PGType string
	// Type is what a file written now stores the column as. Older files may
	// differ; a publisher reads those from the files themselves.
	Type arrow.DataType
	// Missing is the value a file that predates the column reads back as
	// (pg_attribute.attmissingval), or nil for NULL.
	Missing *string
}

// LakeFile is one data file the manifest names.
type LakeFile struct {
	// Rel is "base/000012.parquet" or "delta/000013.parquet".
	Rel string
	// Partial is non-nil for a column-partial patch file, which is not whole
	// rows and cannot be a DuckLake data file.
	Partial []string
	// DVGen is the vector generation the manifest names for the file, zero
	// for none. With the file's identity it identifies Dead exactly.
	DVGen int
	// Dead is the bitmap of dead positions (bit p of word p/64), and
	// DeadCount how many are set. Shared between manifests: read only.
	Dead      []uint64
	DeadCount int
}

// Qualified is schema.table.
func (m *LakeManifest) Qualified() string { return m.Schema + "." + m.Table }

// lakeHook is the publisher's end of the hand-off, kept with the rest of the
// publishing state (publish.go).
type lakeHook struct {
	fn     func(*LakeManifest)
	frozen map[string]*frozenDead // by dvStem
}

type frozenDead struct {
	gen   int
	words []uint64
	n     int
}

// OnManifest registers fn to be called, on the apply goroutine, each time a
// manifest of this table has been made durable. It must not block: the only
// thing between it and the next apply is its own return.
func (t *Table) OnManifest(fn func(*LakeManifest)) {
	t.publisher().lake.fn = fn
}

// LakeManifest describes the manifest this Table holds, for a publisher that
// is starting up and has to reconcile with it. Apply goroutine only, and only
// while the Table is not in the middle of an Apply or a Snapshot.
func (t *Table) LakeManifest() *LakeManifest {
	refusal := t.State.Halted
	if refusal == "" {
		refusal = t.rebuilding
	}
	return t.describe(&t.State, refusal)
}

// notifyLake is the hook: publishManifest calls it once the manifest is
// durable. st is that manifest.
func (t *Table) notifyLake(st *State, refusal string) {
	h := &t.publisher().lake
	if h.fn == nil {
		return
	}
	h.fn(t.describe(st, refusal))
}

func (t *Table) describe(st *State, refusal string) *LakeManifest {
	schema, name := splitQualified(t.Qualified)
	m := &LakeManifest{
		Schema: schema, Table: name, Dir: t.Dir,
		Seq: st.Seq, AppliedLSN: st.AppliedLSN, Refusal: refusal,
	}
	if refusal != "" {
		return m
	}
	h := &t.publisher().lake
	for _, c := range t.Order {
		col := LakeColumn{Name: c, PGType: t.Columns[c], Type: arrowType(t.Columns[c])}
		if v, ok := st.MissingVals[c]; ok {
			v := v
			col.Missing = &v
		}
		m.Columns = append(m.Columns, col)
	}
	live := map[string]bool{}
	add := func(rel string) {
		f := LakeFile{Rel: rel}
		if pc := st.PartialCols[filepath.Base(rel)]; pc != nil && filepath.Dir(rel) == "delta" {
			f.Partial = append([]string{}, pc...)
		}
		stem := dvStem(rel)
		live[stem] = true
		if gen := st.DVGen[stem]; gen != 0 {
			fz := h.frozen[stem]
			if fz == nil || fz.gen != gen {
				d, err := t.deadSetFor(rel)
				if err != nil {
					m.Err = err
					return
				}
				fz = &frozenDead{gen: gen, words: append([]uint64(nil), d.words...), n: d.n}
				if h.frozen == nil {
					h.frozen = map[string]*frozenDead{}
				}
				h.frozen[stem] = fz
			}
			f.DVGen, f.Dead, f.DeadCount = gen, fz.words, fz.n
		}
		m.Files = append(m.Files, f)
	}
	for _, b := range st.BaseFiles {
		add(filepath.Join("base", b))
	}
	for _, d := range st.DeltaFiles {
		add(filepath.Join("delta", d))
	}
	for stem := range h.frozen {
		if !live[stem] {
			delete(h.frozen, stem)
		}
	}
	return m
}

func splitQualified(q string) (string, string) {
	for i := 0; i < len(q); i++ {
		if q[i] == '.' {
			return q[:i], q[i+1:]
		}
	}
	return "", q
}
