package mirror

// Join domains. A domain is a set of uuid columns, possibly in several tables,
// whose values are compared for equality in joins. Each member column gets a
// hidden companion column, __qs_d_<column>, holding the domain dictionary's
// dense int64 id for the value (NULL for a NULL value). A join on the companions
// compares integers where a join on the uuids hashes 16 bytes.
//
// The companion is derived from the member value at write time, in the same
// place the file's values are converted (fillCompanions, called from
// storableValues). It is never carried forward, never read back into a logical
// column, and it is absent from Live, Columns, Order and the key index. It is
// present only in files written with the domain attached.
//
// Everything here is off unless a JoinDomains is attached to a table. With none
// attached, expandCols returns its input unchanged and the writer does exactly
// what it did before.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const companionPrefix = "__qs_d_"

// companionName is the hidden column that carries col's domain id.
func companionName(col string) string { return companionPrefix + col }

// isCompanion reports whether a column name is a hidden companion.
func isCompanion(name string) bool { return strings.HasPrefix(name, companionPrefix) }

// JoinMember is one column of a join domain: Table is schema.table.
type JoinMember struct {
	Table  string
	Column string
}

// JoinDomainSpec is one named domain and its member columns.
type JoinDomainSpec struct {
	Name    string
	Members []JoinMember
}

var joinDomainName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// ParseJoinDomains reads QS_JOIN_DOMAINS: a semicolon-separated list of
// name=schema.table.column,schema.table.column,... An empty string is no
// domains, and the feature is off.
func ParseJoinDomains(env string) ([]JoinDomainSpec, error) {
	if strings.TrimSpace(env) == "" {
		return nil, nil
	}
	var specs []JoinDomainSpec
	seen := map[string]string{} // table.column -> domain name
	names := map[string]bool{}
	for _, part := range strings.Split(env, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, list, ok := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		if !ok || !joinDomainName.MatchString(name) {
			return nil, fmt.Errorf("join domain %q: want name=schema.table.column,... with a name of [a-z][a-z0-9_]*", part)
		}
		if names[name] {
			return nil, fmt.Errorf("join domain %q is named twice", name)
		}
		names[name] = true
		spec := JoinDomainSpec{Name: name}
		for _, m := range strings.Split(list, ",") {
			m = strings.TrimSpace(m)
			p := strings.Split(m, ".")
			if len(p) != 3 || p[0] == "" || p[1] == "" || p[2] == "" {
				return nil, fmt.Errorf("join domain %s: member %q is not schema.table.column", name, m)
			}
			key := p[0] + "." + p[1] + "." + p[2]
			if other, dup := seen[key]; dup {
				return nil, fmt.Errorf("join domain %s: %s is already a member of %s; a column belongs to one domain", name, key, other)
			}
			seen[key] = name
			spec.Members = append(spec.Members, JoinMember{Table: p[0] + "." + p[1], Column: p[2]})
		}
		if len(spec.Members) == 0 {
			return nil, fmt.Errorf("join domain %s has no members", name)
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// JoinDomains is the process-wide set of open domains: one dictionary per
// domain, shared by every table that is a member of it.
type JoinDomains struct {
	root  string
	specs []JoinDomainSpec
	dicts map[string]*joinDict
	mu    sync.Mutex
}

// OpenJoinDomains opens (or creates) every domain's dictionary under
// root/_domains/<name>. maxKeys is the per-domain cap; <= 0 means none.
func OpenJoinDomains(root string, specs []JoinDomainSpec, maxKeys int64) (*JoinDomains, error) {
	jd := &JoinDomains{root: root, specs: specs, dicts: map[string]*joinDict{}}
	for _, s := range specs {
		d, err := openJoinDict(filepath.Join(root, "_domains", s.Name), maxKeys)
		if err != nil {
			jd.Close()
			return nil, fmt.Errorf("join domain %s: %w", s.Name, err)
		}
		jd.dicts[s.Name] = d
	}
	return jd, nil
}

// Close releases every dictionary's log. It does not sync.
func (jd *JoinDomains) Close() error {
	if jd == nil {
		return nil
	}
	var first error
	for _, d := range jd.dicts {
		if err := d.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Sync makes every dictionary's new assignments durable. Each table calls it
// from saveState, before the manifest that might name those ids is written.
func (jd *JoinDomains) Sync() error {
	if jd == nil {
		return nil
	}
	for _, s := range jd.specs {
		if err := jd.dicts[s.Name].Sync(); err != nil {
			return fmt.Errorf("join domain %s: %w", s.Name, err)
		}
	}
	return nil
}

// DomainStatus is one domain's state, for metrics and logs.
type DomainStatus struct {
	Name     string
	Keys     int64
	Disabled bool
	Reason   string
}

// Status reports every domain. Keys is the number of ids assigned.
func (jd *JoinDomains) Status() []DomainStatus {
	if jd == nil {
		return nil
	}
	out := make([]DomainStatus, 0, len(jd.specs))
	for _, s := range jd.specs {
		d := jd.dicts[s.Name]
		off, why := d.Disabled()
		out = append(out, DomainStatus{Name: s.Name, Keys: d.Len(), Disabled: off, Reason: why})
	}
	return out
}

// Specs is the configuration the domains were opened with.
func (jd *JoinDomains) Specs() []JoinDomainSpec {
	if jd == nil {
		return nil
	}
	return jd.specs
}

// joinBinding is one member column of a table: its domain and dictionary.
type joinBinding struct {
	domain string
	dict   *joinDict
}

// AttachJoinDomains makes the table a member of every domain that names it.
// It refuses a member column that is not uuid, or that the table does not have.
// It may be called on a table that already has files: those files carry no
// companions, so the table reports itself incomplete until they are rewritten.
// Attaching nil detaches nothing and is a no-op.
func (t *Table) AttachJoinDomains(jd *JoinDomains) error {
	if jd == nil {
		return nil
	}
	bind := map[string]joinBinding{}
	var dicts []*joinDict
	for _, s := range jd.specs {
		for _, m := range s.Members {
			if m.Table != t.Qualified {
				continue
			}
			pg, ok := t.Columns[m.Column]
			if !ok {
				return fmt.Errorf("join domain %s: %s has no column %s", s.Name, t.Qualified, m.Column)
			}
			if pg != "uuid" {
				return fmt.Errorf("join domain %s: %s.%s is %s; only uuid columns are supported", s.Name, t.Qualified, m.Column, pg)
			}
			d := jd.dicts[s.Name]
			bind[m.Column] = joinBinding{domain: s.Name, dict: d}
			dicts = append(dicts, d)
		}
	}
	if len(bind) == 0 {
		t.joins, t.joinDicts = nil, nil
		t.backfillValid = false
		return nil
	}
	t.joins = bind
	t.joinDicts = dicts
	t.joinFileCache = nil
	t.backfillValid = false
	return nil
}

// errCompanionDropped says a row's member has no companion to write: its domain
// is disabled, or it tripped the cap while this write was in progress. A file
// must never carry a NULL companion for a non-NULL member, so the writer does not
// write the row with NULL: the file is written without the companion column.
// Callers that write a whole batch at once retry without the column (see
// writeParquetCols); a streaming writer returns the error, and its file is
// abandoned, as any failed write is.
var errCompanionDropped = errors.New("join domain: companion dropped; the file is written without it")

// companionsOff reports whether any domain this table belongs to is disabled.
// Disabled is durable and monotone, so a file written once this is true, and
// every file after, carries no companion columns at all.
func (t *Table) companionsOff() bool {
	for _, d := range t.joinDicts {
		if off, _ := d.Disabled(); off {
			return true
		}
	}
	return false
}

// fileCols is the column list a writer lays out for logical columns cols: the
// same columns, then one companion per member among them, at the end. Any
// companions already in cols are dropped first, so it is idempotent. With no
// domain attached it returns cols unchanged. Once a domain this table belongs
// to is disabled, no companion is laid out at all.
func (t *Table) fileCols(cols []string) []string {
	if len(t.joins) == 0 {
		return cols
	}
	out := make([]string, 0, len(cols)+len(t.joins))
	for _, c := range cols {
		if !isCompanion(c) {
			out = append(out, c)
		}
	}
	if t.companionsOff() {
		return out
	}
	n := len(out)
	for _, c := range out[:n] {
		if _, ok := t.joins[c]; ok {
			out = append(out, companionName(c))
		}
	}
	return out
}

// fillCompanions sets each companion in cols from its member's converted value.
// Called with the row already converted, so the key is the one the file holds.
// A NULL member writes a NULL companion, which is correct. A non-NULL member
// whose id cannot be assigned because its domain is disabled returns
// errCompanionDropped: it never writes NULL for it.
func (t *Table) fillCompanions(cols []string, vals []any) error {
	for i, name := range cols {
		if i >= len(vals) || !isCompanion(name) {
			continue
		}
		member := name[len(companionPrefix):]
		vals[i] = nil
		j := indexOfStr(cols, member)
		if j < 0 || j >= len(vals) || vals[j] == nil {
			continue
		}
		b, ok := t.joins[member]
		if !ok {
			continue
		}
		if off, _ := b.dict.Disabled(); off {
			return errCompanionDropped
		}
		key, err := joinValueKey(vals[j])
		if err != nil {
			return fmt.Errorf("%s.%s: %w", t.Qualified, member, err)
		}
		id, err := b.dict.Assign(key)
		if errors.Is(err, ErrJoinDomainDisabled) {
			return errCompanionDropped
		}
		if err != nil {
			return err
		}
		vals[i] = id
	}
	return nil
}

// joinValueKey is a converted uuid value as the 16-byte dictionary key.
func joinValueKey(v any) ([joinDictRecordSize]byte, error) {
	var k [joinDictRecordSize]byte
	switch x := v.(type) {
	case uuidValue:
		return [joinDictRecordSize]byte(x), nil
	case string:
		u, ok := parseUUIDValue(x)
		if !ok {
			return k, fmt.Errorf("%q is not a uuid", x)
		}
		return [joinDictRecordSize]byte(u), nil
	case []byte:
		if len(x) == joinDictRecordSize {
			copy(k[:], x)
			return k, nil
		}
		u, ok := parseUUIDValue(string(x))
		if !ok {
			return k, fmt.Errorf("%q is not a uuid", x)
		}
		return [joinDictRecordSize]byte(u), nil
	}
	return k, fmt.Errorf("value of type %T is not a uuid", v)
}

func indexOfStr(xs []string, x string) int {
	for i, s := range xs {
		if s == x {
			return i
		}
	}
	return -1
}

// syncJoinDicts makes this table's dictionaries durable. saveState calls it
// after the data files and directories are synced and before the manifest is
// replaced, so no manifest names an id that the dictionary log cannot reproduce.
func (t *Table) syncJoinDicts() error {
	seen := map[*joinDict]bool{}
	for _, d := range t.joinDicts {
		if seen[d] {
			continue
		}
		seen[d] = true
		if err := d.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// fileColSet is the set of column names a data file carries, cached per file
// by path, size and mtime. A file's name is never reused for other contents,
// but the check is cheap and keeps the cache honest if one is.
type fileColSet struct {
	size  int64
	mtime int64
	cols  map[string]bool
}

func (t *Table) colsOfFile(rel string) (map[string]bool, error) {
	path := filepath.Join(t.Dir, rel)
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if c, ok := t.joinFileCache[rel]; ok && c.size == st.Size() && c.mtime == st.ModTime().UnixNano() {
		return c.cols, nil
	}
	_, order, err := schemaOf(path)
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(order))
	for _, c := range order {
		set[c] = true
	}
	if t.joinFileCache == nil {
		t.joinFileCache = map[string]fileColSet{}
	}
	t.joinFileCache[rel] = fileColSet{size: st.Size(), mtime: st.ModTime().UnixNano(), cols: set}
	return set, nil
}

// DomainComplete reports whether every data file the manifest names that carries
// column carries its companion, and the domain is enabled. It is derived from
// the file schemas, so it survives a restart with nothing persisted for it.
func (t *Table) DomainComplete(column string) bool {
	return t.domainCompleteIn(&t.State, column)
}

// domainCompleteIn is DomainComplete against the manifest st, which is what
// the lake describes when it is not the State the table holds yet.
//
// One more condition than the rule stated in the design: a file that does not
// carry the column at all reads its rows back as the column's missing value.
// When that value is not NULL the companion would read NULL for a non-NULL
// value, so such a table is not complete.
func (t *Table) domainCompleteIn(st *State, column string) bool {
	b, ok := t.joins[column]
	if !ok {
		return false
	}
	if off, _ := b.dict.Disabled(); off {
		return false
	}
	comp := companionName(column)
	missingNonNull := false
	if _, has := st.MissingVals[column]; has {
		missingNonNull = true
	}
	// The manifest names files by their bare names; the directory is the kind.
	rels := make([]string, 0, len(st.BaseFiles)+len(st.DeltaFiles))
	for _, f := range st.BaseFiles {
		rels = append(rels, filepath.Join("base", f))
	}
	for _, f := range st.DeltaFiles {
		rels = append(rels, filepath.Join("delta", f))
	}
	for _, rel := range rels {
		cols, err := t.colsOfFile(rel)
		if err != nil {
			return false
		}
		if cols[column] && !cols[comp] {
			return false
		}
		if !cols[column] && missingNonNull {
			return false
		}
	}
	return true
}

// DomainColumn describes one member column of a table.
type DomainColumn struct {
	Column    string
	Domain    string
	Companion string
	Complete  bool
}

// JoinMembership is a member column of this table, by name, with its domain and
// companion. Unlike DomainColumns it does not look at the files, so it is cheap
// enough to call on every tick: it says what the table is configured to carry.
type JoinMembership struct {
	Column, Domain, Companion string
}

// JoinMemberships lists this table's configured member columns, sorted.
func (t *Table) JoinMemberships() []JoinMembership {
	out := make([]JoinMembership, 0, len(t.joins))
	for _, c := range t.joinColumns() {
		out = append(out, JoinMembership{Column: c, Domain: t.joins[c].domain, Companion: companionName(c)})
	}
	return out
}

// DomainColumns lists this table's member columns, sorted by column name.
func (t *Table) DomainColumns() []DomainColumn {
	out := make([]DomainColumn, 0, len(t.joins))
	for col, b := range t.joins {
		out = append(out, DomainColumn{
			Column: col, Domain: b.domain, Companion: companionName(col),
			Complete: t.DomainComplete(col),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Column < out[j].Column })
	return out
}

// fileSchemaCols returns the logical columns of a file's schema, with companions
// removed. ColumnsFromFiles and anything that presents a table's columns uses it.
func fileSchemaCols(order []string) []string {
	out := order[:0:0]
	for _, c := range order {
		if !isCompanion(c) {
			out = append(out, c)
		}
	}
	return out
}

// liveCols is the column list and fill decision of a whole-row read of the
// table: the logical columns, as forEachRows has always read them, and with
// withCompanions the companions too.
func (t *Table) liveCols(withCompanions bool) ([]string, bool) {
	if withCompanions {
		return t.fileCols(t.Order), true
	}
	return t.Order, true
}

// forEachRowsDead is forEachRowGroup with the column list and fill given.
func (t *Table) forEachRowsDead(path string, dead map[int]bool, want []string, fill bool,
	fn func(rows []map[string]any, firstPos int) error,
) error {
	var isDead func(int) bool
	if len(dead) > 0 {
		isDead = func(p int) bool { return dead[p] }
	}
	return t.forEachRowsWant(path, isDead, want, fill, 0, fn)
}

// joinColumns is this table's member columns, sorted.
func (t *Table) joinColumns() []string {
	out := make([]string, 0, len(t.joins))
	for c := range t.joins {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
