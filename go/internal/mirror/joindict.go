package mirror

// A join dictionary maps the 16-byte values of one uuid join domain to dense
// int64 ids, so that a join on a companion column can compare 8-byte integers
// where it would otherwise hash 16-byte keys. See joindomain.go for the domain
// as a whole; this file is only the dictionary.
//
// The contract, all of which is pinned by joindict_test.go:
//   - Ids start at 1, are assigned in first-seen order, and never change or
//     get reused: the log is append-only and id = record index + 1.
//   - NULL has no id and is never passed here.
//   - Assign of a value that already has an id writes nothing.
//   - A new id is appended to dict.log before Assign returns, and reaches
//     stable storage only at Sync. The mirror calls Sync before it names a
//     manifest that could contain the id (saveState), so a manifest never
//     names an id the log cannot reproduce.
//   - A torn tail (a length that is not a multiple of 16) is a record whose
//     write did not finish; it is truncated at open, because nothing names it.
//   - Past the cap the domain is disabled, durably, by a file named `disabled`
//     in its directory. A disabled dictionary keeps answering for the values it
//     already has and refuses every new one.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	joinDictRecordSize = 16
	joinDictLogName    = "dict.log"
	joinDictDisabled   = "disabled"
)

// ErrJoinDomainDisabled is returned by Assign once a domain has been disabled.
// The writer treats it as "no companion value for this row": the row is still
// written, the companion is NULL, and the table reports itself incomplete.
var ErrJoinDomainDisabled = errors.New("join domain disabled")

type joinDict struct {
	mu         sync.Mutex
	dir        string
	f          *os.File // dict.log, opened O_APPEND
	ids        map[[joinDictRecordSize]byte]int64
	max        int64
	dirty      bool  // records appended since the last successful Sync
	broken     error // a failed append: the file may end in a partial record
	isDisabled bool
	reason     string
}

// openJoinDict opens (creating if needed) the dictionary in dir and rebuilds
// the in-memory map from its log. maxKeys <= 0 means no cap.
func openJoinDict(dir string, maxKeys int64) (*joinDict, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, joinDictLogName)
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	d := &joinDict{dir: dir, ids: make(map[[joinDictRecordSize]byte]int64, len(b)/joinDictRecordSize), max: maxKeys}

	whole := len(b) / joinDictRecordSize * joinDictRecordSize
	if whole != len(b) {
		// A torn tail. Nothing names it, so it is discarded, and the
		// truncation is made durable before anything is appended after it.
		if err := truncateSync(path, int64(whole)); err != nil {
			return nil, err
		}
		b = b[:whole]
	}
	for i := 0; i < len(b); i += joinDictRecordSize {
		var k [joinDictRecordSize]byte
		copy(k[:], b[i:i+joinDictRecordSize])
		if _, dup := d.ids[k]; dup {
			return nil, fmt.Errorf("%s: value at record %d repeats an earlier record", path, i/joinDictRecordSize+1)
		}
		d.ids[k] = int64(i/joinDictRecordSize + 1)
	}

	if reason, err := os.ReadFile(filepath.Join(dir, joinDictDisabled)); err == nil {
		d.isDisabled = true
		d.reason = strings.TrimSpace(string(reason))
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	d.f = f
	if len(b) == 0 {
		// The log's own entry is new: its directory owes a sync, which the
		// first Sync pays.
		owesDirSync(dir)
	}
	return d, nil
}

// truncateSync shortens path to size and makes that durable.
func truncateSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	err = f.Truncate(size)
	if err == nil {
		err = syncFile(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Assign returns the id of k, assigning the next id if k is new. It writes
// only when it assigns.
func (d *joinDict) Assign(k [joinDictRecordSize]byte) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.ids[k]; ok {
		return id, nil
	}
	if d.broken != nil {
		return 0, d.broken
	}
	if d.isDisabled {
		return 0, ErrJoinDomainDisabled
	}
	if d.max > 0 && int64(len(d.ids)) >= d.max {
		return 0, d.disableLocked(fmt.Sprintf("more than %d keys", d.max))
	}
	if _, err := d.f.Write(k[:]); err != nil {
		// The file may now end in part of a record, and the next append would
		// land after it. Refuse until the dictionary is reopened, which
		// truncates the tail.
		d.broken = fmt.Errorf("join dictionary %s: %w", d.dir, err)
		return 0, d.broken
	}
	id := int64(len(d.ids)) + 1
	d.ids[k] = id
	d.dirty = true
	return id, nil
}

// Sync makes every assignment so far durable. It is a no-op when nothing has
// been assigned since the last successful Sync, so a quiet domain costs no
// fsync.
func (d *joinDict) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.broken != nil {
		return d.broken
	}
	if !d.dirty {
		return nil
	}
	if err := syncFile(d.f); err != nil {
		return err
	}
	d.dirty = false
	return nil
}

// disableLocked marks the domain disabled in memory and on disk. The on-disk
// marker is what survives a restart, so it is written and its directory synced.
func (d *joinDict) disableLocked(reason string) error {
	d.isDisabled = true
	d.reason = reason
	if err := writeFileAtomic(filepath.Join(d.dir, joinDictDisabled), []byte(reason+"\n")); err != nil {
		return fmt.Errorf("join dictionary %s: disabling: %w", d.dir, err)
	}
	if err := syncDir(d.dir); err != nil {
		return fmt.Errorf("join dictionary %s: disabling: %w", d.dir, err)
	}
	return ErrJoinDomainDisabled
}

// Lookup returns the id of k without assigning one.
func (d *joinDict) Lookup(k [joinDictRecordSize]byte) (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id, ok := d.ids[k]
	return id, ok
}

// Len is the number of ids assigned.
func (d *joinDict) Len() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.ids))
}

// Disabled reports whether the domain is disabled and why.
func (d *joinDict) Disabled() (bool, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.isDisabled, d.reason
}

// Close releases the log. It does not sync; call Sync first if the
// assignments matter.
func (d *joinDict) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.f.Close()
}

// joinKeyOf turns a 16-byte value into the record key; kept as a function so
// the uuid-to-key rule has one home.
func joinKeyOf(b []byte) ([joinDictRecordSize]byte, error) {
	var k [joinDictRecordSize]byte
	if len(b) != joinDictRecordSize {
		return k, fmt.Errorf("join key is %d bytes, want %d", len(b), joinDictRecordSize)
	}
	copy(k[:], b)
	return k, nil
}
