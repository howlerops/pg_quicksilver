package mirror

// Backfilling a join domain into a mirror that was built without it.
//
// A domain's companion column is written into every file the table writes once
// the domain is attached (fileCols). Files written before that carry no
// companion, and nothing rewrites a file just because a domain was added, so
// without a trigger the join stays on the slow path until the table happens to
// be compacted for another reason (METRICS 0.2).
//
// The trigger is BackfillDue, asked by compactReason on every tick. When it is
// due, the ordinary base rewrite runs, in the background and one at a time, as
// it runs for churn: the rewrite reads the table through its logical columns
// and writes every live row through the companion-aware writer, so after one
// rewrite every file of the table carries the companion and DomainComplete
// reports the table complete. Nothing new is written for the backfill, so its
// memory bound, its crash ordering and its manifest publish are the rewrite's.
//
// CRASH SAFETY. Completeness is never recorded: it is derived from the files
// the manifest names (domainCompleteIn). A rewrite is published by a manifest
// that names the new base and drops the files it replaced; a crash before that
// leaves the old manifest, whose files still lack companions, so BackfillDue is
// still true after a restart and the backfill starts again. A crash after it
// leaves a manifest whose base carries companions and whose deltas (written
// after the attach) carry them too.

import (
	"path/filepath"
	"strings"
)

// JoinBackfillReason is the compaction reason that rewrites the base so every
// file carries the companions of this table's join domains.
const JoinBackfillReason = "join-domain"

// BackfillDue reports whether any member column of the table has a data file
// that lacks its companion while the domain is still enabled. A disabled domain
// is never due: its companions could not be written, and the rewrite would not
// end. The answer is cached per manifest file list and per attachment, so a
// tick where the files have not changed costs no file reads.
func (t *Table) BackfillDue() bool {
	if len(t.joins) == 0 || t.companionsOff() {
		return false
	}
	sig := t.manifestSig()
	if t.backfillValid && t.backfillSig == sig {
		return t.backfillDue
	}
	due := false
	for col := range t.joins {
		if !t.DomainComplete(col) {
			due = true
			break
		}
	}
	t.backfillSig, t.backfillValid, t.backfillDue = sig, true, due
	return due
}

// BackfillProgress reports how many of the table's current data files carry
// every companion they need, out of how many there are. A file that lacks a
// member column entirely needs nothing for that column. It is what the sidecar
// logs during a backfill: "join domain backfill: X of Y files".
func (t *Table) BackfillProgress() (done, total int, err error) {
	if len(t.joins) == 0 {
		return 0, 0, nil
	}
	rels := t.dataRels()
	for _, rel := range rels {
		cols, err := t.colsOfFile(rel)
		if err != nil {
			return done, len(rels), err
		}
		ok := true
		for col := range t.joins {
			if cols[col] && !cols[companionName(col)] {
				ok = false
				break
			}
		}
		if ok {
			done++
		}
	}
	return done, len(rels), nil
}

// dataRels are the manifest's data files, relative to the table directory.
func (t *Table) dataRels() []string {
	rels := make([]string, 0, len(t.State.BaseFiles)+len(t.State.DeltaFiles))
	for _, f := range t.State.BaseFiles {
		rels = append(rels, filepath.Join("base", f))
	}
	for _, f := range t.State.DeltaFiles {
		rels = append(rels, filepath.Join("delta", f))
	}
	return rels
}

// manifestSig names the files the manifest holds. Files are immutable and their
// names unique, so the same signature means the same answer.
func (t *Table) manifestSig() string {
	return strings.Join(t.State.BaseFiles, ",") + "|" + strings.Join(t.State.DeltaFiles, ",")
}
