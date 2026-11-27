package main

import (
	"errors"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// A join report state machine with a recording sender: what the node is told,
// in order, about each configured column. Nothing here touches a database.
type joinRecorder struct {
	sent []map[joinKey]bool
	fail bool
}

func (r *joinRecorder) send(w map[joinKey]bool) error {
	if r.fail {
		return errors.New("local server unreachable")
	}
	cp := map[joinKey]bool{}
	for k, v := range w {
		cp[k] = v
	}
	r.sent = append(r.sent, cp)
	return nil
}

// everSentTrue reports whether any report since the recorder started said
// complete for the column.
func (r *joinRecorder) everSentTrue(k joinKey) bool {
	for _, w := range r.sent {
		if w[k] {
			return true
		}
	}
	return false
}

func (r *joinRecorder) last(k joinKey) (val, ok bool) {
	if len(r.sent) == 0 {
		return false, false
	}
	val, ok = r.sent[len(r.sent)-1][k]
	return
}

const testTable = "public.orders"

var idCol = joinKey{testTable, "customer_id"}

func newJoinTestLink(t *testing.T) (*extLink, *joinRecorder) {
	t.Helper()
	e := newExtLink(nil, options{})
	r := &joinRecorder{}
	e.sendJoinsFn = r.send
	return e, r
}

// manifest is a manifest of testTable configured for customer_id, complete or
// not, as describe builds it.
func manifest(seq int, complete bool) *mirror.LakeManifest {
	m := &mirror.LakeManifest{Schema: "public", Table: "orders", Seq: seq, JoinMembers: []string{"customer_id"}}
	if complete {
		m.JoinComplete = []string{"customer_id"}
	}
	return m
}

// Before any manifest is published nothing is complete: the report is incomplete
// for every configured column, including after a restart.
func TestJoinStartsIncomplete(t *testing.T) {
	e, r := newJoinTestLink(t)
	e.jdCfg[testTable] = []string{"customer_id"}
	e.jdMu.Lock()
	if err := e.reportJoinsLocked(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	e.jdMu.Unlock()
	if v, ok := r.last(idCol); !ok || v {
		t.Fatalf("before anything is published: reported %v (%v), want incomplete", v, ok)
	}
}

// A column is reported complete only after a complete manifest is published.
func TestJoinCompleteOnlyAfterPublish(t *testing.T) {
	e, r := newJoinTestLink(t)
	e.jdCfg[testTable] = []string{"customer_id"}

	// Published, complete: now, and only now, complete.
	if err := e.beforePublish(manifest(1, true)); err != nil {
		t.Fatal(err)
	}
	if r.everSentTrue(idCol) {
		t.Fatal("a complete manifest was reported complete before it was published")
	}
	e.afterPublish(manifest(1, true))
	e.jdMu.Lock()
	if err := e.reportJoinsLocked(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	e.jdMu.Unlock()
	if v, _ := r.last(idCol); !v {
		t.Fatal("a published, complete column is not reported complete")
	}
}

// An incomplete manifest is reported incomplete BEFORE it is published, and the
// publish waits for the report: beforePublish has sent false when it returns.
func TestJoinFlipPrecedesIncompletePublish(t *testing.T) {
	e, r := newJoinTestLink(t)
	e.jdCfg[testTable] = []string{"customer_id"}
	e.afterPublish(manifest(1, true))
	e.jdMu.Lock()
	_ = e.reportJoinsLocked(t.Context(), false)
	e.jdMu.Unlock()
	if v, _ := r.last(idCol); !v {
		t.Fatal("setup: the complete manifest is not reported complete")
	}
	n := len(r.sent)

	if err := e.beforePublish(manifest(2, false)); err != nil {
		t.Fatal(err)
	}
	if len(r.sent) <= n {
		t.Fatal("beforePublish returned without sending an incomplete report")
	}
	if v, _ := r.last(idCol); v {
		t.Fatal("the report that precedes an incomplete publish says complete")
	}
}

// A failed flip fails the publish (it is tried again), and nothing reports
// complete while a manifest that is incomplete for the column is pending.
func TestJoinFailedFlipFailsThePublish(t *testing.T) {
	e, r := newJoinTestLink(t)
	e.jdCfg[testTable] = []string{"customer_id"}
	e.afterPublish(manifest(1, true))
	r.fail = true
	if err := e.beforePublish(manifest(2, false)); err == nil {
		t.Fatal("an incomplete publish went ahead without its report")
	}
	r.fail = false
	// Still pending: a heartbeat sends the column incomplete, never complete.
	e.jdMu.Lock()
	_ = e.reportJoinsLocked(t.Context(), true)
	e.jdMu.Unlock()
	if v, _ := r.last(idCol); v {
		t.Fatal("a column with a pending incomplete publish was reported complete")
	}
}

// The failed publish is tried again and succeeds: the column is complete only
// once a complete manifest is published, not because a retry got through.
func TestJoinPendingUntilACompletePublish(t *testing.T) {
	e, r := newJoinTestLink(t)
	e.jdCfg[testTable] = []string{"customer_id"}
	e.afterPublish(manifest(1, true))
	if err := e.beforePublish(manifest(2, false)); err != nil {
		t.Fatal(err)
	}
	e.afterPublish(manifest(2, false)) // the incomplete manifest is published
	e.jdMu.Lock()
	_ = e.reportJoinsLocked(t.Context(), true)
	e.jdMu.Unlock()
	if v, _ := r.last(idCol); v {
		t.Fatal("after an incomplete publish the column is reported complete")
	}
	if err := e.beforePublish(manifest(3, true)); err != nil {
		t.Fatal(err)
	}
	e.afterPublish(manifest(3, true))
	e.jdMu.Lock()
	_ = e.reportJoinsLocked(t.Context(), false)
	e.jdMu.Unlock()
	if v, _ := r.last(idCol); !v {
		t.Fatal("a complete manifest was published and the column is still incomplete")
	}
}

// A refusal (halted or rebuilding) has no completeness: the column is reported
// incomplete, and the members are still known so that the report can say so.
func TestJoinRefusalIsIncomplete(t *testing.T) {
	e, r := newJoinTestLink(t)
	e.afterPublish(manifest(1, true))
	refused := &mirror.LakeManifest{Schema: "public", Table: "orders", Seq: 2, Refusal: "halted", JoinMembers: []string{"customer_id"}}
	if err := e.beforePublish(refused); err != nil {
		t.Fatal(err)
	}
	e.afterPublish(refused)
	e.jdMu.Lock()
	_ = e.reportJoinsLocked(t.Context(), true)
	e.jdMu.Unlock()
	if v, ok := r.last(idCol); !ok || v {
		t.Fatalf("a refused table reported %v (%v), want incomplete", v, ok)
	}
}
