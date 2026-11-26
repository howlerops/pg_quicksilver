package mirror

// What the value path and the key index cost, and what they refuse.
//
// Three measurements, none of them assertions about speed, each printed with -v
// so that a change to appendValue, arrayValue or keyIndex can be argued about
// with numbers. They are skipped unless asked for by name, because between them
// they write several million rows and allocate most of a gigabyte:
//
//	QS_COST=compaction taskset -c 1 go test ./internal/mirror/ -run TestValueCostCompaction -v
//	QS_COST=index      taskset -c 1 go test ./internal/mirror/ -run TestValueCostIndexMemory -v
//	QS_COST=values                go test ./internal/mirror/ -run TestValueCostUnrepresentable -v
//
// QS_COST_PROF=<path> additionally writes a CPU profile of the rewrite alone.

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func costGate(t *testing.T, name string) {
	t.Helper()
	if os.Getenv("QS_COST") != name {
		t.Skip("set QS_COST=" + name)
	}
}

func costTable(t *testing.T, name string, cols map[string]string, order []string) *Table {
	t.Helper()
	tbl, err := New(t.TempDir(), "public", name, order[0], cols, order)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// costApplier feeds changes through Apply with a fresh LSN each time, which is
// all the pump does that matters here.
type costApplier struct {
	t   *testing.T
	tbl *Table
	lsn int
}

func (a *costApplier) try(changes []changestream.Change) error {
	a.lsn++
	pos := fmt.Sprintf("0/%X", a.lsn)
	_, err := a.tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: changes}})
	return err
}

func (a *costApplier) apply(changes []changestream.Change) {
	a.t.Helper()
	if err := a.try(changes); err != nil {
		a.t.Fatal(err)
	}
}

func (a *costApplier) name() string { return strings.TrimPrefix(a.tbl.Qualified, "public.") }

func (a *costApplier) changes(op changestream.Op, rows ...map[string]any) []changestream.Change {
	ch := make([]changestream.Change, 0, len(rows))
	for _, r := range rows {
		ch = append(ch, changestream.Change{Op: op, Schema: "public", Table: a.name(), Row: r, Key: r})
	}
	return ch
}

// compact runs the streaming compactor to completion, as the pump would over
// two ticks.
func (a *costApplier) compact() (int, time.Duration) {
	a.t.Helper()
	t0 := time.Now()
	c := a.tbl.BeginCompaction()
	if c == nil {
		a.t.Fatal("no compaction")
	}
	<-c.done
	bg := time.Since(t0)
	n, err := a.tbl.FinishCompaction()
	if err != nil {
		a.t.Fatal(err)
	}
	return n, bg
}

// TestValueCostCompaction is the shape the profile was taken on: one million
// rows of six columns, a fifth of them churned, then the rewrite timed alone.
func TestValueCostCompaction(t *testing.T) {
	costGate(t, "compaction")
	const N = 1_000_000
	cols := map[string]string{"id": "bigint", "qty": "bigint", "amount": "numeric(12,2)",
		"price": "double precision", "ts": "timestamptz", "sku": "text"}
	order := []string{"id", "qty", "amount", "price", "ts", "sku"}
	a := &costApplier{t: t, tbl: costTable(t, "p", cols, order)}
	row := func(i int) map[string]any {
		return map[string]any{"id": fmt.Sprint(i), "qty": fmt.Sprint(i % 1000),
			"amount": fmt.Sprintf("%d.%02d", i%100000, i%100), "price": fmt.Sprintf("%d.%d", i%1000, i%977),
			"ts":  fmt.Sprintf("2026-01-%02d 12:%02d:%02d.%06d+00", 1+i%28, i%60, (i/60)%60, i%1000000),
			"sku": fmt.Sprintf("SKU-%d", i)}
	}
	batch := func(op changestream.Op, lo, hi int) time.Duration {
		const per = 50_000
		var spent time.Duration
		for ; lo < hi; lo += per {
			rows := make([]map[string]any, 0, per)
			for i := lo; i < min(lo+per, hi); i++ {
				rows = append(rows, row(i))
			}
			ch := a.changes(op, rows...)
			t0 := time.Now()
			a.apply(ch)
			spent += time.Since(t0)
		}
		return spent
	}
	d := batch(changestream.OpInsert, 0, N)
	t.Logf("applied %d rows from text in %v (%.0f k rows/s)", N,
		d.Round(time.Millisecond), N/d.Seconds()/1000)
	a.compact()
	batch(changestream.OpUpdate, 0, N/5) // churn a fifth, as the policy would

	if out := os.Getenv("QS_COST_PROF"); out != "" {
		f, err := os.Create(out)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	runtime.GC()
	n, bg := a.compact()
	t.Logf("streaming compaction of %d live rows (6 cols): rewrite %v, %.0f k rows/s",
		n, bg.Round(time.Millisecond), float64(n)/bg.Seconds()/1000)
}

func costUUID(i int) string {
	// UUID-shaped, 36 bytes, distinct, separately allocated like a decoded key.
	x := uint64(i)*0x9E3779B97F4A7C15 + 0x1234567
	y := uint64(i)*0xC2B2AE3D27D4EB4F + 0x89abcdef
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", uint32(x>>32), uint16(x>>16), uint16(x), uint16(y>>48), y&0xffffffffffff)
}

// TestValueCostIndexMemory prints bytes per key and the length of a GC cycle
// with the index live, for each form a key can take.
func TestValueCostIndexMemory(t *testing.T) {
	costGate(t, "index")
	heap := func() (inuse, alloc uint64) {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse, m.HeapAlloc
	}
	run := func(label string, n int, key func(i int) rowKey) {
		bi, ba := heap()
		x := newKeyIndex(n)
		for i := 0; i < n; i++ {
			x.set(key(i), loc{File: 3, Pos: int32(i)})
		}
		ai, aa := heap()
		t0 := time.Now()
		for i := 0; i < 3; i++ {
			runtime.GC()
		}
		gc := time.Since(t0) / 3
		t.Logf("%-18s n=%8d HeapInuse +%7.1f MB = %6.1f B/key | HeapAlloc +%7.1f MB = %6.1f B/key | GC cycle %v",
			label, n, float64(ai-bi)/1e6, float64(ai-bi)/float64(n),
			float64(aa-ba)/1e6, float64(aa-ba)/float64(n), gc.Round(time.Millisecond))
		runtime.KeepAlive(x)
	}
	for _, n := range []int{1_000_000, 4_000_000} {
		run("int64 keys", n, func(i int) rowKey { return keyFor(keyInt, int64(i)) })
		run("uuid keys", n, func(i int) rowKey { return keyFor(keyText, costUUID(i)) })
		// The same 36 bytes with one character no UUID has, so it takes the
		// string path: what a UUID key cost before it had a form of its own.
		run("36-byte text keys", n, func(i int) rowKey { return keyFor(keyText, "g"+costUUID(i)[1:]) })
	}
}

// TestValueCostUnrepresentable prints what the mirror does with each value its
// declared Arrow type cannot hold exactly.
func TestValueCostUnrepresentable(t *testing.T) {
	costGate(t, "values")
	cols := map[string]string{"id": "bigint", "u": "numeric", "c": "numeric(12,2)", "f": "double precision", "i": "bigint"}
	order := []string{"id", "u", "c", "f", "i"}
	in := []map[string]any{
		{"id": "1", "u": "0.123456789012345", "c": "1.50", "f": "1.5", "i": "1"},
		{"id": "2", "u": "NaN", "c": "NaN", "f": "NaN", "i": "2"},
		{"id": "3", "u": "123456789012345678901234567890123", "c": "2.00", "f": "Infinity", "i": "3"},
		{"id": "4", "u": "1e-12", "c": "3.00", "f": "-Infinity", "i": "4"},
		{"id": "5", "u": "12345678901234567890.123456789", "c": "9999999999.99", "f": "1e308", "i": "9223372036854775807"},
	}
	for _, r := range in {
		a := &costApplier{t: t, tbl: costTable(t, "r", cols, order)}
		err := a.try(a.changes(changestream.OpInsert, r))
		if err != nil {
			t.Logf("source u=%-36v c=%-14v f=%-10v -> REFUSED: %v", r["u"], r["c"], r["f"], err)
			continue
		}
		rows, lerr := a.tbl.Live()
		if lerr != nil || len(rows) != 1 {
			t.Fatalf("live: %v (%d rows)", lerr, len(rows))
		}
		m := rows[0]
		t.Logf("source u=%-36v c=%-14v f=%-10v -> mirror u=%-36v c=%-14v f=%v", r["u"], r["c"], r["f"], m["u"], m["c"], m["f"])
	}
}
