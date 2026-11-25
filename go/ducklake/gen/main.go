// gen drives the real mirror package, one command at a time, so that a
// publisher in another process can look at the manifest between applies.
//
// It is a line protocol on stdin/stdout because the thing being measured is
// what a publisher has to do AFTER each apply, and that needs the mirror to
// hold still while it is done:
//
//	load N            N rows, bulk, compacted into one base file
//	step U D I        one apply: U updates, D deletes, I inserts; then the
//	                  sidecar's own maintain() policy (merge / begin / finish)
//	patch N           one apply of N updates carrying only id+status, which the
//	                  mirror stores as a column-partial patch file
//	evolve NAME TYPE DEFAULT
//	                  ADD COLUMN with a default (attmissingval), no rewrite
//	compact           a synchronous base rewrite
//	quit
//
// Every command answers with one JSON line.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

var (
	rng      = rand.New(rand.NewSource(42))
	statuses = []string{"new", "paid", "shipped", "delivered", "cancelled", "refunded"}
	pad      = strings.Repeat("lorem ipsum dolor sit amet ", 12)
	epoch    = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	lsn      uint64
	live     []string
	cols     = map[string]string{"id": "uuid", "amount": "numeric(12,2)",
		"created_at": "timestamptz", "status": "text", "note": "text"}
	order = []string{"id", "amount", "created_at", "status", "note"}
)

func uuid() string {
	var b [16]byte
	rng.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func row(id string) map[string]any {
	// ~200 bytes of text that does not compress to nothing: a random offset
	// into the filler plus a random tail.
	off := rng.Intn(100)
	note := pad[off:off+170] + fmt.Sprintf(" %016x%014x", rng.Uint64(), rng.Uint64()>>8)
	r := map[string]any{
		"id":         id,
		"amount":     fmt.Sprintf("%d.%02d", rng.Intn(100000), rng.Intn(100)),
		"created_at": epoch.Add(time.Duration(rng.Int63n(3*365*86400)) * time.Second).Format("2006-01-02 15:04:05+00"),
		"status":     statuses[rng.Intn(len(statuses))],
		"note":       note,
	}
	for _, c := range order {
		if _, ok := r[c]; !ok {
			r[c] = "x" // a column added by evolve
		}
	}
	return r
}

func nextLSN() string {
	lsn += 16
	return fmt.Sprintf("0/%X", lsn)
}

func txn(changes []changestream.Change) []changestream.Transaction {
	l := nextLSN()
	return []changestream.Transaction{{CommitLSN: l, NextLSN: l, Changes: changes}}
}

func ins(r map[string]any) changestream.Change {
	return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "orders", Row: r, Key: r}
}

func pick() (int, string) {
	i := rng.Intn(len(live))
	return i, live[i]
}

func main() {
	root := os.Args[1]
	t, err := mirror.New(root, "public", "orders", "id", cols, order)
	if err != nil {
		panic(err)
	}
	in := bufio.NewScanner(os.Stdin)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		f := strings.Fields(in.Text())
		if len(f) == 0 {
			continue
		}
		res := map[string]any{"cmd": f[0]}
		t0 := time.Now()
		switch f[0] {
		case "load":
			n, _ := strconv.Atoi(f[1])
			for done := 0; done < n; {
				m := min(100000, n-done)
				ch := make([]changestream.Change, m)
				for i := range ch {
					id := uuid()
					live = append(live, id)
					ch[i] = ins(row(id))
				}
				if _, err := t.Apply(txn(ch)); err != nil {
					panic(err)
				}
				done += m
			}
			if _, err := t.Compact(); err != nil {
				panic(err)
			}
		case "step":
			u, _ := strconv.Atoi(f[1])
			d, _ := strconv.Atoi(f[2])
			n, _ := strconv.Atoi(f[3])
			var ch []changestream.Change
			for i := 0; i < u; i++ {
				_, id := pick()
				r := row(id)
				ch = append(ch, changestream.Change{Op: changestream.OpUpdate,
					Schema: "public", Table: "orders", Row: r, Key: map[string]any{"id": id}})
			}
			for i := 0; i < d; i++ {
				j, id := pick()
				live[j] = live[len(live)-1]
				live = live[:len(live)-1]
				ch = append(ch, changestream.Change{Op: changestream.OpDelete,
					Schema: "public", Table: "orders", Key: map[string]any{"id": id}})
			}
			for i := 0; i < n; i++ {
				id := uuid()
				live = append(live, id)
				ch = append(ch, ins(row(id)))
			}
			if _, err := t.Apply(txn(ch)); err != nil {
				panic(err)
			}
			res["apply_ms"] = float64(time.Since(t0).Microseconds()) / 1000
			// The sidecar's maintain(), in the same order.
			if t.Compacting() {
				if n, err := t.FinishCompaction(); err != nil {
					panic(err)
				} else if n > 0 {
					res["maintain"] = "finish_compaction"
				}
			}
			reason := t.CompactReason()
			switch {
			case t.Compacting() && t.ShouldMergeDeltas():
				if _, err := t.MergeDeltas(); err != nil {
					panic(err)
				}
				res["maintain"] = "merge_during_compaction"
			case t.Compacting():
			case reason != "":
				if c := t.BeginCompaction(); c != nil {
					res["maintain"] = "begin_compaction:" + reason
				}
			case t.ShouldMergeDeltas():
				if _, err := t.MergeDeltas(); err != nil {
					panic(err)
				}
				res["maintain"] = "merge_deltas"
			}
		case "patch":
			n, _ := strconv.Atoi(f[1])
			var ch []changestream.Change
			seen := map[string]bool{}
			for len(ch) < n {
				_, id := pick()
				if seen[id] {
					continue
				}
				seen[id] = true
				r := map[string]any{"id": id, "status": "patched"}
				ch = append(ch, changestream.Change{Op: changestream.OpUpdate,
					Schema: "public", Table: "orders", Row: r, Key: map[string]any{"id": id}})
			}
			if _, err := t.Apply(txn(ch)); err != nil {
				panic(err)
			}
		case "evolve":
			cols[f[1]] = f[2]
			order = append(order, f[1])
			if err := t.Evolve(cols, order, map[string]string{f[1]: f[3]}); err != nil {
				panic(err)
			}
		case "compact":
			for t.Compacting() {
				time.Sleep(10 * time.Millisecond)
				if _, err := t.FinishCompaction(); err != nil {
					panic(err)
				}
			}
			if _, err := t.Compact(); err != nil {
				panic(err)
			}
		case "quit":
			return
		}
		res["ms"] = float64(time.Since(t0).Microseconds()) / 1000
		res["seq"] = t.State.Seq
		res["base"] = len(t.State.BaseFiles)
		res["delta"] = len(t.State.DeltaFiles)
		res["partial"] = len(t.State.PartialCols)
		res["live"] = len(live)
		out.Encode(res)
	}
}
