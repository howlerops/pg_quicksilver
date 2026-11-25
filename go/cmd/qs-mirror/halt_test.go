package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/health"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const nanReason = `halting public.b: column "amount" (numeric(12,2)) holds "NaN", which the mirror's column type decimal(12, 2) cannot store exactly. Storing NULL or a rounded value instead would be silently wrong`

func TestClassifyTableError(t *testing.T) {
	enospc := &fs.PathError{Op: "write", Path: "/m/public.b/delta/000007.parquet", Err: syscall.ENOSPC}
	for _, c := range []struct {
		name   string
		err    error
		halted string
		want   verdict
	}{
		{"the refusal itself", errors.New(nanReason), nanReason, haltTable},
		{"the refusal, wrapped by a caller", fmt.Errorf("snapshot public.b: %w", errors.New(nanReason)), nanReason, haltTable},

		// The mirror did not halt for it, so nothing here may.
		{"an error with no halt recorded", errors.New("parquet: short write"), "", stopProcess},
		{"a refusal-shaped error with no halt recorded", errors.New(nanReason), "", stopProcess},
		{"nil", nil, nanReason, stopProcess},

		// Halted, but the error is not the halt: a rebuild of a halted table
		// that ran out of disk must not read as "the rebuild was refused".
		{"disk full on a table that is already halted", enospc, nanReason, stopProcess},
		{"disk full, wrapped", fmt.Errorf("apply: %w", enospc), nanReason, stopProcess},
		{"an unrelated error on a halted table", errors.New("connection reset by peer"), nanReason, stopProcess},
		{"a bare errno", fmt.Errorf("fsync: %w", syscall.EIO), nanReason, stopProcess},

		// Systemic even when the text happens to carry the reason.
		{"cancelled", fmt.Errorf("%s: %w", nanReason, context.Canceled), nanReason, stopProcess},
		{"deadline", fmt.Errorf("%s: %w", nanReason, context.DeadlineExceeded), nanReason, stopProcess},
		{"I/O error quoting the reason", fmt.Errorf("%s: %w", nanReason, enospc), nanReason, stopProcess},
	} {
		if got := classifyTableError(c.err, c.halted); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTable(t *testing.T, root, name string) *mirror.Table {
	t.Helper()
	tbl, err := mirror.New(root, "public", name, "id",
		map[string]string{"id": "bigint", "amount": "numeric(12,2)"}, []string{"id", "amount"})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func txn(lsn, table, id string, amount any) changestream.Transaction {
	row := map[string]any{"id": id, "amount": amount}
	return changestream.Transaction{CommitLSN: lsn, NextLSN: lsn, Changes: []changestream.Change{
		{Op: changestream.OpInsert, Schema: "public", Table: table, Row: row, Key: row},
	}}
}

// The line, against what Table.Apply really returns: a NaN halts ITS table,
// durably, the other table applies the same batch, and readiness names the one
// that stopped.
func TestANaNHaltsOneTableAndTheOtherApplies(t *testing.T) {
	root := t.TempDir()
	tables := map[string]*mirror.Table{"public.a": newTable(t, root, "a"), "public.b": newTable(t, root, "b")}
	h := health.New(time.Minute)
	h.RecordBootstrapped()
	batch := []changestream.Transaction{
		txn("0/10", "a", "1", "1.50"), txn("0/20", "b", "1", "NaN"), txn("0/30", "a", "2", "2.50"),
	}
	for q, tbl := range tables {
		if _, err := tbl.Apply(batch); err != nil {
			if herr := haltOrFail(quiet(), h, q, tbl, err); herr != nil {
				t.Fatalf("%s: a refused value stopped the process: %v", q, herr)
			}
		}
	}
	if live := noteHalted(quiet(), h, tables); live != 1 {
		t.Fatalf("live tables = %d, want 1", live)
	}
	if tables["public.a"].Halted() != "" || tables["public.a"].State.AppliedLSN != "0/30" {
		t.Errorf("public.a: halted=%q applied=%q", tables["public.a"].Halted(), tables["public.a"].State.AppliedLSN)
	}
	if got := mirror.HaltedOnDisk(root, "public", "b"); got == "" {
		t.Error("public.b's halt is not in its manifest")
	}
	if mirror.HaltedOnDisk(root, "public", "a") != "" || mirror.HaltedOnDisk(root, "public", "nope") != "" {
		t.Error("HaltedOnDisk reported a halt for a table that has none")
	}
	if ok, why := h.Ready(); ok {
		t.Errorf("ready with a halted table: %s", why)
	}
	if _, err := tables["public.b"].ViewSQL(); err == nil {
		t.Error("a halted table produced a view")
	}

	// A rebuild puts it back, and the request is spent.
	marker := filepath.Join(tables["public.b"].Dir, rebuildMarker)
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := finishRebuild(quiet(), h, "public.b", tables["public.b"]); err != nil {
		t.Fatal(err)
	}
	if mirror.HaltedOnDisk(root, "public", "b") != "" || rebuildRequested(tables["public.b"].Dir) {
		t.Error("after a rebuild the halt or the request is still there")
	}
	if ok, why := h.Ready(); !ok {
		t.Errorf("not ready after the rebuild: %s", why)
	}
}

func TestPlanHalted(t *testing.T) {
	for _, c := range []struct {
		name      string
		halted    map[string]string
		requested map[string]bool
		want      haltedPlan
	}{
		{"nothing halted", map[string]string{"public.a": "", "public.b": ""}, nil, haltedPlan{}},
		// The case that matters most: a restart alone changes nothing.
		{"halted, no request", map[string]string{"public.a": "", "public.b": "NaN"}, nil,
			haltedPlan{Stay: []string{"public.b"}}},
		{"halted and requested", map[string]string{"public.a": "", "public.b": "NaN"},
			map[string]bool{"public.b": true}, haltedPlan{Rebuild: []string{"public.b"}}},
		// A request never rebuilds a healthy table, and never waits for a halt.
		{"requested, not halted", map[string]string{"public.a": "", "public.b": ""},
			map[string]bool{"public.a": true}, haltedPlan{Stale: []string{"public.a"}}},
		{"a request for one does not rebuild the other",
			map[string]string{"public.a": "x", "public.b": "y", "public.c": ""},
			map[string]bool{"public.b": true, "public.c": true, "public.zzz": true},
			haltedPlan{Rebuild: []string{"public.b"}, Stay: []string{"public.a"}, Stale: []string{"public.c"}}},
		{"all halted", map[string]string{"public.a": "x", "public.b": "y"}, nil,
			haltedPlan{Stay: []string{"public.a", "public.b"}}},
	} {
		if got := planHalted(c.halted, c.requested); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestRebuildRequestedIsAFileInTheTableDirectory(t *testing.T) {
	dir := t.TempDir()
	if rebuildRequested(dir) {
		t.Fatal("a request with no file")
	}
	if err := os.WriteFile(filepath.Join(dir, "REBUILD"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if !rebuildRequested(dir) {
		t.Fatal("the file is there and the request is not seen")
	}
}

func TestServeWithHaltedIsOffUnlessSaidPlainly(t *testing.T) {
	for v, want := range map[string]bool{"": false, "0": false, "false": false, "maybe": false,
		"1": true, "true": true, "TRUE": true, " yes ": true, "on": true} {
		t.Setenv("QS_SERVE_WITH_HALTED_TABLES", v)
		if got := serveWithHalted(); got != want {
			t.Errorf("%q: got %v, want %v", v, got, want)
		}
	}
}
