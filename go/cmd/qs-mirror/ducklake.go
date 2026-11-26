package main

// QS_DUCKLAKE=on publishes every mirrored table as a DuckLake table as well
// (internal/ducklake): the same Parquet files, hard-linked under
// <mirror>/_ducklake, behind a catalog a reader attaches with
//
//	ATTACH 'ducklake:sqlite:<mirror>/_ducklake/catalog.sqlite' AS lake (READ_ONLY);
//
// Off by default. Three things change when it is on, and nothing else:
//
//   - Every manifest a table commits is handed to the publisher (one callback
//     at the end of saveState). The hand-off stores a pointer; the publishing
//     happens on the publisher's goroutine, and the apply loop never waits
//     for it.
//   - Column-partial deltas are turned off (as QS_PARTIAL_DELTAS=0): a
//     DuckLake data file is whole rows. A table that already holds patch
//     files, or files whose types cannot be read as one column, is refused in
//     the lake — a reader gets an error naming the reason, not a wrong table
//     — and a base rewrite is started here; it is published when that lands.
//   - After a restart, each table's current manifest is submitted once, and
//     the lake is brought to it whatever state it was left in.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/ducklake"
	"github.com/howlerops/pg_quicksilver/go/internal/health"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// lake is the publisher, nil unless QS_DUCKLAKE is on. It outlives runMirror:
// a restart of the stream is not a restart of the lake.
var lake *ducklake.Publisher

func ducklakeOn() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QS_DUCKLAKE"))) {
	case "on", "1", "true", "yes":
		return true
	}
	return false
}

// openLake opens the lake under the mirror path. An error here stops the
// sidecar: QS_DUCKLAKE=on with a lake that cannot be written (another
// filesystem, another format version, another writer) is a configuration to
// fix, not one to run without.
func openLake(ctx context.Context, log *slog.Logger, o options, h *health.Health) error {
	if !ducklakeOn() {
		return nil
	}
	retention := ducklake.DefaultRetention
	if v := os.Getenv("QS_DUCKLAKE_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("QS_DUCKLAKE_RETENTION=%q is not a positive duration (for example 5m)", v)
		}
		retention = d
	}
	if err := os.MkdirAll(o.mirrorPath, 0o755); err != nil {
		return err
	}
	p, err := ducklake.Open(ctx, ducklake.Options{
		MirrorRoot: o.mirrorPath, Dir: os.Getenv("QS_DUCKLAKE_PATH"), Retention: retention, Log: log,
	})
	if err != nil {
		return err
	}
	lake = p
	// Patch files cannot be published; see the top of this file.
	mirror.PartialDeltas = false
	h.AddMetrics(p.Metrics)
	log.Info("ducklake publishing on", "catalog", ducklake.CatalogPath(o.mirrorPath),
		"format", ducklake.FormatVersion, "retention", retention,
		"attach", ducklake.AttachSQL(ducklake.CatalogPath(o.mirrorPath), "lake"))
	return nil
}

// lakeAttach hooks a table that has just been opened.
func lakeAttach(t *mirror.Table) {
	if lake != nil {
		t.OnManifest(lake.Submit)
	}
}

// lakeReconcile submits where every table stands now. A table whose manifest
// has not moved since the last process published it costs one comparison; one
// the lake is behind, ahead of, or has never seen is brought to the manifest.
func lakeReconcile(tables map[string]*mirror.Table) {
	if lake == nil {
		return
	}
	for _, t := range tables {
		lake.Submit(t.LakeManifest())
	}
}

// lakeRewrites starts the base rewrite a table needs before it can be
// published. The swap, and everything else about the rewrite, is maintain's.
func lakeRewrites(log *slog.Logger, tables map[string]*mirror.Table) {
	if lake == nil {
		return
	}
	for q, t := range tables {
		if t.Halted() != "" || t.Compacting() || !lake.NeedsRewrite(q) {
			continue
		}
		if c := t.BeginCompaction(); c != nil {
			log.Info("compaction started", "table", q,
				"why", "the table cannot be published to DuckLake until its base is rewritten")
		}
	}
}
