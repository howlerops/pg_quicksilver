package mirror

// A temporal column now stores an instant rather than a sentence about one, and
// the mirror's checksum still compares TEXT — so every value has to come back
// spelled exactly as PostgreSQL spells it. A round trip that is off by the
// minutes of a UTC offset, or by a trailing zero, reads as every row differing.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestTemporalRoundTripsExactly(t *testing.T) {
	if !TemporalTypes {
		t.Skip("QS_TEMPORAL_TYPES=0")
	}
	cases := []struct {
		kind temporalKind
		in   string
	}{
		// PostgreSQL prints "+00", not "+00:00", for a whole-hour offset. Get
		// this wrong and every row of every timestamptz column diverges.
		{kindTimestampTZ, "2026-01-15 12:00:00+00"},
		{kindTimestampTZ, "2026-01-15 12:00:00.5+00"},
		{kindTimestampTZ, "2026-01-15 12:00:00.123456+00"},
		{kindTimestampTZ, "1970-01-01 00:00:00+00"},
		{kindTimestampTZ, "1900-03-01 04:05:06+00"},
		{kindTimestampTZ, "2262-04-11 23:47:16+00"},
		// infinity is not an instant, and round-trips as itself because it is
		// stored the way PostgreSQL stores it.
		{kindTimestampTZ, "infinity"},
		{kindTimestampTZ, "-infinity"},
		{kindTimestamp, "2026-01-15 12:00:00"},
		{kindTimestamp, "2026-01-15 12:00:00.000001"},
		{kindDate, "2026-01-15"},
		{kindDate, "1970-01-01"},
		{kindDate, "1000-01-01"},
		{kindDate, "infinity"},
		{kindTime, "12:00:00"},
		{kindTime, "00:00:00"},
		{kindTime, "23:59:59.999999"},
		{kindTime, "12:30:00.5"},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			n, ok := parseTemporal(c.kind, c.in)
			if !ok {
				t.Fatalf("%q did not parse", c.in)
			}
			if got := renderTemporal(c.kind, n); got != c.in {
				t.Errorf("round trip changed the value:\n  in:  %q\n  out: %q", c.in, got)
			}
		})
	}
}

// The values Arrow cannot hold must be REFUSED, not stored as something else.
// A zero here is 1970-01-01, a real instant, indistinguishable from data.
func TestTemporalRefusesWhatItCannotHold(t *testing.T) {
	if !TemporalTypes {
		t.Skip("QS_TEMPORAL_TYPES=0")
	}
	for _, s := range []string{
		"0044-03-15 12:00:00+00 BC", // BC, which Go cannot parse at all
		"294248-01-01 00:00:00+00",  // beyond the int64 microsecond range
		"not a timestamp",
		"",
	} {
		if n, ok := parseTemporal(kindTimestampTZ, s); ok {
			t.Errorf("%q was accepted as %d; it must be refused so the mirror "+
				"halts instead of storing a plausible wrong instant", s, n)
		}
	}
}

// A mirror must never write a value it cannot represent. It halts, durably,
// naming the table, the column and the value — because "which row" is the only
// part an operator can act on.
func TestUnrepresentableTemporalHaltsRatherThanGuessing(t *testing.T) {
	if !TemporalTypes {
		t.Skip("QS_TEMPORAL_TYPES=0")
	}
	cols := map[string]string{"id": "bigint", "at": "timestamptz"}
	order := []string{"id", "at"}
	tbl, err := New(t.TempDir(), "public", "e", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	row := map[string]any{"id": "1", "at": "0044-03-15 12:00:00+00 BC"}
	_, err = tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: []changestream.Change{{
			Op: changestream.OpInsert, Schema: "public", Table: "e",
			Row: row, Key: row,
		}},
	}})
	if err == nil {
		t.Fatal("a BC timestamp was accepted; the mirror would hold a wrong instant")
	}
	if h := tbl.Halted(); h == "" {
		t.Error("the mirror did not halt, so a restart would resume and store it")
	} else {
		for _, want := range []string{"public.e", "at", "0044-03-15", "QS_TEMPORAL_TYPES=0"} {
			if !strings.Contains(h, want) {
				t.Errorf("the halt reason does not mention %q: %s", want, h)
			}
		}
	}
	// Durable: a halt that lives only in a running process is undone by the
	// next restart.
	reopened, err := New(strings.TrimSuffix(tbl.Dir, "/public.e"), "public", "e", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Halted() == "" {
		t.Error("the halt did not survive a reopen")
	}
}

// End to end through the writer and the reader: what goes in comes back out,
// and the Parquet column is a TIMESTAMP rather than a string.
func TestTemporalColumnsSurviveTheWriter(t *testing.T) {
	if !TemporalTypes {
		t.Skip("QS_TEMPORAL_TYPES=0")
	}
	cols := map[string]string{
		"id": "bigint", "at": "timestamptz", "naive": "timestamp",
		"day": "date", "clock": "time",
	}
	order := []string{"id", "at", "naive", "day", "clock"}
	tbl, err := New(t.TempDir(), "public", "e", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]any{}
	var changes []changestream.Change
	for i := 1; i <= 50; i++ {
		row := map[string]any{
			"id": fmt.Sprint(i),
			// No trailing zeros: PostgreSQL prints .00001 for ten microseconds,
			// not .000010, and the mirror's text has to match the source's.
			"at": fmt.Sprintf("2026-01-%02d 12:00:00.%s+00", (i%28)+1,
				strings.TrimRight(fmt.Sprintf("%06d", i), "0")),
			"naive": fmt.Sprintf("2026-02-%02d 00:00:00", (i%28)+1),
			"day":   fmt.Sprintf("2026-03-%02d", (i%28)+1),
			"clock": fmt.Sprintf("%02d:00:00", i%24),
		}
		changes = append(changes, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "e",
			Row: row, Key: row,
		})
		want[fmt.Sprint(i)] = row
	}
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: "0/1", NextLSN: "0/1", Changes: changes,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}

	n := 0
	if err := tbl.ForEachLive(func(r map[string]any) error {
		n++
		w := want[Render(r["id"])]
		for _, c := range []string{"at", "naive", "day", "clock"} {
			if Render(r[c]) != w[c] {
				t.Errorf("id=%v column %s: mirror=%q source=%q",
					r["id"], c, Render(r[c]), w[c])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Errorf("read back %d rows, expected 50", n)
	}

	// ...and the view tells an engine these are temporal, which is the whole
	// point: a VARCHAR cannot answer `WHERE at > now() - interval '1 day'`.
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"TIMESTAMP WITH TIME ZONE", "TIMESTAMP", "DATE", "TIME"} {
		if !strings.Contains(sql, want) {
			t.Errorf("the view does not present a column as %s", want)
		}
	}
}

// With the escape hatch off, temporal columns stay text — which is what every
// mirror written before this held, and the fallback an operator is told to use.
func TestTemporalTypesCanBeTurnedOff(t *testing.T) {
	defer func(prev bool) { TemporalTypes = prev }(TemporalTypes)
	TemporalTypes = false
	if temporalOf("timestamptz") != notTemporal {
		t.Error("a timestamptz was still classified as temporal with the flag off")
	}
	if arrowType("timestamptz").String() != "utf8" {
		t.Errorf("with the flag off a timestamptz should be stored as text, got %s",
			arrowType("timestamptz"))
	}
}
