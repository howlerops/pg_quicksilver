package mirror

import "testing"

// TestClusterRankIsByDeclaredType pins the tie-break for both storages. A time
// column outranks the key whether it is stored as a timestamp or, under
// QS_TEMPORAL_TYPES=0, as text: the rank asks what the column is, not how it
// is stored, and the flag used to demote the time column below the key.
func TestClusterRankIsByDeclaredType(t *testing.T) {
	for _, on := range []bool{true, false} {
		func() {
			defer func(prev bool) { TemporalTypes = prev }(TemporalTypes)
			TemporalTypes = on
			tbl := &Table{
				Key:     "id",
				Columns: map[string]string{"id": "bigint", "ts": "timestamp without time zone", "v": "text"},
			}
			if ts, id, v := tbl.clusterRank("ts"), tbl.clusterRank("id"), tbl.clusterRank("v"); !(ts > id && id > v) {
				t.Errorf("TemporalTypes=%v: rank ts=%d id=%d v=%d, want ts > id > v", on, ts, id, v)
			}
		}()
	}
}
