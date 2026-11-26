package mirror

import (
	"fmt"
	"math/big"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// numericSpelling returns a random spelling of the decimal value n * 10^-scale.
// Every spelling denotes the same numeric value, so a key written in one and
// updated, deleted or re-inserted in another must find the same row.
func numericSpelling(r *rand.Rand, n int64, scale int) string {
	neg := n < 0
	if neg {
		n = -n
	}
	digits := fmt.Sprint(n)
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	ip, fp := digits[:len(digits)-scale], digits[len(digits)-scale:]
	if n == 0 {
		ip, fp = "0", strings.Repeat("0", scale)
	}
	sign := ""
	switch {
	case neg && n != 0:
		sign = "-"
	case !neg && r.IntN(4) == 0:
		sign = "+"
	}
	switch r.IntN(6) {
	case 0: // exponent form: the same digits as an integer times 10^-scale
		m := fmt.Sprint(n)
		return sign + m + "e-" + fmt.Sprint(scale)
	case 1: // extra fractional zeros
		return sign + ip + "." + fp + strings.Repeat("0", r.IntN(4))
	case 2: // leading zeros on the integer part
		return sign + strings.Repeat("0", r.IntN(3)) + ip + "." + fp
	case 3: // an integer written without its point, or with a bare trailing point
		if strings.Trim(fp, "0") == "" {
			return sign + ip + "." + strings.Repeat("0", r.IntN(2))
		}
		return sign + ip + "." + fp
	}
	if scale == 0 {
		return sign + ip
	}
	return sign + ip + "." + fp
}

// numericValueKey is the model's key: the exact value, whatever the spelling.
func numericValueKey(t *testing.T, s string) string {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("the test spelled %q, which is not a number", s)
	}
	return r.RatString()
}

// TestRandomNumericKeySpellingsAgreeWithTheValueModel drives a numeric primary
// key through random inserts, updates, deletes and re-inserts, each key in a
// random spelling of its value, with compactions and reopens in between. The
// live rows must match a Go map keyed by the exact value at every check.
func TestRandomNumericKeySpellingsAgreeWithTheValueModel(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy; run without -short")
	}
	defer func(prev string) { NumericScale = prev }(NumericScale)
	NumericScale = ""
	cols := map[string]string{"k": "numeric", "v": "text"}
	order := []string{"k", "v"}

	for seed := uint64(1); seed <= 4; seed++ {
		r := rand.New(rand.NewPCG(seed, 0x6e756d))
		dir := t.TempDir()
		tbl, err := New(dir, "public", "nk", "k", cols, order)
		if err != nil {
			t.Fatal(err)
		}
		type entry struct{ spell, v string }
		model := map[string]entry{} // value -> the spelling last written, and v
		lsn := 1
		check := func(stage string) {
			t.Helper()
			rows, err := tbl.Live()
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, row := range rows {
				k := numericValueKey(t, fmt.Sprint(row["k"]))
				if _, dup := got[k]; dup {
					t.Fatalf("seed %d %s: two live rows for the key %s", seed, stage, k)
				}
				got[k] = fmt.Sprint(row["v"])
			}
			if len(got) != len(model) {
				t.Fatalf("seed %d %s: %d live rows, the model has %d", seed, stage, len(got), len(model))
			}
			for k, e := range model {
				if got[k] != e.v {
					t.Fatalf("seed %d %s: key %s holds %q, the model %q", seed, stage, k, got[k], e.v)
				}
			}
		}

		const keys = 24
		for op := 0; op < 400; op++ {
			n := int64(r.IntN(keys)) - 4
			scale := r.IntN(3)
			spell := numericSpelling(r, n, scale)
			value := numericValueKey(t, spell)
			_, present := model[value]
			v := fmt.Sprintf("v%d", op)
			var ch changestream.Change
			switch {
			case !present:
				ch = typedChange(changestream.OpInsert, "nk", map[string]any{"k": spell, "v": v})
				model[value] = entry{spell, v}
			case r.IntN(2) == 0:
				ch = typedChange(changestream.OpUpdate, "nk", map[string]any{"k": spell, "v": v})
				model[value] = entry{spell, v}
			default:
				ch = typedChange(changestream.OpDelete, "nk", map[string]any{"k": spell})
				delete(model, value)
			}
			tapply(t, tbl, lsn, ch)
			lsn++
			if op%97 == 96 {
				if _, err := tbl.Compact(); err != nil {
					t.Fatal(err)
				}
				check("after compaction")
			}
		}
		check("before the final compaction")
		if _, err := tbl.Compact(); err != nil {
			t.Fatal(err)
		}
		tbl.Close()

		tbl, err = New(dir, "public", "nk", "k", cols, order)
		if err != nil {
			t.Fatal(err)
		}
		check("after reopen")

		// A delete and a re-insert in other spellings of the surviving keys.
		for value, e := range model {
			tapply(t, tbl, lsn, typedChange(changestream.OpDelete, "nk", map[string]any{"k": e.spell}))
			lsn++
			delete(model, value)
		}
		check("after deleting every key")
		tbl.Close()
	}
}
