package main

import (
	"os"
	"testing"
)

func mustRead(b *testing.B, p string) []byte {
	b.Helper()
	buf, err := os.ReadFile(p)
	if err != nil {
		b.Fatal(err)
	}
	return buf
}

// What the router does to a statement before it touches a socket.
func BenchmarkAnalysePointLookup(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := analyse("SELECT * FROM orders WHERE id = 424242;", "public", "qs_mirror")
		if ok, _ := in.plainRead(false, []string{"avg", "pg_typeof", "current_setting"}); !ok {
			b.Fatal("refused")
		}
	}
}

func BenchmarkDecideJoinPlan(b *testing.B) {
	p, err := ParsePlan(mustRead(b, "testdata/join.json"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if d := Decide(p, testCatalog, testThresholds, false); d.Side != Mirror {
			b.Fatal(d)
		}
	}
}
