// Go JSON decode benchmark on the IDENTICAL wal2json payload used for the
// Python and Rust(orjson) numbers, so the three are comparable on one box.
//
// Two variants, because they answer different questions:
//   encoding/json into map[string]any  — the naive port of the Python code
//   encoding/json into a typed struct  — what a real Go ingest would write
//
// Decimal handling: json.Number keeps the exact decimal TEXT rather than
// going through float64, which is the same correctness property Python needs
// parse_float=Decimal for — but in Go it costs nothing extra.
package main

import (
	"bufio"
	"encoding/json"

	gojson "github.com/goccy/go-json"
	"fmt"
	"os"
	"time"
)

type col struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value json.RawMessage `json:"value"`
}
type msg struct {
	Action  string `json:"action"`
	Lsn     string `json:"lsn"`
	NextLsn string `json:"nextlsn"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
	Columns []col  `json:"columns"`
}

func load(path string) [][]byte {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var out [][]byte
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		b := make([]byte, len(sc.Bytes()))
		copy(b, sc.Bytes())
		out = append(out, b)
	}
	return out
}

func main() {
	raw := load("/var/lib/postgresql/qsbench/gobench/payload.ndjson")
	var nbytes int
	for _, r := range raw {
		nbytes += len(r)
	}
	mb := float64(nbytes) / 1e6
	fmt.Printf("payload: %d messages, %.1f MB\n\n", len(raw), mb)

	bench := func(name string, fn func([]byte)) {
		best := time.Duration(1 << 62)
		for rep := 0; rep < 3; rep++ {
			t := time.Now()
			for _, r := range raw {
				fn(r)
			}
			if d := time.Since(t); d < best {
				best = d
			}
		}
		s := best.Seconds()
		fmt.Printf("%-44s%8.3fs%10.0f MB/s%12.0f msg/s\n",
			name, s, mb/s, float64(len(raw))/s)
	}

	bench("encoding/json -> map[string]any", func(b []byte) {
		var m map[string]any
		_ = json.Unmarshal(b, &m)
	})
	bench("encoding/json -> typed struct", func(b []byte) {
		var m msg
		_ = json.Unmarshal(b, &m)
	})
	bench("goccy/go-json -> typed struct", func(b []byte) {
		var m msg
		_ = gojson.Unmarshal(b, &m)
	})
	bench("encoding/json -> struct, UseNumber (exact dec)", func(b []byte) {
		var m msg
		d := json.NewDecoder(newReader(b))
		d.UseNumber()
		_ = d.Decode(&m)
	})
}

type br struct {
	b []byte
	i int
}

func newReader(b []byte) *br { return &br{b: b} }
func (r *br) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, os.ErrClosed
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
