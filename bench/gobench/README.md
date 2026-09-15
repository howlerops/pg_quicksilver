# Go JSON decode benchmark

Measures Go on the **identical wal2json payload** used for the Python and
Rust(`orjson`) numbers, so all three are comparable on one machine rather than
cited from other people's benchmarks.

```bash
# payload comes from bench/scripts/json_parser_shootout.py (writes payload.ndjson)
go build -o gobench . && ./gobench
```

Result (40,002 messages, 16.0 MB — see [../../docs/adr/0009-implementation-language-revised.md](../../docs/adr/0009-implementation-language-revised.md)):

| Parser | MB/s |
|---|---|
| Go `encoding/json` → `map[string]any` | 48 |
| Go `encoding/json` → typed struct | 50 |
| Go `encoding/json` + `UseNumber` (exact decimals) | 47 |
| **`goccy/go-json` → typed struct** | **207** |

The headline: **Go's standard library JSON is the slowest option measured** — slower
than CPython's C scanner — but `goccy/go-json` is **4.1× faster than the stdlib and
matches Rust's `orjson` (191 MB/s)**. The language was never the constraint; the
library choice was.

Note also that Go's `json.Number` preserves exact decimal text — the same
correctness property Python needs `parse_float=Decimal` for — at a cost of ~6%,
versus Python's much larger penalty.
