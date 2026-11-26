| query | state | heap, earlier (pg) | Quicksilver, earlier (qs) | Mason, earlier | heap, this run | routed, this run | DuckDB direct, this run |
|---|---|---:|---:|---:|---:|---:|---:|
| count_by_status | A: fresh after load | 609.5 / 640.2 | 19.2 / 21.3 | 23.0 / 24.1 | 590.7 / 626.0 | 36.4 / 40.7 | 48.2 / 50.3 |
| revenue_by_month_join | A: fresh after load | 9580.0 / 9854.1 | 1376.1 / 1387.0 | 233.5 / 238.2 | 3739.2 / 3866.0 | 3844.7 / 3913.2 (heap) | 3211.2 / 3296.5 |
| pk_lookup | A: fresh after load | 0.1 / 0.1 | 237.9 / 325.1 | 14.5 / 15.3 | 0.1 / 0.2 | 0.2 / 0.4 (heap) | 23.4 / 26.1 |
| count_star | A: fresh after load | 245.3 / 249.3 | 3.0 / 3.1 | 4.0 / 4.0 | 251.9 / 267.6 | 8.5 / 10.1 | 2.8 / 3.1 |
| sum_amount | A: fresh after load | 455.7 / 462.6 | 36.6 / 42.6 | 7.4 / 7.9 | 498.3 / 505.3 | 30.2 / 33.1 | 48.4 / 51.3 |
| month_filter_agg | A: fresh after load | 51.0 / 61.8 | 10.1 / 10.9 | 5.5 / 6.5 | 66.6 / 77.5 | 20.4 / 26.1 | 17.5 / 17.8 |
| count_by_status | B: after the change workload | 700.2 / 714.7 | 112.2 / 120.6 | 28.5 / 35.7 | 759.0 / 789.0 | 71.9 / 78.8 | 97.1 / 102.6 |
| revenue_by_month_join | B: after the change workload | 9667.9 / 9780.0 | 1519.0 / 1566.6 | 259.4 / 265.8 | 5041.6 / 5649.1 | 4194.4 / 4600.2 (heap) | 3298.7 / 3425.8 |
| pk_lookup | B: after the change workload | 0.1 / 0.1 | 296.3 / 305.5 | 14.7 / 15.6 | 0.1 / 0.2 | 0.2 / 0.4 (heap) | 46.5 / 49.0 |
| count_star | B: after the change workload | 1370.3 / 1417.5 | 331.8 / 341.3 | 7.2 / 7.3 | 1538.6 / 1656.7 | 7.5 / 7.9 | 2.8 / 2.9 |
| sum_amount | B: after the change workload | 561.1 / 579.8 | 127.4 / 129.7 | 13.2 / 15.8 | 590.5 / 596.9 | 31.0 / 35.2 | 49.4 / 51.6 |
| month_filter_agg | B: after the change workload | 70.7 / 71.8 | 47.5 / 55.3 | 5.8 / 6.1 | 94.7 / 97.9 | 21.3 / 23.0 | 17.1 / 17.3 |
| count_by_status | C: after the mirror compacted | 618.9 / 687.9 | 120.9 / 137.1 | n/a | 710.2 / 732.1 | 33.8 / 35.5 | 55.0 / 55.7 |
| revenue_by_month_join | C: after the mirror compacted | 9009.6 / 9232.4 | 1506.8 / 1549.5 | n/a | 3683.8 / 3791.8 | 3824.1 / 4014.1 (heap) | 3100.3 / 3129.6 |
| pk_lookup | C: after the mirror compacted | 0.2 / 0.3 | 302.7 / 323.0 | n/a | 0.1 / 0.2 | 0.2 / 0.4 (heap) | 24.4 / 24.8 |
| count_star | C: after the mirror compacted | 1369.8 / 1436.2 | 340.0 / 352.5 | n/a | 1614.1 / 1618.3 | 7.9 / 8.2 | 3.0 / 3.1 |
| sum_amount | C: after the mirror compacted | 534.3 / 541.9 | 138.0 / 144.5 | n/a | 568.8 / 583.6 | 28.8 / 29.8 | 46.7 / 51.8 |
| month_filter_agg | C: after the mirror compacted | 68.0 / 70.4 | 66.5 / 72.0 | n/a | 94.3 / 99.4 | 20.8 / 22.0 | 15.3 / 15.7 |
