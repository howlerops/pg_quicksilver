#!/usr/bin/env python3
"""One named, server-side prepared statement executed with an unselective and
a selective parameter, three rounds. Prints answer and route for each; used by
side_proof.sh while the heap has one order the mirror does not.

    python3 prepared_sides.py ROUTER_DSN
"""
import sys

import psycopg

with psycopg.connect(sys.argv[1], autocommit=True) as conn:
    q = "SELECT count(*) FROM orders WHERE customer_id = %s"
    for _ in range(3):
        for cust in (1, 5):
            n = conn.execute(q, (cust,), prepare=True).fetchone()[0]
            route = conn.execute("SHOW qs.last_route", prepare=False).fetchone()[0]
            print(cust, n, route.split(":")[0])
    names = conn.execute("SELECT count(*) FROM pg_prepared_statements WHERE NOT from_sql", prepare=False).fetchone()[0]
    print("server-side statements:", names)
