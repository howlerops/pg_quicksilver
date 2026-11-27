#!/usr/bin/env python3
"""explain.py <variant> <threads> <query>... : EXPLAIN ANALYZE text into raw/explain_<variant>_<query>_t<threads>.txt"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *
variant, threads = sys.argv[1], int(sys.argv[2])
c = connect(threads)
setup(c, variant)
q = dict(QUERIES)
for name in sys.argv[3:]:
    c.execute(q[name]).fetchall()
    c.execute(q[name]).fetchall()
    txt = c.execute("EXPLAIN ANALYZE " + q[name]).fetchall()[0][1]
    open(f"{RAW}/explain_{variant}_{name}_t{threads}.txt", "w").write(txt)
    print("=====", variant, name)
    print(txt)
