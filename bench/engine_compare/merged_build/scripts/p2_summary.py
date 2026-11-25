import json, os, re
B="/tmp/claude-0/-home-user-pg-quicksilver/58894071-fd3a-5a32-94ab-41a0530826ff/scratchpad/bench"
O=f"{B}/results"; N=f"{B}/results_merged"
def q(path):
    if not os.path.exists(path): return None
    d=json.load(open(path))["queries"]; return {k:(v.get("best_ms"),v.get("median_ms"),v.get("ok")) for k,v in d.items()}
def hwm(path):
    m=0
    if not os.path.exists(path): return None
    for l in open(path):
        p=l.split()
        if "VmHWM:" in p: m=max(m,int(p[p.index("VmHWM:")+1]))
    return round(m/1024)
cols=[("earlier A (3 cpu)",f"{O}/qs_A_warm.json"),("old A",f"{N}/qs_A_warm_old.json"),("merged A",f"{N}/qs_A_warm_new.json"),
      ("earlier B (3 cpu)",f"{O}/qs_B_warm.json"),("old B",f"{N}/qs_B_warm_old.json"),("merged B2 pre-rewrite",f"{N}/qs_B2_warm_new.json"),
      ("merged B post-idle",f"{N}/qs_B_warm_new_idle.json"),("merged B2 post-idle",f"{N}/qs_B2_warm_new_idle.json"),("earlier C (3 cpu)",f"{O}/qs_C_warm.json")]
data=[(n,q(p)) for n,p in cols]
names=["count_by_status","revenue_by_month_join","pk_lookup","count_star","sum_amount","month_filter_agg"]
print("best/median ms | "+" | ".join(n for n,_ in data))
for k in names:
    print(k+" | "+" | ".join("-" if d is None or k not in d else f"{d[k][0]:.1f}/{d[k][1]:.1f}"+("" if d[k][2] else " WRONG") for _,d in data))
print()
for tag,p in [("earlier initial",f"{O}/qs_ready_initial.txt"),("old initial",f"{N}/qs_ready_initial_old.txt"),("merged initial",f"{N}/qs_ready_initial_new.txt")]:
    print(tag, open(p).read().strip())
for tag,p in [("earlier initial",f"{O}/qs_rss_initial.log"),("old initial",f"{N}/qs_rss_initial_old.log"),("merged initial",f"{N}/qs_rss_initial_new.log"),
              ("earlier catchup",f"{O}/qs_rss_catchup.log"),("old catchup",f"{N}/qs_rss_catchup_old.log"),("merged catchup+idle rewrite",f"{N}/qs_rss_catchup_new.log"),("merged catchup2+idle rewrite",f"{N}/qs_rss_catchup2_new.log")]:
    print("peak RSS MB", tag, hwm(p))
for tag,p in [("earlier",f"{O}/qs_catchup_catchup.json"),("old",f"{N}/qs_catchup_catchup_old.json"),("merged r1",f"{N}/qs_catchup_catchup_new.json"),("merged r2",f"{N}/qs_catchup_catchup2_new.json")]:
    if os.path.exists(p):
        d=json.load(open(p)); print("drain", tag, "start_to_visible_s=%.1f first_apply_after_start_s=%.1f"%(d["start_to_visible_s"],d["first_apply_after_start_s"]))
for f in sorted(os.listdir(O))+sorted(os.listdir(N)):
    if f.startswith("live_lag_qs"):
        p=(O if os.path.exists(f"{O}/{f}") else N)+"/"+f; d=json.load(open(p)); print("live lag", f, "median %.0f best %.0f all %s"%(d["median_ms"],d["best_ms"],[round(x) for x in d["commit_to_visible_ms"]]))
for f in ("qs_mirror_initial.log",):
    for l in open(f"{O}/{f}"):
        if "snapshot complete" in l or "key index" in l: print("earlier:", re.sub(r"time=\S+ level=\S+ ","",l.strip()))
w0=json.load(open(f"{O}/workload.json")); w1=json.load(open(f"{N}/workload_round1.json")); w2=json.load(open(f"{N}/workload.json"))
for t,w in (("earlier",w0),("round1",w1),("round2",w2)): print("workload",t,"s=%.1f wal_bytes=%d"%(w["workload_s"],w["wal_bytes"]))
