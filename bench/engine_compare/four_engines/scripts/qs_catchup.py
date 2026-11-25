import json, subprocess, time, sys, os
Q="/tmp/qsb"; tag=sys.argv[1]
w=json.load(open(f"{Q}/results/workload.json"))
def L(s):
    a,b=s.split("/"); return (int(a,16)<<32)|int(b,16)
target=L(w["lsn_before_last_txn"])
t0=time.time()
subprocess.run([f"{Q}/scripts/qs_start.sh", tag], check=True)
first_move=None; start_lsn=None
while True:
    try:
        st=json.load(open(f"{Q}/mirror/public.orders/state.json"))
        cur=L(st["applied_lsn"])
        if start_lsn is None: start_lsn=cur
        if first_move is None and cur!=start_lsn: first_move=time.time()
        if cur>target: break
    except Exception as e:
        pass
    time.sleep(0.05)
t1=time.time()
out=dict(sidecar_start=t0, caught_up=t1, start_to_visible_s=t1-t0, first_apply_after_start_s=(first_move or t1)-t0,
         last_commit_to_visible_s=t1-w["t_last_commit"], applied_lsn=st["applied_lsn"])
json.dump(out,open(f"{Q}/results/qs_catchup_{tag}.json","w"),indent=1); print(out)
