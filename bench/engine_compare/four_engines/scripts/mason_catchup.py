import json, subprocess, time, psycopg, os
Q="/tmp/qsb"
w=json.load(open(f"{Q}/results/workload.json")); url=open(f"{Q}/results/mason_url.txt").read().strip()
env=dict(os.environ, PATH="/opt/node22/bin:"+os.environ["PATH"])
t0=time.time()
subprocess.run(["npx","mason","start","--json"],cwd=f"{Q}/mason",env=env,stdout=subprocess.DEVNULL,check=True)
t_up=time.time(); conn=None; polls=0
while True:
    try:
        if conn is None: conn=psycopg.connect(url,autocommit=True)
        polls+=1
        n=conn.execute(f"SELECT count(*) FROM orders WHERE id = '{w['marker_insert_id']}'").fetchone()[0]
        if n==1: break
    except Exception as e:
        print("poll error:",type(e).__name__,str(e)[:200]); conn=None
    time.sleep(0.1)
t1=time.time()
out=dict(start=t0, start_cmd_returned_s=t_up-t0, start_to_visible_s=t1-t0, polls=polls, last_commit_to_visible_s=t1-w["t_last_commit"])
json.dump(out,open(f"{Q}/results/mason_catchup.json","w"),indent=1); print(out)
