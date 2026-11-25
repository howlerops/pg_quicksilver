import psycopg,sys
Q="/tmp/qsb"
dsn = open(f"{Q}/results/mason_url.txt").read().strip() if sys.argv[1]=="mason" else f"host={Q}/sock port=5440 dbname=app user=postgres"
c=psycopg.connect(dsn,autocommit=True); c.execute("SELECT 1 FROM orders LIMIT 0").fetchall(); c.close()
