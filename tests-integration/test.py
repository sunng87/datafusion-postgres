import psycopg

conn = psycopg.connect("host=127.0.0.1 port=5432 user=tom password=pencil dbname=localdb")
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM delhi")
    results = cur.fetchone()
    assert results[0] == 1462

with conn.cursor() as cur:
    cur.execute("SELECT * FROM delhi ORDER BY date LIMIT 10")
    results = cur.fetchall()
    assert len(results) == 10

with conn.cursor() as cur:
    cur.execute("SELECT date FROM delhi WHERE meantemp > %s ORDER BY date", [30])
    results = cur.fetchall()
    assert len(results) == 527
    assert len(results[0]) == 1
    print(results[0])
