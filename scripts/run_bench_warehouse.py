#!/usr/bin/env python3
"""Roda as queries do repo num SQL warehouse (via API) e grava em {catalog}.{schema}.bench_results.

Alternativa ao notebook 03 quando se quer medir num SQL warehouse especifico
(ex.: BenchCreditas) em vez do compute serverless do notebook.

Uso:
  python scripts/run_bench_warehouse.py \
      --profile fe-vm-jsf-demo --warehouse-id <id> \
      --catalog main --schema tpcds_bench \
      [--only q1,q3] [--mode append|overwrite]
"""
import subprocess, json, time, glob, os, argparse, datetime

def api(method, path, profile, body=None):
    cmd = ["databricks", "api", method, path, "--profile", profile]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    try:
        return json.loads(r.stdout)
    except Exception:
        return {"_raw": r.stdout, "_err": r.stderr}

def run_stmt(sql, args, catalog=None, schema=None):
    body = {"warehouse_id": args.warehouse_id, "statement": sql,
            "wait_timeout": "50s", "on_wait_timeout": "CONTINUE",
            "format": "JSON_ARRAY", "disposition": "INLINE"}
    if catalog: body["catalog"] = catalog
    if schema:  body["schema"] = schema
    t0 = time.time()
    d = api("post", "/api/2.0/sql/statements", args.profile, body)
    sid = d.get("statement_id"); state = d.get("status", {}).get("state")
    while state in ("PENDING", "RUNNING") and sid:
        time.sleep(5)
        d = api("get", f"/api/2.0/sql/statements/{sid}", args.profile)
        state = d.get("status", {}).get("state")
    secs = round(time.time() - t0, 2)
    if state == "SUCCEEDED":
        res = d.get("result", {})
        rows = res.get("row_count") or len(res.get("data_array") or [])
        return state, secs, rows, ""
    return state or "UNKNOWN", secs, 0, d.get("status", {}).get("error", {}).get("message", "")[:400]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--only", default="")
    ap.add_argument("--mode", default="append", choices=["append", "overwrite"])
    args = ap.parse_args()

    qdir = os.path.join(os.path.dirname(__file__), "..", "queries")
    files = sorted(glob.glob(f"{qdir}/*.sql"),
                   key=lambda p: (len(os.path.basename(p)), os.path.basename(p)))
    only = [q.strip() for q in args.only.split(",") if q.strip()]
    if only:
        files = [p for p in files if os.path.basename(p)[:-4] in only]

    run_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fq = f"{args.catalog}.{args.schema}.bench_results"
    print(f"Rodando {len(files)} queries no warehouse {args.warehouse_id} -> {fq}")

    # tabela de resultados
    run_stmt(f"""CREATE TABLE IF NOT EXISTS {fq} (
        query STRING, state STRING, seconds DOUBLE, num_rows BIGINT,
        error STRING, run_id STRING, engine STRING, run_ts TIMESTAMP)""", args)
    if args.mode == "overwrite":
        run_stmt(f"TRUNCATE TABLE {fq}", args)

    results = []
    for p in files:
        name = os.path.basename(p)[:-4]
        sql = open(p).read().strip().rstrip(";")
        state, secs, rows, err = run_stmt(sql, args, args.catalog, args.schema)
        results.append((name, state, secs, rows, err))
        print(f"  {name:6} {state:10} {secs:8.2f}s rows={rows} {err}")

    # grava resultados
    def esc(s): return s.replace("'", "''")
    values = ",".join(
        f"('{n}','{st}',{sc},{rw},'{esc(er)}','{run_id}','warehouse:{args.warehouse_id}',current_timestamp())"
        for n, st, sc, rw, er in results)
    run_stmt(f"""INSERT INTO {fq}
        (query,state,seconds,num_rows,error,run_id,engine,run_ts) VALUES {values}""", args)

    ok = [r for r in results if r[1] == "SUCCEEDED"]
    tot = sum(r[2] for r in ok)
    print(f"\n=== {len(ok)}/{len(results)} ok | total {tot:.1f}s ({tot/60:.1f} min) | run_id={run_id} ===")
    print(f"Gravado em {fq}")

if __name__ == "__main__":
    main()
