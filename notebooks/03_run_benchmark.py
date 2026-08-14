# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Roda as 99 queries (serial ou paralelo) e grava `bench_results`
# MAGIC
# MAGIC Le os `queries/*.sql` **da pasta do repositorio** (Git folder / arquivos do bundle),
# MAGIC executa cada query cronometrando e grava em **`{catalog}.{schema}.bench_results`**.
# MAGIC
# MAGIC - `execution_mode = serial` → uma query por vez
# MAGIC - `execution_mode = parallel` → `concurrency` queries ao mesmo tempo (thread pool)
# MAGIC - `warehouse_id` vazio → executa via `spark.sql` no compute do notebook;
# MAGIC   preenchido → executa no **SQL warehouse** informado (via WorkspaceClient).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "tpcds_bench", "Schema das tabelas TPC-DS (USE)")
dbutils.widgets.text("results_schema", "", "Schema da bench_results (vazio = mesmo do schema)")
dbutils.widgets.dropdown("execution_mode", "serial", ["serial", "parallel"], "Modo de execucao")
dbutils.widgets.text("concurrency", "103", "Concorrencia (modo parallel)")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse id (vazio = spark do notebook)")
dbutils.widgets.text("queries_dir", "", "Pasta das queries (vazio = auto-detecta)")
dbutils.widgets.text("only_queries", "", "Somente estas queries (ex: q1,q3), vazio=todas")
dbutils.widgets.dropdown("mode", "append", ["append", "overwrite"], "Escrita da bench_results")

CATALOG   = dbutils.widgets.get("catalog")
SCHEMA    = dbutils.widgets.get("schema")
RES_SCHEMA = dbutils.widgets.get("results_schema").strip() or SCHEMA
EXEC_MODE = dbutils.widgets.get("execution_mode")
CONC      = max(1, int(dbutils.widgets.get("concurrency")))
WID       = dbutils.widgets.get("warehouse_id").strip()
ONLY      = [q.strip() for q in dbutils.widgets.get("only_queries").split(",") if q.strip()]
WRITE     = dbutils.widgets.get("mode")

# COMMAND ----------

import os, glob, time, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def queries_dir():
    override = dbutils.widgets.get("queries_dir").strip()
    if override:
        return override.replace("dbfs:", "")
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    nb = ctx.notebookPath().get()                 # .../<repo>/notebooks/03_run_benchmark
    repo = os.path.dirname(os.path.dirname(nb))    # .../<repo>
    return f"/Workspace{repo}/queries"

QDIR = queries_dir()
qfiles = sorted(glob.glob(f"{QDIR}/*.sql"),
                key=lambda p: (len(os.path.basename(p)), os.path.basename(p)))
if ONLY:
    qfiles = [p for p in qfiles if os.path.basename(p)[:-4] in ONLY]
assert qfiles, f"Nenhuma query encontrada em {QDIR}"
print(f"QDIR={QDIR}\n{len(qfiles)} queries | modo={EXEC_MODE} | conc={CONC if EXEC_MODE=='parallel' else 1} | "
      f"target={'warehouse:'+WID if WID else 'spark-notebook'}")

# COMMAND ----------

# ---- executores: spark.sql (compute do notebook) ou WorkspaceClient (SQL warehouse) ----
if WID:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    _w = WorkspaceClient()

    def exec_query(sql):
        r = _w.statement_execution.execute_statement(
            warehouse_id=WID, statement=sql, catalog=CATALOG, schema=SCHEMA,
            wait_timeout="50s", on_wait_timeout="CONTINUE")
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(3)
            r = _w.statement_execution.get_statement(r.statement_id)
        if r.status.state != StatementState.SUCCEEDED:
            raise RuntimeError((r.status.error.message if r.status.error else str(r.status.state))[:400])
        rows = (r.manifest.total_row_count if r.manifest else None) or 0
        return rows
    ENGINE = f"warehouse:{WID}"
else:
    def exec_query(sql):
        return spark.sql(sql).count()
    ENGINE = "spark-notebook"

def run_one(path):
    name = os.path.basename(path)[:-4]
    with open(path) as f:
        sql = f.read().strip().rstrip(";")
    try:
        t0 = time.time()
        rows = exec_query(sql)
        secs = round(time.time() - t0, 2)
        print(f"[q] {name:6} ok       {secs:8.2f}s rows={rows}", flush=True)
        return (name, "SUCCEEDED", secs, int(rows), "")
    except Exception as e:
        print(f"[q] {name:6} FAILED: {str(e)[:120]}", flush=True)
        return (name, "FAILED", 0.0, 0, str(e)[:500])

# COMMAND ----------

if not WID:
    spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")

RUN_ID = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
engine_label = ENGINE + (f":parallel(c={CONC})" if EXEC_MODE == "parallel" else ":serial")

wall0 = time.time()
if EXEC_MODE == "parallel":
    results = []
    with ThreadPoolExecutor(max_workers=CONC) as ex:
        for fut in as_completed([ex.submit(run_one, p) for p in qfiles]):
            results.append(fut.result())
else:
    results = [run_one(p) for p in qfiles]
wall = round(time.time() - wall0, 2)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

df = (spark.createDataFrame([Row(query=r[0], state=r[1], seconds=float(r[2]),
                                 num_rows=int(r[3]), error=r[4]) for r in results])
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("engine", lit(engine_label))
        .withColumn("run_ts", current_timestamp()))
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{RES_SCHEMA}")
(df.write.mode(WRITE).option("mergeSchema", "true")
   .saveAsTable(f"{CATALOG}.{RES_SCHEMA}.bench_results"))

ok = [r for r in results if r[1] == "SUCCEEDED"]
tot = sum(r[2] for r in ok)
print(f"\n=== RESUMO (run {RUN_ID} | {engine_label}) ===")
print(f"{len(ok)}/{len(results)} ok")
print(f"Wall clock do lote : {wall:.1f}s ({wall/60:.1f} min)")
print(f"Soma dos tempos    : {tot:.1f}s" + (f"  (speedup ~{tot/max(wall,0.01):.1f}x)" if EXEC_MODE=="parallel" else ""))
print(f"Gravado em {CATALOG}.{RES_SCHEMA}.bench_results (mode={WRITE})")
