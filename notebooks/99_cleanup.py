# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Limpeza (deleta arquivos e recursos)
# MAGIC
# MAGIC Remove os artefatos do benchmark para cortar custo de storage.
# MAGIC Controlado por toggles — por padrao **apaga os arquivos CSV e as tabelas**,
# MAGIC mas preserva `bench_results`, o Volume e o Schema (ajuste os widgets).

# COMMAND ----------

dbutils.widgets.text("catalog", "bench_databricks", "Catalog")
dbutils.widgets.text("schema", "tpcds", "Schema")
dbutils.widgets.text("volume", "tpcds_data", "Volume")
dbutils.widgets.text("csv_subdir", "csv", "Subpasta dos CSVs")
dbutils.widgets.dropdown("delete_csv_files", "true",  ["true", "false"], "Deletar arquivos CSV")
dbutils.widgets.dropdown("drop_tables",      "true",  ["true", "false"], "Dropar tabelas TPC-DS")
dbutils.widgets.dropdown("drop_bench_results","false",["true", "false"], "Dropar tabela bench_results")
dbutils.widgets.dropdown("drop_volume",      "false", ["true", "false"], "Dropar o Volume")
dbutils.widgets.dropdown("drop_schema",      "false", ["true", "false"], "Dropar o Schema inteiro")
dbutils.widgets.text("warehouse_name", "bench_tpcds", "Nome do SQL warehouse a deletar")
dbutils.widgets.dropdown("drop_warehouse",   "true",  ["true", "false"], "Deletar o SQL warehouse")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")
CSV_SUB = dbutils.widgets.get("csv_subdir").strip("/")
WNAME   = dbutils.widgets.get("warehouse_name").strip()
def flag(w): return dbutils.widgets.get(w) == "true"

CSV_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{CSV_SUB}"
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# 1) arquivos CSV
if flag("delete_csv_files"):
    try:
        dbutils.fs.rm(CSV_DIR, recurse=True)
        print(f"[del] arquivos removidos: {CSV_DIR}")
    except Exception as e:
        print(f"[del] nada a remover em {CSV_DIR}: {e}")
else:
    print("[skip] delete_csv_files=false")

# COMMAND ----------

# 2) tabelas TPC-DS (preserva bench_results salvo se drop_bench_results=true)
if flag("drop_tables"):
    tbls = [r.tableName for r in spark.sql(f"SHOW TABLES IN {FQ}").collect()]
    for t in tbls:
        if t == "bench_results" and not flag("drop_bench_results"):
            continue
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t}")
        print(f"[drop table] {t}")
else:
    print("[skip] drop_tables=false")

if flag("drop_bench_results"):
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.bench_results")
    print("[drop table] bench_results")

# COMMAND ----------

# 3) Volume e/ou Schema
if flag("drop_volume"):
    spark.sql(f"DROP VOLUME IF EXISTS {FQ}.{VOLUME}")
    print(f"[drop volume] {FQ}.{VOLUME}")

if flag("drop_schema"):
    spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
    print(f"[drop schema] {FQ} (CASCADE)")

# COMMAND ----------

# 4) SQL warehouse (deleta por nome)
if flag("drop_warehouse") and WNAME:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    achou = False
    for e in w.warehouses.list():
        if e.name == WNAME:
            w.warehouses.delete(id=e.id)
            print(f"[drop warehouse] {WNAME} (id={e.id})")
            achou = True
    if not achou:
        print(f"[drop warehouse] nenhum warehouse chamado '{WNAME}'")
else:
    print("[skip] drop_warehouse=false ou warehouse_name vazio")

print("\nLimpeza concluida.")
