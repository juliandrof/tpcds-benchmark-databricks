# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Export TPC-DS → CSV no Volume
# MAGIC
# MAGIC Le as tabelas de um schema TPC-DS de origem (default `samples.tpcds_sf1000`)
# MAGIC e escreve **CSV com header** num Volume do Unity Catalog.
# MAGIC O catalog/schema/volume sao **parametrizados** e criados **se nao existirem**.
# MAGIC Idempotente por tabela (pula quem ja tem `_SUCCESS`).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog destino")
dbutils.widgets.text("schema", "tpcds_bench", "Schema destino (volume+tabelas+bench_results)")
dbutils.widgets.text("volume", "tpcds_data", "Nome do Volume")
dbutils.widgets.text("csv_subdir", "csv", "Subpasta dentro do volume p/ os CSVs")
dbutils.widgets.text("source_schema", "samples.tpcds_sf1000", "Schema TPC-DS de origem")
dbutils.widgets.text("max_rows_per_file", "5000000", "Max linhas por arquivo CSV")
dbutils.widgets.text("only_tables", "", "Somente estas tabelas (csv), vazio=todas")

CATALOG   = dbutils.widgets.get("catalog")
SCHEMA    = dbutils.widgets.get("schema")
VOLUME    = dbutils.widgets.get("volume")
CSV_SUB   = dbutils.widgets.get("csv_subdir").strip("/")
SRC       = dbutils.widgets.get("source_schema").rstrip(".")
MAX_ROWS  = int(dbutils.widgets.get("max_rows_per_file"))
ONLY      = [t.strip() for t in dbutils.widgets.get("only_tables").split(",") if t.strip()]

OUT_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{CSV_SUB}"
print(f"SRC={SRC}\nOUT_DIR={OUT_DIR}\nMAX_ROWS_PER_FILE={MAX_ROWS}\nONLY={ONLY or 'TODAS'}")

# COMMAND ----------

# MAGIC %md ## Cria catalog / schema / volume se nao existirem

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME  IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"OK: {CATALOG}.{SCHEMA}.{VOLUME} pronto")

# COMMAND ----------

# MAGIC %md ## Export tabela a tabela

# COMMAND ----------

import time

tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {SRC}").collect()]
if ONLY:
    tables = [t for t in tables if t in ONLY]
print(f"{len(tables)} tabelas: {tables}")

def already_done(path):
    try:
        return any(f.name == "_SUCCESS" for f in dbutils.fs.ls(path))
    except Exception:
        return False

for t in tables:
    dst = f"{OUT_DIR}/{t}"
    if already_done(dst):
        print(f"[skip] {t}")
        continue
    t0 = time.time()
    (spark.read.table(f"{SRC}.{t}")
        .write.mode("overwrite")
        .option("header", "true")
        .option("compression", "none")
        .option("maxRecordsPerFile", MAX_ROWS)
        .csv(dst))
    print(f"[ok]   {t:24} {(time.time()-t0)/60:6.1f} min")

print("Export concluido ->", OUT_DIR)
