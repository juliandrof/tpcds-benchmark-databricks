# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Load dos CSVs (COPY INTO)
# MAGIC
# MAGIC Carrega os CSVs do Volume nas tabelas Delta usando `COPY INTO` com
# MAGIC **CAST explicito por coluna** (CSV chega como STRING; o cast garante os
# MAGIC tipos da tabela e evita `DELTA_FAILED_TO_MERGE_FIELDS`). Idempotente.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog destino")
dbutils.widgets.text("schema", "tpcds_bench", "Schema destino")
dbutils.widgets.text("volume", "tpcds_data", "Nome do Volume")
dbutils.widgets.text("csv_subdir", "csv", "Subpasta dos CSVs")
dbutils.widgets.text("only_tables", "", "Somente estas tabelas (csv), vazio=todas")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")
CSV_SUB = dbutils.widgets.get("csv_subdir").strip("/")
ONLY    = [t.strip() for t in dbutils.widgets.get("only_tables").split(",") if t.strip()]

CSV_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{CSV_SUB}"
print(f"CSV_DIR={CSV_DIR}")

# COMMAND ----------

import time
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()]
tables = [t for t in tables if not t.startswith("_") and t != "bench_results"]
if ONLY:
    tables = [t for t in tables if t in ONLY]

for t in tables:
    cols = spark.table(f"{CATALOG}.{SCHEMA}.{t}").schema
    sel = ",\n      ".join(
        f"CAST({f.name} AS {f.dataType.simpleString().upper()}) AS {f.name}" for f in cols)
    stmt = f"""COPY INTO {CATALOG}.{SCHEMA}.{t}
      FROM (SELECT
      {sel}
      FROM '{CSV_DIR}/{t}')
      FILEFORMAT = CSV
      FORMAT_OPTIONS ('header'='true', 'nullValue'='', 'delimiter'=',')
      COPY_OPTIONS ('mergeSchema'='false')"""
    t0 = time.time()
    spark.sql(stmt)
    print(f"[load] {t:24} {(time.time()-t0)/60:6.1f} min")

print("\nContagens:")
for t in tables:
    print(f"  {t:24} {spark.table(f'{CATALOG}.{SCHEMA}.{t}').count():>15,}")
