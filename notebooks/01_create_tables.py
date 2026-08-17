# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Cria as tabelas Delta (DDL)
# MAGIC
# MAGIC Cria as 24 tabelas TPC-DS em `{catalog}.{schema}` com o **schema exato**
# MAGIC copiado do schema de origem (garante tipos identicos aos dados do CSV).

# COMMAND ----------

dbutils.widgets.text("catalog", "bench_databricks", "Catalog destino")
dbutils.widgets.text("schema", "tpcds", "Schema destino")
dbutils.widgets.text("source_schema", "samples.tpcds_sf1000", "Schema TPC-DS de origem")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
SRC     = dbutils.widgets.get("source_schema").rstrip(".")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {SRC}").collect()]
print(f"{len(tables)} tabelas")

for t in tables:
    cols = spark.table(f"{SRC}.{t}").schema
    defs = ",\n  ".join(f"{f.name} {f.dataType.simpleString().upper()}" for f in cols)
    ddl = f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{t} (\n  {defs}\n) USING DELTA"
    spark.sql(ddl)
    print(f"[ddl] {t}")

print("DDL concluido")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))
