# TPC-DS Benchmark on Databricks

Pipeline reprodutível para rodar o benchmark **TPC-DS** no Databricks: exporta os dados
de referência (`samples.tpcds_sf1000`, 1 TB) para CSV num **Volume** do Unity Catalog,
cria as tabelas Delta, carrega via `COPY INTO`, roda as **99 queries** do TPC-DS e grava
os tempos numa tabela `bench_results`.

Tudo é **parametrizado** por `catalog` e `schema` — Volume, tabelas e `bench_results`
ficam todos no mesmo schema.

## Estrutura

```
tpcds-benchmark-databricks/
├── databricks.yml             # Databricks Asset Bundle: variaveis + 4 jobs
├── notebooks/
│   ├── 00_export_csv.py       # samples.* -> CSV no Volume (cria catalog/schema/volume se nao existir)
│   ├── 01_create_tables.py    # DDL das 24 tabelas (schema copiado da origem)
│   ├── 02_load_copy_into.py   # COPY INTO com CAST explicito (CSV -> Delta)
│   ├── 03_run_benchmark.py    # roda queries/ (serial|parallel) e grava bench_results
│   └── 99_cleanup.py          # deleta arquivos CSV e dropa tabelas/volume/schema (toggles)
├── queries/                   # q1.sql .. q99.sql (103 arquivos, TPC-DS do apache/spark)
├── scripts/
│   ├── run_bench_warehouse.py  # (opcional/local) roda as queries em SERIE num SQL warehouse
│   └── run_bench_parallel.py   # (opcional/local) teste de execucao PARALELA das 103 queries
└── conf/params.example.json
```

### Jobs (criados pelo bundle)

| Job | O que faz |
|---|---|
| `tpcds-benchmark-pipeline` | `export → create_tables → load` (preparacao dos dados) |
| `tpcds-benchmark-serial` | roda as 103 queries **em serie** → `bench_results` |
| `tpcds-benchmark-parallel` | roda as 103 queries **em paralelo** (concurrency configuravel) → `bench_results` |
| `tpcds-benchmark-cleanup` | deleta arquivos CSV e recursos (toggles) |

> **As queries ficam no repositório** (`queries/`), **não no Volume**. O notebook
> `03_run_benchmark` lê os `.sql` da pasta do repo (arquivos sincronizados pelo bundle /
> Git folder; auto-detecta o caminho, com override via o widget `queries_dir`).

## Pré-requisitos

- Databricks CLI autenticado num workspace com Unity Catalog e compute **serverless**.
  ```bash
  databricks auth login --host https://<seu-workspace> --profile <perfil>
  ```
- Acesso de leitura ao catálogo `samples`.
- Permissão para criar catalog/schema/volume no destino.

## Passo a passo (Databricks Asset Bundle)

O repo é um **Databricks Asset Bundle** (`databricks.yml`): o `deploy` sobe os notebooks
e a pasta `queries/` para o workspace e cria os 4 jobs. Parametrização via `variables`
(catalog, schema, results_schema, volume, source_schema, warehouse_id, concurrency);
o target `dev` já traz defaults do ambiente atual.

### 1. Ajustar os parâmetros
Edite as `variables` em `databricks.yml` (ou o bloco `targets.dev.variables`) para o seu
`catalog` / `schema` / `warehouse_id`. Deixe `warehouse_id` vazio para rodar as queries no
compute serverless do notebook, ou informe um SQL warehouse.

### 2. Deployar o bundle
```bash
databricks bundle validate -t dev -p <perfil>
databricks bundle deploy   -t dev -p <perfil>
```
Isso cria os jobs `tpcds-benchmark-pipeline`, `-serial`, `-parallel` e `-cleanup`.
(No modo `development` os nomes aparecem prefixados com `[dev <voce>]`.)

### 3. Preparar os dados (uma vez) e rodar o benchmark
```bash
databricks bundle run tpcds_pipeline           -t dev -p <perfil>   # export -> create -> load
databricks bundle run tpcds_benchmark_serial   -t dev -p <perfil>   # 103 queries em serie
databricks bundle run tpcds_benchmark_parallel -t dev -p <perfil>   # 103 queries em paralelo
```

### 4. Ver os resultados
```sql
SELECT run_id, count(*) queries, round(sum(seconds),1) total_s,
       round(avg(seconds),2) avg_s, round(max(seconds),2) max_s
FROM <catalog>.<schema>.bench_results
GROUP BY run_id ORDER BY run_id DESC;

-- top 10 mais lentas do ultimo run
SELECT query, seconds, num_rows
FROM <catalog>.<schema>.bench_results
WHERE run_id = (SELECT max(run_id) FROM <catalog>.<schema>.bench_results)
ORDER BY seconds DESC LIMIT 10;
```

### 5. (Opcional) Medir num SQL warehouse específico
```bash
python scripts/run_bench_warehouse.py \
  --profile <perfil> --warehouse-id <id> \
  --catalog main --schema tpcds_bench --mode append
```

### 5b. (Opcional) Teste de execução PARALELA

Submete as 103 queries concorrentemente para medir concorrência/throughput do warehouse
(útil para avaliar auto-scaling / multi-cluster). Mede o **wall clock do lote** e o
**speedup** vs a soma dos tempos individuais:
```bash
python scripts/run_bench_parallel.py \
  --profile <perfil> --warehouse-id <id> \
  --catalog main --schema tpcds_bench \
  --concurrency 10 --mode append
```
Grava em `bench_results` com `engine = warehouse:<id>:parallel(c=N)`, permitindo comparar
serial × paralelo no SQL:
```sql
SELECT engine, run_id, count(*) queries, round(sum(seconds),1) soma_s
FROM <catalog>.<schema>.bench_results GROUP BY engine, run_id ORDER BY run_id DESC;
```

### 6. Limpeza
```bash
databricks bundle run tpcds_cleanup -t dev -p <perfil>
```
Toggles do job/notebook `99_cleanup`: `delete_csv_files`, `drop_tables`,
`drop_bench_results`, `drop_volume`, `drop_schema`. Por padrão apaga os CSVs
(~900 GB no SF1000) e as tabelas, preservando `bench_results`.

## Tabela `bench_results`

| coluna | tipo | descrição |
|---|---|---|
| `query` | string | nome (ex.: `q24a`) |
| `state` | string | `SUCCEEDED` / `FAILED` |
| `seconds` | double | tempo de execução (wall) |
| `num_rows` | bigint | linhas retornadas |
| `error` | string | mensagem (se falhou) |
| `run_id` | string | id do run (timestamp) |
| `engine` | string | `spark-notebook:serial` / `...:parallel(c=N)` ou `warehouse:<id>:...` |
| `run_ts` | timestamp | quando rodou |

## Notas de custo

- SF1000 gera **~900 GB de CSV** no Volume — rode a limpeza quando terminar.
- As queries vêm do repositório `apache/spark` (`sql/core/src/test/resources/tpcds/`),
  já em Spark SQL. São 103 arquivos (99 queries + variantes `a`/`b`: q14, q23, q24, q39).
