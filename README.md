# TPC-DS Benchmark on Databricks

Pipeline reprodutível para rodar o benchmark **TPC-DS** no Databricks: exporta os dados
de referência (`samples.tpcds_sf1000`, 1 TB) para CSV num **Volume** do Unity Catalog,
cria as tabelas Delta, carrega via `COPY INTO`, roda as **99 queries** do TPC-DS e grava
os tempos numa tabela `bench_results`.

Tudo é **parametrizado** por `catalog` e `schema` — Volume, tabelas e `bench_results`
ficam todos no mesmo schema.

## 🤖 Prompt de execução (para um agente de IA)

> Copie o bloco abaixo (ou aponte seu agente para esta URL). Ele contém tudo o que é
> necessário para rodar o benchmark completo de ponta a ponta.

```text
Você é um agente com acesso ao Databricks CLI. Rode o benchmark TPC-DS deste repositório
(https://github.com/juliandrof/tpcds-benchmark-databricks) de ponta a ponta.

CONTEXTO
- Este repo é um Databricks Asset Bundle (databricks.yml) que cria 4 jobs serverless:
  tpcds_pipeline (export CSV -> DDL -> COPY INTO), tpcds_benchmark_serial,
  tpcds_benchmark_parallel e tpcds_cleanup.
- Fonte dos dados: samples.tpcds_sf1000 (TPC-DS 1 TB, já disponível no catálogo `samples`).
  Para 1 GB use samples.tpcds_sf1 (parâmetro source_schema).
- As 99 queries (103 arquivos) estão em queries/ e são lidas do próprio repo.
- As queries rodam SEMPRE num SQL warehouse chamado `bench_tpcds` (size Large, serverless,
  autoscale 1-10) — o job de benchmark cria esse warehouse se ele não existir, e o job de
  cleanup o deleta.

PARÂMETROS (use os defaults, salvo pedido do usuário)
- profile        : profile do Databricks CLI já autenticado (ex.: meu-profile)
- catalog        : default bench_databricks
- schema         : default tpcds  (Volume + tabelas + bench_results)
- source_schema  : samples.tpcds_sf1000 (1 TB) ou samples.tpcds_sf1 (1 GB)
- warehouse_name : default bench_tpcds  (SQL warehouse Large, autoscale 1-10)
- concurrency    : concorrência do job paralelo (default 103)

PASSOS
1. Autentique se necessário:
   databricks auth login --host <host-do-workspace> --profile <profile>
2. Clone o repo localmente e entre nele:
   git clone https://github.com/juliandrof/tpcds-benchmark-databricks && cd tpcds-benchmark-databricks
3. Valide e faça deploy do bundle (cria os 4 jobs no workspace):
   databricks bundle validate -t dev -p <profile>
   databricks bundle deploy   -t dev -p <profile>
   # (opcional) sobrescreva defaults: --var catalog=<c> --var schema=<s> --var source_schema=<ss>
4. Prepare os dados (export CSV -> cria tabelas -> COPY INTO). Demora bastante no SF1000:
   databricks bundle run tpcds_pipeline -t dev -p <profile>
5. Rode o benchmark serial e o paralelo:
   databricks bundle run tpcds_benchmark_serial   -t dev -p <profile>
   databricks bundle run tpcds_benchmark_parallel -t dev -p <profile>
6. Reporte os resultados consultando a tabela <catalog>.<schema>.bench_results:
   SELECT engine, run_id, count(*) queries, round(sum(seconds),1) soma_s,
          round(avg(seconds),2) avg_s, round(max(seconds),2) max_s
   FROM <catalog>.<schema>.bench_results GROUP BY engine, run_id ORDER BY run_id DESC;
7. (Opcional) Ao terminar, libere storage rodando a limpeza:
   databricks bundle run tpcds_cleanup -t dev -p <profile>

REGRAS
- Nunca comite segredos/tokens. Não versione dados.
- Verifique cada job terminar com sucesso (state=TERMINATED, code=SUCCESS) antes do próximo.
- Se um COPY INTO falhar com DELTA_FAILED_TO_MERGE_FIELDS, confirme que está usando o
  notebook 02_load_copy_into (que faz CAST explícito) — não um COPY INTO de CSV sem cast.
- Ao final, entregue um resumo: nº de queries ok/falhas, tempo total serial vs paralelo.
```

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
(catalog, schema, results_schema, volume, source_schema, warehouse_name, warehouse_size,
warehouse_min/max, concurrency).

Defaults: `catalog=bench_databricks`, `schema=tpcds`. As queries rodam num SQL warehouse
`bench_tpcds` (Large, serverless, autoscale 1→10) — **criado automaticamente** pelo job de
benchmark se não existir, e **deletado** pelo job de cleanup.

### 1. Ajustar os parâmetros (opcional)
Edite as `variables` em `databricks.yml` se quiser outro `catalog` / `schema` /
`warehouse_name` / tamanho do warehouse. Os defaults já funcionam de imediato.

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
  --catalog bench_databricks --schema tpcds --mode append
```

### 5b. (Opcional) Teste de execução PARALELA

Submete as 103 queries concorrentemente para medir concorrência/throughput do warehouse
(útil para avaliar auto-scaling / multi-cluster). Mede o **wall clock do lote** e o
**speedup** vs a soma dos tempos individuais:
```bash
python scripts/run_bench_parallel.py \
  --profile <perfil> --warehouse-id <id> \
  --catalog bench_databricks --schema tpcds \
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
