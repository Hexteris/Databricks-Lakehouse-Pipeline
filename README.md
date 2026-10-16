# Databricks Lakehouse Pipeline: social service referrals

A re-runnable data ingestion pipeline on Databricks (Free Edition) that takes raw files through
**bronze → silver → gold**, with data quality rules, quarantine and remediation, PII protection, and SCD Type 2 history.

The scenario is a fictional social service agency that tracks clients and the programmes they are referred to.
Data arrives in three batches, the way a source system would send it: an initial load, a file of changes, and a corrections file.

> All client, referral and programme data is **synthetic** (`FAKE-` IDs, `0000` phone numbers, `example.com` emails),
> and the programme and agency names are invented. The only real dataset is the public Census of Population 2020
> resident counts by planning area (data.gov.sg).

## What it demonstrates

| Concept | Where |
|---|---|
| Medallion architecture | one schema per layer: `landing`, `bronze`, `silver`, `gold`, plus `ops` |
| Idempotent ingestion | Auto Loader checkpoints in bronze; MERGE with change detection in silver |
| Control tables | run log, data quality rules stored as data, rule results, quarantine |
| Data quality and remediation | rejected rows quarantined with reasons; corrections replayed through bronze and re-checked |
| PII protection | salted SHA-256 hashing, AES encryption with keys in a secret scope, Unity Catalog column masks |
| Dimensional modelling | star schema with SCD Type 1 and Type 2 dimensions, point-in-time join, and a wide table |
| Orchestration | one Databricks Job with task dependencies, a job parameter and retries |

## Architecture

```
landing volume (raw files, never changed)
   │  Auto Loader, checkpoint per source
   ▼
bronze   brz_*            all STRING + _source_file, _ingest_ts, _run_id   (append-only)
   │  clean → dedup → DQ rules (ops.dq_rule) → quarantine → hash/encrypt PII → MERGE
   ▼
silver   slv_*            one row per business key, current state
   │  SCD1 / SCD2 MERGE, point-in-time join
   ▼
gold     dim_* , fact_referral (star)   obt_referral_wide (wide)

ops      batch_run_log · dq_rule · dq_result · quarantine
```

## Notebooks (run in this order)

| Notebook | Layer | What it does |
|---|---|---|
| `00_overview` | intro | Architecture, concepts, and lake vs warehouse vs lakehouse vs mesh |
| `01_setup` | setup | Schemas, volumes, control tables, DQ rules. Optional reset that keeps the raw files. |
| `00_common` | shared | Settings, `RUN_ID`, run-log helpers (`%run` by every notebook below) |
| `10_bronze` | bronze | Auto Loader per source, lineage columns |
| `40_remediation` | bronze | Batch 3 only: replays corrected quarantine rows into bronze |
| `20_silver_reference` | silver | Programmes; census reshaped into one row per planning area |
| `21_silver_clients` | silver | Clean, dedup, DQ, quarantine, ID hashing, AES phone/email, MERGE, release fixed rows |
| `22_silver_referrals` | silver | Same pattern, plus checks against silver clients and programmes |
| `30_gold_dimensions` | gold | `dim_date`, `dim_programme` / `dim_location` (SCD1), `dim_client` (SCD2) |
| `31_gold_facts` | gold | `fact_referral` (point-in-time join), `obt_referral_wide`, star vs wide comparison |
| `50_governance` | gold | Unity Catalog column masks on `dim_client` (run last, after every rebuild) |
| `60_showcase` | read-only | Two clients followed end to end: SCD2 history, and quarantine → correction |

## How to run

1. Run `python src/generate_data.py` locally. It writes `data/` and `data/PLANTED_DEFECTS.txt` (the expected results).
2. Run `01_setup`. The catalog name is set once in `CATALOG` (default `msf_demo`); change it there and in `00_common` if needed.
3. Upload **one batch at a time** into `/Volumes/<catalog>/landing/raw_files/<source>/`, then run the notebooks with `batch_label` set:
   - **batch1**: `programmes.csv`, `clients_batch1.csv`, `referrals_batch1.csv`, plus the census CSV in `population/`
   - **batch2**: `clients_batch2.csv`, `referrals_batch2.csv`
   - **batch3**: `corrections_batch3.csv`, then `10` → `40` → `21` → `22` → `30` → `31`
4. Run `50_governance` once at the end.
5. To rebuild from scratch: `01_setup` with reset = `yes` (drops bronze/silver/gold/ops and checkpoints, keeps landing).

Design details: [docs/design.md](docs/design.md). Decisions and lessons learned: [docs/decision_log.md](docs/decision_log.md).
