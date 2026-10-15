# MSF Lakehouse Demo: social service referrals

A small, re-runnable ingestion pipeline on Databricks (Free Edition) that takes raw files through
**bronze → silver → gold**, with data quality rules, quarantine and remediation, PII protection, and SCD Type 2 history.

> All client and referral data is **synthetic** (`FAKE-` NRICs, `0000` phone numbers, `example.com` emails).
> Programme and agency names are illustrative and do not represent real MSF schemes or data.
> The only real dataset is the public Census of Population 2020 resident counts by planning area (data.gov.sg).

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
| `01_setup` | setup | Schemas, volumes, control tables, DQ rules. Optional reset that keeps the raw files. |
| `00_common` | shared | Settings, `RUN_ID`, run-log helpers (`%run` by every notebook below) |
| `10_bronze` | bronze | Auto Loader per source, lineage columns |
| `40_remediation` | bronze | Batch 3 only: replays corrected quarantine rows into bronze |
| `20_silver_reference` | silver | Programmes; census reshaped into one row per planning area |
| `21_silver_clients` | silver | Clean, dedup, DQ, quarantine, NRIC hash, AES phone/email, MERGE, release fixed rows |
| `22_silver_referrals` | silver | Same pattern + checks against silver clients and programmes |
| `30_gold_dimensions` | gold | `dim_date`, `dim_programme` / `dim_location` (SCD1), `dim_client` (SCD2) |
| `31_gold_facts` | gold | `fact_referral` (point-in-time join), `obt_referral_wide`, star vs wide comparison |

## How to run

1. Run `python src/generate_data.py` locally. It writes `data/` and `data/PLANTED_DEFECTS.txt` (the expected results).
2. Run `01_setup`.
3. Upload **one batch at a time** into `/Volumes/msf_demo/landing/raw_files/<source>/`, then run the notebooks with `batch_label` set:
   - **batch1**: `programmes.csv`, `clients_batch1.csv`, `referrals_batch1.csv`, plus the census CSV in `population/`
   - **batch2**: `clients_batch2.csv`, `referrals_batch2.csv`
   - **batch3**: `corrections_batch3.csv`, then `10` → `40` → `21` → `22` → `30` → `31`
4. To rebuild from scratch: `01_setup` with reset = `yes` (drops bronze/silver/gold/ops and checkpoints, keeps landing).

Design details: [docs/design.md](docs/design.md). Decisions and lessons learned: [docs/decision_log.md](docs/decision_log.md).
