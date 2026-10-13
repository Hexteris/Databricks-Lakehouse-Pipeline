# Naming Conventions (WBS 2.5)

| Thing | Convention | Example |
|---|---|---|
| Catalog | one per domain | `msf_demo` |
| Schemas | one per layer | `landing`, `bronze`, `silver`, `gold`, `ops` |
| Tables | layer prefix + snake_case | `brz_beneficiary`, `slv_assistance_txn`, `dim_scheme`, `fact_assistance`, `obt_assistance_wide`, `vw_beneficiary_masked` |
| Ops tables | no prefix | `ops.batch_run_log` |
| Columns | snake_case | `monthly_household_income` |
| Pipeline metadata columns | leading underscore | `_ingest_ts`, `_source_file`, `_batch_date`, `_run_id`, `_rescued_data` |
| Business key | `*_id` / `*_code` | `beneficiary_id`, `scheme_code` |
| Surrogate key | `*_sk` | `beneficiary_sk` |
| Protected columns | `*_hash`, `*_enc`, `*_masked` | `nric_hash`, `phone_enc`, `nric_masked` |
| Dates / timestamps | `*_date` DATE, `*_ts` / `*_at` TIMESTAMP (UTC) | `disbursement_date`, `_ingest_ts` |
| SCD2 columns | fixed names | `effective_from`, `effective_to` (`9999-12-31`), `is_current` |
| Landing path | source first, then batch | `/Volumes/msf_demo/landing/raw_files/beneficiary/batch_date=2026-10-01/beneficiary.csv` |
| Checkpoints | inside the landing volume | `/Volumes/msf_demo/landing/raw_files/_checkpoints/<source>/` |
| Secrets | scope `msf_demo` | keys `aes_key`, `hash_salt` |
| Notebooks | `NN_verb_noun` (10s = bronze, 20s = silver, 30s = gold) | `20_silver_transform` |
| DQ rules | `<ENTITY>_<NNN>` | `TXN_004` |
| Commits | imperative, one change per commit | `Add SCD2 merge for dim_beneficiary` |
