# Data Dictionary (WBS 2.1–2.2)

All personal data is **synthetic**. Scheme names are real MSF schemes, but every amount and rule here is invented for illustration.

## 1. Sources

| Source | File | Format | Origin | Business key |
|---|---|---|---|---|
| beneficiary | `beneficiary.csv` | CSV | Generated (`02_generate_data`) | `beneficiary_id` |
| assistance_txn | `assistance_txn.json` | JSON Lines (one object per line) | Generated | `txn_id` |
| scheme | `scheme.csv` | CSV | Generated (illustrative) | `scheme_code` |
| office | `social_service_offices.geojson` | GeoJSON | **Real**: data.gov.sg *Social Service Offices* | derived `office_code` |
| planning_area | `planning_area.csv` | CSV | Generated from URA Master Plan 2019 names (55 areas, 5 regions) | `planning_area` |

Landing path: `/Volumes/msf_demo/landing/raw_files/<source>/batch_date=YYYY-MM-DD/<file>`

## 2. Batch design (locked)

| Batch | batch_date | Contents |
|---|---|---|
| Day 1 | 2026-10-01 | 2,000 beneficiaries, ~10,000 txns (disbursed 2025-10-01 → 2026-09-30), 5 schemes, reference data. Clean. |
| Day 2 | 2026-10-02 | 150 changed beneficiaries (address / income / household / employment), 100 new, ~2,000 txns, 1 scheme `max_monthly_amount` change (SCD1). Injected faults are listed in the next table. |
| Day 3 | 2026-10-03 | New column `preferred_language` (schema drift), 20 late beneficiaries arrive (resolve inferred members), a corrections file for Day 2 quarantined txns, and **one bad file** (`assistance_txn_bad.json`, amounts sent in cents instead of dollars). |

Day 2 injected faults (exact counts, so the demo is predictable):

| Fault | Count | Caught by |
|---|---|---|
| Null NRIC | 10 | BEN_001 (REJECT) |
| Malformed NRIC | 5 | BEN_002 (REJECT) |
| Negative amount | 20 | TXN_001 (REJECT), fixed by the Day 3 corrections file |
| Unparseable date | 15 | TXN_002 (REJECT), fixed by the Day 3 corrections file |
| Unknown scheme code | 15 | TXN_003 (REJECT) |
| Duplicate txn_id | 25 | Silver dedup (row_number) |
| Txn for a not-yet-arrived beneficiary | 20 | **Not a DQ failure**: inferred member in `dim_beneficiary` |
| Dirty formats (`male`, `+65 9123 4567`, 5-digit postal) | ~60 | Fixed by standardisation, not rejected |

## 3. PII classification

| Class | Meaning | Columns | Treatment |
|---|---|---|---|
| P1 | Direct identifier | nric, full_name, phone, address | NRIC: salted SHA-256 hash + `S****567A` display form, raw value dropped after silver. Phone / address: AES-GCM encrypted. Name: UC column mask. |
| P2 | Quasi-identifier | dob, gender, postal_code | dob: UC column mask (year only) |
| P3 | Sensitive attribute | monthly_household_income, employment_status, household_size | income: UC column mask (banded) |
| - | Not personal | everything else | none |

Bronze holds raw PII, so it is readable only by the pipeline. Analysts start at gold.

## 4. Bronze (raw, all STRING)

Every bronze table has the source columns as STRING, plus:

| Column | Type | Meaning |
|---|---|---|
| `_ingest_ts` | TIMESTAMP | When Auto Loader wrote the row |
| `_source_file` | STRING | `_metadata.file_path` |
| `_batch_date` | DATE | Parsed from the `batch_date=` folder |
| `_run_id` | STRING | Pipeline run that loaded it |
| `_rescued_data` | STRING | Data that didn't fit the schema (Auto Loader) |

## 5. Silver

### `silver.slv_beneficiary` (current state, MERGE on `beneficiary_id`)
| Column | Type | PII | Rule / transform |
|---|---|---|---|
| beneficiary_id | STRING | - | Business key, `B` + 6 digits |
| nric_hash | STRING | P1 | `sha2(concat(upper(trim(nric)), secret('msf_demo','hash_salt')), 256)` |
| nric_masked | STRING | P1 | first char + `****` + last 4 chars |
| full_name | STRING | P1 | trim, title case |
| dob | DATE | P2 | `to_date`. Must be valid and not in the future (BEN_003) |
| gender | STRING | P2 | `M`/`F` (maps `male`, `f`, …) |
| phone_enc | STRING | P1 | digits only, last 8 → `base64(aes_encrypt(…, secret('msf_demo','aes_key')))` |
| address_enc | STRING | P1 | trim → AES-GCM encrypted, base64 |
| postal_code | STRING | P2 | left-pad to 6 digits (BEN_004 WARN) |
| planning_area | STRING | - | upper, trim. Must exist in `slv_location` |
| household_size | INT | P3 | 1–15 (BEN_005) |
| monthly_household_income | DECIMAL(10,2) | P3 | ≥ 0 (BEN_006) |
| employment_status | STRING | P3 | EMPLOYED / PART_TIME / UNEMPLOYED / RETIRED / STUDENT |
| preferred_language | STRING | - | Added Day 3 (schema evolution), NULL before |
| updated_at | TIMESTAMP | - | Source change time, drives dedup + SCD2 |
| _ingest_ts, _run_id | | | Carried from bronze for lineage |

### `silver.slv_assistance_txn` (MERGE on `txn_id`)
| Column | Type | Rule |
|---|---|---|
| txn_id | STRING | Business key, deduped |
| beneficiary_id | STRING | May not exist yet, which triggers an inferred member |
| scheme_code | STRING | Must exist in `slv_scheme` (TXN_003) |
| office_code | STRING | Must exist in `slv_office` |
| disbursement_date | DATE | Valid, not in the future (TXN_002) |
| amount | DECIMAL(10,2) | > 0 (TXN_001), ≤ scheme max (TXN_004 WARN) |
| status | STRING | DISBURSED / PENDING / REVERSED (TXN_005) |
| case_officer_id | STRING | `CO` + 4 digits |
| _ingest_ts, _run_id | | Lineage |

### Reference
| Table | Columns |
|---|---|
| `slv_scheme` | scheme_code, scheme_name, agency, category, max_monthly_amount DECIMAL(10,2), updated_at |
| `slv_office` | office_code, office_name, address, postal_code, latitude, longitude |
| `slv_location` | planning_area, region |

## 6. Gold

The model is in [data_model.md](data_model.md).

| Table | Type | Key columns |
|---|---|---|
| `dim_beneficiary` | SCD2 | beneficiary_sk (identity), beneficiary_id, nric_hash, nric_masked, full_name, gender, dob, address_enc, phone_enc, planning_area, household_size, monthly_household_income, employment_status, preferred_language, row_hash, effective_from, effective_to, is_current, is_inferred |
| `dim_scheme` | SCD1 | scheme_sk, scheme_code, scheme_name, agency, category, max_monthly_amount, updated_ts |
| `dim_office` | SCD1 | office_sk, office_code, office_name, address, postal_code |
| `dim_region` | Snowflake | region_sk, region_name |
| `dim_location` | Snowflake | location_sk, planning_area, region_sk |
| `dim_location_flat` | Star | location_sk, planning_area, region_name |
| `dim_date` | Static | date_sk (yyyymmdd INT), date, year, quarter, month, month_name, day_of_week, is_weekend (2020–2030) |
| `fact_assistance` | Fact | txn_id, date_sk, beneficiary_sk, scheme_sk, office_sk, location_sk, amount, status, case_officer_id, _run_id |
| `obt_assistance_wide` | Wide | fact + all dimension attributes flattened |
