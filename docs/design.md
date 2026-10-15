# Design

## Sources

| Source | File(s) | Origin | Business key | Arrives |
|---|---|---|---|---|
| programmes | `programmes.csv` | synthetic | `programme_id` | batch 1 |
| clients | `clients_batch1.csv`, `clients_batch2.csv` | synthetic | `client_id` | batches 1, 2 |
| referrals | `referrals_batch1.csv`, `referrals_batch2.csv` | synthetic | `referral_id` | batches 1, 2 |
| corrections | `corrections_batch3.csv` (`target_table, business_key, column_name, corrected_value`) | synthetic | n/a | batch 3 |
| population | Census 2020 resident population by planning area (CSV) | **real**, data.gov.sg | `planning_area` | batch 1 |

Synthetic data is seeded and consistent: postal sector matches planning area, programmes fit the household type,
and no referral is dated before its client exists. `data/PLANTED_DEFECTS.txt` lists every planted defect and the expected counts.

## Batches and expected results

| | Batch 1 | Batch 2 | Batch 3 (corrections) |
|---|---|---|---|
| Clients | 200 in, **3 quarantined** | 30 moved (SCD2), 5 phone-only (SCD1), 2 re-sent unchanged, 10 new (1 WARN) | C0025, C0060 corrected |
| Referrals | 400 in, **8 quarantined** (4 planted + 4 cascaded from bad clients) | 300 new, 3 re-sent, 1 in-file duplicate, **5 quarantined** | 6 corrected |
| Quarantine after | 11 OPEN | 16 OPEN | **12 RESOLVED, 4 OPEN** (no usable key: needs a source fix) |

## Control tables (`ops`)

| Table | Answers | Written by |
|---|---|---|
| `batch_run_log` | What ran, status (STARTED → SUCCESS / FAILED), rows in/out/rejected, note | every step |
| `dq_rule` | Which checks apply. `rule_expr` is SQL, TRUE = pass, NULL = fail. REJECT or WARN. | `01_setup` |
| `dq_result` | Rows checked and failed per rule per run | silver |
| `quarantine` | Rejected rows as JSON, failed rules, status OPEN → REPLAYED → RESOLVED | silver, remediation |

Incrementality: Auto Loader checkpoints track files (landing → bronze). Silver re-reads bronze and MERGEs on the business key,
with `row_hash` so only real changes are written. No watermark is needed at this volume.

## DQ rules

| Rule | Table | Check | Severity |
|---|---|---|---|
| CLI_001 | clients | `client_id` is `C` + 4 digits | REJECT |
| CLI_002 | clients | postal code is 6 digits | REJECT |
| CLI_003 | clients | known household type after cleaning | WARN |
| REF_001 | referrals | `referral_id` is `R` + 5 digits | REJECT |
| REF_002 | referrals | date is `yyyy-MM-dd` and not in the future | REJECT |
| REF_003 | referrals | client exists **as a valid silver client** | REJECT |
| REF_004 | referrals | programme exists | REJECT |
| REF_005 | referrals | known status after cleaning | REJECT |

## PII

| Column | Silver / gold | Why |
|---|---|---|
| NRIC | `nric_hash` = SHA-256(salt + NRIC), `nric_masked` = `FAKE-***1234` | one-way, stable join key |
| phone, email | AES (GCM) encrypted, base64 | reversible for staff with the key |
| keys | secret scope `msf_demo` (`aes_key`, `hash_salt`), read with `secret()` | never in code or Git |

Raw PII exists only in bronze. `obt_referral_wide` holds no personal data at all.

## Gold model

Grain: **one row in `fact_referral` = one referral.**

```
            dim_date
               │ date_sk
dim_client ── fact_referral ── dim_programme
 (SCD2)        │ location_sk      (SCD1)
            dim_location (SCD1, census figures)
```

| Rule | Detail |
|---|---|
| Surrogate keys | `*_sk BIGINT GENERATED ALWAYS AS IDENTITY`. `dim_date` uses `yyyyMMdd`. |
| SCD2 columns (`dim_client`) | planning_area, household_type, postal_code. Detected with `scd_hash`. |
| SCD1 columns (`dim_client`) | name, NRIC hash/mask, phone, email. Overwritten when only `row_hash` changes. |
| Validity | `[effective_from, effective_to)`. First version from `1900-01-01`, current version to `9999-12-31`. |
| Point-in-time join | the referral links to the client version valid on `referral_date`. Location comes from that version. |
| Unknown keys | `-1` (checks expect zero) |
