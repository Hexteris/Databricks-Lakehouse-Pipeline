# Decision Log

## D1 – Smoke test results (Mon 12 Oct)
| Feature | Result | Decision |
|---|---|---|
| Catalog / schema / volume | PASS | Use catalog `msf_demo`, schema per layer |
| File into Volume (code + UI) | PASS | data.gov.sg files uploaded manually (outbound internet restricted) |
| Auto Loader availableNow | PASS | Checkpoint gives idempotent bronze |
| Schema evolution | PASS with `mergeSchema` on writer | Session autoMerge blocked; opt in per table. Stream fails once on new column, so Job needs a retry |
| aes_encrypt / decrypt | PASS | AES-GCM for phone/address (non-deterministic, not joinable) |
| Salted SHA-256 | PASS | Hashed NRIC as join key |
| UC column mask | PASS | Role-aware masks; masked view kept as fallback only |
| Secret scope | PASS | Key + salt in scope `msf_demo`, generated, never displayed |
| Job + parameters | PASS | `batch_date` job parameter; failed run writes nothing |

## D2 – Lesson: CSVs must be plain UTF-8
An Excel file saved as `.csv` produced binary junk columns. Use Notepad or Python, not Excel. Watch for the BOM in data.gov.sg downloads.

## D3 – Office dimension uses Social Service Offices, not FSCs
ComCare assistance is administered through MSF's Social Service Offices (SSOs), while FSCs provide casework and counselling. The office dimension is therefore built from the data.gov.sg *Social Service Offices* GeoJSON (real public data), and transactions carry `office_code`.

## D4 – Landing folders are source-first
`raw_files/<source>/batch_date=YYYY-MM-DD/`, so each Auto Loader stream watches exactly one source folder and gets its own checkpoint and schema. The batch date is recoverable from the path.

## D5 – PII: transform in storage, mask at presentation
Raw NRIC lives only in bronze (pipeline-only access). Silver keeps `nric_hash` (salted SHA-256, used as a join key) and `nric_masked` (for display). Phone and address are AES-GCM encrypted. Name, dob and income are protected with UC column masks in gold.

## D6 – Two incrementality mechanisms
Bronze uses Auto Loader checkpoints (file-level). Silver uses a watermark on `_ingest_ts` stored in `ops.ingestion_control` (row-level). Backfill means resetting one of the two.

## D7 – DQ rules are SQL expressions held as data
Rules live in `ops.dq_rule`, so a new rule is an INSERT, not a code change. NULL counts as a failure. REJECT sends the row to quarantine, WARN keeps it and records the failure. Duplicates are handled by dedup, not by a rule. Txns for unknown beneficiaries are **not** DQ failures: they are late-arriving dimensions, handled with inferred members.

## D8 – Fact grain and point-in-time join
One fact row per disbursement. The fact links to the SCD2 version valid on `disbursement_date`, using the half-open interval `[effective_from, effective_to)`. The first version starts at `1900-01-01` so that historical txns always find a match.

## D9 – `ingestion_control.reader_options` added (change after design lock)
CSV needs `header=true`, and the office GeoJSON needs `multiLine=true`. Holding reader options per source keeps the bronze loop fully metadata-driven, with no `if source == ...` branches in code.

## D10 – Config is code, seeded with MERGE
Sources and DQ rules are defined in `01_setup` (versioned in Git) and MERGEd into `ops` tables. Re-running setup never resets watermarks. Removed rules are deactivated, not deleted, so historical `dq_result` rows stay interpretable.
