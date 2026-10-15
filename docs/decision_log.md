# Decision Log

## D1 – Smoke-test every platform feature before designing (Mon 12 Oct)
| Feature | Result | Decision |
|---|---|---|
| Catalog / schema / volume | PASS | catalog `msf_demo`, one schema per layer |
| File into volume (code + UI) | PASS | public files uploaded manually (outbound internet restricted) |
| Auto Loader `availableNow` | PASS | checkpoint gives idempotent bronze |
| Schema evolution | PASS only with `mergeSchema` on the writer | session auto-merge is blocked; opt in per table. Stream fails once on a new column, so a Job needs a retry. |
| `aes_encrypt` / `sha2` | PASS | AES for reversible PII, salted hash for the join key |
| UC column mask | PASS | role-aware masks; masked view only as fallback |
| Secret scope | PASS | key and salt generated into scope `msf_demo`, never displayed |
| Job with parameters | PASS | a failed run writes nothing |

## D2 – Cut scope to what can be explained line by line
Four synthetic sources plus one real one, explicit code per source instead of a metadata-driven framework,
four control tables. Understanding beats breadth in a 30-minute walkthrough.

## D3 – Keep programme names generic
Attaching invented volumes and approval rates to real MSF schemes would misrepresent them. In production, `dim_programme` would load the real scheme catalogue.

## D4 – No watermark
The Auto Loader checkpoint tracks files. Silver re-reads bronze each run and MERGEs with `row_hash` change detection: idempotent and cheap at this size. At scale, silver would stream from bronze.

## D5 – Rules are data; a NULL result is a failure
Rules live in `ops.dq_rule` as SQL. Adding a check is an INSERT. `coalesce(rule, false)` means missing values can't slip through.

## D6 – Referential checks against silver, not bronze
A referral only passes if its client is a valid silver client, so rejects cascade. Remediation then releases the referrals blocked by a fixed client.

## D7 – Remediation goes back through bronze
Corrections rebuild the original row with the fixed value and append it to bronze, tagged `remediation:`. Silver re-checks it with the same rules. Nothing is edited by hand, bronze stays append-only, and every table can still be rebuilt from bronze.

## D8 – Reset keeps landing
The raw files are the source of truth. Reset drops bronze, silver, gold and ops (including checkpoints) and replays from landing.

## D9 – SCD2 needs every batch to flow to gold
Silver holds the current state only, so gold must run after each batch or intermediate versions are lost.

## Lessons from the data
- **Excel-saved CSVs break ingestion** (binary content, BOM). Use plain UTF-8.
- **Real census labels are inconsistent:** `Changi- Total` vs `Ang Mo Kio - Total` silently dropped one planning area. Reconciliation caught it.
- **Census figures are rounded to 10:** reconcile within a tolerance (difference of 40 across 55 areas), not exactly.
- **Synthetic data needs sanity checks too:** postal codes didn't match planning areas, and 119 referrals didn't fit the household. Fixed in the generator without changing any other value, then rebuilt from scratch with the reset.

## D10 – One Job, setup excluded
`msf_pipeline` runs bronze → (reference ∥ remediation → clients) → referrals → dimensions → facts → governance, with `batch_label` as a job parameter and one retry on bronze. `01_setup` is excluded because it can reset the platform. The first Job run (no new files) proved end-to-end idempotency, that MERGE works on the masked `dim_client`, and caught up a `dim_client` step skipped during manual batch 3 runs.
