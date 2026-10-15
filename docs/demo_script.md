# Demo script (30 min)

**Before you start:** notebooks open in tabs in this order: `00_overview`, `01_setup`, `10_bronze`, `21`, `22`, `30`, `31`, `50`, `40`, plus the Job run page.
`50_governance` switch set to **no**. Compute warmed up with the status cell in `00_overview`.
**Only run read-only check cells live** (SELECTs). Never re-run a MERGE or replay cell during the demo; point at its saved output instead.

| Min | Segment | Open | Show | Say |
|---|---|---|---|---|
| 0–3 | The problem | `00_overview` | intro + architecture diagram | "How do you ingest data about vulnerable clients so it can be trusted, PII is protected, history stays correct and mistakes can be fixed? One re-runnable pipeline, three batches with changes, bad rows and corrections." |
| 3–5 | Concept map | `00_overview` | "Where each concept lives", then run the status cell | "This table is my map. Stop me anywhere and I'll jump to the notebook." |
| 5–7 | Control tables | `01_setup` | the DQ rules list; then `SELECT * FROM msf_demo.ops.dq_rule` | "Rules are data. Adding a check is one line, the silver code doesn't change. A NULL result counts as a failure." |
| 7–10 | Bronze | `10_bronze` | `load_to_bronze()`; the per-file count cell | "Everything as text, three lineage columns, a checkpoint so each file loads exactly once. The Job re-run loaded 0 rows." Show `batch_run_log` rows with `job_rerun`. |
| 10–14 | Silver: DQ + PII | `21_silver_clients` | step 1 (clean, `record_json`); `failures()`; quarantine cell; PII cell; decrypt cell | "Clean first, so rules catch only real problems. Rejected rows keep the original values. NRIC is hashed for matching, phone and email are encrypted for staff who need them back. `row_hash` means only real changes are written." |
| 14–16 | Cascade | `22_silver_referrals` | quarantine reasons cell (`client_failed_rules`) | "A referral only passes if its client is valid, so fixing a client releases its referrals." |
| 16–21 | Gold: SCD + star/wide | `30`, `31` | SCD2 markdown + multi-version cell; **C0130** cell; "as it was vs now"; star vs wide cells | "C0130 moved from Toa Payoh to Jurong West. Old referrals still count for Toa Payoh. Without SCD2, these areas would be miscounted. Same question, star with a join, wide without." Mention the random-assignment caveat. |
| 21–24 | Masking | `50_governance` | C0130 query with switch **no** → set **yes**, re-run the functions cell + query | "Same query, different result depending on who asks. The rule is on the column, so no view can bypass it. The wide table has no PII at all." Switch back to **no**. |
| 24–28 | Remediation | `40_remediation` | design table at the top; status summary; outcomes; audit trail; DQ trend by batch | "Corrections go back through bronze and get the same rules. 12 resolved, 4 need a source fix because they have no key. The bad row and its fix both stay in bronze." Mention RESTORE vs targeted fix. |
| 28–30 | Orchestration + wrap-up | Job run page | graph; Repair run button | "One Job, parallel where possible, retry on bronze, `batch_label` as a parameter. In production: Asset Bundles, CI/CD, alerts on FAILED runs and DQ spikes." |

## Stories to drop in when relevant (things that actually happened)
- **Excel CSV** broke the smoke test → plain UTF-8 only.
- **`Changi- Total`**: one census label had no space, so a planning area was silently dropped. Reconciliation against the national total caught it.
- **Census rounding:** after the fix the totals still differed by 40, which is rounding to 10 across 55 areas, so I reconcile within a tolerance.
- **My own synthetic data** had postal codes unrelated to planning areas and 119 referrals that didn't fit the household. Fixed the generator without changing any other value, then rebuilt from scratch with the reset.
- **Mislabelled batches** when running by hand → that's why the Job passes `batch_label` as a parameter.
- **A skipped step:** `dim_client` wasn't updated for batch 3, so 4 referrals got `-1` client keys. The `-1` keys made it visible, and the Job caught it up.

## If something breaks live
Don't debug on screen. Say "this is exactly what the run log is for", open the HTML export or screenshot of that notebook, and carry on.
