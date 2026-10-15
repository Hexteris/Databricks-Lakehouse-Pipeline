# Demo script (30 minutes)

Each segment has: **Goal** (what the interviewer should take away), **Do** (exactly what to click or run), **You'll see** (the real output),
**Say** (narration you can speak almost word for word), **Under the hood** (what is actually happening, so you can answer follow-ups),
**If they ask** (likely interruptions), and **Bridge** (the sentence that takes you to the next segment).

---

## Before the interview (13:00–13:45)

| Check | Detail |
|---|---|
| Tabs open, in this order | `00_overview`, `01_setup`, `10_bronze`, `21_silver_clients`, `22_silver_referrals`, `30_gold_dimensions`, `31_gold_facts`, `40_remediation`, `50_governance`, the `msf_pipeline` Job run page, `docs/walkthrough.html` (backup) |
| Compute warm | run section 12's first query in `00_overview` once |
| `50_governance` | switch set to **no** and the functions cell re-run, so masks are on |
| Browser | zoom 125%, notifications off, other tabs closed |
| Rule for the whole demo | **only run SELECT cells live.** Never re-run a MERGE, replay or bronze cell; point at the output already saved under it |

---

## 0:00–3:00 · The problem and the shape of the solution

**Goal:** they understand what you built and why, before any code.

**Do:** open `00_overview`, scroll to section 1 (Architecture).

**You'll see:** the layer diagram and the layer table.

**Say:**
> "MSF works with data about vulnerable people that changes over time and arrives imperfect. So I framed the demo around four questions: can I trust the data, is personal data protected, is history correct, and can mistakes be fixed safely?
>
> I built one pipeline on Databricks Free Edition. Raw files land unchanged in a governed volume. Bronze copies them into tables exactly as received. Silver cleans, checks and protects them. Gold shapes them into a star schema for questions. Alongside, an ops schema records everything the pipeline did.
>
> The data arrives in three batches, like a real source system: an initial load, a change file, and a corrections file. Each batch has problems planted on purpose, so every rule in the pipeline has something to prove. The client data is synthetic, because I needed realistic personal data and must never use real data. One source is real: the Census 2020 population by planning area."

**Under the hood:** the domain is referrals. A referral is one event, "client X referred to programme Y on date Z", which becomes the fact table; clients, programmes, dates and planning areas are dimensions.

**If they ask "why not real data?":** "I needed NRICs, phones and emails to protect, and defects I control. Real personal data isn't acceptable for a demo. The census gave me the real-world messiness."

**Bridge:** "Before the data, let me show how the pipeline is controlled."

---

## 3:00–6:00 · Control tables

**Goal:** the pipeline is governed by data, and everything it does is recorded.

**Do:**
1. Open `01_setup`, scroll to **Data quality rules** (the Python list of 8 rules).
2. Run in a new cell: `SELECT rule_id, rule_expr, severity FROM msf_demo.ops.dq_rule ORDER BY rule_id`
3. Run: `SELECT batch_label, step, table_name, status, rows_out, note FROM msf_demo.ops.batch_run_log ORDER BY started_at LIMIT 15`

**You'll see:** 8 rules, 7 REJECT and 1 WARN; run log rows per step with notes like `inserted=10 updated=35 unchanged=162`.

**Say:**
> "There are four control tables. The rules table holds data quality checks as SQL conditions: true means the row passes. Adding a check is one line here, and the silver code doesn't change. A NULL result counts as a failure, so a missing value can't slip through.
>
> REJECT sends a row to quarantine. WARN keeps the row but counts the failure, for things worth watching but not worth blocking.
>
> Every step writes to the run log: a row goes in as STARTED, then becomes SUCCESS or FAILED with row counts. If something crashes mid-way, the row stays STARTED, so a failure is never silent."

**Under the hood:** setup MERGEs the rules, so re-running it updates rather than duplicates. Each notebook gets a new `RUN_ID`, and every row it writes carries it.

**If they ask "why rules as data?":** "Rules change more often than code. As data they're versioned, auditable and reviewable by non-engineers. The pipeline code just applies whatever is active."

**Bridge:** "Now the data itself, starting where it arrives."

---

## 6:00–9:00 · Bronze

**Goal:** ingestion is safe to re-run and fully traceable.

**Do:**
1. Open `10_bronze`, scroll to `load_to_bronze()`.
2. Point at `inferColumnTypes = false`, the three `withColumn` lines, `checkpointLocation`, `availableNow`.
3. Scroll to the last cell (rows per source file) and show its saved output.

**You'll see:** each bronze table with its source file and row count, e.g. `clients_batch1.csv` 200, `clients_batch2.csv` 47, and two `remediation:` rows.

**Say:**
> "Bronze uses Auto Loader. Every column is read as text, because bronze shouldn't guess. A phone number like 00002124 would lose its leading zeros as an integer.
>
> Every row gets three lineage columns: which file, when it was loaded, and which run loaded it. With the run id I can remove one bad load without touching the others.
>
> The checkpoint is Auto Loader's memory of which files it has processed. `availableNow` processes whatever is new, then stops. When the Job re-ran the whole pipeline with no new files, bronze loaded zero rows. That's idempotency without writing any file-tracking code."

**Under the hood:** one stream per source, each with its own checkpoint and schema location. Empty folders are skipped because Auto Loader can't infer a schema from nothing. `mergeSchema` is set on the writer because session-wide auto-merge is blocked on Free Edition.

**If they ask "what about a new column?":** "I tested it: Auto Loader stops once, records the new schema, and succeeds on restart. That's why bronze has a retry in the Job."

**If they ask "why not COPY INTO?":** "It's also idempotent and fine for simple SQL loads. Auto Loader scales better to many files and handles schema evolution."

**Bridge:** "Bronze accepts almost anything. The quality gate is silver."

---

## 9:00–14:00 · Silver: data quality and PII

**Goal:** bad data is caught with a reason, nothing is silently lost, and personal data is protected on the way in.

**Do:**
1. Open `21_silver_clients`. Scroll through steps 1 to 3 slowly, pointing at: the clean query, `record_json`, the dedup `WHERE rn = 1 OR client_id IS NULL`, and `failures()`.
2. Show the saved output of the **quarantine** check cell.
3. Show the saved output of the **PII** cell, then the **decrypt** cell.

**You'll see:** quarantine with the empty id and C0025/C0060 with `[CLI_002]` (now RESOLVED after batch 3); `FAKE-***1234`, unreadable hash and ciphertext; three decrypted `0000xxxx` phone numbers.

**Say:**
> "Silver does five things in order.
>
> First, it cleans what formatting can fix: trim spaces, fix letter case. `  TWO-PARENT ` becomes `Two-parent` and passes. A five-digit postal code can't be fixed by formatting, so the rules will catch it. And I keep the original row as JSON, so if it's rejected I still have exactly what the source sent.
>
> Second, it deduplicates: one row per client, the newest. MERGE fails if two source rows share a key. Rows with no id are all kept, otherwise they'd collapse into one and the rest would vanish.
>
> Third, it applies the rules from the control table. Each rule becomes a CASE expression that returns its id if the row fails, so every row carries a list of the rules it broke.
>
> Fourth, it records the results: totals per rule into dq_result, rejected rows into quarantine.
>
> Fifth, it protects personal data before it lands. The NRIC is hashed with a secret salt: one-way, but the same every time, so it still works as a join key. Phone and email are encrypted with AES, because a case officer may need them back. Here's the decrypt with the key from the secret scope. The keys are never in code or Git.
>
> Finally, the MERGE only writes when something really changed. Encryption gives a different result every time, so I compare a fingerprint of the plain values instead."

**Under the hood:** `row_hash` = SHA-256 of salt + every value. The quarantine MERGE matches on `record_json`, so the same bad row is never quarantined twice. Step 6 marks quarantine rows RESOLVED once their key has loaded into silver after being quarantined.

**If they ask "why salt the hash?":** "NRICs have a known format and limited combinations, so an unsalted hash can be reversed by hashing them all. The salt is a secret."

**If they ask "why not join on the encrypted column?":** "AES-GCM uses a random IV, so the same phone encrypts differently every time. That's why the join key is the hash."

**Bridge:** "Clients are checked on their own. Referrals are checked against clients."

---

## 14:00–16:00 · The cascade

**Goal:** consistency across tables.

**Do:** open `22_silver_referrals`, show the saved output of the **quarantine reasons** cell (with `client_failed_rules`), then the **reconciliation** cell.

**You'll see:** R00087 and R00273 with client C0025 and `[CLI_002]`; reconciliation 697 + 3 = 700.

**Say:**
> "A referral only passes if its client is a valid silver client, not just present in bronze. So when C0025 was rejected for its postal code, its referrals were quarantined too. That's the cascade.
>
> It matters later: when the client is fixed, those referrals are released automatically, without any correction of their own.
>
> And I reconcile: every distinct referral in bronze is either in silver or still in quarantine. Nothing disappears."

**Under the hood:** two LEFT JOINs add `client_exists` and `programme_exists`; REF_003 and REF_004 just test those columns.

**If they ask "isn't rejecting referrals harsh?":** "The alternative is an inferred member, a placeholder client filled in later. I chose quarantine so nothing reaches gold attached to a client we don't trust. Both are valid; the choice depends on whether the business prefers completeness or certainty."

**Bridge:** "Now the gold layer, where history matters."

---

## 16:00–21:00 · Gold: SCD2, point-in-time, star vs wide

**Goal:** history is correct, and you understand the modelling trade-offs.

**Do:**
1. Open `30_gold_dimensions`, read the SCD2 markdown table (Type 2 vs Type 1 columns), then point at the staged-union MERGE.
2. Show the saved output of the **multi-version clients** cell.
3. Open `31_gold_facts`, run the **C0130** cell (SELECT, cheap).
4. Show the **"as it was vs now"** output, then the two **star vs wide** outputs side by side.

**You'll see:** C0130 with two versions; five referrals, three on Toa Payoh (version 127) and two on Jurong West; identical top-10 lists from star and wide.

**Say:**
> "Programmes and locations are Type 1: if a name changes, the old one isn't needed. Clients are Type 2 for their circumstances: planning area, household type, postal code. Contact details are Type 1 in the same table, because an old phone number has no business value.
>
> I use two fingerprints. If the Type 2 fingerprint changes, I close the current version and add a new one. If only the full fingerprint changes, I overwrite in place.
>
> One MERGE can't both close a row and insert its replacement, so changed clients appear twice in the source: once to close the old row and once with a NULL key, which never matches, so it's inserted. It's one transaction, so a client never has zero or two current rows.
>
> Here's C0130. They lived in Toa Payoh and moved to Jurong West on 1 July. The fact table joins each referral to the version valid on the referral date. Their three earlier referrals still count for Toa Payoh; the two later ones count for Jurong West. Without SCD2, all five would be counted in Jurong West, and this query shows which areas would be miscounted.
>
> The grain is one row per referral. Here's the same question, referrals per 100,000 residents, asked of the star schema with a join and of the wide table with none. Same answer. The wide table is simpler for dashboards and ML but repeats values and must be rebuilt when dimensions change. It also has no personal data at all. One honest caveat: the generator places clients randomly, so the ranking reflects my synthetic data, not real need."

**Under the hood:** validity is `[effective_from, effective_to)`; first versions start 1900-01-01 so older referrals find a match; missing lookups become `-1` and checks expect zero. Location on the fact comes from the client version, not the current address.

**If they ask "snowflake?":** "A snowflake would split region out of location into its own table: less repetition, one more join. At 55 rows I kept it as one table."

**If they ask "is time travel a replacement for SCD2?":** "No. Time travel is table versions, limited by VACUUM retention. SCD2 is permanent business history you can query."

**Bridge:** "The dimension holds personal data, so who can see it?"

---

## 21:00–24:00 · Masking

**Goal:** access is controlled per user, at query time, in one place.

**Do:**
1. Open `50_governance`, run the **C0130** query (switch = no). Masked.
2. Set the switch to **yes**, re-run the **mask functions** cell, then the C0130 query. Unmasked.
3. Set it back to **no** and re-run the functions cell.

**You'll see:** `T***`, `[hidden]`, `31****`/`64****`, then real values.

**Say:**
> "Hashing and encryption changed what's stored. Masks change what each person sees. The rule is a function attached to the column in Unity Catalog, so every query, notebook or dashboard goes through it, and no view can bypass it.
>
> Same query, nothing changed in the SELECT, different result depending on who asks. In production, case officers in `pii_readers` see real values, analysts see these. Planning area and household type stay visible, so analysis still works.
>
> The wide table needs no mask because I never copied personal data into it."

**Under the hood:** the column references the function by name, so redefining the function changes every query immediately. The switch only adds your user to the condition; the real rule is group membership.

**If they ask about row filters:** "Same pattern: a function returning true or false per row, e.g. officers only see clients of their own office."

**Bridge:** "Finally, what happens when the source sends fixes."

---

## 24:00–28:00 · Remediation

**Goal:** mistakes are fixed without manual edits, with a full audit trail.

**Do:**
1. Open `40_remediation`, read the step table at the top.
2. Show the saved **status summary** and **outcome per row** outputs.
3. Run the **audit trail** cell (SELECT).
4. Show the **DQ trend** output.

**You'll see:** 12 RESOLVED, 4 OPEN; outcomes 8 corrected, 4 released with client, 4 needs source fix; two bronze rows each for C0025 and C0060; CLI_002 2 → 2 → 0, REF_002 3 → 4 → 0, REF_003 4 → 8 → 2.

**Say:**
> "In batch three the source sent eight corrections: a table, a key, a column and the right value.
>
> I don't fix silver directly. Remediation rebuilds the original rejected row from quarantine with the corrected value and appends it to bronze, tagged as remediation. Then silver runs as normal, so the fix goes through the same rules as any other data. If a correction were wrong, it would be quarantined again.
>
> Status goes from OPEN to REPLAYED, and only becomes RESOLVED when the row actually lands in silver.
>
> Result: 12 resolved. Eight were corrected directly, and four referrals were released because their client was fixed. Four stay open: they have no business key, so a correction can't target them. They need a fix in the source system, and they stay visible rather than being dropped.
>
> Bronze is still append-only. Here's C0025: the bad row and its fix side by side. And the DQ trend per batch shows the failures falling to zero."

**Under the hood:** `from_json` with the bronze schema turns `record_json` back into columns; a map of `column → corrected value` per key handles several corrections on one row; re-running replays nothing because nothing is OPEN.

**If they ask "how would you roll back a bad batch?":** "If nothing good has loaded since, `RESTORE TABLE ... TO VERSION AS OF`. Otherwise delete by `_run_id`, which removes only that load. RESTORE is all-or-nothing from that version."

**Bridge:** "All of this runs as one Job."

---

## 28:00–30:00 · Orchestration and wrap-up

**Goal:** it's a pipeline, not a set of notebooks, and you know what production needs.

**Do:** open the Job run page, show the graph (all green, 6m 22s), point at **Repair run**.

**Say:**
> "One Job runs every layer. Reference data and remediation run in parallel; referrals wait for both clients and reference. `batch_label` is a job parameter passed to every notebook. Bronze retries once, for the schema-evolution restart. Setup isn't in the Job, because it can reset the platform.
>
> Running it with no new files proved the whole pipeline is idempotent, and it also caught up a step I had skipped by hand, which my -1 keys had made visible.
>
> For production I'd deploy with Asset Bundles and CI/CD across environments, trigger on file arrival, alert on failed runs and DQ spikes, use groups and service principals only, and switch to streaming silver and an incremental fact at scale.
>
> What I've built is a lakehouse: Delta gives transactions, MERGE and history on open files, and Unity Catalog gives one governance layer. And it's the kind of template a central platform team could give each domain if MSF moved towards a data mesh."

---

## Stories to use when relevant

| Story | Use it when they ask about |
|---|---|
| Excel CSV broke the smoke test; Delta refused the junk columns | schema enforcement, data contracts |
| `Changi- Total` dropped a planning area; reconciliation caught a gap of 1,810 | testing, real data, reconciliation |
| After the fix the gap was 40: census rounding to 10 | tolerances, data understanding |
| My own synthetic postal codes and programmes were inconsistent; fixed and rebuilt with the reset | reproducibility, attention to detail |
| Batches mislabelled by hand | why orchestration and parameters |
| A skipped step left 4 referrals with `-1` keys | observability, why LEFT JOIN + unknown keys |

## If something breaks live
Don't debug on screen. Say "this is what the run log is for", open the HTML export or `docs/walkthrough.html` for that step, and continue.

## If you're running short of time
Cut in this order: star vs wide comparison (just say it), the census story, the DQ trend. Never cut SCD2 (C0130), quarantine/cascade, or masking.
