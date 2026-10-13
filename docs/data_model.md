# Gold Data Model (WBS 2.3)

## Fact grain
**One row in `fact_assistance` = one disbursement transaction (`txn_id`).**
`txn_id` is kept on the fact as a degenerate dimension.

## ERD (snowflake variant)

```mermaid
erDiagram
    fact_assistance }o--|| dim_date : date_sk
    fact_assistance }o--|| dim_beneficiary : beneficiary_sk
    fact_assistance }o--|| dim_scheme : scheme_sk
    fact_assistance }o--|| dim_office : office_sk
    fact_assistance }o--|| dim_location : location_sk
    dim_location }o--|| dim_region : region_sk

    fact_assistance {
        string txn_id "degenerate dim"
        int date_sk FK
        bigint beneficiary_sk FK
        bigint scheme_sk FK
        bigint office_sk FK
        bigint location_sk FK
        decimal amount
        string status
        string case_officer_id
        string _run_id
    }
    dim_beneficiary {
        bigint beneficiary_sk PK
        string beneficiary_id "business key"
        string nric_hash
        string planning_area
        decimal monthly_household_income
        string row_hash
        date effective_from
        date effective_to
        boolean is_current
        boolean is_inferred
    }
    dim_scheme {
        bigint scheme_sk PK
        string scheme_code
        string scheme_name
        decimal max_monthly_amount
    }
    dim_office {
        bigint office_sk PK
        string office_code
        string office_name
    }
    dim_location {
        bigint location_sk PK
        string planning_area
        bigint region_sk FK
    }
    dim_region {
        bigint region_sk PK
        string region_name
    }
    dim_date {
        int date_sk PK
        date date
        int year
        int month
    }
```

**Star variant:** replace `dim_location` + `dim_region` with `dim_location_flat` (planning_area, region_name), using the same `location_sk`. The fact table doesn't change.

**Wide variant:** `obt_assistance_wide` = fact joined to every dimension and flattened into one table. No joins are needed at query time.

## Key rules

| Rule | Detail |
|---|---|
| Surrogate keys | `*_sk BIGINT GENERATED ALWAYS AS IDENTITY`. Facts reference surrogate keys, never business keys. |
| SCD2 tracked columns | address_enc, planning_area, household_size, monthly_household_income, employment_status. Change is detected with `row_hash = sha2(concat_ws('\|', …), 256)`. |
| SCD2 untracked (Type 1) | full_name, phone_enc, preferred_language: overwritten on the current row. |
| SCD2 validity | `effective_from` = source `updated_at` date (first version: `1900-01-01`), `effective_to` = next version's `effective_from`, or `9999-12-31` for the current row. Half-open interval: `[from, to)`. |
| Point-in-time join | `fact.beneficiary_sk` = the version where `disbursement_date >= effective_from AND disbursement_date < effective_to`. |
| Location on fact | Taken from **that** beneficiary version's planning_area, so a move changes the location of later txns only. |
| Inferred member | A txn for an unknown `beneficiary_id` creates a placeholder row (`is_inferred = true`, attributes `UNKNOWN`). When the real record arrives, it is updated in place (no new version) and `is_inferred` becomes false. |
| Unknown member | `-1` rows in each dimension for unresolvable lookups, so fact FKs are never NULL. |

## When to use which

| Model | Strength | Cost | Used here for |
|---|---|---|---|
| Star | Simple joins, BI-friendly | Some redundancy (region repeated per area) | Default reporting |
| Snowflake | Normalised, one place to change region | More joins | Showing the trade-off |
| Wide (OBT) | No joins, fast scans, ML-ready | Big table, has to be rebuilt when dimensions change | Dashboards, ML features |
