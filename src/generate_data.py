"""
Synthetic data generator for the referral pipeline demo.
Python standard library only. Seeded, so every run produces identical files.
Files are UTF-8 without BOM, with \n line endings (safe for Spark).
Do NOT open the CSVs in Excel and re-save them.

SCHEMA
  programmes : programme_id, programme_name, agency_name
  clients    : client_id, full_name, nric_fake, phone, email,
               planning_area, household_type, postal_code, updated_date
  referrals  : referral_id, client_id, programme_id, referral_date, status
  corrections: target_table, business_key, column_name, corrected_value

BATCHES
  1: 200 clients, 400 referrals, planted defects
  2: 30 clients changed planning_area (SCD2 trigger), 5 clients changed phone
     only (SCD1 trigger), 2 clients re-sent UNCHANGED (no new version),
     10 new clients (1 with an uncleanable household_type -> WARN),
     300 new referrals, 3 re-sent duplicates from batch 1,
     1 duplicate inside the same file, 2 orphan client_ids, 1 bad date
  3: corrections file for quarantined rows that can be fixed by business key

Run:  python generate_data.py
Then upload each file into the matching folder of the Unity Catalog Volume,
ONE BATCH AT A TIME (batch 1 -> run pipeline -> batch 2 -> run -> corrections).
"""
import csv
import os
import random
from datetime import date, timedelta

rng = random.Random(42)
OUT = "data"

AREAS = [
    "Ang Mo Kio", "Bedok", "Bishan", "Bukit Batok", "Bukit Merah",
    "Bukit Panjang", "Choa Chu Kang", "Clementi", "Geylang", "Hougang",
    "Jurong East", "Jurong West", "Kallang", "Pasir Ris", "Punggol",
    "Queenstown", "Sembawang", "Sengkang", "Serangoon", "Tampines",
    "Toa Payoh", "Woodlands", "Yishun",
]
HOUSEHOLDS = [
    "Single-parent", "Two-parent", "Elderly-living-alone",
    "Multigenerational", "Couple-no-children",
]
STATUSES = ["Submitted", "Approved", "Rejected", "Closed"]

# First two digits of a Singapore postal code identify the postal sector.
# One typical HDB sector per planning area (illustrative), so a client's postal code matches where they live.
POSTAL_SECTOR = {
    "Ang Mo Kio": "56", "Bedok": "46", "Bishan": "57", "Bukit Batok": "65", "Bukit Merah": "15",
    "Bukit Panjang": "67", "Choa Chu Kang": "68", "Clementi": "12", "Geylang": "38", "Hougang": "53",
    "Jurong East": "60", "Jurong West": "64", "Kallang": "33", "Pasir Ris": "51", "Punggol": "82",
    "Queenstown": "14", "Sembawang": "75", "Sengkang": "54", "Serangoon": "55", "Tampines": "52",
    "Toa Payoh": "31", "Woodlands": "73", "Yishun": "76",
}
PROGRAMMES = [
    ("P01", "Emergency Financial Aid", "Social Support Unit"),
    ("P02", "Food Assistance", "Social Support Unit"),
    ("P03", "Rental Housing Support", "Social Support Unit"),
    ("P04", "Parenting Workshops", "Family Support Unit"),
    ("P05", "Family Counselling", "Family Support Unit"),
    ("P06", "Childcare Subsidy Help", "Family Support Unit"),
    ("P07", "Youth Mentoring", "Youth Services Unit"),
    ("P08", "After-School Care", "Youth Services Unit"),
    ("P09", "Eldercare Befriending", "Community Care Unit"),
    ("P10", "Caregiver Respite", "Community Care Unit"),
    ("P11", "Disability Support Referral", "Community Care Unit"),
    ("P12", "Job Placement Help", "Social Support Unit"),
]

# Which households each programme makes sense for. Programmes not listed suit everyone.
WITH_CHILDREN = {"Single-parent", "Two-parent", "Multigenerational"}
PROGRAMME_FIT = {
    "P04": WITH_CHILDREN,                                   # Parenting Workshops
    "P06": WITH_CHILDREN,                                   # Childcare Subsidy Help
    "P07": WITH_CHILDREN,                                   # Youth Mentoring
    "P08": WITH_CHILDREN,                                   # After-School Care
    "P09": {"Elderly-living-alone", "Multigenerational"},   # Eldercare Befriending
    "P10": set(HOUSEHOLDS) - {"Elderly-living-alone"},      # Caregiver Respite (needs a caregiver)
}

CLIENT_COLS = ["client_id", "full_name", "nric_fake", "phone", "email",
               "planning_area", "household_type", "postal_code", "updated_date"]
REFERRAL_COLS = ["referral_id", "client_id", "programme_id",
                 "referral_date", "status"]

manifest = []      # human-readable list of planted defects
corrections = []   # rows for the batch 3 corrections file


def write_csv(subfolder, filename, columns, rows):
    folder = os.path.join(OUT, subfolder)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    # encoding="utf-8" writes NO byte-order mark; newline="" + lineterminator
    # keeps line endings as plain \n on Windows.
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path}  ({len(rows)} rows)")


def rand_date(start, end):
    return start + timedelta(days=rng.randint(0, (end - start).days))


def postal_for(area, digits):
    # Sector from the planning area, last four digits from the random number.
    return POSTAL_SECTOR[area] + f"{digits % 10000:04d}"


def make_client(n, updated):
    nric = f"FAKE-{rng.randint(1000000, 9999999)}"
    phone = f"0000{rng.randint(1000, 9999)}"
    area = rng.choice(AREAS)
    household = rng.choice(HOUSEHOLDS)
    # Same random call as before (same range), so every other random value in the files stays identical.
    digits = rng.randint(100000, 899999)
    return {
        "client_id": f"C{n:04d}",
        "full_name": f"Test Client {n:04d}",
        "nric_fake": nric,
        "phone": phone,
        "email": f"client{n:04d}@example.com",
        "planning_area": area,
        "household_type": household,
        "postal_code": postal_for(area, digits),
        "updated_date": updated,
    }


def fit_programme(referral, household):
    """If the programme doesn't suit the client's household, swap it for one that does.
    Deterministic (no random call), so the rest of the data doesn't shift."""
    allowed = PROGRAMME_FIT.get(referral["programme_id"])
    if household is None or allowed is None or household in allowed:
        return
    options = [p for p, _, _ in PROGRAMMES if household in PROGRAMME_FIT.get(p, set(HOUSEHOLDS))]
    referral["programme_id"] = options[int(referral["referral_id"][1:]) % len(options)]


def make_referral(n, client_ids, d_start, d_end):
    return {
        "referral_id": f"R{n:05d}",
        "client_id": rng.choice(client_ids),
        "programme_id": rng.choice(PROGRAMMES)[0],
        "referral_date": rand_date(d_start, d_end).isoformat(),
        "status": rng.choice(STATUSES),
    }


# ---------------------------------------------------------------- programmes
write_csv("programmes", "programmes.csv",
          ["programme_id", "programme_name", "agency_name"],
          [dict(programme_id=p, programme_name=n, agency_name=a)
           for p, n, a in PROGRAMMES])

# ------------------------------------------------------------ batch 1 clients
base = [make_client(i, "2026-01-01") for i in range(1, 201)]
valid_ids = [c["client_id"] for c in base]
clients1 = [dict(c) for c in base]   # copy, so base stays clean

# clients we must not damage with defects (so batch 2 changes stay clean)
protected = {9, 24, 59, 4, 14, 30}
candidates = [i for i in range(200) if i not in protected]
changed_idx = rng.sample(candidates, 30)          # planning_area changes
rest = [i for i in candidates if i not in changed_idx]
phone_idx = rng.sample(rest, 5)                   # phone-only changes
# re-sent unchanged in batch 2. Picked without rng so every other random value stays the same.
unchanged_idx = [i for i in rest if i not in phone_idx][:2]
demo_idx = changed_idx[0]
demo_id = base[demo_idx]["client_id"]             # the demo story client

# defect: null key (cannot be fixed by key, needs a source-system fix)
clients1[9]["client_id"] = ""
manifest.append("batch1 clients: row 10 (was C0010) has an empty client_id "
                "-> quarantine, NOT correctable by key")
# defect: invalid postal codes
clients1[24]["postal_code"] = "12345"
# The correction restores the value the source should have sent
corrections.append(("clients", clients1[24]["client_id"], "postal_code", base[24]["postal_code"]))
manifest.append(f"batch1 clients: {clients1[24]['client_id']} postal_code '12345' (5 digits)")
clients1[59]["postal_code"] = "ABC123"
corrections.append(("clients", clients1[59]["client_id"], "postal_code", base[59]["postal_code"]))
manifest.append(f"batch1 clients: {clients1[59]['client_id']} postal_code 'ABC123'")
# cleanable mess: whitespace and case (silver standardises these, no quarantine)
for i in (4, 14, 30):
    clients1[i]["household_type"] = "  " + clients1[i]["household_type"].upper() + " "
    manifest.append(f"batch1 clients: {clients1[i]['client_id']} household_type has "
                    "whitespace/upper case (standardise, not quarantine)")

write_csv("clients", "clients_batch1.csv", CLIENT_COLS, clients1)

# ---------------------------------------------------------- batch 1 referrals
referrals1 = [make_referral(n, valid_ids, date(2026, 1, 2), date(2026, 6, 30))
              for n in range(1, 401)]
referrals1[50]["client_id"] = demo_id   # guarantees an OLD referral for demo client
household_of = {c["client_id"]: c["household_type"] for c in base}
for r in referrals1:
    fit_programme(r, household_of[r["client_id"]])
refs_clean = [dict(r) for r in referrals1]   # clean copy for re-sent duplicates

manifest.append(f"DEMO CLIENT: {demo_id} has a batch 1 referral "
                f"({referrals1[50]['referral_id']}) and a batch 2 referral; "
                "their planning_area changes in batch 2")

referrals1[6]["referral_id"] = ""
manifest.append("batch1 referrals: row 7 has an empty referral_id -> quarantine, "
                "NOT correctable by key")

referrals1[19]["referral_date"] = "15/03/2026"
corrections.append(("referrals", referrals1[19]["referral_id"], "referral_date", "2026-03-15"))
manifest.append(f"batch1 referrals: {referrals1[19]['referral_id']} date '15/03/2026' (wrong format)")

referrals1[88]["referral_date"] = "March 5, 2026"
corrections.append(("referrals", referrals1[88]["referral_id"], "referral_date", "2026-03-05"))
manifest.append(f"batch1 referrals: {referrals1[88]['referral_id']} date 'March 5, 2026' (wrong format)")

referrals1[150]["referral_date"] = "2027-03-15"
corrections.append(("referrals", referrals1[150]["referral_id"], "referral_date", "2026-03-15"))
manifest.append(f"batch1 referrals: {referrals1[150]['referral_id']} future date 2027-03-15")

for i, bad in ((3, " approved "), (40, "REJECTED"), (77, "closed  ")):
    referrals1[i]["status"] = bad
    manifest.append(f"batch1 referrals: {referrals1[i]['referral_id']} status '{bad}' "
                    "(standardise, not quarantine)")

write_csv("referrals", "referrals_batch1.csv", REFERRAL_COLS, referrals1)

# ------------------------------------------------------------ batch 2 clients
clients2 = []
for i in changed_idx:
    c = dict(base[i])
    old_area = c["planning_area"]
    c["planning_area"] = rng.choice([a for a in AREAS if a != old_area])
    # A move changes the postal code too: new area's sector, same last four digits (no extra random call).
    c["postal_code"] = POSTAL_SECTOR[c["planning_area"]] + c["postal_code"][2:]
    c["updated_date"] = "2026-07-01"
    clients2.append(c)
manifest.append("batch2 clients: 30 clients moved planning_area and postal_code (SCD2 new version)")

for i in phone_idx:
    c = dict(base[i])
    c["phone"] = f"0000{rng.randint(1000, 9999)}"
    c["updated_date"] = "2026-07-01"
    clients2.append(c)
manifest.append("batch2 clients: 5 clients changed phone ONLY (SCD1 overwrite, no new version)")

# Sources often re-send records that haven't changed. SCD2 must NOT create a new version for these.
for i in unchanged_idx:
    clients2.append(dict(base[i]))
manifest.append(f"batch2 clients: {', '.join(base[i]['client_id'] for i in unchanged_idx)} "
                "re-sent UNCHANGED (SCD2 must not create a new version)")

new_clients = [make_client(i, "2026-07-01") for i in range(201, 211)]
household_of.update({c["client_id"]: c["household_type"] for c in new_clients})   # before the typo below
# defect: a value cleaning cannot fix (space instead of hyphen). WARN rule: row is kept, failure is counted.
new_clients[0]["household_type"] = "Single parent"
manifest.append(f"batch2 clients: {new_clients[0]['client_id']} household_type 'Single parent' "
                "(WARN: kept in silver, counted in dq_result)")
clients2.extend(new_clients)
manifest.append("batch2 clients: 10 brand-new clients C0201-C0210")

write_csv("clients", "clients_batch2.csv", CLIENT_COLS, clients2)

# ---------------------------------------------------------- batch 2 referrals
all_ids = valid_ids + [c["client_id"] for c in new_clients]
referrals2 = [make_referral(n, all_ids, date(2026, 7, 2), date(2026, 9, 30))
              for n in range(401, 701)]
referrals2[10]["client_id"] = demo_id   # guarantees a NEW referral for demo client

referrals2[40]["client_id"] = "C9001"
corrections.append(("referrals", referrals2[40]["referral_id"], "client_id", "C0050"))
referrals2[41]["client_id"] = "C9002"
corrections.append(("referrals", referrals2[41]["referral_id"], "client_id", "C0051"))
# Orphans: fit the programme to the client the correction file will assign them to
true_client = {referrals2[40]["referral_id"]: "C0050", referrals2[41]["referral_id"]: "C0051"}
for r in referrals2:
    fit_programme(r, household_of.get(true_client.get(r["referral_id"], r["client_id"])))
manifest.append(f"batch2 referrals: {referrals2[40]['referral_id']} and "
                f"{referrals2[41]['referral_id']} are orphans (client C9001/C9002 not in clients)")

referrals2[120]["referral_date"] = "31/08/2026"
corrections.append(("referrals", referrals2[120]["referral_id"], "referral_date", "2026-08-31"))
manifest.append(f"batch2 referrals: {referrals2[120]['referral_id']} date '31/08/2026' (wrong format)")

for i in (100, 200, 300):
    referrals2.append(dict(refs_clean[i]))
    manifest.append(f"batch2 referrals: {refs_clean[i]['referral_id']} re-sent from batch 1 "
                    "(duplicate, MERGE should ignore it)")

# The same key twice in ONE file makes MERGE fail ("multiple source rows matched"),
# so silver must deduplicate before merging.
referrals2.append(dict(referrals2[5]))
manifest.append(f"batch2 referrals: {referrals2[5]['referral_id']} appears twice in the same file "
                "(silver dedup must remove it before MERGE)")

write_csv("referrals", "referrals_batch2.csv", REFERRAL_COLS, referrals2)

# ------------------------------------------------------ batch 3 corrections
write_csv("corrections", "corrections_batch3.csv",
          ["target_table", "business_key", "column_name", "corrected_value"],
          [dict(target_table=t, business_key=k, column_name=c, corrected_value=v)
           for t, k, c, v in corrections])

# ------------------------------------------------------------ expected results
# Verified by running the DQ rules against these files. Use them to reconcile during the demo.
# Referrals for a quarantined client are ALSO quarantined (REF_003 checks valid silver clients),
# so the totals are higher than the number of planted defects.
EXPECTED = """
EXPECTED RESULTS (after silver)

Batch 1
  clients   : 200 in, 197 valid, 3 quarantined (empty id, C0025, C0060)
  referrals : 400 in, 392 valid, 8 quarantined
              4 planted: empty referral_id, R00020, R00089, R00151 (dates)
              4 cascaded: R00087, R00273 (client C0025), R00387 (C0060), R00335 (empty-id client C0010)
Batch 2
  clients   : 47 in = 30 moved + 5 phone + 2 unchanged + 10 new
              all valid; C0201 fails WARN rule CLI_003 but is kept
              SCD2: 30 new versions only (phone-only and unchanged clients create none)
  referrals : 304 rows in file, 303 after in-file dedup, 3 of those already exist (no change)
              5 quarantined: R00441, R00442 (orphans), R00521 (date),
              + 2 cascaded: R00609 (C0010), R00647 (C0025)
After batch 3 corrections + re-check of all OPEN quarantine rows
  resolved  : 12 (C0025, C0060, R00020, R00089, R00151, R00441, R00442, R00521,
                  and cascaded R00087, R00273, R00387, R00647 now pass without a correction row)
  still OPEN: 4 (empty client_id, empty referral_id, R00335, R00609)
              -> need a fix in the source system, cannot be corrected by key
"""

with open(os.path.join(OUT, "PLANTED_DEFECTS.txt"), "w", encoding="utf-8", newline="") as f:
    f.write("PLANTED DEFECTS AND CHANGES (reference only, do not upload)\n\n")
    f.write("\n".join(manifest) + "\n")
    f.write(EXPECTED)
print("wrote data/PLANTED_DEFECTS.txt")
