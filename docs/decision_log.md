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
