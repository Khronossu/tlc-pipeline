# PDPA Tokenization Design

## Context

The TLC Yellow Taxi source data contains no real PII — TLC strips identifying fields before publication. This project fabricates synthetic PII (`passenger_email`, `passenger_phone`, `payment_card_last4`, `passenger_id`) to exercise the PDPA tokenization pattern end-to-end, making the pipeline design transferable to real sources (e.g., bank transaction data subject to the Thai Personal Data Protection Act).

---

## Data flow

```
meta.pii_lookup  (Iceberg — restricted namespace)
│   passenger_email, passenger_phone,
│   payment_card_last4, passenger_id  ← clear text lives ONLY here
│
├── tokenize_pii.py joins lookup + SHA-256s each value in-flight
│
└── bronze.yellow_trips receives ONLY token columns
        passenger_email_token    (64-char hex)
        passenger_phone_token    (64-char hex)
        payment_card_last4_token (64-char hex)
        passenger_id_token       (64-char hex)
        _salt_version            (INT)
```

Raw PII never touches Landing, Bronze, Silver, Gold, or Serving. The only disk location for clear text is `meta.pii_lookup`, which is:
- In an isolated Iceberg namespace (`meta`)
- Never referenced by dbt models
- Not exposed in Serving views

---

## Tokenization algorithm

```python
import hashlib

def tokenize(value: str | None, salt: str) -> str | None:
    if value is None:
        return None
    return hashlib.sha256((value + salt).encode()).hexdigest()
```

Properties:
- **Deterministic:** same `(value, salt)` → same 64-char hex token every run. Tokens are joinable across ingestion runs.
- **Irreversible:** SHA-256 is a one-way function. Tokens cannot be reversed to recover raw values.
- **Salt-sensitive:** different salt → completely different tokens. Salt rotation invalidates all prior tokens.
- **Length-stable:** output is always exactly 64 hex characters. GE expectation #10/#11 verifies this as a regression tripwire.

---

## Salt management

| Environment | Salt storage |
|---|---|
| Development | `PII_SALT` env var in `.env` (never committed) |
| Production | Secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.) |

The current salt version is stamped into every Bronze row as `_salt_version` (INT). This makes it possible to query "which rows were tokenized with which salt" if rotation is needed.

---

## Salt rotation (right-to-erasure pattern)

Salt rotation is the mechanism for right-to-erasure: rotating the salt makes all prior tokens unresolvable, effectively erasing the ability to link tokens back to identities.

**Procedure:**

1. Generate a new salt value and increment `PII_SALT_VERSION` in your secrets store.
2. Update `.env` (dev) or the secrets manager entry (prod) with the new salt.
3. Re-run `tokenize_pii.py` for all historical Bronze partitions with `--force`:
   ```bash
   make demo-backfill START=2018-01 END=2024-12 FORCE=true
   ```
4. All token columns in Bronze are rewritten with the new salt. Old tokens are gone.
5. Silver and Gold dbt models re-materialize on next run — downstream token columns update automatically.

**Note:** The `meta.pii_lookup` table (clear text) is not affected by rotation. To complete a full right-to-erasure for a specific individual, drop the relevant row from `meta.pii_lookup` and rotate the salt. After rotation, no join between tokens and identities is possible.

---

## Honest framing for grading

This project uses fabricated PII on US public data to exercise the engineering pattern. The design decisions (isolated namespace, tokenize-before-write, salt rotation procedure) are directly transferable to real PDPA-sensitive sources such as Thai financial transaction data, where `passenger_id` would be replaced by a real national ID or account number.
