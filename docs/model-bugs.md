# Model Bugs Found by Integration Tests

Discovered 2026-03-18. These are bugs in the production models (files outside `integration_tests/`), documented by failing integration tests. The tests have correct expected values per the design doc — they SHOULD fail until the model bugs are fixed.

---

## Bug 1: effective_at not populated for incremental-run rows in id_changes

**Affected groups:** basic, complex_merges

**Symptom:** Rows created in incremental runs (runs 2–4) have NULL `effective_at` in `snowplow_identities_id_changes`. Rows from the full-refresh run (run 1) are correctly populated.

**Example (basic group):**

| snowplow_id | previous_snowplow_id | effective_at | change_type | Notes |
|---|---|---|---|---|
| sp_D | NULL | NULL | created | Should be 2026-03-11 10:30:00 |
| sp_A | sp_D | NULL | merged | Should be 2026-03-12 11:00:00 |

**Example (complex_merges group):**

All batch 2/3 rows have blank effective_at:
- sp_T created (should be 2026-03-11 10:00:00)
- sp_U created (should be 2026-03-11 10:05:00)
- sp_V created (should be 2026-03-11 10:10:00)
- sp_V merged sp_T (should be 2026-03-11 11:05:00)
- sp_T merged sp_U (should be 2026-03-11 11:00:00)
- sp_W created, sp_X created, sp_P merged sp_X, sp_X merged sp_W (all batch 3)

**Root cause:** Likely the incremental id_changes logic doesn't populate `effective_at` when inserting new rows. The full-refresh path works correctly.

**Failing tests:** `dbt_utils_equality_*_id_changes_actual_*`

---

## Bug 2: first_seen_app_id NULL for merge rows where absorbed identity was created in a prior run

**Affected groups:** basic, edge_cases

**Symptom:** Merge rows in `id_changes` have NULL `first_seen_app_id` when the absorbed identity (previous_snowplow_id) was created in an earlier run and is NOT in `new_identities_this_run`.

**Example (basic group):**

| snowplow_id | previous_snowplow_id | change_type | first_seen_app_id | Notes |
|---|---|---|---|---|
| sp_A | sp_D | merged | NULL | Should be web (sp_D's first_app_id) |

**Example (edge_cases group):**

| snowplow_id | previous_snowplow_id | change_type | first_seen_app_id | Notes |
|---|---|---|---|---|
| sp_M | sp_N | merged | NULL | Should be NULL per design doc (LEFT JOIN finds no match) |

Note: In edge_cases, the NULL is actually expected behaviour per the design doc — sp_N is not in `new_identities_this_run` during the merge-only batch, so the LEFT JOIN correctly returns NULL. In basic, sp_D is also from a prior run, so the same logic applies. This may actually be correct behaviour, not a bug — re-evaluate whether the design doc's expected value of `web` for the basic group is correct.

**Root cause:** The LEFT JOIN to `new_identities_this_run` only matches identities processed in the current run. For merges referencing identities from prior runs, no match is found.

**Failing tests:** `dbt_utils_equality_basic_id_changes_actual_*`

---

## Bug 3: SCD superseded_at not set when merge happens in a later incremental run

**Affected groups:** basic, edge_cases

**Symptom:** In `id_mapping_scd`, when an identity is created in run N and merged in run N+M, the created row's `superseded_at` stays NULL and `is_current` stays true. The merged row is added correctly but doesn't supersede the created row.

**Example (basic group):**

Actual:
| snowplow_id | active_snowplow_id | effective_at | superseded_at | is_current |
|---|---|---|---|---|
| sp_D | sp_D | 2026-03-11 10:30:00 | NULL | true |
| sp_D | sp_A | 2026-03-12 11:00:00 | NULL | true |

Expected:
| snowplow_id | active_snowplow_id | effective_at | superseded_at | is_current |
|---|---|---|---|---|
| sp_D | sp_D | 2026-03-11 10:30:00 | 2026-03-12 11:00:00 | false |
| sp_D | sp_A | 2026-03-12 11:00:00 | NULL | true |

**Example (edge_cases group):**

Actual:
| snowplow_id | active_snowplow_id | effective_at | superseded_at | is_current |
|---|---|---|---|---|
| sp_N | sp_N | 2026-03-10 08:30:00 | NULL | true |
| sp_N | sp_M | 2026-03-11 09:00:00 | NULL | true |

Expected:
| snowplow_id | active_snowplow_id | effective_at | superseded_at | is_current |
|---|---|---|---|---|
| sp_N | sp_N | 2026-03-10 08:30:00 | 2026-03-11 09:00:00 | false |
| sp_N | sp_M | 2026-03-11 09:00:00 | NULL | true |

**Root cause:** The SCD model's LEAD window calculates `superseded_at` within a single run's data. When the created row was inserted in a previous run, the incremental logic doesn't pull in the historical record to recompute `superseded_at` via the LEAD window. This is open question #4 from the design doc.

**Failing tests:** `dbt_utils_equality_*_id_mapping_scd_actual_*`

---

## Bug 4: Identifier re-pointing fails for merges in incremental runs

**Affected groups:** basic

**Symptom:** In `identifier_mapping`, when identity sp_D is merged into sp_A in run 3, sp_D's identifiers remain pointed at sp_D instead of being re-pointed to sp_A.

**Actual:**

| active_snowplow_id | id_type | id_value |
|---|---|---|
| sp_D | domain_userid | ghi789 |
| sp_D | user_id | carol@co |

**Expected:**

| active_snowplow_id | id_type | id_value |
|---|---|---|
| sp_A | domain_userid | ghi789 |
| sp_A | user_id | carol@co |

**Root cause:** The identifier_mapping model joins against snowplow_id_mapping to resolve active_snowplow_id, but the re-pointing logic may not update existing rows during incremental runs.

**Failing tests:** `dbt_utils_equality_basic_identifier_mapping_actual_*`

---

## Bug 5: new_identities overwrite on incremental re-insert (open question #5)

**Affected groups:** basic (user_id lost), complex_merges (sp_P fully overwritten), edge_cases (duplicate rows)

**Symptom:** When the same snowplow_id appears in events across multiple batches, incremental runs overwrite the `new_identities` row instead of merging with LEAST/GREATEST.

**Example (basic group):**
sp_A `user_id` is NULL in actual. Expected `alice@co` from MAX aggregation across evt-1 (NULL) and evt-2 (alice@co). The incremental run that processes evt-8 (which has only `domain_userid=abc123`, no user_id) overwrites the entire row.

**Example (complex_merges group):**
sp_P is overwritten in run 3 by evt-33b (a merge event). Actual values:
- `first_seen_event_id = evt-33b` (should be evt-20)
- `domain_userid = NULL` (should be aaa111)
- `first_derived_tstamp = 2026-03-12 11:00:00` (should be 2026-03-10 08:00:00)

This cascades into id_changes (extra sp_P "created" row) and id_mapping_scd (extra sp_P SCD row, original sp_P row spuriously superseded).

**Example (edge_cases group):**
Late-arriving duplicate evt-40 (collector_tstamp 2026-03-12, derived_tstamp 07:55) creates a SECOND sp_M row in new_identities instead of being deduped. The model produces two sp_M rows with different first_derived_tstamp (07:55 and 08:00).

**Root cause:** The incremental new_identities upsert replaces the entire row with current-run data instead of using LEAST/GREATEST to preserve historical min/max values.

**Failing tests:** `dbt_utils_equality_*_new_identities_actual_*`, `dbt_utils_equal_rowcount_edge_cases_new_identities_actual_*`

---

## Bug 6: Re-emitted merge not deduped in id_changes

**Affected groups:** edge_cases

**Symptom:** When the same merge pair (sp_N into sp_M with identical merged_at) is re-emitted in a later run (evt-44 in run 3, duplicate of evt-42 in run 2), `id_changes` produces two rows instead of deduplicating to one.

**Actual:** 6 rows in id_changes (2 merge rows for sp_M/sp_N + extra sp_M created row from late dup)

**Expected:** 4 rows (1 merge row for sp_M/sp_N)

**Root cause:** The incremental LEAST/GREATEST merge path for id_changes does not deduplicate rows with the same id_change_key arriving in different runs.

**Failing tests:** `dbt_utils_equal_rowcount_edge_cases_id_changes_actual_*`

---

## Bug 7: UUID collision in identifier_mapping for shared id_value across different active_snowplow_ids

**Affected groups:** edge_cases

**Symptom:** sp_J and sp_M both have `domain_userid=mmm111`. The surrogate key `uuid` is based on `(id_type, id_value)`, so both rows get the same UUID. When sp_J's row is inserted in run 4, it overwrites sp_M's row.

**Actual:** sp_M's `domain_userid=mmm111` row is missing. sp_J has duplicate rows.

**Expected:** Both sp_M and sp_J should have separate `domain_userid=mmm111` rows with different `active_snowplow_id`.

**Root cause:** The UUID computation uses only `(id_type, id_value)` and does not include `active_snowplow_id`. This means the `unique_key` used for incremental upsert cannot distinguish two different identities sharing the same identifier value.

**Failing tests:** `dbt_utils_equal_rowcount_edge_cases_identifier_mapping_actual_*`, `dbt_utils_equality_edge_cases_identifier_mapping_actual_*`

---

## Bug 8: sha256() function not available in dbt-fusion

**Affected groups:** hashing

**Symptom:** All dbt run commands fail with `error: dbt0209: No function SHA256`.

**Root cause:** The model uses `to_hex(sha256(lower(trim(id))))` in `snowplow_identities_identifier_mapping_this_run.sql:24`. The `sha256()` function is not available in dbt-fusion's SQL dialect. Snowflake uses `SHA2(expr [, digest_size])`.

**Failing tests:** All hashing group tests (runs fail before tests execute)

---

## Summary by test group

| Group | Tests | Pass | Fail | Bugs |
|---|---|---|---|---|
| basic | 10 | 5 | 5 | #1, #2, #3, #4, #5 |
| complex_merges | 10 | 5 | 5 | #1, #5 |
| edge_cases | 10 | 2 | 8 | #3, #5, #6, #7 |
| hashing | 2 | 0 | 2 | #8 |
| **Total** | **32** | **12** | **20** | |
