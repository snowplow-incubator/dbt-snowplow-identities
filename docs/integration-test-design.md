# Integration Test Design

## Approach

Four test groups, each with its own seed data and expected output. Groups 1–3 verify **all 5 derived tables**. Group 4 verifies only `identifier_mapping` with hashing enabled. Each group runs the full pipeline (seed, full-refresh, incremental runs). Groups are isolated from each other — no shared state.

**Derived tables verified per group:**
1. `new_identities` — one row per snowplow_id, with identifiers and timestamps
2. `snowplow_id_mapping` — child-to-parent merge mappings
3. `id_changes` — changelog of identity creation and merge events
4. `id_mapping_scd` — Type 2 SCD timeline of identity-to-active-id mappings
5. `identifier_mapping` — identifier values (domain_userid, user_id) linked to active snowplow_ids

---

## Group 1: Basic — creation, simple merges, dedup, incremental updates

Tests the happy path: identities are created, some are merged (single-level only), duplicates are handled, and incremental runs preserve timestamps correctly.

### Seed events

**Batch 1 (run 1, full-refresh):**

- **evt-1**: `page_view` for sp_A. Carries identity context with `snowplow_id=sp_A`, `created_at=2026-03-10 08:00`. Has `domain_userid=abc123`, `user_id` is NULL. `derived_tstamp=2026-03-10 08:00`, `app_id=web`.
- **evt-2**: `page_view` for sp_A. Same identity context (`snowplow_id=sp_A`). Has `domain_userid=abc123`, `user_id=alice@co`. `derived_tstamp=2026-03-10 08:30`. Tests that identifiers from different events are resolved via MAX aggregation.
- **evt-3**: `page_view` for sp_B. Identity context with `snowplow_id=sp_B`, `created_at=2026-03-10 08:15`. Has `domain_userid=xyz789`. `derived_tstamp=2026-03-10 08:15`, `app_id=web`.
- **evt-4**: `page_view` for sp_C. Identity context with `snowplow_id=sp_C`, `created_at=2026-03-10 08:20`. Has `domain_userid=def456`, `user_id=bob@co`. `derived_tstamp=2026-03-10 08:20`, `app_id=mobile`.
- **evt-5**: `page_view` with NO identity context (NULL context column). `derived_tstamp=2026-03-10 08:25`. Tests that events without identity context are excluded from all output tables.
- **evt-6**: Duplicate of evt-1 — same `event_id`, later `collector_tstamp`. Tests deduplication keeps earliest collector_tstamp copy.
- **evt-7**: `identity_merge` — sp_B merged into sp_A. `active_snowplow_id=sp_A`, merged array contains `[{snowplow_id: sp_B, merged_at: 2026-03-10 09:00}]`. Tests simple merge mapping and identifier re-pointing.

**Batch 2 (run 2, incremental):**

- **evt-8**: `page_view` for sp_A. Has `domain_userid=abc123`. `derived_tstamp=2026-03-11 10:00`. Tests that incremental runs produce new rows in id_changes (if new events arrive) and that identifier_mapping timestamps are preserved via LEAST/GREATEST.
- **evt-9**: `page_view` for sp_D. Identity context with `snowplow_id=sp_D`, `created_at=2026-03-11 10:30`. Has `domain_userid=ghi789`, `user_id=carol@co`. `app_id=web`. Tests new identity creation in incremental mode.

**Batch 3 (run 3, incremental):**

- **evt-10**: `identity_merge` — sp_D merged into sp_A. `active_snowplow_id=sp_A`, merged array contains `[{snowplow_id: sp_D, merged_at: 2026-03-12 11:00}]`. Tests second merge into same target, and that sp_D's identifiers get re-pointed to sp_A.

**Batch 4 (run 4, incremental):**

- No new events. Tests that empty incremental runs produce no changes.

### Expected results after all runs

#### new_identities

| snowplow_id | created_at | first_seen_event_id | first_app_id | last_app_id | domain_userid | user_id | first_derived_tstamp | last_derived_tstamp |
|---|---|---|---|---|---|---|---|---|
| sp_A | 2026-03-10 08:00 | evt-1 | web | web | abc123 | alice@co | 2026-03-10 08:00 | 2026-03-11 10:00 |
| sp_B | 2026-03-10 08:15 | evt-3 | web | web | xyz789 | NULL | 2026-03-10 08:15 | 2026-03-10 08:15 |
| sp_C | 2026-03-10 08:20 | evt-4 | mobile | mobile | def456 | bob@co | 2026-03-10 08:20 | 2026-03-10 08:20 |
| sp_D | 2026-03-11 10:30 | evt-9 | web | web | ghi789 | carol@co | 2026-03-11 10:30 | 2026-03-11 10:30 |

Notes:
- sp_A's `user_id=alice@co` comes from MAX aggregation across evt-1 (NULL) and evt-2 (alice@co).
- sp_A's `last_derived_tstamp` reflects evt-8 from batch 2. However, `first_derived_tstamp` may be overwritten to batch 2's value due to the known bug (open question #5) — the expected output here captures the *actual* (buggy) behaviour. If/when the bug is fixed, update the expected value.
- evt-5 (no identity context) produces no row.
- evt-6 (duplicate) is deduped and does not produce a separate identity.

#### snowplow_id_mapping

| snowplow_id | active_snowplow_id | merged_at |
|---|---|---|
| sp_B | sp_A | 2026-03-10 09:00 |
| sp_D | sp_A | 2026-03-12 11:00 |

Notes:
- sp_C is never merged, so no mapping row.
- sp_A is the true parent (never itself merged).

#### id_changes

| snowplow_id | previous_snowplow_id | effective_at | change_type | first_seen_event_id | first_seen_app_id |
|---|---|---|---|---|---|
| sp_A | NULL | 2026-03-10 08:00 | created | evt-1 | web |
| sp_B | NULL | 2026-03-10 08:15 | created | evt-3 | web |
| sp_C | NULL | 2026-03-10 08:20 | created | evt-4 | mobile |
| sp_A | sp_B | 2026-03-10 09:00 | merged | evt-7 | web |
| sp_D | NULL | 2026-03-11 10:30 | created | evt-9 | web |
| sp_A | sp_D | 2026-03-12 11:00 | merged | evt-10 | web |

Notes:
- Merge rows have `snowplow_id` = the parent (survivor), `previous_snowplow_id` = the absorbed child.
- `first_seen_app_id` for merge rows comes from the absorbed identity's `first_app_id` in new_identities_this_run (LEFT JOIN). sp_B's `first_app_id=web`, sp_D's `first_app_id=web`.

#### id_mapping_scd

| snowplow_id | active_snowplow_id | effective_at | superseded_at | change_type | is_current |
|---|---|---|---|---|---|
| sp_A | sp_A | 2026-03-10 08:00 | NULL | created | true |
| sp_B | sp_B | 2026-03-10 08:15 | 2026-03-10 09:00 | created | false |
| sp_B | sp_A | 2026-03-10 09:00 | NULL | merged | true |
| sp_C | sp_C | 2026-03-10 08:20 | NULL | created | true |
| sp_D | sp_D | 2026-03-11 10:30 | 2026-03-12 11:00 | created | false |
| sp_D | sp_A | 2026-03-12 11:00 | NULL | merged | true |

Notes:
- sp_B's "created" row is superseded when sp_B gets merged into sp_A. LEAD window over (snowplow_id ORDER BY effective_at) sets `superseded_at`.
- sp_A's "created" row is never superseded — sp_A is always the parent.
- sp_D's "created" row is superseded when sp_D gets merged into sp_A in run 3.

#### identifier_mapping

| active_snowplow_id | id_type | id_value | first_app_id | last_app_id | first_seen_at | last_seen_at | first_seen_event_id |
|---|---|---|---|---|---|---|---|
| sp_A | domain_userid | abc123 | web | web | 2026-03-10 08:00 | 2026-03-11 10:00 | evt-1 |
| sp_A | user_id | alice@co | web | web | 2026-03-10 08:00 | 2026-03-10 08:30 | evt-1 |
| sp_A | domain_userid | xyz789 | web | web | 2026-03-10 08:15 | 2026-03-10 08:15 | evt-3 |
| sp_C | domain_userid | def456 | mobile | mobile | 2026-03-10 08:20 | 2026-03-10 08:20 | evt-4 |
| sp_C | user_id | bob@co | mobile | mobile | 2026-03-10 08:20 | 2026-03-10 08:20 | evt-4 |
| sp_A | domain_userid | ghi789 | web | web | 2026-03-11 10:30 | 2026-03-11 10:30 | evt-9 |
| sp_A | user_id | carol@co | web | web | 2026-03-11 10:30 | 2026-03-11 10:30 | evt-9 |

Notes:
- sp_B's `domain_userid=xyz789` is re-pointed to `active_snowplow_id=sp_A` after the merge in run 1.
- sp_D's identifiers are re-pointed to sp_A after the merge in run 3.
- sp_A's `domain_userid=abc123` has `last_seen_at` updated to reflect evt-8 from batch 2 (GREATEST).
- `first_seen_event_id` is preserved from the original event via LEAST on first_seen_at.

### Behaviours verified

1. Identity creation from events with identity context
2. Events without identity context are excluded
3. Duplicate event_id deduplication (keeps earliest collector_tstamp)
4. Multiple events per identity resolve identifiers via MAX
5. Simple merge (one child into one parent) produces correct mapping
6. Merge re-points identifiers from child to parent in identifier_mapping
7. New identity creation in incremental mode
8. Second merge into same target (sp_D into sp_A) works correctly
9. Incremental timestamp preservation (LEAST/GREATEST) for identifier_mapping
10. Empty incremental run produces no changes
11. SCD timeline tracks creation and merge events with correct superseded_at
12. id_changes captures both creation and merge events

---

## Group 2: Complex merges — transitive chains, diamonds, multi-child events

Tests merge resolution edge cases that involve non-trivial parent-child relationships. These need isolation because the merge logic is the code most likely to have subtle bugs.

### Seed events

**Batch 1 (run 1, full-refresh):**

- **evt-20**: `page_view` for sp_P. Identity context `snowplow_id=sp_P`, `created_at=2026-03-10 08:00`. Has `domain_userid=aaa111`. `app_id=web`.
- **evt-21**: `page_view` for sp_Q. Identity context `snowplow_id=sp_Q`, `created_at=2026-03-10 08:05`. Has `domain_userid=bbb222`. `app_id=web`.
- **evt-22**: `page_view` for sp_R. Identity context `snowplow_id=sp_R`, `created_at=2026-03-10 08:10`. Has `domain_userid=ccc333`. `app_id=web`.
- **evt-23**: `page_view` for sp_S. Identity context `snowplow_id=sp_S`, `created_at=2026-03-10 09:05`. Has `domain_userid=ddd444`. `app_id=mobile`. `derived_tstamp=2026-03-10 08:15`. Note: `created_at` deliberately differs from `derived_tstamp`. The model uses `first_derived_tstamp` (08:15) as `effective_at` for created rows, not `created_at` (09:05).
- **evt-24**: `identity_merge` — sp_Q merged into sp_P. Merged array: `[{snowplow_id: sp_Q, merged_at: 2026-03-10 09:00}]`. Simple merge baseline.
- **evt-25**: `identity_merge` — sp_R AND sp_S merged into sp_P in a single event. Merged array: `[{snowplow_id: sp_R, merged_at: 2026-03-10 09:05}, {snowplow_id: sp_S, merged_at: 2026-03-10 09:05}]`. Tests multi-child merge in a single event.

**Batch 2 (run 2, incremental):**

- **evt-26**: `page_view` for sp_T. Identity context `snowplow_id=sp_T`, `created_at=2026-03-11 10:00`. Has `domain_userid=eee555`. `app_id=web`.
- **evt-27**: `page_view` for sp_U. Identity context `snowplow_id=sp_U`, `created_at=2026-03-11 10:05`. Has `domain_userid=fff666`. `app_id=web`.
- **evt-28**: `page_view` for sp_V. Identity context `snowplow_id=sp_V`, `created_at=2026-03-11 10:10`. Has `domain_userid=ggg777`. `app_id=web`.
- **evt-29**: `identity_merge` — sp_U merged into sp_T. Merged array: `[{snowplow_id: sp_U, merged_at: 2026-03-11 11:00}]`.
- **evt-30**: `identity_merge` — sp_T merged into sp_V. Merged array: `[{snowplow_id: sp_T, merged_at: 2026-03-11 11:05}]`. Creates a transitive chain: sp_U -> sp_T -> sp_V. The model filters out sp_U's mapping to sp_T (sp_T is not a true parent) and only keeps sp_T -> sp_V. sp_U is NOT re-pointed to sp_V in this run. This documents the known single-level resolution behaviour.

**Batch 3 (run 3, incremental):**

- **evt-31**: `page_view` for sp_W. Identity context `snowplow_id=sp_W`, `created_at=2026-03-12 10:00`. Has `domain_userid=hhh888`. `app_id=web`.
- **evt-32**: `page_view` for sp_X. Identity context `snowplow_id=sp_X`, `created_at=2026-03-12 10:05`. Has `domain_userid=iii999`. `app_id=web`.
- **evt-33**: `identity_merge` — sp_W merged into sp_X, AND sp_X merged into sp_P (diamond). Merged arrays: evt-33a merges sp_W into sp_X `[{snowplow_id: sp_W, merged_at: 2026-03-12 11:00}]`, evt-33b merges sp_X into sp_P `[{snowplow_id: sp_X, merged_at: 2026-03-12 11:00}]`. sp_X is not a true parent (merged into sp_P), so sp_W -> sp_X is filtered out. Only sp_X -> sp_P survives. sp_W would be resolved in a later run.

**Batch 4 (run 4, incremental):**

- No new events. After this run, sp_W and sp_U may still not be fully resolved (documenting eventual consistency behaviour).

### Expected results after all runs

#### new_identities

| snowplow_id | created_at | first_seen_event_id | first_app_id | domain_userid | user_id |
|---|---|---|---|---|---|
| sp_P | 2026-03-10 08:00 | evt-20 | web | aaa111 | NULL |
| sp_Q | 2026-03-10 08:05 | evt-21 | web | bbb222 | NULL |
| sp_R | 2026-03-10 08:10 | evt-22 | web | ccc333 | NULL |
| sp_S | 2026-03-10 09:05 | evt-23 | mobile | ddd444 | NULL |
| sp_T | 2026-03-11 10:00 | evt-26 | web | eee555 | NULL |
| sp_U | 2026-03-11 10:05 | evt-27 | web | fff666 | NULL |
| sp_V | 2026-03-11 10:10 | evt-28 | web | ggg777 | NULL |
| sp_W | 2026-03-12 10:00 | evt-31 | web | hhh888 | NULL |
| sp_X | 2026-03-12 10:05 | evt-32 | web | iii999 | NULL |

#### snowplow_id_mapping

| snowplow_id | active_snowplow_id | merged_at |
|---|---|---|
| sp_Q | sp_P | 2026-03-10 09:00 |
| sp_R | sp_P | 2026-03-10 09:05 |
| sp_S | sp_P | 2026-03-10 09:05 |
| sp_T | sp_V | 2026-03-11 11:05 |
| sp_X | sp_P | 2026-03-12 11:00 |

Notes:
- sp_U -> sp_T is NOT in this table because sp_T is not a true parent (sp_T is itself merged into sp_V). This documents the known limitation (open question #2). sp_U's mapping is lost for this batch.
- sp_W -> sp_X is NOT in this table because sp_X is not a true parent (sp_X is merged into sp_P). sp_W's mapping is also lost.
- Only direct mappings to true parents survive the filter.

#### id_changes

| snowplow_id | previous_snowplow_id | effective_at | change_type | first_seen_app_id |
|---|---|---|---|---|
| sp_P | NULL | 2026-03-10 08:00 | created | web |
| sp_Q | NULL | 2026-03-10 08:05 | created | web |
| sp_R | NULL | 2026-03-10 08:10 | created | web |
| sp_S | NULL | 2026-03-10 08:15 | created | mobile |
| sp_P | sp_Q | 2026-03-10 09:00 | merged | web |
| sp_P | sp_R | 2026-03-10 09:05 | merged | web |
| sp_P | sp_S | 2026-03-10 09:05 | merged | mobile |
| sp_T | NULL | 2026-03-11 10:00 | created | web |
| sp_U | NULL | 2026-03-11 10:05 | created | web |
| sp_V | NULL | 2026-03-11 10:10 | created | web |
| sp_V | sp_T | 2026-03-11 11:05 | merged | web |
| sp_T | sp_U | 2026-03-11 11:00 | merged | web |
| sp_W | NULL | 2026-03-12 10:00 | created | web |
| sp_X | NULL | 2026-03-12 10:05 | created | web |
| sp_P | sp_X | 2026-03-12 11:00 | merged | web |
| sp_X | sp_W | 2026-03-12 11:00 | merged | web |

Notes:
- Multi-child merge (evt-25) produces two separate merge rows (sp_R and sp_S).
- The transitive chain produces merge rows for both levels (sp_U->sp_T and sp_T->sp_V) because id_changes records the raw merge events, not the resolved mappings.

#### id_mapping_scd

| snowplow_id | active_snowplow_id | effective_at | superseded_at | change_type | is_current |
|---|---|---|---|---|---|
| sp_P | sp_P | 2026-03-10 08:00 | NULL | created | true |
| sp_Q | sp_Q | 2026-03-10 08:05 | 2026-03-10 09:00 | created | false |
| sp_Q | sp_P | 2026-03-10 09:00 | NULL | merged | true |
| sp_R | sp_R | 2026-03-10 08:10 | 2026-03-10 09:05 | created | false |
| sp_R | sp_P | 2026-03-10 09:05 | NULL | merged | true |
| sp_S | sp_S | 2026-03-10 08:15 | 2026-03-10 09:05 | created | false |
| sp_S | sp_P | 2026-03-10 09:05 | NULL | merged | true |
| sp_T | sp_T | 2026-03-11 10:00 | 2026-03-11 11:05 | created | false |
| sp_T | sp_V | 2026-03-11 11:05 | NULL | merged | true |
| sp_U | sp_U | 2026-03-11 10:05 | 2026-03-11 11:00 | created | false |
| sp_U | sp_T | 2026-03-11 11:00 | NULL | merged | true |
| sp_V | sp_V | 2026-03-11 10:10 | NULL | created | true |
| sp_W | sp_W | 2026-03-12 10:00 | 2026-03-12 11:00 | created | false |
| sp_W | sp_X | 2026-03-12 11:00 | NULL | merged | true |
| sp_X | sp_X | 2026-03-12 10:05 | 2026-03-12 11:00 | created | false |
| sp_X | sp_P | 2026-03-12 11:00 | NULL | merged | true |

Notes:
- sp_S's created row has `effective_at=2026-03-10 08:15` (first_derived_tstamp from evt-23) while its merged row has `effective_at=2026-03-10 09:05` (merged_at). Although `created_at=09:05` was set to equal `merged_at` to test SCD LEAD tie-breaking, the model uses `first_derived_tstamp` (not `created_at`) for created rows' effective_at, so the values differ and no tie-breaking is exercised.
- sp_U shows `active_snowplow_id=sp_T` (not sp_V) because the SCD records the direct merge target, not the resolved chain. This is the SCD accurately reflecting what happened: sp_U was merged into sp_T. The fact that sp_T was then merged into sp_V is tracked in sp_T's own SCD row.
- sp_W shows `active_snowplow_id=sp_X` (not sp_P) for the same reason.

#### identifier_mapping

| active_snowplow_id | id_type | id_value |
|---|---|---|
| sp_P | domain_userid | aaa111 |
| sp_P | domain_userid | bbb222 |
| sp_P | domain_userid | ccc333 |
| sp_P | domain_userid | ddd444 |
| sp_V | domain_userid | eee555 |
| sp_V | domain_userid | fff666 |
| sp_V | domain_userid | ggg777 |
| sp_P | domain_userid | hhh888 |
| sp_P | domain_userid | iii999 |

Notes:
- sp_Q's, sp_R's, sp_S's identifiers are all re-pointed to sp_P.
- sp_T's identifier (eee555) is re-pointed to sp_V (direct merge target). NOT to sp_P, because sp_T's mapping goes to sp_V and the identifier_mapping model joins against snowplow_id_mapping for resolution.
- sp_U's identifier (fff666) — here is where the known limitation shows. sp_U has no row in snowplow_id_mapping (filtered out because sp_T is not a true parent). So sp_U's identifier may remain pointed at sp_U or sp_T depending on resolution order. **This expected output needs verification against actual model behaviour.**
- sp_W's and sp_X's identifiers similarly depend on how the model handles the diamond. sp_X is mapped to sp_P, so sp_X's identifier goes to sp_P. sp_W has no mapping row, so its identifier may not be re-pointed.

**Important:** The exact identifier_mapping output for transitive chain scenarios (sp_U, sp_W) depends on model behaviour that is currently documented as an open question. The integration test will **document actual behaviour** — whichever result the model produces becomes the expected output, and the test locks it in. If the behaviour is later deemed a bug and fixed, the expected output is updated.

### Behaviours verified

1. Multi-child merge in a single event (sp_R and sp_S into sp_P)
2. Transitive chain resolution — documents that only one level is resolved per run (sp_T->sp_V kept, sp_U->sp_T dropped)
3. Diamond merge — sp_W->sp_X and sp_X->sp_P in same batch, non-true-parent filtering
4. SCD correctly tracks direct merge targets (not resolved targets)
5. id_changes records all merge events regardless of true-parent filtering
6. Identifier re-pointing through merge resolution (and its limitations for unresolved chains)
7. SCD LEAD tie-breaking when created_at equals merged_at (sp_S has two SCD rows with identical effective_at)

---

## Group 3: Edge cases — late duplicates, merge-only batches, re-emitted merges

Tests unusual incremental paths that don't fit cleanly into the happy path or complex merge groups.

### Seed events

**Batch 1 (run 1, full-refresh):**

- **evt-40**: `page_view` for sp_M. Identity context `snowplow_id=sp_M`, `created_at=2026-03-10 08:00`. Has `domain_userid=mmm111`, `user_id=mike@co`. `app_id=web`. `derived_tstamp=2026-03-10 08:00`.
- **evt-41**: `page_view` for sp_N. Identity context `snowplow_id=sp_N`, `created_at=2026-03-10 08:30`. Has `domain_userid=nnn222`. `app_id=web`. `derived_tstamp=2026-03-10 08:30`.

**Batch 2 (run 2, incremental):**

- **evt-42**: `identity_merge` — sp_N merged into sp_M. Merged array: `[{snowplow_id: sp_N, merged_at: 2026-03-11 09:00}]`. This batch has ONLY a merge event and no page_views with identity context. Tests that merge-only batches work correctly. Also tests that the absorbed identity (sp_N, created in run 1) is NOT in `new_identities_this_run`, so the LEFT JOIN produces `first_seen_app_id = NULL` on the merge row in id_changes. Stress-tests open question #4 (SCD historical record pull) without a safety net.

**Batch 3 (run 3, incremental):**

- **evt-44**: Re-emission of the same merge pair (sp_N into sp_M) with the SAME `merged_at`. Merged array: `[{snowplow_id: sp_N, merged_at: 2026-03-11 09:00}]`. Tests three things: (1) snowplow_id_mapping dedup — ROW_NUMBER keeps exactly one copy when the duplicate is identical; (2) id_changes incremental LEAST/GREATEST — the same id_change_key `(sp_M, sp_N, 2026-03-11 09:00)` arrives in both run 2 and run 3, exercising the dedup merge path; (3) id_mapping_scd dedup — the exact triple `(sp_N, sp_M, 2026-03-11 09:00)` appears in both new data and history, exercising `QUALIFY ROW_NUMBER()` dedup.
- **evt-45**: Late-arriving duplicate — same `event_id` as evt-40 but with a LATER `collector_tstamp` and an EARLIER `derived_tstamp` of `2026-03-10 07:55`. Tests dedup behaviour for late arrivals. The duplicate should be discarded (earlier collector_tstamp copy wins).

**Batch 4 (run 4, incremental):**

- **evt-46**: `page_view` for sp_J. Identity context `snowplow_id=sp_J`, `created_at=2026-03-13 08:00`. Has `domain_userid=mmm111` (same value as sp_M). Tests that two different snowplow_ids can share the same identifier value without interference.

### Expected results after all runs

#### new_identities

| snowplow_id | created_at | first_seen_event_id | first_app_id | domain_userid | user_id |
|---|---|---|---|---|---|
| sp_M | 2026-03-10 08:00 | evt-40 | web | mmm111 | mike@co |
| sp_N | 2026-03-10 08:30 | evt-41 | web | nnn222 | NULL |
| sp_J | 2026-03-13 08:00 | evt-46 | web | mmm111 | NULL |

Notes:
- sp_N has no new page_view events after run 1, so its timestamps remain from evt-41 only.
- sp_M's timestamps may be affected by the new_identities overwrite bug (open question #5) if evt-45 triggers a re-insert.
- sp_J shares `domain_userid=mmm111` with sp_M — they are independent identities.

#### snowplow_id_mapping

| snowplow_id | active_snowplow_id | merged_at |
|---|---|---|
| sp_N | sp_M | 2026-03-11 09:00 |

Notes:
- Only one row for sp_N, despite two merge events (evt-42 and evt-44) with identical merged_at. ROW_NUMBER dedup keeps exactly one copy.

#### id_changes

| snowplow_id | previous_snowplow_id | effective_at | change_type | first_seen_app_id |
|---|---|---|---|---|
| sp_M | NULL | 2026-03-10 08:00 | created | web |
| sp_N | NULL | 2026-03-10 08:30 | created | web |
| sp_M | sp_N | 2026-03-11 09:00 | merged | NULL |
| sp_J | NULL | 2026-03-13 08:00 | created | web |

Notes:
- The merge row has `first_seen_app_id = NULL` because sp_N was created in run 1 and is NOT in `new_identities_this_run` during run 2. The LEFT JOIN finds no match.
- Only one merge row despite the re-emission (evt-44). The same id_change_key `(sp_M, sp_N, 2026-03-11 09:00)` arrives in both run 2 and run 3. The incremental LEAST/GREATEST merge keeps the earliest timestamps from run 2. The `first_seen_event_id` is preserved from the historical record (both have the same effective_at, so the historical one wins via `CASE WHEN`).

#### id_mapping_scd

| snowplow_id | active_snowplow_id | effective_at | superseded_at | change_type | is_current |
|---|---|---|---|---|---|
| sp_M | sp_M | 2026-03-10 08:00 | NULL | created | true |
| sp_N | sp_N | 2026-03-10 08:30 | 2026-03-11 09:00 | created | false |
| sp_N | sp_M | 2026-03-11 09:00 | NULL | merged | true |
| sp_J | sp_J | 2026-03-13 08:00 | NULL | created | true |

Notes:
- sp_N's "created" row has `superseded_at` set to the merge time. This depends on the historical record being pulled in correctly (open question #4). Without evt-43 (removed), sp_N is NOT in `new_identities_this_run` during run 2, so the model must pull in sp_N's historical SCD record via the `ids_affected` set alone. **If the model fails to pull it in, sp_N's "created" row will be missing or will have incorrect `superseded_at`. The test documents actual behaviour.**
- The re-emitted merge (evt-44) produces the exact same triple `(sp_N, sp_M, 2026-03-11 09:00)` in run 3. `QUALIFY ROW_NUMBER()` dedup must keep exactly one merge row — no duplicates.

#### identifier_mapping

| active_snowplow_id | id_type | id_value | first_seen_at | last_seen_at |
|---|---|---|---|---|
| sp_M | domain_userid | mmm111 | 2026-03-10 08:00 | 2026-03-10 08:00 |
| sp_M | user_id | mike@co | 2026-03-10 08:00 | 2026-03-10 08:00 |
| sp_M | domain_userid | nnn222 | 2026-03-10 08:30 | 2026-03-10 08:30 |
| sp_J | domain_userid | mmm111 | 2026-03-13 08:00 | 2026-03-13 08:00 |

Notes:
- sp_N's `domain_userid=nnn222` is re-pointed to `active_snowplow_id=sp_M` after the merge.
- sp_J and sp_M both have `domain_userid=mmm111` — they produce separate rows with different `active_snowplow_id` values. The surrogate key `uuid` is based on `(id_type, id_value)`, so these two rows would have the SAME uuid. **This may cause a data correctness issue with the incremental upsert — verify against actual behaviour.** If the model's `unique_key=uuid` causes sp_J's row to overwrite sp_M's row, that's a bug to document.

### Behaviours verified

1. Pure merge-only batch (no new page_view identities, no new_identities_this_run rows) processes correctly
2. Absorbed identity from a prior run — LEFT JOIN to new_identities_this_run produces NULL first_seen_app_id
3. Re-emitted merge pair with identical merged_at — snowplow_id_mapping dedup keeps one copy
4. Same id_change_key across runs — id_changes incremental LEAST/GREATEST dedup preserves earliest timestamps
5. Exact SCD triple duplicate across runs — QUALIFY ROW_NUMBER() dedup keeps one row
6. Late-arriving duplicate event deduplication
7. Two snowplow_ids sharing the same identifier value — independent rows
8. Potential uuid collision in identifier_mapping when same id_value appears for different active_snowplow_ids
9. SCD historical record pull for merge-only batches without safety net (open question #4)
10. new_identities timestamp overwrite behaviour on incremental re-insert (open question #5)

---

## Group 4: Identifier hashing — `snowplow__hash_identifiers=true`

Tests the SHA-256 hashing path in `identifier_mapping`. Run with `--vars '{snowplow__hash_identifiers: true}'`. Only `identifier_mapping` output is verified — other tables are unaffected by this flag.

### Seed events

**Batch 1 (run 1, full-refresh):**

- **evt-50**: `page_view` for sp_H1. Identity context `snowplow_id=sp_H1`, `created_at=2026-03-10 08:00`. Has `domain_userid=hash_me_123`, `user_id=hash@co`. `app_id=web`. `derived_tstamp=2026-03-10 08:00`.
- **evt-51**: `page_view` for sp_H2. Identity context `snowplow_id=sp_H2`, `created_at=2026-03-10 08:05`. Has `domain_userid=hash_me_456`. `app_id=web`. `derived_tstamp=2026-03-10 08:05`.
- **evt-52**: `identity_merge` — sp_H2 merged into sp_H1. Merged array: `[{snowplow_id: sp_H2, merged_at: 2026-03-10 09:00}]`.

**Batch 2 (run 2, incremental):**

- No new events. Verifies incremental path with hashing — no regressions.

### Expected results after all runs

#### identifier_mapping

| active_snowplow_id | id_type | id_value |
|---|---|---|
| sp_H1 | domain_userid | SHA256('hash_me_123') |
| sp_H1 | user_id | SHA256('hash@co') |
| sp_H1 | domain_userid | SHA256('hash_me_456') |

Notes:
- `id_value` contains `SHA256(LOWER(TRIM(value)))`, not the raw value. The exact hash values will be computed during test implementation.
- sp_H2's `domain_userid=hash_me_456` is re-pointed to `active_snowplow_id=sp_H1` after the merge, AND hashed.
- All other tables (`new_identities`, `id_changes`, `snowplow_id_mapping`, `id_mapping_scd`) contain raw (unhashed) identifier values. This group only spot-checks those tables — they are thoroughly verified in Groups 1–3.

### Behaviours verified

1. Identifier values are SHA-256 hashed in identifier_mapping when `snowplow__hash_identifiers=true`
2. Hashing applies to both original and merge-repointed identifiers
3. Incremental path with hashing produces no regressions

---

## Open questions that these tests will answer definitively

| # | Question | Which group tests it | How |
|---|---|---|---|
| 1 | Does MAX aggregation lose identifier values? | Not directly tested (would need two different domain_userid values for the same snowplow_id in one batch — add to Group 1 if desired) | |
| 2 | Does transitive chain resolution work in one pass? | Group 2 (sp_U->sp_T->sp_V) | Check snowplow_id_mapping for sp_U row |
| 3 | Integration tests exist? | All groups | They exist now. |
| 4 | Does id_mapping_scd pull historical records for merge-only children? | Group 3 (evt-42 pure merge-only batch, no safety net) | Check sp_N's SCD superseded_at |
| 5 | Does new_identities overwrite first_derived_tstamp? | Group 1 (evt-8 re-appears sp_A) | Check sp_A's first_derived_tstamp after run 2 |
