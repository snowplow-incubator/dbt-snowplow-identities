# Integration Test Scenario Registry

This document maps each integration test scenario group to the unit tests it covers. Every scenario group uses self-documenting ID prefixes (e.g., `sp_CHAIN_A`, `duid_CHAIN_A`, `evt_CHAIN_01`) so data can be traced at a glance.

See the [design spec](../docs/superpowers/specs/2026-03-28-integration-tests-design.md) for the full architecture and batch strategy.

## How to Read This Document

- **Story**: What happens across batches
- **Batches**: Which events land in which batch
- **Unit tests exercised**: Which unit test scenarios this group covers

## How to Add a New Scenario

1. Choose a short prefix (e.g., `NEWSCN`)
2. Create events with IDs like `sp_NEWSCN_A`, `duid_NEWSCN_A`, `evt_NEWSCN_01`
3. Add rows to `data/source/snowplow_identities_events.csv`
4. Update all 6 expected output CSVs in `data/expected/`
5. Add a section below documenting the group
6. Update the traceability matrix at the bottom

---

## GROUP 1: SIMPLE — Basic create + merge lifecycle

**Story**: Two identities are created in batch 1 and one is immediately merged into the other. In batch 4, the surviving identity reappears with no changes, proving the pipeline is stable under idle incremental runs.

| Batch | Events |
|-------|--------|
| 1 | `evt_SIMPLE_01`: page_view, `sp_SIMPLE_A`, duid=`duid_SIMPLE_A`, uid=`uid_SIMPLE_A`, app=web |
| 1 | `evt_SIMPLE_02`: page_view, `sp_SIMPLE_B`, duid=`duid_SIMPLE_B`, uid=null, app=web |
| 1 | `evt_SIMPLE_03`: identity_merge, `sp_SIMPLE_A` absorbs `sp_SIMPLE_B` |
| 4 | `evt_SIMPLE_04`: page_view, `sp_SIMPLE_A`, duid=`duid_SIMPLE_A`, uid=`uid_SIMPLE_A`, app=web |

**Unit tests exercised**:

1. `merge_events_this_run::simple_merge` — `evt_SIMPLE_03` parsed as a merge event
2. `new_identities_this_run::two_users_created` — `evt_SIMPLE_01` + `evt_SIMPLE_02` create two identities
3. `snowplow_id_mapping_this_run::simple_merge` — B→A mapping within batch 1
4. `id_changes_this_run::two_created_one_merged` — 2 created + 1 merged in batch 1
5. `identifier_mapping_this_run::two_users_merged` — both duids resolve to A
6. `identifier_mapping_this_run::multiple_identifier_types` — A has both domain_userid and user_id
7. `snowplow_id_mapping::first_run` — first run stores mapping
8. `id_changes::first_run` — first run passes through
9. `id_mapping_scd::created_then_merged` — B's SCD: created→superseded, merged→current

---

## GROUP 2: CHAIN — Transitive chain + repointing + identifier lifecycle

**Story**: A merges into B in batch 1; then C merges into B in batch 2, making C the new root. This creates a two-hop transitive chain (A→B→C) that must resolve to A→C and B→C. In batch 3, A reappears with activity, exercising identifier first-seen preservation.

| Batch | Events |
|-------|--------|
| 1 | `evt_CHAIN_01`: page_view, `sp_CHAIN_A`, duid=`duid_CHAIN_A`, app=web |
| 1 | `evt_CHAIN_02`: page_view, `sp_CHAIN_B`, duid=`duid_CHAIN_B`, app=web |
| 1 | `evt_CHAIN_03`: identity_merge, `sp_CHAIN_B` absorbs `sp_CHAIN_A` |
| 2 | `evt_CHAIN_04`: page_view, `sp_CHAIN_C`, duid=`duid_CHAIN_C`, app=web |
| 2 | `evt_CHAIN_05`: page_view, `sp_CHAIN_B`, duid=`duid_CHAIN_B`, app=web |
| 2 | `evt_CHAIN_06`: identity_merge, `sp_CHAIN_C` absorbs `sp_CHAIN_B` |
| 3 | `evt_CHAIN_07`: page_view, `sp_CHAIN_A`, duid=`duid_CHAIN_A`, app=web |

**Unit tests exercised**:

1. `snowplow_id_mapping::repoints_stale_parent` — batch 2: A repointed from B to C
2. `id_changes::new_record_added` — batch 2: C's creation is a new record
3. `identifier_mapping::repoint_existing_on_merge` — batch 2: `duid_CHAIN_A` repointed from B to C
4. `identifier_mapping::merge_with_new_events` — batch 2: B has new events AND is merged
5. `identifier_mapping::preserve_first_seen_at` — batch 3: `duid_CHAIN_A` preserves first_seen from batch 1
6. `id_mapping_scd::incremental_new_merge` — batch 2: B's creation superseded by merge

---

## GROUP 3: FANIN — Fan-in merge + repointing

**Story**: Three identities are created in batch 1, and two of them (B and C) are merged into A in the same batch. In batch 2, a new identity D absorbs A, causing B and C to be repointed from A to D.

| Batch | Events |
|-------|--------|
| 1 | `evt_FANIN_01`: page_view, `sp_FANIN_A`, duid=`duid_FANIN_A`, app=web |
| 1 | `evt_FANIN_02`: page_view, `sp_FANIN_B`, duid=`duid_FANIN_B`, app=web |
| 1 | `evt_FANIN_03`: page_view, `sp_FANIN_C`, duid=`duid_FANIN_C`, app=web |
| 1 | `evt_FANIN_04`: identity_merge, `sp_FANIN_A` absorbs `sp_FANIN_B` |
| 1 | `evt_FANIN_05`: identity_merge, `sp_FANIN_A` absorbs `sp_FANIN_C` |
| 2 | `evt_FANIN_06`: page_view, `sp_FANIN_D`, duid=`duid_FANIN_D`, app=web |
| 2 | `evt_FANIN_07`: identity_merge, `sp_FANIN_D` absorbs `sp_FANIN_A` |

**Unit tests exercised**:

1. `snowplow_id_mapping_this_run::fan_in` — batch 1: B and C both merge into A
2. `snowplow_id_mapping::fan_in_repoint` — batch 2: B and C repoint from A to D

---

## GROUP 4: FANOUT — Multi-child single event + history resolution

**Story**: A merges into X in batch 1. In batch 2, a single merge event names A as the parent of both B and C simultaneously. Because A is already a historical child of X, B and C must resolve all the way through to X.

| Batch | Events |
|-------|--------|
| 1 | `evt_FANOUT_01`: page_view, `sp_FANOUT_A`, duid=`duid_FANOUT_A`, app=web |
| 1 | `evt_FANOUT_02`: page_view, `sp_FANOUT_X`, duid=`duid_FANOUT_X`, app=web |
| 1 | `evt_FANOUT_03`: identity_merge, `sp_FANOUT_X` absorbs `sp_FANOUT_A` |
| 2 | `evt_FANOUT_04`: page_view, `sp_FANOUT_B`, duid=`duid_FANOUT_B`, app=web |
| 2 | `evt_FANOUT_05`: page_view, `sp_FANOUT_C`, duid=`duid_FANOUT_C`, app=web |
| 2 | `evt_FANOUT_06`: identity_merge, `sp_FANOUT_A` absorbs `sp_FANOUT_B` AND `sp_FANOUT_C` (single event, multi-child merged array) |

**Unit tests exercised**:

1. `merge_events_this_run::multi_child_merge` — `evt_FANOUT_06` has 2 children in the merged array
2. `snowplow_id_mapping_this_run::fan_out_single_event` — single event produces 2 mapping rows
3. `snowplow_id_mapping::fan_out_into_history` — B and C merge into A, which is a historical child (A→X)

---

## GROUP 5: DIAMOND — Diamond topology + cross-run extension

**Story**: In batch 1, a diamond topology forms: A is absorbed by both B and C, and both B and C are absorbed by D. All four resolve to D within the same batch. In batch 2, E absorbs D, causing all prior mappings to repoint to E.

| Batch | Events |
|-------|--------|
| 1 | `evt_DIAMOND_01`–`04`: page_view for `sp_DIAMOND_A`, `sp_DIAMOND_B`, `sp_DIAMOND_C`, `sp_DIAMOND_D` |
| 1 | `evt_DIAMOND_05`: identity_merge, `sp_DIAMOND_B` absorbs `sp_DIAMOND_A` |
| 1 | `evt_DIAMOND_06`: identity_merge, `sp_DIAMOND_C` absorbs `sp_DIAMOND_A` |
| 1 | `evt_DIAMOND_07`: identity_merge, `sp_DIAMOND_D` absorbs `sp_DIAMOND_B` |
| 1 | `evt_DIAMOND_08`: identity_merge, `sp_DIAMOND_D` absorbs `sp_DIAMOND_C` |
| 2 | `evt_DIAMOND_09`: page_view, `sp_DIAMOND_E`, app=web |
| 2 | `evt_DIAMOND_10`: identity_merge, `sp_DIAMOND_E` absorbs `sp_DIAMOND_D` |

**Unit tests exercised**:

1. `snowplow_id_mapping_this_run::diamond` — batch 1: A→B, A→C, B→D, C→D resolved in one batch
2. `snowplow_id_mapping::diamond_cross_run` — batch 2: all historical mappings superseded by E

---

## GROUP 6: DISJOINT — Isolated groups that must not interfere

**Story**: Two independent merge pairs (A+B and C+D) are created in batch 1. A third pair (E+F) is added in batch 2. All three groups must remain fully isolated with no cross-contamination across batches.

| Batch | Events |
|-------|--------|
| 1 | `evt_DISJOINT_01`–`04`: page_view for `sp_DISJOINT_A`, `sp_DISJOINT_B`, `sp_DISJOINT_C`, `sp_DISJOINT_D` |
| 1 | `evt_DISJOINT_05`: identity_merge, `sp_DISJOINT_A` absorbs `sp_DISJOINT_B` |
| 1 | `evt_DISJOINT_06`: identity_merge, `sp_DISJOINT_C` absorbs `sp_DISJOINT_D` |
| 2 | `evt_DISJOINT_07`, `evt_DISJOINT_08`: page_view for `sp_DISJOINT_E`, `sp_DISJOINT_F` |
| 2 | `evt_DISJOINT_09`: identity_merge, `sp_DISJOINT_E` absorbs `sp_DISJOINT_F` |

**Unit tests exercised**:

1. `snowplow_id_mapping_this_run::disjoint` — batch 1: B→A and D→C are independent
2. `snowplow_id_mapping::disjoint_cross_run` — batch 2: E→F doesn't affect batch 1 groups

---

## GROUP 7: LONGCHAIN — Deep chain within single batch

**Story**: A 3-hop chain (A→B→C→D) is created entirely within batch 1. All intermediate hops must be transitively resolved so the final mappings show A→D, B→D, and C→D without multi-hop entries.

| Batch | Events |
|-------|--------|
| 1 | `evt_LCHAIN_01`–`04`: page_view for `sp_LCHAIN_A`, `sp_LCHAIN_B`, `sp_LCHAIN_C`, `sp_LCHAIN_D` |
| 1 | `evt_LCHAIN_05`: identity_merge, `sp_LCHAIN_B` absorbs `sp_LCHAIN_A` |
| 1 | `evt_LCHAIN_06`: identity_merge, `sp_LCHAIN_C` absorbs `sp_LCHAIN_B` |
| 1 | `evt_LCHAIN_07`: identity_merge, `sp_LCHAIN_D` absorbs `sp_LCHAIN_C` |

**Unit tests exercised**:

1. `snowplow_id_mapping_this_run::transitive_chain` — A→B, B→C resolves A→C (subset of chain)
2. `snowplow_id_mapping_this_run::long_chain` — full 3-hop chain resolved in one batch

---

## GROUP 8: ALTCHAIN — Alternating history/new chain resolution

**Story**: Batch 1 produces three separate merge pairs (A→B, C→D, E→F). Batch 2 links them together by merging across boundaries using a mix of historical children and new identities as connectors, producing a full alternating-edge chain (X→A→B→C→D→E→F) that must all resolve to F.

| Batch | Events |
|-------|--------|
| 1 | `evt_ALTCH_01`–`06`: page_view for `sp_ALTCH_A`, `sp_ALTCH_B`, `sp_ALTCH_C`, `sp_ALTCH_D`, `sp_ALTCH_E`, `sp_ALTCH_F` |
| 1 | `evt_ALTCH_07`: identity_merge, `sp_ALTCH_B` absorbs `sp_ALTCH_A` |
| 1 | `evt_ALTCH_08`: identity_merge, `sp_ALTCH_D` absorbs `sp_ALTCH_C` |
| 1 | `evt_ALTCH_09`: identity_merge, `sp_ALTCH_F` absorbs `sp_ALTCH_E` |
| 2 | `evt_ALTCH_10`: page_view, `sp_ALTCH_X`, app=web |
| 2 | `evt_ALTCH_11`: identity_merge, `sp_ALTCH_C` absorbs `sp_ALTCH_B` (connects groups 1+2) |
| 2 | `evt_ALTCH_12`: identity_merge, `sp_ALTCH_E` absorbs `sp_ALTCH_D` (connects groups 2+3) |
| 2 | `evt_ALTCH_13`: identity_merge, `sp_ALTCH_A` absorbs `sp_ALTCH_X` (new ID merges into historical child) |

**Unit tests exercised**:

1. `snowplow_id_mapping::multi_hop_stale_chain` — B→C (new) resolves through C→D (history)
2. `snowplow_id_mapping::forward_chain_within_same_run` — X→A where A is a historical child, resolves to F
3. `snowplow_id_mapping::alternating_chain` — full alternating history/new chain

---

## GROUP 9: MERGEONLY — Merge without creation in same batch

**Story**: A and B are created in batch 1 with no merge. In batch 2, a merge event arrives with no accompanying page_views. The pipeline must look up A's app_id from the historical `new_identities` table rather than the current run's data.

| Batch | Events |
|-------|--------|
| 1 | `evt_MERGE_01`: page_view, `sp_MERGE_A`, duid=`duid_MERGE_A`, app=web |
| 1 | `evt_MERGE_02`: page_view, `sp_MERGE_B`, duid=`duid_MERGE_B`, app=web |
| 2 | `evt_MERGE_03`: identity_merge, `sp_MERGE_B` absorbs `sp_MERGE_A` (merge only, no page_views) |

**Unit tests exercised**:

1. `id_changes_this_run::merge_from_prior_run` — batch 2: A only in historical new_identities
2. `id_mapping_scd::merge_only_in_new_batch` — batch 2: merge arrives without A's creation

---

## GROUP 10: MULTIID — Multiple identifier types + cross-app + preserve historical

**Story**: A single Snowplow identity accumulates events across three apps and two identifier types (domain_userid and user_id). The user_id appears on the iOS event in batch 1 but is absent in batch 3; it must be preserved. Timestamps must track the true first and last seen across all batches.

| Batch | Events |
|-------|--------|
| 1 | `evt_MULTI_01`: page_view, `sp_MULTI_A`, duid=`duid_MULTI_A`, uid=null, app=web |
| 1 | `evt_MULTI_02`: page_view, `sp_MULTI_A`, duid=`duid_MULTI_A`, uid=`uid_MULTI_A`, app=ios |
| 3 | `evt_MULTI_03`: page_view, `sp_MULTI_A`, duid=`duid_MULTI_A`, uid=null, app=mobile |

**Unit tests exercised**:

1. `new_identities_this_run::first_last_timestamps` — multiple events, different apps
2. `identifier_mapping_this_run::first_seen_event_per_identifier` — domain_userid→`evt_MULTI_01`, user_id→`evt_MULTI_02`
3. `id_mapping_scd::identity_created_no_merges` — A never merged, is_current=true
4. `new_identities::preserve_historical_values` — user_id preserved despite NULL in batch 3

---

## GROUP 11: LATE — Late-arriving / reprocessed data

**Story**: An identity is created in batch 1. In batch 3, a late-arriving event for the same identity arrives with an earlier derived_tstamp (09:50 vs 10:00). The pipeline must backdate the creation record to the earlier timestamp without creating a duplicate SCD row.

| Batch | Events |
|-------|--------|
| 1 | `evt_LATE_01`: page_view, `sp_LATE_A`, duid=`duid_LATE_A`, app=web, derived_tstamp=2026-01-01 10:00:00 |
| 3 | `evt_LATE_02`: page_view, `sp_LATE_A`, duid=`duid_LATE_A`, app=web, derived_tstamp=2026-01-01 09:50:00, load_tstamp=2026-01-03 (late-arriving) |

**Unit tests exercised**:

1. `id_changes::late_arriving_data` — earlier effective_at wins over historical
2. `id_mapping_scd::dedup_late_arriving` — same id_change_key reprocessed, no duplicate
3. `id_mapping_scd::dedup_reseen_different_effective_at` — different effective_at, earliest kept

---

## Traceability Matrix

Complete mapping of 38 out of 39 unit tests to scenario groups:

| # | Unit Test | Group | Batch |
|---|-----------|-------|-------|
| 1 | `merge_events_this_run::simple_merge` | SIMPLE | 1 |
| 2 | `merge_events_this_run::multi_child_merge` | FANOUT | 2 |
| 3 | `new_identities_this_run::two_users_created` | SIMPLE | 1 |
| 4 | `new_identities_this_run::first_last_timestamps` | MULTIID | 1 |
| 5 | `snowplow_id_mapping_this_run::simple_merge` | SIMPLE | 1 |
| 6 | `snowplow_id_mapping_this_run::transitive_chain` | LONGCHAIN | 1 |
| 7 | `snowplow_id_mapping_this_run::fan_in` | FANIN | 1 |
| 8 | `snowplow_id_mapping_this_run::fan_out_single_event` | FANOUT | 2 |
| 9 | `snowplow_id_mapping_this_run::long_chain` | LONGCHAIN | 1 |
| 10 | `snowplow_id_mapping_this_run::diamond` | DIAMOND | 1 |
| 11 | `snowplow_id_mapping_this_run::disjoint` | DISJOINT | 1 |
| 12 | `id_changes_this_run::two_created_one_merged` | SIMPLE | 1 |
| 13 | `id_changes_this_run::merge_from_prior_run` | MERGEONLY | 2 |
| 14 | `identifier_mapping_this_run::two_users_merged` | SIMPLE | 1 |
| 15 | `identifier_mapping_this_run::multiple_identifier_types` | SIMPLE | 1 |
| 16 | `identifier_mapping_this_run::first_seen_event_per_identifier` | MULTIID | 1 |
| 17 | `snowplow_id_mapping::first_run` | SIMPLE | 1 |
| 19 | `snowplow_id_mapping::repoints_stale_parent` | CHAIN | 2 |
| 20 | `snowplow_id_mapping::multi_hop_stale_chain` | ALTCHAIN | 2 |
| 21 | `snowplow_id_mapping::forward_chain_within_same_run` | ALTCHAIN | 2 |
| 22 | `snowplow_id_mapping::fan_in_repoint` | FANIN | 2 |
| 23 | `snowplow_id_mapping::disjoint_cross_run` | DISJOINT | 2 |
| 24 | `snowplow_id_mapping::fan_out_into_history` | FANOUT | 2 |
| 25 | `snowplow_id_mapping::diamond_cross_run` | DIAMOND | 2 |
| 26 | `snowplow_id_mapping::alternating_chain` | ALTCHAIN | 2 |
| 27 | `id_changes::first_run` | SIMPLE | 1 |
| 28 | `id_changes::new_record_added` | CHAIN | 2 |
| 29 | `id_changes::late_arriving_data` | LATE | 3 |
| 30 | `identifier_mapping::repoint_existing_on_merge` | CHAIN | 2 |
| 31 | `identifier_mapping::merge_with_new_events` | CHAIN | 2 |
| 32 | `identifier_mapping::preserve_first_seen_at` | CHAIN | 3 |
| 33 | `id_mapping_scd::identity_created_no_merges` | MULTIID | 1 |
| 34 | `id_mapping_scd::created_then_merged` | SIMPLE | 1 |
| 35 | `id_mapping_scd::incremental_new_merge` | CHAIN | 2 |
| 36 | `id_mapping_scd::merge_only_in_new_batch` | MERGEONLY | 2 |
| 37 | `id_mapping_scd::dedup_late_arriving` | LATE | 3 |
| 38 | `id_mapping_scd::dedup_reseen_different_effective_at` | LATE | 3 |
| 39 | `new_identities::preserve_historical_values` | MULTIID | 3 |

38 of 39 unit tests are covered. The following unit test is not covered by integration tests:

- `snowplow_id_mapping::remapped_to_new_parent` — removed because re-merging an identity to a different parent is not a valid real-world scenario.
