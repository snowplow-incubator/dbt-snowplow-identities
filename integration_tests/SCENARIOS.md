# Integration Test Scenarios

Each scenario is identified by `name_tracker` in the source CSV. Events are spread across 4 batch days via `load_tstamp`. The test suite runs 1 full-refresh + 3 incremental runs.

## Merge Scenarios

### simple
Batch 1: A created (duid, uid, nuid). B created (duid, nuid). Bridge duid_B + uid_A → A absorbs B.
Batch 4: A reappears. No changes.
Tests: basic merge, identifier resolution, idle incremental stability.

### chain
Batch 1: A created, B created. Bridge → A absorbs B.
Batch 2: C created. B has activity. Bridge C via B → resolves to A, A absorbs C.
Batch 3: A reappears.
Tests: cross-batch merge resolution, identifier first-seen preservation.

### fanin
Batch 1: A, B, C created. Bridge B→A, then C→A.
Batch 2: D created. Bridge D→A.
Tests: multiple children merging into single root, cross-batch fan-in.

### fanout
Batch 1: A, X created. Bridge → A absorbs X.
Batch 2: B, C created. Bridge B via X's nuid, then C via X's nuid.
Tests: resolution through a merged parent's identifiers.

### diamond
Batch 1: A, B, C, D created. Bridge B→A, C→A, D→B (resolves to A).
Batch 2: E created. Bridge E→D (resolves to A).
Tests: converging merge paths, cross-batch extension.

### disjoint
Batch 1: A+B merged, C+D merged (independent pairs).
Batch 2: E+F merged.
Tests: isolated groups remain separate, no cross-contamination.

### longchain
Batch 1: A, B, C, D created. Bridge D→C, C→B, B→A sequentially.
Tests: deep transitive chain resolution within single batch, cumulative merge events.

### altchain
Batch 1: Three pairs created: A+B, C+D, E+F (older absorbs younger in each).
Batch 2: X created. Cross-group bridges link B's group to C's, D's to E's, X to A.
Tests: alternating history/new chain, cross-group merges, large repointing cascade.

### mergeonly
Batch 1: A and B created (page_views only).
Batch 2: Bridge A+B (merge event only, no page_views in this batch).
Tests: merge without creation in same batch, historical app_id lookup.

### remap
Batch 1: A, B, C created. Bridge → A absorbs B.
Batch 3: Bridge C via B → resolves to A, A absorbs C.
Tests: new identity bridging through an existing child.

## Identifier Scenarios

### multiid
Batch 1: A on web (duid, nuid). A on ios (duid, uid).
Batch 3: A on mobile (duid only).
Tests: multiple identifier types, cross-app tracking, historical identifier preservation.

### uid_conflict
Batch 1: A created (uid=alice). B created (uid=bob). Bridge A+B attempted.
Config: `(unique :user_id)` prevents merge.
Tests: unique identifier constraint blocks merge.

## Timing Scenarios

### late
Batch 1: A created at t=200s.
Batch 3: Late-arriving event for A with derived_tstamp before batch 1's event.
Tests: first_derived_tstamp backdated, no duplicate rows.

### stable
Batches 1-4: A appears in every batch with same identifiers, no merges.
Tests: stable identity across all incremental runs, no duplicates, timestamps updated.

## Edge Case Scenarios

### reobserve
Batch 1: A, B created. Bridge → A absorbs B.
Batches 2-3: B reappears with old identifiers (old cookie still active).
Tests: re-observation of merged child, identifier last_seen updated.

### selfbridge
Batch 1: A created (duid, uid, nuid).
Batch 2: Event carries A's duid + A's nuid (both already belong to A).
Tests: no spurious self-merge when both identifiers resolve to same identity.

### rapid_remerge
Batch 1: A, B created. Bridge → A absorbs B.
Batch 2: Z created. Bridge Z with A.
Tests: root gets absorbed, existing children repointed to new root.

### highfanin
Batch 1: Root + 6 children created. All 6 bridged to root sequentially.
Tests: high fan-in within single batch, cumulative merge event growth.

### bridge_creates
Batch 1: A created.
Batch 2: Bridge event carries unknown identifier + A's nuid. Engine creates new identity and merges.
Tests: identity created by bridge event, not by standalone page_view.

### three_way_bridge
Batch 1: A, B, C created (each with different identifier types). Single bridge event carries one identifier from each.
Tests: three-way merge from single event.

### cross_batch_triple
Batches 1-4: A created in batch 1. B created and bridged in batch 2. C in batch 3. D in batch 4.
Tests: progressive incremental merging, one child per batch.

### shared_identifier_collision
Batch 1: A created (uid=alice, duid, nuid). B created (uid=bob, duid, nuid_shared).
Batch 2: A gains nuid_shared via new event.
Config: `(unique :user_id)` prevents merge, so nuid_shared belongs to both A and B.
Tests: identifier_mapping uuid collision when same identifier maps to multiple identities.

### late_merge
Batch 1: A and B created separately.
Batch 3: Late-arriving bridge event (derived_tstamp before batch 1) carries identifiers from both.
Tests: SCD backdating of merge effective_at, late-arriving bridge.

### reverse_chain
Batch 1: A(t=10), B(t=5), C(t=1) created.
Batch 2: A+B bridge → B is root (older).
Batch 3: B+C bridge → C becomes root (oldest). A must cascade-repoint from B to C.
Tests: cascade repointing when root changes, SCD chain accuracy.

### merge_then_uid
Batch 1: A(duid, nuid) and B(duid, nuid) created.
Batch 2: A+B bridge (no uid on either, merge succeeds).
Batch 3: A gains uid=alice, B gains uid=bob.
Config: `(unique :user_id)`.
Tests: already-merged identities gaining conflicting unique identifiers.

### interleaved_merge
Batch 1: A, B, C, D created.
Batch 2: A+B bridge → A absorbs B.
Batch 3: C+D bridge → C absorbs D.
Batch 4: B+D bridge → cross-group merge through merged children. Engine resolves to A+C merge.
Tests: cross-group merge via intermediaries, 4-batch cascade repointing.

### deep_single_batch
Batch 1: A, B, C, D, E, F created.
Batch 2: Sequential bridges with drain between each: A+B, B+C, C+D, D+E, E+F.
Engine emits one cumulative merge event with all 5 children merged into A.
Tests: deep chain in single batch, cumulative merge event at depth.

### merge_limit
Batch 1: A created (duid_ML_A, uid_ML_U1). B, C, D created. Bridge B→A, C→A, D→A, taking A to the 3-merge limit.
Batch 2: E created (duid_ML_E, nuid_ML_E). Event `5f980243` then carries duid_ML_E + uid_ML_U1, which would merge E into A. A is at its limit, so the engine links instead and duid_ML_E ends up under both E and A.
Batch 3: The E device sends duid_ML_E + nuid_ML_E again. duid_ML_E now resolves to both E and A, so the event would merge them once more. A is still at its limit, so event `f57e6c42` is refused too and links nuid_ML_E to A.
Config: no unique identifier, so nothing but the limit blocks the merge. Events `5f980243` and `f57e6c42` carry `decision_reasons` `["merge_limit_exceeded"]`.
Tests: the reason names the identity the engine linked to, so duid_ML_E and nuid_ML_E are labelled `merge_limited` under both owners and sp_GILCR4ZVSNMENJ3IFFIW6AQSVA — A, the identity that was at its limit — is preferred over sp_GL7BJXWCMJPKFBDUV2QDZ4L3DI, the identity created for the E device in batch 2. `mapping_state` describes the identifier rather than one owner of it, so each identifier's two rows read `merge_limited` and only `is_preferred` separates them. The preference does not go through the recency heuristic: `snowplow__merge_limit_collapse` is left at its default `false` here, and before the event carried a reason both rows were `multiple` with neither preferred.

nuid_ML_E takes the label from batch 3 alone. It did not ride on `5f980243`; it carries the reason because the resend in batch 3 was refused as well. A reason is a fact about one event, and only the identifiers on that event take it, so do not spread a reason to the other identifiers of an owner.

uid_ML_U1 shows the other side of that grain. It rode on the flagged event, so it stores `merge_limit_exceeded` too, but it has one owner and is labelled `single`: `identity_count = 1` short-circuits ahead of the `merge_limited` branch. A rule keying off `decision_reasons` for a single-owner identifier would be reading a reason recorded about a different identifier's decision.

### merge_limit_partial
Batch 1: A created (duid_MLP_A, uid_MLP_U1). B, C, D created and bridged to A, taking it to the 3-merge limit. Batch 2: E created (duid_MLP_E, nuid_MLP_E), then duid_MLP_E + uid_MLP_U1 arrives under A. Batches 3-4: nuid_MLP_E, then duid_MLP_E again, arrive under A.
Config: no unique identifier. Events `c20197d3` (batch 2), `cfdbdc38` (batch 3) and `1e9c26cc` (batch 4) each carry `decision_reasons` `["merge_limit_exceeded"]`.
Tests: the label reaches the two identifiers through different batches: duid_MLP_E in batches 2 and 4, nuid_MLP_E in batch 3 only. Both are `merge_limited` under both owners with sp_QI5JZSCFYRNY7PJQVCQANC7OSA, A, preferred over sp_2F2HIJWD5ZOX3J5OM3GW3NUQBA, so the reason nuid_MLP_E took in batch 3 survives batch 4, which does not see it. The engine refuses this shape every time it recurs, so the seed cannot hold a reasonless control; the unit test `merge_limit_link_prefers_nothing_by_default` covers that case.

### merge_limit_tie
Batch 1: A created (duid_MLT_A, uid_MLT_U1). B, C, D created and bridged to A, taking it to the 3-merge limit. Batch 2: E created (duid_MLT_E). Batch 3: duid_MLT_E + uid_MLT_U1 arrives under A at the same derived_tstamp as E's own event. A is at its limit, so event `98d9c38f` is refused and carries `decision_reasons` `["merge_limit_exceeded"]`.
Config: no unique identifier.
Tests: both owners of duid_MLT_E hold last_seen_at 2020-01-03 00:00:08, so recency cannot separate them, and the label does. duid_MLT_E is `merge_limited` under both owners and sp_6B524JBBBJJEDCURC5ENI3EGXM, A, is preferred, with `snowplow__merge_limit_collapse` at its default.

### ttl_reappear
Batch 1: A created (duid_TTLR_A, nuid_TTLR_A). Batch 3: nuid_TTLR_A reappears alone under a new identity, A's state having expired from the engine.
Tests: an identifier belonging to two identities with no merge and no reason. nuid_TTLR_A is `multiple` under both owners and neither is preferred — the expiry case that is indistinguishable from a refused merge unless the engine says which it was, which is what `merge_limited` exists to separate out.

### merge_limit_split
Batch 1: A created (duid_MLS_A, uid_MLS_U1). B, C, D created. Bridge B→A, C→A, D→A, taking A to the 3-merge limit.
Batch 2: E created (duid_MLS_E, nuid_MLS_E). Event `05eb2b5c` then carries duid_MLS_E + uid_MLS_U1, which would merge E into A. A is at its limit, so the engine links instead and duid_MLS_E ends up under both E and A.
Batch 3: duid_MLS_X arrives on E's nuid.
Config: no unique identifier. Event `05eb2b5c` carries `decision_reasons` `["merge_limit_exceeded"]`.
Tests: the identifiers E collects after the refusal do not take the label. duid_MLS_E is `merge_limited` under both owners with sp_TRK3PCI2MJKJRE2YRNP3L4Z3UI, A, preferred, while nuid_MLS_E and duid_MLS_X, which reach E only through unrefused events, stay `single` under E. A unique conflict alongside the limit is in `merge_limit_unique_conflict`.

### merge_limit_chain
Batch 1: A created (duid_MLC_A, uid_MLC_A) and B created (duid_MLC_B, uid_MLC_B), each then bridged to one more identity, taking both to the 1-merge limit. The E device is first seen alone (duid_MLC_E) and gets its own identity.
Batch 2: Event `5fd11965` carries duid_MLC_E + uid_MLC_B, which would merge E's identity into B. B is at its limit, so the engine links duid_MLC_E to B and records `["merge_limit_exceeded"]`.
Batch 3: Event `e201d2c1` carries duid_MLC_E + uid_MLC_A, which would merge E's identity and B into A. A is at its limit too, so the engine links duid_MLC_E to A and records `["merge_limit_exceeded"]` again.
Config: no unique identifier.
Tests: duid_MLC_E has three owners and two of them carry the label, so the label alone no longer names one of them. The service links into the oldest identity, and A (sp_6I2WHENQDBJFTFTMSXUC7BSR44, created first) also holds duid_MLC_E's latest sighting (batch 3), which rules out a TTL eviction. All three rows are therefore `merge_limited` and A is preferred over B and E's own identity, with `snowplow__merge_limit_collapse` left at its default `false`, because the collapse rests on the label, not the recency heuristic. The eviction shape this guard exists to exclude is unit-tested (`chained_eviction_falls_back`), as is the guard that collapses when the younger owner holds the latest sighting (`chained_short_visit_still_collapses`).

### merge_limit_unique_conflict
Batch 1: B created (duid_MLUC_B) and bridged to the identity of nuid_MLUC_B2, taking B to the 1-merge limit. A created with uid_MLUC_X (duid_MLUC_A), and C with uid_MLUC_Y (duid_MLUC_C).
Batch 2: Event `53c80630` carries duid_MLUC_B + uid_MLUC_X, which would merge A into B, the older. B is at its limit, so the engine links uid_MLUC_X to B and records `["merge_limit_exceeded"]`. B and A now both hold uid_MLUC_X.
Batch 3: Event `6c46a41b` carries duid_MLUC_C + uid_MLUC_X. C holds uid_MLUC_Y, so the event conflicts on the unique identifier. The engine plans a merge of the two holders of uid_MLUC_X, A into B, and the limit refuses it, so the event carries `["unique_identifier_conflict", "merge_limit_exceeded"]`.
Config: `(unique :user_id)`.
Tests: a unique-identifier conflict alongside a merge limit suppresses the `merge_limited` label, because the conflict means the identities are legitimately separate and the limit alone must not name a winner. uid_MLUC_X, under B and A, and duid_MLUC_C, under B and C, stay `multiple` with neither preferred, although uid_MLUC_X also carries batch 2's `merge_limit_exceeded` on its own. Like the other unique-identifier scenarios it is left out of the `snowplow__merge_limit_collapse` comparison.

### degraded_split
Batch 1: A created (duid_DS_E); its event `c284a4cb` is a normal answer.
Batch 2: Event `246c1228` carries duid_DS_E + uid_DS_B while the database find is held past its time budget. The registry no longer remembers duid_DS_E, so the service falls back to the deterministic identity for uid_DS_B, B, and records `["degraded_fallback"]`. B is tied to duid_DS_E by nothing but that degraded answer.
Config: no unique identifier; `user_id` first in priority, so the fallback identity is B rather than A.
Tests: duid_DS_E is `multiple` under both owners and neither is preferred by default; B still counts as an owner. With `snowplow__merge_limit_collapse: true` the recency pick skips B, whose only tie is degraded, and prefers A.
