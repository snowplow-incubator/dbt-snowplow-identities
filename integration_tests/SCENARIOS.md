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
Batch 3: The E device sends duid_ML_E + nuid_ML_E again. duid_ML_E now resolves to A, so nuid_ML_E arrives under A too.
Config: no unique identifier, so nothing but the limit blocks the merge. The refused event carries `decision_reasons` `["merge_limit_exceeded"]`.
Tests: the reason names the identity the engine linked to, so duid_ML_E is labelled `merge_limited` under both owners and sp_GILCR4ZVSNMENJ3IFFIW6AQSVA — A, the identity that was at its limit — is preferred over sp_GL7BJXWCMJPKFBDUV2QDZ4L3DI, the identity created for the E device in batch 2. `mapping_state` describes the identifier rather than one owner of it, so both rows read `merge_limited` and only `is_preferred` separates them. The preference does not go through the recency heuristic: `snowplow__merge_limit_collapse` is left at its default `false` here, and before the event carried a reason both rows were `multiple` with neither preferred.

nuid_ML_E stays `multiple` under both owners, and that asymmetry with duid_ML_E is deliberate. The flagged event carried duid_ML_E and uid_ML_U1 only, so nuid_ML_E rode on no event the engine explained; it reaches two owners through ordinary events in batches 2 and 3, which is exactly the ambiguity `multiple` exists to report. Do not close the gap by spreading a reason to the other identifiers of an owner — a reason is a fact about one event, and only the identifiers on that event were part of what the engine decided.

uid_ML_U1 shows the other side of that grain. It rode on the flagged event, so it stores `merge_limit_exceeded` too, but it has one owner and is labelled `single`: `identity_count = 1` short-circuits ahead of the `merge_limited` branch. A rule keying off `decision_reasons` for a single-owner identifier would be reading a reason recorded about a different identifier's decision.

### merge_limit_partial
Batch 1: A created (duid_MLP_A, uid_MLP_U1). B, C, D created and bridged to A, taking it to the 3-merge limit. Batch 2: E created (duid_MLP_E, nuid_MLP_E), then duid_MLP_E + uid_MLP_U1 arrives under A. Batches 3-4: nuid_MLP_E, then duid_MLP_E again, arrive under A.
Config: no unique identifier. No event carries `decision_reasons`.
Tests: negative control for `merge_limit` — the same refused-merge shape with the reason absent leaves duid_MLP_E and nuid_MLP_E `multiple` under both owners and neither preferred, so the label comes from the reason and not from the shape.

### merge_limit_tie
Batch 1: A created (duid_MLT_A, uid_MLT_U1). B, C, D created and bridged to A, taking it to the 3-merge limit. Batch 2: E created (duid_MLT_E). Batch 3: duid_MLT_E + uid_MLT_U1 arrives under A at the same derived_tstamp as E's own event, carrying no reason.
Config: no unique identifier. No event carries `decision_reasons`.
Tests: both owners of duid_MLT_E hold last_seen_at 2020-01-03 00:00:08, so recency cannot separate them and the rank falls through to `created_at`, which can. The state is therefore `multiple` rather than `unranked`, and with `snowplow__merge_limit_collapse` at its default neither row is preferred.

### ttl_reappear
Batch 1: A created (duid_TTLR_A, nuid_TTLR_A). Batch 3: nuid_TTLR_A reappears alone under a new identity, A's state having expired from the engine.
Tests: an identifier belonging to two identities with no merge and no reason. nuid_TTLR_A is `multiple` under both owners and neither is preferred — the expiry case that is indistinguishable from a refused merge unless the engine says which it was, which is what `merge_limited` exists to separate out.

### merge_limit_split
Batch 1: A created (duid_MLS_A, uid_MLS_U1). B, C, D created. Bridge B→A, C→A, D→A, taking A to the 3-merge limit.
Batch 2: E created (duid_MLS_E, nuid_MLS_E). Event `05eb2b5c` then carries duid_MLS_E + uid_MLS_U1, which would merge E into A. A is at its limit, so the engine links instead and duid_MLS_E ends up under both E and A.
Batch 3: duid_MLS_X arrives on E's nuid.
Config: `(unique :user_id)`. The refused event carries `decision_reasons` `["merge_limit_exceeded", "unique_identifier_conflict"]`.
Tests: a unique-identifier conflict occurring alongside a merge limit suppresses the `merge_limited` label, because the conflict means the two identities are legitimately separate and the limit alone must not name a winner. duid_MLS_E stays `multiple` under both owners and neither is preferred. Dropping `unique_identifier_conflict` from that event labels both duid_MLS_E rows `merge_limited` and prefers A, so the suppression is load-bearing rather than a fixture that changes nothing.

Engine agreement, unverified and partly negative: the service would **not** emit both reasons for this event shape. `planOperations` only reaches `planConflictedResolution` when `detectUniqueIdentifierConflicts` finds more than one distinct unique-identifier value across the group's stored rows, registry and batch. Here uid_MLS_U1 is the only user_id in play (E carries none), so no conflict is flagged and the decision goes straight to `planMergeOrDowngrade`, which would record `merge_limit_exceeded` on its own. The two reasons *can* co-occur — `planConflictedResolution`'s default branch calls `planMergeOrDowngrade`, and that is where the limit blocks a merge — but only when the group is flagged as conflicted, carries exactly one unique value, and two or more of the resolved identities already store that same value, so that the merge the limit then blocks is between them. This scenario does not seed that shape; the reason pair is carried here to exercise the model's suppression guard. Read from `pkg/service/batch_processor.go` in the identity service, where `decision_reasons` does not exist yet, so this describes planned control flow rather than observed output.
