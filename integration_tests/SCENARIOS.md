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
