# Refinement note: schema interactions across regions and atomic batches

## Model scope in one paragraph

A versioned tree replicates from a source region to a receiver region. Schema
definitions are region-local: each region holds its own policy, its own version
target and its own upcaster catalog (startup declarations plus the durable runtime
registry), and nothing the replication stream carries changes the receiver's
catalog. The source ships each entry stamped at the version it held when it judged
and wrote it: a plain last-writer-wins write, a typed-CRDT delta, or one entry of
one replicated atomic batch. The receiver merges a plain write below the write
check, so it is stored with the stamp it carries even under strict ingest. Without
strict ingest a delta or batch entry is stored verbatim too. Under strict ingest
each delta and batch entry reaches the write choke point as a system-origin write
and is judged by the receiver's own policy and catalog: an entry whose stamp the
receiver cannot serve (newer than its target, or with no hop to it) or whose body
its policy refuses is diverted to the dead-letter queue; an admitted delta is lifted
to the receiver's target once, at the boundary; an admitted batch entry keeps its
stamp and is staged for its batch's decision, which reveals the staged entries
together while a diverted sibling is left out. The stream, the receiver's catalog,
policy and dead-letter collaborators, and the receiver's physical copy (which a
coordinated restore can hold receive-closed) can each be unavailable for a while:
nothing is judged, admitted or diverted while they are, and the entry is
re-delivered. A read decodes the stored stamp at the receiver's target or fails
explicitly. The receiver's operator registers the source's newer version and its
hop, and later advances the target, or never does. Safety holds whatever is
unavailable, with no fairness; convergence is conditional on the stream and
collaborators recovering, and on the receiver eventually holding the source's
version as its target.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `kind`, `body`, `stamp` | Each replicated entry's shape (plain write, delta, batch entry), its body as the receiver's policy sees it, and the version the source stamped. | `WalRecord`, `LatticeSchemaEnvelope.Encode`. |
| `strict` | Whether both strict-ingest flags (the global option and the tree's own) are on at the receiver. | `LatticeSchemaVersioningOptions.StrictIngest`, `LatticeSchemaVersionConfig.StrictIngest`. |
| `tgt`, `hop` | The receiver's version target, and whether its catalog holds the source's newer version and the hop to it. | `LatticeSchemaVersionConfig.TargetVersion`, `SchemaRuntimeRegistry.ResolveAsync`. |
| `st`, `tag` | Each entry's state at the receiver (pending, stored, staged, diverted) and the stamp it was stored with. | `LatticeGrain.ApplySetAsync`, `LatticeGrain.ApplyPreparedSetAsync`, `IReplicationApplyGrain.ReadStoredWithVersionAsync`. |
| `retried` | Whether an entry's attempt failed on an unavailable collaborator and is awaiting re-delivery. | `DeadLetterTrackingReplicationApplier.ApplyAsync`. |
| `decided` | Whether the batch's commit decision reached the receiver. | `LatticeGrain.ApplyTxTerminalAsync`. |
| `up`, `faults` | Which of the stream, the receiver's collaborators and its physical copy are available, and the faults left. | `ReplicationApplier.ApplyAsync`, `ICopyReceiveFenceGrain`. |
| `lastRead` | The last client read and whether it served a body or failed. | `LatticeSchemaVersionDecoder.DecodeAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Deliver(e)` | The receiver applies a replicated entry: a plain write merges below the check; under strict ingest a delta or batch entry is judged by the receiver's own policy and catalog and is admitted (a delta lifted to the target, a batch entry staged with its stamp) or diverted; an entry routed to a restored copy held closed, or judged while a collaborator is unavailable, fails and is re-delivered. | `ReplicationApplier.ApplyAsync`, `LatticeGrain.ApplySetAsync`, `LatticeGrain.ApplyCrdtDeltaWithExpiryAsync`, `LatticeGrain.ApplyPreparedSetAsync`, `LatticeWriteInterceptorEnforcement.InterceptPointAsync`. | Yes: `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_is_stored_verbatim_under_strict_ingest_and_its_read_fails_explicitly`, `SchemaCrossRegionIngestIntegrationTests.Strict_crdt_delta_stamped_past_the_receivers_target_is_dead_lettered_and_not_applied`, `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_stamped_past_the_receivers_target_is_dead_lettered_and_its_batch_siblings_commit_together`, `SchemaStrictIngestDeadLetterTests.Failed_policy_lookup_fails_the_write_instead_of_admitting_it`. |
| `Decide` | The batch's commit terminal, which the source ships only after every prepare of its saga was acknowledged, reaches the receiver and reveals the staged entries; a diverted entry counts as settled, so it does not hold the decision back. | `LatticeGrain.ApplyTxTerminalAsync`. | Yes: `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_the_receivers_policy_refuses_is_dead_lettered_and_its_batch_siblings_commit_together`, `StrictIngestLatePrepareIntegrationTests.Refused_lww_atomic_entry_whose_prepare_precedes_the_commit_decision_is_diverted_not_applied`. |
| `Read(e)` | A client read decodes the stored stamp at the receiver's target, upcasting along its own catalog, or fails explicitly. | `LatticeSchemaVersionDecoder.DecodeAsync`. | Yes: `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_is_stored_verbatim_under_strict_ingest_and_its_read_fails_explicitly`, `LatticeSchemaVersionDecoderTests.DecodeAsync_stale_value_with_no_upcast_path_throws_not_supported`. |
| `Register` | The receiver's operator registers the source's newer version and the hop to it in the receiver's own catalog; nothing stored is rewritten. | `SchemaRuntimeRegistry.RegisterAsync`, `LatticeSchemaControl.RegisterSchemaAsync`. | Yes: `SchemaTargetAdvanceIntegrationTests.AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read`, `SchemaRuntimeRegistryTests.A_later_registration_keeps_every_earlier_entry`. |
| `Advance` | The receiver's operator advances its target, which the facade admits only to a version registered in the receiver's catalog and reachable from its current target. | `LatticeSchemaControl.AdvanceTargetVersionAsync`. | Yes: `LatticeSchemaControlVersionTargetTests.AdvanceTargetVersion_refuses_a_target_with_no_registered_path`, `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_becomes_readable_once_the_receiver_advances_to_it`. |
| `Fault(c)` | Environment: the stream, a receiver collaborator, or the receiver's physical copy becomes unavailable. | No production counterpart: an outage or a coordinated restore's receive fence is the environment here. | Not applicable. |
| `Recover(c)` | Environment: it becomes available again. | No production counterpart: recovery from a fault. | Not applicable. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `StrictAdmitsOnlyServable` | Under strict ingest every delta and batch entry the receiver admits carries a body its own policy accepts and a stamp its own catalog serves; the source's verdict and the source's catalog are never trusted. | Yes: `SchemaCrossRegionIngestIntegrationTests.Strict_crdt_delta_stamped_past_the_receivers_target_is_dead_lettered_and_not_applied`, `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_stamped_past_the_receivers_target_is_dead_lettered_and_its_batch_siblings_commit_together`, `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_the_receivers_policy_refuses_is_dead_lettered_and_its_batch_siblings_commit_together`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_newer_than_target_dead_letters`. |
| `AtomicAllOrNothing` | A reader sees none of a replicated batch, or every entry of it the receiver admitted, and only once every entry was admitted or diverted. | Yes: `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_stamped_past_the_receivers_target_is_dead_lettered_and_its_batch_siblings_commit_together`, `CrossClusterAtomicVisibilityTests.Multi_shard_saga_through_the_applier_stays_invisible_until_every_source_shard_terminal_arrives`. |
| `StoredStampFixed` | A stored stamp is fixed when the entry lands: registering a definition, advancing the target, reading or folding never rewrites it. | Yes: `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_becomes_readable_once_the_receiver_advances_to_it`, `SchemaTargetAdvanceIntegrationTests.AdvanceTargetVersionAsync_rewrites_no_stored_value_and_a_later_registered_hop_upcasts_it_on_read`, `LatticeSchemaEnvelopeCodecTests.StripForFold_is_version_agnostic_across_targets`. |
| `CrdtLiftedAtIngest` | Under strict ingest a typed-CRDT delta is lifted to the receiver's target once, at the boundary, so every later fold strips the same stored stamp and never upcasts. | Yes: `SchemaCrossRegionIngestIntegrationTests.Strict_crdt_delta_stamped_below_the_receivers_target_is_lifted_to_the_target_once_at_ingest`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_crdt_delta_upcasts_and_reenvelopes_at_target`. |
| `UncheckedStoredVerbatim` | What is not judged - a plain write in any mode, or a delta or batch entry without strict ingest - is stored with the stamp it carried, never re-stamped and never diverted. | Yes: `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_is_stored_verbatim_under_strict_ingest_and_its_read_fails_explicitly`, `SchemaCrossRegionIngestIntegrationTests.Crdt_delta_stamped_past_the_target_of_a_tree_without_strict_ingest_is_stored_verbatim`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_non_strict_crdt_ingest_trusts_enveloped_delta`. |
| `NothingAdmittedWhileUnavailable` | A failed or cancelled policy lookup, catalog resolution or dead-letter append fails the strict-ingest write, so nothing is admitted or diverted until it is re-delivered with the collaborator available. | Yes: `SchemaStrictIngestDeadLetterTests.Failed_policy_lookup_fails_the_write_instead_of_admitting_it`, `SchemaStrictIngestDeadLetterTests.Failed_dead_letter_append_fails_the_write_instead_of_diverting_it`, `DeadLetterTrackingReplicationApplierTests.ApplyAsync_rethrows_failure_below_threshold`. |
| `NothingLandsOnFencedCopy` | No replicated entry of any kind - plain write, delta, full-state row or batch entry - lands on, or is diverted from, a physical copy a coordinated restore holds receive-closed; it is deferred and re-shipped. | Yes: `CoordinatedRestoreCopyReceiveFenceTests.Every_replicated_write_path_refuses_a_closed_copy`, `CoordinatedRestoreCopyReceiveFenceTests.A_peer_write_that_passed_the_receive_gate_before_the_pause_never_lands_on_the_restored_copy`, `FullStateCopyReceiveFenceIntegrationTests.Full_state_pn_counter_row_routed_to_a_closed_restored_copy_is_deferred_not_applied`, `FullStateCopyReceiveFenceIntegrationTests.Full_state_or_map_row_routed_to_a_closed_restored_copy_is_deferred_not_applied`, `FullStateCopyReceiveFenceIntegrationTests.Full_state_pn_counter_row_admitted_below_the_copy_floor_is_deferred_not_applied`, `FullStateCopyReceiveFenceIntegrationTests.Full_state_or_map_row_admitted_below_the_copy_floor_is_deferred_not_applied`. |
| `ReadsServeOrFail` | A read serves a body exactly when the receiver's own catalog decodes the stored stamp at its target, and otherwise fails explicitly rather than serving a body of another version. | Yes: `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_is_stored_verbatim_under_strict_ingest_and_its_read_fails_explicitly`, `LatticeSchemaVersionDecoderTests.DecodeAsync_stale_value_with_no_upcast_path_throws_not_supported`. |
| `AdmittedNeverDropped` | An entry the receiver admitted stays admitted: a diverted sibling is left out of its batch and the receiver commits the rest. | Yes: `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_the_receivers_policy_refuses_is_dead_lettered_and_its_batch_siblings_commit_together`, `StrictIngestLatePrepareIntegrationTests.Refused_lww_atomic_entry_whose_prepare_follows_the_commit_decision_is_diverted_not_applied`. |
| `EveryEntryResolved` | Once the stream and collaborators stop failing, every replicated entry is eventually admitted and visible, or diverted; a diversion settles its entry and does not strand its batch. | Yes: `SchemaCrossRegionIngestIntegrationTests.Strict_atomic_entry_stamped_past_the_receivers_target_is_dead_lettered_and_its_batch_siblings_commit_together`, `StrictIngestFullStateDivertIntegrationTests.Refused_full_state_pn_counter_row_under_strict_ingest_is_diverted_once_and_the_apply_completes`, `DeadLetterTrackingReplicationApplierTests.ApplyAsync_rethrows_failure_below_threshold`. |
| `ConvergesWhenAligned` | If the receiver eventually holds the source's version as its target, every entry it did not divert becomes readable there; a diverted entry stays in the dead-letter queue for the operator. | Yes: `SchemaCrossRegionIngestIntegrationTests.Plain_lww_entry_stamped_past_the_receivers_target_becomes_readable_once_the_receiver_advances_to_it`, `LatticeSchemaControlVersionTargetTests.AdvanceTargetVersion_refuses_a_target_with_no_registered_path`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiments. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, StrictAdmitsOnlyServable and AtomicAllOrNothing are state safety.
StoredStampFixed, CrdtLiftedAtIngest, UncheckedStoredVerbatim,
NothingAdmittedWhileUnavailable, NothingLandsOnFencedCopy, ReadsServeOrFail and
AdmittedNeverDropped are action safety, and hold under any availability with no
fairness: safety under unavailable collaborators. EveryEntryResolved is temporal
liveness under weak fairness of delivery, the decision and recovery, with faults
bounded. ConvergesWhenAligned is conditional convergence: it additionally assumes
that the receiver's target eventually stays at the source's newer version, an
operator step that is not fair. The base checks two entries and one fault; the
ThreeEntries variant re-checks every state and action property over three entries
and leaves both liveness properties bounded-out to the base for the TLC budget.

| Mutation | Classification |
|----------|----------------|
| TypeOKFaultUnbounded | Anchored action: Fault. |
| TypeOKRecoverOverCounts | Anchored action: Recover. |
| StrictAdmitsOnlyServableIgnoresCatalog | Anchored action: Deliver. |
| StrictAdmitsOnlyServableJudgedBySourcePolicy | Anchored action: Deliver. |
| AtomicAllOrNothingDecidesEarly | Anchored action: Decide. |
| AtomicAllOrNothingEntryAppliedUnstaged | Anchored action: Deliver. |
| StoredStampFixedAdvanceRestamps | Anchored action: Advance. |
| StoredStampFixedRegisterRestamps | Anchored action: Register. |
| CrdtLiftedAtIngestKeepsStamp | Anchored action: Deliver. |
| UncheckedStoredVerbatimStampedAtTarget | Anchored action: Deliver. |
| NothingAdmittedWhileUnavailableTrustsFailedLookup | Anchored action: Deliver. |
| NothingLandsOnFencedCopyFoldUnfenced | Anchored action: Deliver. |
| ReadsServeOrFailMisdecodes | Anchored action: Read. |
| AdmittedNeverDroppedDivertAbortsBatch | Anchored action: Deliver. |
| EveryEntryResolvedDiversionStrandsBatch | Anchored action: Decide. |
| EveryEntryResolvedStreamNeverRecovers | Anchored action: Recover. |
| ConvergesWhenAlignedAdvanceWithoutHop | Anchored action: Advance. |

## Detectors

`SchemaCrossRegionIngestIntegrationTests` runs a receiver region with the real
schema add-ons (enforcement composed ahead of versioning, both strict, a host
catalog declaring two versions and the hop between them) and delivers entries a
source region stamped through the real replication applier: plain writes stamped
past the receiver's target, deltas stamped above and below it with and without the
tree's strict flag, and two-entry atomic batches with one entry the receiver cannot
serve or its policy refuses. It reads the stored form through the apply seam and
the served form through the client. The unavailable-collaborator rows are pinned by
`SchemaStrictIngestDeadLetterTests`, the applier's re-delivery by
`DeadLetterTrackingReplicationApplierTests`, the copy receive fence by
`CoordinatedRestoreCopyReceiveFenceTests`, and the facade's catalog-checked advance
by `LatticeSchemaControlVersionTargetTests`.

## Rely-guarantee composition

This module restates, rather than instances, what other modules check:

| Assumption here | Discharged by |
|-----------------|---------------|
| The source judged each entry by its own policy and stamped it at its own target before shipping it. | `SchemaStageOrdering` (`ValidationBeforeCommit`, `StampedWhenVersioned`). |
| One delivery is judged once, a diversion is reported only after its durable record, and a failed append or lookup fails the write. | `SchemaStrictIngest` (`OneRecordPerDelivery`, `DiversionAckedOnlyWhenRecorded`, `AckedResolved`). |
| The facade advances a target only to a registered version reachable from the current one, and no configuration change or registration rewrites stored data. | `SchemaVersionFamily` (`ValidatedTarget`, `ConfigNeverRewritesData`). |
| A batch's terminal reaches the receiver only after every prepare of its saga, and reveals the staged entries together. | `AtomicCommitCrossCluster` (`RAllOrNothing`, `RNoStrandedPrepare`). |
| An entry routed to a copy a coordinated restore holds closed is refused and deferred, and the copy opens when the restore completes. | `BackupRestore` (`RestoredCutNotReAdvanced`, `RestoreConverges`). |
| The stream re-delivers every unacknowledged entry. | `Replication` (`EventualConvergence`). |

## Defect mutations whose fixes have landed

NothingLandsOnFencedCopyFoldUnfenced is #5294: the replication applier's
full-state CRDT fold read and wrote through the apply seam without the
replication-apply mark, so a bootstrap row admitted under a stale receive-gate
answer could land on a restored copy a coordinated restore held closed; the fold
now resolves its read and write under the mark and defers on a closed copy, like
every other replicated write path. It is fixed in the schema-assurance integration
bucket; this is integration-branch evidence, not a claim the fix is released.

## Deliberate abstraction gaps

- **Schema definitions are region-local.** Neither a policy, a version config nor
  a runtime catalog registration is replicated: the replication stream carries
  stamped values, never the definitions that interpret them, so the operator
  registers the same descriptors and hops, and installs the same host transforms,
  in every participating region before advancing a target or shipping values at a
  version. The model fixes the source's definitions and lets only the receiver's
  move.
  Production symbols: `SchemaRuntimeRegistry`, `LatticeSchemaRuntimeRegistryGrain`, `LatticeSchemaPolicyStore`, `LatticeSchemaVersionStore`.
- **Writes below the check stay unchecked.** A plain last-writer-wins replication
  apply, a backup restore and a tree merge write straight into the shards through
  the batched merge, never reaching the write stages, so even under strict ingest
  their values are stored with whatever stamp they carry and are neither judged nor
  dead-lettered. Only the read decoder stands between such a value and a reader.
  Production symbols: `LatticeGrain.ApplySetAsync`, `IShardRootGrain.MergeManyAsync`.
- **The merge-result observer does not see batched merges.** The opt-in
  merge-result observer runs on the foreground leaf write and on CRDT apply, local
  and replicated. The batched merges - a plain last-writer-wins replication apply,
  a tree merge, a merging restore, and the snapshot and resize copies - as well as
  bulk load and WAL replay, are below it: no merge-result violation is observed or
  reported for them. Where it does run, a violation is counted and logged and the
  merge is kept; the BPlusLeafGrainTests merge-observer cases pin both that and
  the batched merge never consulting it.
  Production symbols: `ILatticeMergeObserver`, `IShardRootGrain.MergeManyAsync`.
- **A diversion is permanent.** An entry diverted because the receiver's catalog
  could not yet serve its stamp is not re-applied when the receiver later registers
  the version; it stays in the dead-letter queue for the operator to inspect and
  re-submit. Strict ingest therefore turns a transient catalog mismatch into a
  divergence between regions that only out-of-band repair closes, which is why the
  convergence property exempts diverted entries.
  Production symbols: `LatticeSchemaDeadLetterStore.AppendAsync`.
- **CRDT state is one stamp per entry.** A delta folds into the key's state, and a
  trusted delta at an older version folds at its stored version with read-time
  upcasting lifting the converged state; the model keeps one stored stamp per
  entry and does not model the fold, its determinism under replay, or a full-state
  row, which strict ingest judges like a batch entry and keeps at its own stamp.
  A bootstrap hold's held copy and its mirror pass, which join CRDT rows on a
  physical copy id and must resolve the tree's registered CRDT shape for it, are
  not modelled either: the receiver here has one physical copy.
  Production symbols: `LatticeSchemaEnvelopeCodec.StripForFold`, `CrdtShapeRegistry`.
- **One batch, its terminal after its prepares.** All batch entries belong to one
  batch whose terminal follows its prepares. A prepare settled against a decision
  the receiver already holds (a late prepare after a bootstrap) is judged the same
  way and is SchemaStrictIngest's; cross-tree barriers, terminal tallies and
  every loss path are AtomicCommitCrossCluster's.
  Production symbols: `LatticeGrain.ApplyTxTerminalAsync`.
- **Two versions, one hop, one direction.** The source is never behind the
  receiver's target, the receiver starts one version behind, and the only catalog
  change is the registration of the source's version with its hop. Version
  families, foreign stamps, descending hops and conflicting registrations are
  SchemaVersionFamily's.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **Bodies are good or refused by the receiver.** Rule semantics, a policy change
  at either region and its propagation across silos are SchemaPolicyConfig's and
  SchemaStrictIngest's; here the receiver's policy is fixed and differs from the
  source's on one body.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **Collaborator outages are a single bounded flag each.** The catalog grain, the
  policy store and the dead-letter tree are one collaborator whose failure fails a
  judged write; the stream and the physical copy are one each. How the applier
  counts failures and parks an entry is SchemaStrictIngest's.
  Production symbols: `DeadLetterTrackingReplicationApplier.ApplyAsync`, `CopyReceiveFencedException`.
