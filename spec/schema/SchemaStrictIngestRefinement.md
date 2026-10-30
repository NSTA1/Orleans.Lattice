# Refinement note: strict ingest and the durable dead-letter queue

## Model scope in one paragraph

A receiver ingests a replicated stream under the supported composition
(enforcement then versioning) with strict ingest on globally and per tree. Each
entry reaches the `LatticeGrain` write choke point as a system-origin point
write, whether it is a typed-CRDT delta, a full-state CRDT row or an entry of a
replicated atomic batch, and whether or not its batch's decision reached the
receiver first. The strict stages look up the policy the silo holds and judge
the body a reader is served (an envelope the decoder cannot serve fails
closed). A refused entry is diverted: the stage appends a record to the
dead-letter queue and only then reports the diversion, so the write is dropped
and acknowledged. An accepted entry is committed. The policy lookup, the
dead-letter append and the commit can each fail or be cancelled before anything
becomes durable, or become durable and lose their response; every such failure
fails the write. The replication applier re-throws a failure so the sender
re-delivers, and parks the entry after repeated failures. The transport can lose
an acknowledgement, so a handled entry is delivered and judged again, and an
administrator can change the policy, which reaches the receiving silo later.
Dead-letter records are therefore at least once: re-delivery may add a
duplicate, but a record is never lost, overwritten or claimed without being
durable.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `val`, `late` | Each replicated entry's bytes, and whether its batch's decision was recorded on the receiver before it arrived. | `ReplicationApplier.ApplyAsync`, `LatticeGrain.ApplyPreparedSetAsync`. |
| `pol`, `seen` | The tree's durable policy and the one the receiving silo holds. | `LatticeSchemaPolicyStore`, `LatticeSchemaPolicyProvider.GetCompiledPolicyAsync`. |
| `sstate`, `lostLeft` | What the sender knows of each entry's acknowledgement, and the acknowledgements the transport may still lose. | `ReplicationApplier.ApplyAsync`. |
| `cur`, `rphase` | The entry in hand and its progress through the choke point. | `LatticeWriteInterceptorEnforcement.InterceptPointAsync`. |
| `judged` | The policy the delivery was judged by. | `LatticeSchemaWriteInterceptor.OnWriteAsync`. |
| `appended`, `reported`, `dlq` | Whether this delivery appended a record or reported a diversion, and the durable records per entry. | `LatticeSchemaDeadLetterStore.AppendAsync`, `LatticeWriteDecision.DeadLetter`. |
| `fails`, `parked` | The applier's failure count per entry and the entries it quarantined. | `DeadLetterTrackingReplicationApplier.ApplyAsync`. |
| `stored`, `storedUnder` | The committed entries and the policy each was judged by. | `LatticeGrain.SetAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Deliver(e)` | A replicated entry reaches the choke point as a system-origin point write that the strict composition intercepts. | `ReplicationApplier.ApplyAsync`, `LatticeGrain.ApplyPreparedSetAsync`, `LatticeWriteInterceptorEnforcement.InterceptPointAsync`. | Yes: `LatticeWriteInterceptorEnforcementTests.Skips_is_false_on_a_system_origin_turn_for_an_opting_interceptor`, `SchemaWriteStageRegistrationTests.Composed_registration_opts_into_system_origin_when_either_stage_is_globally_strict`, `SchemaStrictIngestDeadLetterTests.Refused_entry_is_diverted_only_after_its_record_is_durable`, `StrictIngestLatePrepareIntegrationTests.Refused_lww_atomic_entry_whose_prepare_precedes_the_commit_decision_is_diverted_not_applied`, `StrictIngestLatePrepareIntegrationTests.Refused_lww_atomic_entry_whose_prepare_follows_the_commit_decision_is_diverted_not_applied`. |
| `Judge(r)` | Look up the policy the silo holds and judge the served body; a lookup that throws or is cancelled fails the write. | `LatticeSchemaWriteInterceptor.OnWriteAsync`, `ILatticeSchemaPolicyProvider.GetCompiledPolicyAsync`. | Yes: `SchemaStrictIngestDeadLetterTests.Failed_policy_lookup_fails_the_write_instead_of_admitting_it`, `SchemaStrictIngestDeadLetterTests.Each_delivery_is_judged_by_the_policy_the_silo_holds_at_that_delivery`, `SchemaEnforcementEnvelopedBodyTests.Strict_ingest_of_enveloped_value_with_noncompliant_body_is_dead_lettered`. |
| `DeadLetter(r)` | Append the dead-letter record, then report the diversion; an append that fails, is cancelled or loses its response fails the write. | `LatticeSchemaDeadLetterStore.AppendAsync`, `LatticeSchemaWriteInterceptor.OnWriteAsync`, `LatticeSchemaVersionWriteInterceptor.OnWriteAsync`. | Yes: `SchemaStrictIngestDeadLetterTests.Refused_entry_is_diverted_only_after_its_record_is_durable`, `SchemaStrictIngestDeadLetterTests.Failed_dead_letter_append_fails_the_write_instead_of_diverting_it`, `SchemaStrictIngestDeadLetterTests.Cancelled_dead_letter_append_fails_the_write_instead_of_diverting_it`, `SchemaStrictIngestDeadLetterTests.Dead_letter_append_whose_response_is_lost_fails_the_write_and_redelivery_records_it_again`. |
| `Commit(r)` | The choke point proceeds with the admitted bytes; a failed WAL append fails the write. | `LatticeWriteInterceptorEnforcement.InterceptPointAsync`, `LatticeGrain.SetAsync`. | Yes: `LatticeWriteInterceptorEnforcementTests.InterceptPoint_accept_proceeds_with_the_original_value`, `SchemaStrictIngestDeadLetterTests.Valid_enveloped_entry_is_admitted_verbatim_with_no_record`. |
| `Settle` | The applier re-throws a failed apply below its retry threshold, so the sender re-delivers, and parks it at the threshold. | `DeadLetterTrackingReplicationApplier.ApplyAsync`. | Yes: `DeadLetterTrackingReplicationApplierTests.ApplyAsync_rethrows_failure_below_threshold`, `DeadLetterTrackingReplicationApplierTests.ApplyAsync_parks_entry_when_threshold_reached`. |
| `LoseAck(e)` | Environment: the transport loses an acknowledgement, so the entry is delivered again. | No production counterpart: a lost reply is a transport fault. | Not applicable. |
| `ChangePolicy` | An administrator replaces the tree's policy. | `LatticeSchemaAdmin.SetPolicyAsync`. | Yes: `LatticeSchemaAdminTests.SetPolicyAsync_delegates_and_invalidates_cache`. |
| `Propagate` | The change reaches the receiving silo, which evicts its cached policy. | `LatticeSchemaPolicyProvider.OnMutationAsync`. | Yes: `LatticeSchemaPolicyProviderTests.OnMutationAsync_policy_tree_write_evicts_affected_tree`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `StoredValidWhenJudged` | Strict ingest commits no entry the policy its silo held refuses, no undecodable envelope and nothing it never judged, whatever the order in which the entry and its batch's decision arrived. | Yes: `SchemaStrictIngestDeadLetterTests.Undecodable_enveloped_entry_is_diverted_never_admitted`, `SchemaStrictIngestDeadLetterTests.Failed_policy_lookup_fails_the_write_instead_of_admitting_it`, `SchemaEnforcementEnvelopedBodyTests.Strict_ingest_of_enveloped_value_with_noncompliant_body_is_dead_lettered`, `StrictIngestLatePrepareIntegrationTests.Refused_lww_atomic_entry_whose_prepare_follows_the_commit_decision_is_diverted_not_applied`. |
| `AckedResolved` | An ingested entry is acknowledged only once it is committed, diverted with a durable record, or parked; a failed or cancelled lookup, append or commit fails the write. | Yes: `SchemaStrictIngestDeadLetterTests.Failed_dead_letter_append_fails_the_write_instead_of_diverting_it`, `SchemaStrictIngestDeadLetterTests.Cancelled_dead_letter_append_fails_the_write_instead_of_diverting_it`, `SchemaStrictIngestDeadLetterTests.Failed_policy_lookup_fails_the_write_instead_of_admitting_it`, `DeadLetterTrackingReplicationApplierTests.ApplyAsync_rethrows_failure_below_threshold`. |
| `OneRecordPerDelivery` | A delivery appends at most one dead-letter record; duplicates come only from re-delivery. | Yes: `SchemaStrictIngestDeadLetterTests.Refused_entry_is_diverted_only_after_its_record_is_durable`, `SchemaStrictIngestDeadLetterTests.Dead_letter_append_whose_response_is_lost_fails_the_write_and_redelivery_records_it_again`, `StrictIngestFullStateDivertIntegrationTests.Refused_full_state_pn_counter_row_under_strict_ingest_is_diverted_once_and_the_apply_completes`, `StrictIngestFullStateDivertIntegrationTests.Refused_full_state_or_map_row_under_strict_ingest_is_diverted_once_and_the_apply_completes`. |
| `DiversionAckedOnlyWhenRecorded` | A diversion is reported only after its own record is durable, and a record never replaces an earlier one for the same key and instant. | Yes: `SchemaStrictIngestDeadLetterTests.Failed_dead_letter_append_fails_the_write_instead_of_diverting_it`, `SchemaStrictIngestDeadLetterTests.Repeated_diversions_of_one_key_at_one_instant_are_all_retained`. |
| `DiversionSettles` | A reported diversion drops the write and acknowledges the entry; it is never re-surfaced as a failure for re-delivery, so an entry is diverted exactly once per delivery. | Yes: `SchemaStrictIngestDeadLetterTests.Refused_entry_is_diverted_only_after_its_record_is_durable`, `StrictIngestFullStateDivertIntegrationTests.Refused_full_state_pn_counter_row_under_strict_ingest_is_diverted_once_and_the_apply_completes`, `StrictIngestFullStateDivertIntegrationTests.Refused_full_state_or_map_row_under_strict_ingest_is_diverted_once_and_the_apply_completes`. |
| `DeadLettersOnlyRefused` | Only an entry the judging policy refuses is dead-lettered; a valid body inside an envelope the decoder serves is admitted verbatim. | Yes: `SchemaStrictIngestDeadLetterTests.Valid_enveloped_entry_is_admitted_verbatim_with_no_record`, `SchemaEnforcementEnvelopedBodyTests.Strict_ingest_of_enveloped_value_with_compliant_body_is_accepted_verbatim`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_upcastable_item_accepts`. |
| `JudgedByHeldPolicy` | Every delivery is judged afresh by the policy its silo holds at that delivery, never by an earlier verdict. | Yes: `SchemaStrictIngestDeadLetterTests.Each_delivery_is_judged_by_the_policy_the_silo_holds_at_that_delivery`. |
| `ParkOnlyAfterRepeatedFailure` | The applier parks an entry only when a failed apply reaches its retry threshold. | Yes: `DeadLetterTrackingReplicationApplierTests.ApplyAsync_rethrows_failure_below_threshold`, `DeadLetterTrackingReplicationApplierTests.ApplyAsync_parks_entry_when_threshold_reached`. |
| `EveryEntryAcknowledged` | Every ingested entry is eventually acknowledged: committed, diverted or parked. | Yes: `DeadLetterTrackingReplicationApplierTests.ApplyAsync_parks_entry_when_threshold_reached`, `SchemaStrictIngestDeadLetterTests.Dead_letter_append_whose_response_is_lost_fails_the_write_and_redelivery_records_it_again`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiments. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, StoredValidWhenJudged and AckedResolved are state safety.
OneRecordPerDelivery, DiversionAckedOnlyWhenRecorded, DiversionSettles,
DeadLettersOnlyRefused, JudgedByHeldPolicy and ParkOnlyAfterRepeatedFailure are
action safety. EveryEntryAcknowledged is temporal liveness under weak fairness
of delivery, judgement, the append, the commit and the applier's settlement;
the transport's lost acknowledgements are bounded and unfair. The base checks
one entry; the TwoEntries variant re-checks every state and action property
over two entries, and leaves EveryEntryAcknowledged bounded-out to the base for
the TLC budget.

| Mutation | Classification |
|----------|----------------|
| TypeOKPropagateUndeclaredPolicy | Anchored action: Propagate. |
| TypeOKChangePolicyUndeclared | Anchored action: ChangePolicy. |
| StoredValidWhenJudgedTrustsFailedLookup | Anchored action: Judge. |
| StoredValidWhenJudgedUndecodableTrusted | Anchored action: Judge. |
| StoredValidWhenJudgedLatePrepareUnjudged | Anchored action: Deliver. |
| AckedResolvedFailedAppendAcknowledged | Anchored action: DeadLetter. |
| AckedResolvedFailedCommitAcknowledged | Anchored action: Commit. |
| AckedResolvedSpuriousAck | Anchored action: LoseAck. |
| OneRecordPerDeliveryAppendRetried | Anchored action: DeadLetter. |
| DiversionAckedOnlyWhenRecordedOverwrites | Anchored action: DeadLetter. |
| DiversionSettlesReportedAsFailure | Anchored action: DeadLetter. |
| DeadLettersOnlyRefusedJudgesEnvelope | Anchored action: Judge. |
| JudgedByHeldPolicyReadsDurable | Anchored action: Judge. |
| ParkOnlyAfterRepeatedFailureParksFirst | Anchored action: Settle. |
| EveryEntryAcknowledgedParkUnacknowledged | Anchored action: Settle. |
| EveryEntryAcknowledgedNoRedelivery | Anchored action: Deliver. |

## Detectors

`SchemaStrictIngestDeadLetterTests` drives the real registration extensions in
the supported order, both stages globally strict, through the core enforcement
primitive on a system-origin turn, which is where every strict-ingest path
meets the stages. Only the per-tree providers and the dead-letter store are
substituted: the store is scripted to succeed, fail, honour cancellation, or
record and then lose its response, and the policy provider to fail or to answer
with a changed policy between deliveries. Its last test runs the real
dead-letter store over an in-memory tree. The applier's retry and park
behaviour is pinned by `DeadLetterTrackingReplicationApplierTests`, and the
policy change and eviction by the `SchemaPolicyConfig` detectors.
`StrictIngestFullStateDivertIntegrationTests` drives a refused full-state CRDT
row through a real replication receiver and checks it is diverted once and the
apply completes. `StrictIngestLatePrepareIntegrationTests` replicates a refused
entry of an atomic batch whose prepare precedes, and one whose prepare follows,
the batch's commit decision, and checks each is diverted and never applied.

## Defect mutations whose fixes have landed

DeadLettersOnlyRefusedJudgesEnvelope and StoredValidWhenJudgedUndecodableTrusted
are the two halves of #5239 seen from strict ingest: the enforcement stage judged
an already-enveloped value's header, so valid replicated entries were diverted,
and the fix judges the body the read decoder serves, failing closed on an
envelope it cannot serve. DiversionSettlesReportedAsFailure and
OneRecordPerDeliveryAppendRetried are #5265: the full-state CRDT row fold read
a diverted conditional write as a version mismatch, so one delivery appended a
record per retry of its fold and the apply was retried and finally parked
instead of acknowledged; the fold now settles on a diversion.
StoredValidWhenJudgedLatePrepareUnjudged is #5266: an LWW prepare that arrived
after the receiver recorded its batch's commit was applied below the choke
point, so strict ingest never judged it; the settle now judges it first. All are
fixed in the schema-assurance integration bucket; this is integration-branch
evidence, not a claim the fixes are released.

## Deliberate abstraction gaps

- **One supported configuration.** Strict ingest is on globally and per tree
  under enforcement composed ahead of versioning. Which stages run, the
  system-origin opt-in and the per-stage flags for every other registration
  and flag combination are `SchemaStageOrdering`'s, whose checked properties
  this module relies on.
  No production symbol: the gap delegates to another model, not to a production mechanism.
- **Writes below the choke point.** Plain last-writer-wins replication apply,
  backup restore, tree merge, WAL replay, splits and moves never reach the
  strict stages, so nothing here holds for them and strict ingest is not a
  universal validation guarantee.
  Production symbols: `LatticeWriteInterceptorEnforcement.Skips`.
- **Body validity and decodability are abstract.** A body is good, legacy
  (valid only under the previous policy) or bad, and an envelope is servable or
  not. Rule semantics, the upcaster graph, the runtime catalog and an older
  upcastable tag (which strict ingest admits with its own tag) are left to the
  models that own them.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **One policy change, propagated eventually.** The model changes the policy
  once and lets the receiving silo learn of it at any later step. The bound on
  that lag, the cache-load race and the admin silo's local eviction are
  `SchemaPolicyConfig`'s. A value committed under the previous policy is not
  retracted when a re-delivery is refused under the new one; bringing stored
  values into line is remediation's job.
  Production symbols: `LatticeSchemaPolicyProvider.GetCompiledPolicyAsync`.
- **Dead-letter records are at least once and are never retired.** A
  re-delivered entry that is refused again is recorded again, under a fresh key
  suffix. Nothing deduplicates records, and replay, requeue, retention and caps
  are not shipped.
  Production symbols: `SchemaDeadLetterKey.Encode`, `LatticeSchemaDeadLetterStore.AppendAsync`.
- **One entry in flight, re-judged on every re-delivery.** The receiver handles
  one entry at a time and judges every re-delivery afresh; the replication
  dedupe cache and high-water mark that skip some re-deliveries only remove
  behaviours. Batch-level abort and drop semantics are `SchemaStageOrdering`'s,
  and a saga's terminal and its never-parked deferral are the atomic and
  replication models'.
  Production symbols: `DeadLetterTrackingReplicationApplier.ApplyAsync`.
- **CRDT deltas and opaque values.** A CRDT delta that is not JSON is admitted
  unvalidated by enforcement, and an older delta's upcast and re-stamp belong to
  the versioning and replication models; here every entry is a whole value.
  Production symbols: `SchemaValueChecks.IsWellFormedJson`.
