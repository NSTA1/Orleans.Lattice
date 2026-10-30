# Refinement note: schema write stage ordering and registration combinations

## Model scope in one paragraph

A write reaches the `LatticeGrain` write choke point as a batch of one or more
entries, local or system-origin, each raw or already inside a version envelope
whose tag is the target version, an older upcastable one, or one that cannot be
upcast. The registered add-ons decide which stages run: none, enforcement only,
versioning only, enforcement then versioning (the supported composition), or
versioning registered first, which the registration accepts and which leaves
enforcement alone. The core skips the interceptor for system-origin writes
unless a registered stage's global strict flag opts it in; each stage then
applies its own per-tree strict flag. Enforcement judges the body a reader is served (the read decoder strips the envelope and upcasts; with versioning not registered the whole value is judged; a value that cannot be decoded fails closed),
rejects a local violation and dead-letters strict ingest; versioning stamps a
raw entry at the target and, on strict ingest, dead-letters an enveloped entry
it cannot upcast. A refusal short-circuits the chain; an atomic batch aborts on
any refusal and a non-atomic batch drops a dead-lettered entry. Writes that
land below the choke point are environment steps that no stage sees.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `reg` | Which add-ons are registered, and in which order. | `LatticeSchemaEnforcementServiceCollectionExtensions.AddLatticeSchemaEnforcement`, `LatticeSchemaVersioningServiceCollectionExtensions.AddLatticeSchemaVersioning`. |
| `gStrictE`, `gStrictV` | Each stage's global strict-ingest option. | `LatticeSchemaEnforcementOptions.StrictIngest`, `LatticeSchemaVersioningOptions.StrictIngest`. |
| `policyOn`, `tStrictE` | The tree's policy and its per-tree strict flag. | `CompiledSchemaPolicy.StrictIngest`. |
| `versionOn`, `tStrictV` | The tree's version config and its per-tree strict flag. | `LatticeSchemaVersionConfig.StrictIngest`. |
| `phase`, `path`, `idx` | The write's progress through the gate and the stage chain. | `LatticeWriteInterceptorEnforcement.Skips`, `CompositeLatticeWriteInterceptor.OnWriteAsync`. |
| `origin`, `atomic`, `req`, `ei` | The submitted write: its origin, batch mode, entries and the entry under judgement. | `LatticeAccessGateContext.IsSystemOrigin`, `LatticeWriteInterceptorEnforcement.InterceptEntriesAsync`. |
| `val`, `kept`, `dropped` | The bytes threaded between stages, the entries kept and the entries dropped. | `CompositeLatticeWriteInterceptor.OnWriteAsync`, `LatticeWriteInterceptorEnforcement.InterceptEntriesAsync`. |
| `outcome`, `refusedBy`, `dlq` | The write's verdict, the refusing stage and dead-letter appends. | `LatticeWriteRejectedException`, `LatticeSchemaDeadLetterStore.AppendAsync`. |
| `stored` | What the choke point appended. | `LatticeWriteInterceptorEnforcement.InterceptPointAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Submit(o,a,r)` | A point or batch write reaches the choke point after the access gate. | `LatticeWriteInterceptorEnforcement.InterceptPointAsync`, `LatticeWriteInterceptorEnforcement.InterceptEntriesAsync`. | Yes: `LatticeWriteInterceptorEnforcementTests.InterceptPoint_accept_proceeds_with_the_original_value`, `LatticeWriteInterceptorEnforcementTests.InterceptEntries_all_accept_returns_the_same_list_reference`. |
| `Admit` | Skip the null interceptor, and a system-origin write unless the interceptor opts in. | `LatticeWriteInterceptorEnforcement.Skips`, `CompositeLatticeWriteInterceptor.InterceptsSystemOrigin`. | Yes: `LatticeWriteInterceptorEnforcementTests.Skips_is_true_for_the_null_interceptor`, `LatticeWriteInterceptorEnforcementTests.Skips_is_true_on_a_system_origin_turn_for_a_non_opting_interceptor`, `LatticeWriteInterceptorEnforcementTests.Skips_is_false_on_a_system_origin_turn_for_an_opting_interceptor`, `SchemaWriteStageRegistrationTests.Composed_registration_opts_into_system_origin_when_either_stage_is_globally_strict`. |
| `RunEnforcement` | Judge the entry's served body, failing closed when it cannot be decoded; reject a local violation, dead-letter or trust strict or lax ingest. | `LatticeSchemaWriteInterceptor.OnWriteAsync`. | Yes: `LatticeSchemaWriteInterceptorTests.OnWriteAsync_invalid_local_value_throws_violation`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_strict_ingest_invalid_item_dead_letters`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_non_strict_policy_trusts_ingest`, `SchemaWriteStageRegistrationTests.Enforcement_only_rejects_an_invalid_local_write_and_accepts_a_valid_one_unchanged`. |
| `RunVersioning` | Stamp a raw entry at the target; keep an enveloped one, or dead-letter it on strict ingest when it cannot be upcast. | `LatticeSchemaVersionWriteInterceptor.OnWriteAsync`. | Yes: `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_local_write_stamps_envelope_at_target`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_already_enveloped_local_write_accepts_verbatim`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_non_upcastable_item_dead_letters`, `SchemaWriteStageRegistrationTests.Versioning_only_stamps_a_raw_local_write_at_the_target`. |
| `Commit` | Append the entries the chain kept, with their transformed bytes. | `LatticeWriteInterceptorEnforcement.InterceptPointAsync`, `LatticeWriteInterceptorEnforcement.InterceptEntriesAsync`. | Yes: `LatticeWriteInterceptorEnforcementTests.InterceptPoint_accept_transformed_proceeds_with_the_replacement_value`, `LatticeWriteInterceptorEnforcementTests.InterceptEntries_transform_replaces_only_the_targeted_value`, `CompositeLatticeWriteInterceptorTests.OnWriteAsync_threads_transformed_value_into_next_stage`. |
| `Bypass(r)` | Environment: a write below the choke point, which no stage sees. | No production counterpart: plain LWW replication apply, backup restore and tree merge never reach the interceptor. | Not applicable. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `EnforcementSeesSubmittedValue` | In the composed interceptor enforcement runs first, on the value as submitted, never on the envelope versioning adds. | Yes: `SchemaWriteStageRegistrationTests.Composed_registration_runs_enforcement_on_the_unstamped_value`, `CompositeLatticeWriteInterceptorTests.OnWriteAsync_second_stage_sees_first_stages_transform`. |
| `ValidationBeforeCommit` | Under every registration that includes enforcement, the reversed one included, a local write whose served body is invalid or cannot be decoded is rejected before the WAL append. | Yes: `SchemaEnforcementEnvelopedBodyTests.Local_write_of_enveloped_value_newer_than_target_is_rejected`, `SchemaEnforcementEnvelopedBodyTests.Without_versioning_an_envelope_shaped_prefix_is_judged_as_part_of_the_value`, `SchemaWriteStageRegistrationTests.Enforcement_only_rejects_an_invalid_local_write_and_accepts_a_valid_one_unchanged`, `SchemaWriteStageRegistrationTests.Composed_registration_validates_the_submitted_value_then_stamps_it`, `SchemaWriteStageRegistrationTests.Reversed_registration_keeps_enforcement_and_drops_stamping`. |
| `RefusalAppliesNothing` | A rejected write, or an atomic batch aborted by a dead-letter, applies no entry. | Yes: `LatticeWriteInterceptorEnforcementTests.InterceptPoint_reject_throws_with_context`, `LatticeWriteInterceptorEnforcementTests.InterceptEntries_reject_aborts_the_batch_on_both_modes`, `CrossTreeSchemaEnforcementIntegrationTests.Cross_tree_atomic_write_rejects_a_non_compliant_leg_and_mutates_no_tree`. |
| `BatchAccounting` | Every entry is applied or dropped as dead-lettered, and an atomic batch never drops one. | Yes: `LatticeWriteInterceptorEnforcementTests.InterceptEntries_atomic_dead_letter_aborts_the_batch`, `LatticeWriteInterceptorEnforcementTests.InterceptEntries_non_atomic_dead_letter_drops_the_entry`, `LatticeWriteInterceptorEnforcementTests.InterceptPoint_dead_letter_does_not_proceed`. |
| `ValidBodyNeverRefusedByEnforcement` | Enforcement refuses only an entry whose served body breaks the policy or cannot be decoded; an entry inside a version envelope that decodes to a valid body is accepted. | Yes: `SchemaEnforcementEnvelopedBodyTests.Local_rewrite_of_enveloped_value_with_compliant_body_is_accepted_verbatim`, `SchemaEnforcementEnvelopedBodyTests.Strict_ingest_of_enveloped_value_with_compliant_body_is_accepted_verbatim`, `SchemaEnforcementEnvelopedBodyTests.Max_length_rule_counts_the_body_not_the_envelope_header`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_valid_local_value_accepts`. |
| `StampedWhenVersioned` | Under a supported registration with versioning, every intercepted entry is stored self-describing. | Yes: `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_local_write_stamps_envelope_at_target`, `SchemaWriteStageRegistrationTests.Versioning_only_stamps_a_raw_local_write_at_the_target`, `SchemaWriteStageRegistrationTests.Composed_registration_validates_the_submitted_value_then_stamps_it`. |
| `EnvelopedValuesVerbatim` | An already-enveloped entry is stored with its own tag and never re-stamped. | Yes: `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_already_enveloped_local_write_accepts_verbatim`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_upcastable_item_accepts`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_non_strict_tree_trusts_enveloped_ingest`. |
| `SystemOriginGate` | A system-origin write is intercepted exactly when a registered stage's global strict option is on. | Yes: `CompositeLatticeWriteInterceptorTests.InterceptsSystemOrigin_is_true_when_any_stage_intercepts`, `SchemaWriteStageRegistrationTests.Composed_registration_opts_into_system_origin_when_either_stage_is_globally_strict`, `LatticeWriteInterceptorEnforcementTests.InterceptPoint_skipped_system_origin_proceeds_without_calling`. |
| `StrictFlagsPerStage` | Each stage dead-letters only under its own per-tree strict flag; otherwise it trusts the ingest. | Yes: `LatticeSchemaWriteInterceptorTests.OnWriteAsync_non_strict_policy_trusts_ingest`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_non_strict_tree_trusts_enveloped_ingest`. |
| `StrictIngestDiverts` | Strict ingest stores no entry with a violating or undecodable body and no entry that cannot be upcast to the target. | Yes: `SchemaEnforcementEnvelopedBodyTests.Strict_ingest_of_enveloped_value_with_noncompliant_body_is_dead_lettered`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_strict_ingest_invalid_item_dead_letters`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_non_upcastable_item_dead_letters`, `LatticeSchemaVersionWriteInterceptorTests.OnWriteAsync_strict_ingest_newer_than_target_dead_letters`. |
| `LocalRefusalRejects` | A local violation is thrown to the caller, never quietly dead-lettered. | Yes: `LatticeSchemaWriteInterceptorTests.OnWriteAsync_invalid_local_value_throws_violation`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_invalid_local_value_persists_nothing_to_dlq`. |
| `WriteDecides` | Every intercepted write reaches accept, reject or dead-letter; a refusal short-circuits the remaining stages. | Yes: `CompositeLatticeWriteInterceptorTests.OnWriteAsync_all_accept_returns_plain_accept`, `CompositeLatticeWriteInterceptorTests.OnWriteAsync_reject_short_circuits_remaining_stages`, `CompositeLatticeWriteInterceptorTests.OnWriteAsync_dead_letter_short_circuits_remaining_stages`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK and EnforcementSeesSubmittedValue are state safety. ValidationBeforeCommit,
RefusalAppliesNothing, BatchAccounting, ValidBodyNeverRefusedByEnforcement,
StampedWhenVersioned, EnvelopedValuesVerbatim, SystemOriginGate,
StrictFlagsPerStage, StrictIngestDiverts and LocalRefusalRejects are action
safety. WriteDecides is temporal liveness under weak fairness of the gate, each
stage and the append. The base checks a point write; the Batch variant re-checks
every property, the temporal one included, over two-entry atomic and non-atomic
batches.

| Mutation | Classification |
|----------|----------------|
| TypeOKSubmitUndeclaredOrigin | Anchored action: Submit. |
| TypeOKBypassUndeclaredPath | Anchored action: Bypass. |
| EnforcementSeesSubmittedValueStampFirst | Anchored definition: Chain, the composed stage order. |
| ValidationBeforeCommitLocalAccepted | Anchored action: RunEnforcement. |
| ValidationBeforeCommitUndecodableAdmitted | Anchored action: RunEnforcement. |
| RefusalAppliesNothingAppendedFirst | Anchored action: RunEnforcement. |
| BatchAccountingAtomicDropsEntry | Anchored action: RunEnforcement. |
| BatchAccountingDeadLetterStillWritten | Anchored action: RunVersioning. |
| ValidBodyNeverRefusedByEnforcementJudgesEnvelope | Anchored action: RunEnforcement. |
| StampedWhenVersionedRawNotStamped | Anchored action: RunVersioning. |
| StampedWhenVersionedTransformLost | Anchored action: Commit. |
| EnvelopedValuesVerbatimRestamped | Anchored action: RunVersioning. |
| SystemOriginGateIgnoresOptIn | Anchored action: Admit. |
| StrictFlagsPerStageFlagsCrossed | Anchored action: RunEnforcement. |
| StrictIngestDivertsTrustsInvalid | Anchored action: RunEnforcement. |
| StrictIngestDivertsOrphanAccepted | Anchored action: RunVersioning. |
| LocalRefusalRejectsDeadLetters | Anchored action: RunEnforcement. |
| WriteDecidesVersioningStalls | Anchored action: RunVersioning. |

## Detectors

The stage detectors drive the real enforcement, versioning and composed
interceptors and the core enforcement primitive.
`SchemaWriteStageRegistrationTests` drives the real registration extensions in
each supported order and the reversed one, substituting only the per-tree
providers and the dead-letter store, so the stage set, its order, the
system-origin opt-in and the threading of transformed bytes are the production
composition. The cross-tree integration fixture drives a rejected atomic write
through a real cluster and reads back every tree unchanged.

## Defect mutations whose fixes have landed

ValidBodyNeverRefusedByEnforcementJudgesEnvelope is the #5239 mechanism: the
enforcement stage judged an already-enveloped value's envelope bytes, which no
JSON policy accepts, so valid replicated atomic entries were dead-lettered and
local re-writes of stored bytes were rejected. The fix judges the body the read
decoder serves; ValidationBeforeCommitUndecodableAdmitted pins its fail-closed
half, an envelope no reader can be served being refused rather than trusted. It is fixed in the
schema-assurance integration bucket; this is integration-branch evidence, not a
claim the fix is released.

## Deliberate abstraction gaps

- **Reversed registration order is a non-guarantee.** Registering versioning
  before enforcement is accepted, and enforcement's registration replaces the
  composed interceptor, so new writes are not stamped and versioning strict
  ingest does not run, while the decoder and envelope codec stay installed.
  The model includes the order and checks that enforcement still holds there;
  `StampedWhenVersioned` is guaranteed only for the supported orders. A
  fail-fast guard or re-composition would be a separately agreed capability;
  this module models the shipped, documented behaviour and does not change it.
  Production symbols: `LatticeSchemaEnforcementServiceCollectionExtensions.AddLatticeSchemaEnforcement`, `LatticeSchemaVersioningServiceCollectionExtensions.AddLatticeSchemaVersioning`.
- **Writes below the choke point.** Plain LWW replication apply, backup
  restore, tree merge, bulk paths below the grain write choke point, WAL
  replay, splits and moves never consult the interceptor, so nothing here holds
  for them. They are the `Bypass` environment action; every property is scoped
  to the intercepted path. Strict ingest is not a universal validation
  guarantee.
  Production symbols: `LatticeWriteInterceptorEnforcement.Skips`.
- **CRDT deltas and opaque values.** Whole-value writes only. A CRDT delta that
  is not JSON is accepted unvalidated by enforcement, and an enveloped delta's
  strict-ingest upcast and re-stamp belong to the strict-ingest and replication
  models.
  Production symbols: `SchemaValueChecks.IsWellFormedJson`.
- **Body validity and upcast reachability are abstract.** A body is good or bad
  and a tag is target, upcastable or not; rule semantics, the upcaster graph and
  the runtime catalog are not modelled here.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **Dead-letter durability.** A dead-letter append is a counter step that always
  succeeds. Its failure, retries, duplicates after a lost response and the
  receiver committing the rest of a replicated batch belong to the strict-ingest
  model.
  No production symbol: the gap delegates to another model, not to a production mechanism.
