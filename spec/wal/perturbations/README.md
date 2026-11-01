# Production perturbations: WAL

Each `.perturbation` file here is a production-perturbation experiment for one
behaviour row of a refinement note in this area: an anchored edit to one file
under `src/` that breaks the behaviour the row describes, and the detectors the
row cites, at least one of which must go red when the edit is applied. The
format, the gate and the runner are described in the
[spec README](../../README.md#module-layout).

Run one, or every one in this area, with:

```text
python3 tools/perturbations/run-perturbations.py spec/wal/perturbations/<Name>.perturbation
python3 tools/perturbations/run-perturbations.py --area wal --output result.json
```

## Seeded rows

`WalPinRetirement` ([`PinRetirementRefinement.md`](../PinRetirementRefinement.md))
is seeded on the rows issue #5358 broke: `Decide(p, c, k)` and `NoTrimPastLiveNeed`
(the unfenced removal), `Read(p)` (the drive retiring on its NotDriven verdict) and `Enum(p, k)`
(the drive reading its leaf before it takes its tokens, a two-block edit), on
`PurgeTailRemove`, the row issue #5417 broke (the purge tail's re-drive acting on a tree id),
and on `Recreate`, the row issue #5458 broke (a purged shard admitting a write while the
purge still held the tree's registry entry).
`WalDurability` ([`Refinement.md`](../Refinement.md)) is seeded on the live
checkpoint advance (issues #5572, #5593): `LiveAdvance(l)` - its unapplied-append
count, its leaf-wide saga clamp, its arming rule, its stamp target, its partition
attribution, and the born rule for an adopted row, a re-bind and a bulk-load leaf -
and `NewbornPinEventuallyUsable` (the coverage-lag tick running the advance rather
than the starvation drive). Each perturbation names only the detectors that target
it, so a run proves each one red on its own. `WalMove` has no perturbations yet, so
the gate does not require its rows to be listed.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| WalDurability | Append | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | FlushAck(o) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | ShardCrash | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | ScheduleReplay(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | ReadStep(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | Abandon(o) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | LateLand(o) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | SettleHole(o) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | PersistCheckpoint(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | PersistFail(l) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | Capture(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | CaptureFail(l) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | PublishPin(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | GcTrim | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | LeafStop(l) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | Activate(l) | Its birth half is seeded through LiveAdvance(l): the born rule is decided where an activation binds the leaf, and LiveAdvanceBindIsBirth perturbs it. Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | ActivateLoadFail(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | SnapshotVanish(l) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | ActivateRowless(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | LeafRowVanish(l) | An environment event with no production code path of its own to perturb; its detectors are the fault-handling tests the row cites. |
| WalDurability | PinRetire(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | MarkPurge(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | PurgeClear(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | Recover(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | ReplayFaultRearm(l) | Its detectors were proven red by perturbing production when the row was written (#4432 and the fix named in the row); not on the live advance path this module seeds. |
| WalDurability | AckedWriteDurable | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | TrimCoveredBySnapshot | Broken by the seeded LiveAdvanceIgnoresUnappliedCount edit as well as by the action rows that establish it. |
| WalDurability | ReadPositionHonest | Broken by the seeded LiveAdvanceUnarmed and LiveAdvanceBindIsBirth edits as well as by the action rows that establish it. |
| WalDurability | ClearRecorded | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | ShippingNeverSkips | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | LogPrefixApplied | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | OffsetContiguity | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | RecoveryNeverFallsOffLog | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | PersistedBeliefHonest | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | ReleaseBackedBySnapshot | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | SnapshotCoverageMonotonic | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | PublishedPinWithinPersistedBelief | Broken by the same edits as the action rows that establish it; those rows are detected as the refinement note records. |
| WalDurability | EveryAckedWriteMaterialised | A liveness row: no single anchored edit breaks it without also breaking a safety row; ReadPositionHonest subsumes its protocol-defect pairing. |
| WalDurability | ReclamationEventuallyAdvances | A liveness row: its production detectors measure reclamation through the real timer and GC, which no single anchored edit breaks without also breaking ScheduleReplay(l) or PublishPin(l). |
| WalPinRetirement | Write | The birth-seed ordering belongs to the leaf (`BPlusLeafGrain.SetTreeIdAsync`); this area seeds the GC retirement path the defect was in. The leaf ordering is covered by its detectors and by `NoTrimPastLiveNeedWriteBeforeSeed` in the model. |
| WalPinRetirement | Checkpoint | WalDurability's checkpoint discipline (#3476), whose detectors are cited by that note; not on the retirement path. |
| WalPinRetirement | Publish | WalDurability's publication rule, as for Checkpoint. |
| WalPinRetirement | Seed | The seed's merge stamp is detected at the pin grain (`WalMaterialiserPinGrainTests`); a single-file edit to the stamp also breaks the Decide row, which is seeded. |
| WalPinRetirement | RouteChange | An environment event with no production code path of its own to perturb: a routing change is a configuration change. |
| WalPinRetirement | SoftDelete | The deleted-tree hold's refusal to retire is shared with DiscardRetire, whose edit would be the same; neither is on the fenced retirement path this area seeds. |
| WalPinRetirement | RecoverTree | Recovery is the tree-lifecycle area's (`TreeDeletionGrain.RecoverPhysicalAsync`), detected by its integration tests. |
| WalPinRetirement | MarkPurge | The purge's marker ordering is WalDurability's `MarkPurge` row, detected there. |
| WalPinRetirement | Fold | The fold's drain is spec/bplustree's reclaim; its pin retirement order is detected by the leaf clear tests cited. |
| WalPinRetirement | ClearRow | WalDurability's `PurgeClear` row covers the same clear ordering and is detected there. |
| WalPinRetirement | SelfRetire(k) | The leaf's own retirement, detected by the leaf clear tests; not on the GC retirement path. |
| WalPinRetirement | SelfRetireFail(k) | An environment event: a swallowed failure has no production edit that breaks it without breaking the leaf's terminal step. |
| WalPinRetirement | Discard | The discard decision is the tree-lifecycle area's, detected by the undone-resize integration tests. |
| WalPinRetirement | DiscardRetire | A one-line edit to the deleted-tree hold would also be seeded for SoftDelete; the row is detected by `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_retires_the_pins_of_a_discarded_tree_without_touching_its_leaves`. |
| WalPinRetirement | GcTrim | WalDurability's trim, detected by that note's rows. |
| WalPinRetirement | PurgeTailSettle | Its edit would be the one seeded for `PurgeTailRemove`: production records the landed removal in the branch the lineage check selects, so removing the check breaks both rows at once. |
| WalPinRetirement | Start(p) | Starting a run decides nothing; its detectors are the coordination fixtures. |
| WalPinRetirement | ReadFail(p) | Fail-closed on a faulted read is a catch arm whose removal is a compile-shape change rather than an anchored edit; detected by `LatticeWalGcSchedulerCadenceTests.The_sweep_fails_closed_when_a_leaf_state_read_throws`. |
| WalPinRetirement | Done(p) | Ending a run decides nothing. |
| WalPinRetirement | Abandon(p) | An environment event (cancellation, a timeout, the coordinator's loss). |
| WalPinRetirement | LivePublisherPinned | Broken by the same edit as the seeded NoTrimPastLiveNeed and Decide rows. |
| WalPinRetirement | RetireOnlyHusk | Broken by the same edit as the seeded Decide row. |
| WalPinRetirement | UnreadableNeverRetired | As ReadFail(p). |
| WalPinRetirement | OrphanEventuallyRetired | A liveness row: its production detectors measure retirement throughput, which no single anchored edit breaks without also breaking a seeded safety row. |
