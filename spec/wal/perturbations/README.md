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
(the unfenced removal) and `Read(p)` (the drive retiring on its NotDriven verdict), and on
`PurgeTailRemove`, the row issue #5417 broke (the purge tail's re-drive acting on a tree id).
`WalDurability` and `WalMove` have no perturbations yet, so the gate does not
require their rows to be listed.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
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
| WalPinRetirement | Recreate | Re-creation admission is the leaf's (#4700), detected by its row-record and recovery tests. |
| WalPinRetirement | Discard | The discard decision is the tree-lifecycle area's, detected by the undone-resize integration tests. |
| WalPinRetirement | DiscardRetire | A one-line edit to the deleted-tree hold would also be seeded for SoftDelete; the row is detected by `LatticeWalGcSchedulerCadenceTests.ExecuteAsync_retires_the_pins_of_a_discarded_tree_without_touching_its_leaves`. |
| WalPinRetirement | GcTrim | WalDurability's trim, detected by that note's rows. |
| WalPinRetirement | PurgeTailSettle | Its edit would be the one seeded for `PurgeTailRemove`: production records the landed removal in the branch the lineage check selects, so removing the check breaks both rows at once. |
| WalPinRetirement | Start(p) | Starting a run decides nothing; its detectors are the coordination fixtures. |
| WalPinRetirement | Enum(p, k) | The order that matters is every token before the leaf read the removal is decided on, and both retirement paths take that read only after their key loop. Moving the drive's leaf read above its token loop turns `LatticeWalGcSchedulerCadenceTests.The_drive_does_not_remove_a_pin_that_a_leaf_recreated_after_its_read_seeded_into` red (checked by the #5351 review); that is a two-anchor edit, which a single perturbation cannot express. Reordering a key's token and its pin read is harmless, because the leaf read still follows both. The Coyote guard `WalPinRetirementCoyoteTests.A_token_taken_after_the_read_does_not_fence_the_recreation` proves the token-before-read rule on the model, not on production's call order. |
| WalPinRetirement | ReadFail(p) | Fail-closed on a faulted read is a catch arm whose removal is a compile-shape change rather than an anchored edit; detected by `LatticeWalGcSchedulerCadenceTests.The_sweep_fails_closed_when_a_leaf_state_read_throws`. |
| WalPinRetirement | Done(p) | Ending a run decides nothing. |
| WalPinRetirement | Abandon(p) | An environment event (cancellation, a timeout, the coordinator's loss). |
| WalPinRetirement | LivePublisherPinned | Broken by the same edit as the seeded NoTrimPastLiveNeed and Decide rows. |
| WalPinRetirement | RetireOnlyHusk | Broken by the same edit as the seeded Decide row. |
| WalPinRetirement | UnreadableNeverRetired | As ReadFail(p). |
| WalPinRetirement | OrphanEventuallyRetired | A liveness row: its production detectors measure retirement throughput, which no single anchored edit breaks without also breaking a seeded safety row. |
