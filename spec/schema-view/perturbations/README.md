# Production perturbations: schema and view composition

Each `.perturbation` file here is a production-perturbation experiment for one
behaviour row of [`SchemaViewCompositionRefinement.md`](../SchemaViewCompositionRefinement.md):
an anchored edit to one file under `src/` that breaks the behaviour the row
describes, and the detectors the row cites, at least one of which must go red
when the edit is applied. The format, the gate and the runner are described in
the [spec README](../../README.md#module-layout).

Run one, or every one in this area, with:

```text
python3 tools/perturbations/run-perturbations.py spec/schema-view/perturbations/<Name>.perturbation
python3 tools/perturbations/run-perturbations.py --area schema-view --output result.json
```

## Seeded rows

Each derivation path is seeded by bypassing the projection-input seam at that
path's own call site (`Apply`, `ApplyBatch`, `Reconcile`) and at the seam itself
(`QuiescentAtStampedVersion`). The write path is seeded by dropping an accepting
stage's transform (`Write`) and by accepting a refused write
(`RejectedContributesNothing`), and the rewrite by re-stamping without the
upcast (`BuildRewrite`) and by writing remediated values without an envelope
(`IntendedContribution`).

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| SchemaViewComposition | Retry | An environment action: a lost response is the caller's re-submission, and the re-submitted write takes the same path the `Write` and `RejectedContributesNothing` perturbations seed. |
| SchemaViewComposition | Replicate | An environment action: strict-ingest diversion is seeded by the `SchemaStrictIngest` area's `Ingest*` perturbations against the same detectors, and the below-check merge path stores what it copies by design. |
| SchemaViewComposition | Advance | A target advance makes no view-side change to break: the production edit that would violate the row adds a rebuild or a rewrite that does not exist, rather than removing a behaviour. The version family's advance is seeded by its own area. |
| SchemaViewComposition | ChangeDefinition | The projection-version rebuild is the same production edit the numeric companion's `VersionChangeKeepsView` seeds against the same detectors. |
| SchemaViewComposition | BeginRewrite | Capturing the migration's target is seeded by the `SchemaVersionFamily` area's `MigrateBeginWrongTarget` perturbation. |
| SchemaViewComposition | Cutover | The alias move and its fence are seeded by the remediation and version-family areas; the view's response to the move is the `Rebind` row. |
| SchemaViewComposition | Fail | An environment action: a crash or lost response is injected by the detectors' harnesses, so no production code implements it; the replay it exercises is seeded through the numeric companion's checkpoint perturbations. |
| SchemaViewComposition | Rebind | The rebuild a rebind runs is the shadow build the `ShadowRebuildBypassesSeam` perturbation already seeds for `Reconcile`; an edit to the identity heal itself yields a convergence timeout rather than an assertion. |
| SchemaViewComposition | WaitHead | The identity-aware head barrier is the #5308 fix, which is not in this tree yet; its perturbation lands with it. |
| SchemaViewComposition | NoDuplicateContribution | The retract/add fold is the applier's, seeded by the numeric companion's `SumRetractForgotten` and `RegroupLeavesOldSlot` perturbations; a cutover's rebuild replaces membership and slots together. |
| SchemaViewComposition | BindingConsistent | Binding the generation, cursor and checkpoint to one copy is seeded by the numeric companion's `Rebind` path and the materialised-view tail area; an edit that splits them yields a heal that never converges, which is a timeout rather than an assertion. |
| SchemaViewComposition | BarrierExact | The identity-aware head barrier is the #5308 fix, which is not in this tree yet; its perturbation lands with it. |
| SchemaViewComposition | AdvanceReprojectsNothing | As for `Advance`: violating it needs added behaviour, not a removed one. |
| SchemaViewComposition | AbortTouchesNothing | The abort contract is seeded by the version-family and remediation areas (`MigrationThrowsInsteadOfAborting`, `ReservationNeverReleased`) against the abort detectors this row cites alongside its own. |
| SchemaViewComposition | Converges | Liveness. The single-file edits that break it are the seeded safety rows' edits; a progress-only edit yields a timeout, which is not detector evidence. |
