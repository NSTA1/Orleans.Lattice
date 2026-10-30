# Production perturbations: numeric projection companion

Each `.perturbation` file here is a production-perturbation experiment for one
behaviour row of [`RefinementNumeric.md`](../RefinementNumeric.md): an anchored
edit to one file under `src/` that breaks the behaviour the row describes, and
the detectors the row cites, at least one of which must go red when the edit is
applied. The format, the gate and the runner are described in the
[spec README](../../README.md#module-layout).

Run one, or every one in this area, with:

```text
python3 tools/perturbations/run-perturbations.py spec/view-numeric/perturbations/<Name>.perturbation
python3 tools/perturbations/run-perturbations.py --area view-numeric --output result.json
```

## Seeded rows

`MaterialisedViewNumeric` has one perturbation for every behaviour row except
those listed below: the extraction and encoding rows (`QuiescentExact`,
`Publish(k,g)`), the applier's flip and its arithmetic (`Flip`,
`SlotsMatchMembership`), the durable publication obligation
(`IntentCoversStale`, `CheckpointAfterPublication`), the replay identity
(`NoIdentityConflict`) and the projection-version rebuild (`ChangeFilter`).

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| MaterialisedViewNumeric | Write | An environment action: the source tree's own ordinary and atomic write path. An edit to it breaks every view and every source test alike, so it measures the source tree's contract rather than the numeric projection's. |
| MaterialisedViewNumeric | Swap | An environment action: the physical-copy swap belongs to resize, reshard and restore. The view's response to it is the `Rebind` row. |
| MaterialisedViewNumeric | Fail | An environment action: a lost response or crash is injected by the detectors' stores and harnesses, so no production code implements it. The recovery it exercises is seeded through `IntentCoversStale` and `CheckpointAfterPublication`. |
| MaterialisedViewNumeric | Advance | Moving past a unit is the shared WAL subscriber's offset progression, common to every view kind; an edit that stops it turns the detectors into timeouts rather than assertion failures, which are not detector evidence. |
| MaterialisedViewNumeric | FlushBatch | The batch flush reuses the applier's flip through a capturing store, which `Flip` and `SlotsMatchMembership` seed; the capture-then-atomic publication itself is shared with every view kind and owned by the staging companions. |
| MaterialisedViewNumeric | Checkpoint | The banking step is the same production edit `CheckpointBeforeRepair` seeds for the `CheckpointAfterPublication` row; a second perturbation would repeat that experiment against the same detectors. |
| MaterialisedViewNumeric | Rebind | The version arm is the same production edit `ChangeFilter` seeds. The identity-swap arm's detectors are cluster heal tests, whose failure under a single-file edit to the heal path is a convergence timeout rather than an assertion. |
| MaterialisedViewNumeric | Reconcile | The shadow-swap rebuild writes a fresh generation tree with its own saga namespace, so removing the generation bump is not observable through it; the freshened identity of an in-place rebuild is pinned only at the applier, by a fixture whose name another test project also declares, so it cannot be named as a perturbation detector. |
| MaterialisedViewNumeric | AbortedContributesNothing | Abort handling is the shared staging path's decision gate for every view kind, owned and exercised by the staging and aggregation companions; this row's detectors observe it through that path. |
| MaterialisedViewNumeric | Converges | Liveness. The single-file edits that break it are the seeded safety rows' edits; a progress-only edit yields a timeout, which is not detector evidence. |
| MaterialisedViewNumeric | ObligationsRetire | Liveness. Losing the obligation is seeded through `IntentCoversStale` and `CheckpointAfterPublication`; never retiring it yields a stalled drain, which is a timeout rather than an assertion. |
