# Production perturbations: backup retention

Each `.perturbation` file here is a production-perturbation experiment for one
behaviour row of [`RefinementRetention.md`](../RefinementRetention.md): an
anchored edit to one file under `src/` that breaks the behaviour the row
describes, and the detectors the row cites, at least one of which must go red
when the edit is applied. The format, the gate and the runner are described in
the [spec README](../../README.md#module-layout).

Run one, or every one in this area, with:

```text
python3 tools/perturbations/run-perturbations.py spec/backup/perturbations/<Name>.perturbation
python3 tools/perturbations/run-perturbations.py --area backup --output result.json
```

## Seeded rows

`BackupRetention` is seeded at the fence that #5357 added, its most
safety-critical rows: the increment's base re-check (`Check(c)`), the pass's
dependent scan (`Scan(p)`) and the one-pass guard (`PruneList(p)`); and at the
two rows the module's independent review (#5352) found its detectors did not
reach: a listing read under that guard (`ListedRestorable`, #5412) and the
grain's tip-first delete order (`SinkChainIntact`); and, after the confirmation
review, at the increment's registration (`Register(c)`) and the set membership
stamp that #5449 made a member's only registration (`Stamp(c)`); and at the
per-id hold that serialises a republished content-addressed id against a pass
(#5418): a republish waits while a pass retires its id (`Acquire(c)`), a pass
never dooms an id a capture is publishing (`ListedInSink`), and a republish
refreshes the backup's ranking time (`KeepPolicyHonoured`). No other backup
module has a perturbation yet.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| BackupRetention | Tick | An environment action: the wall clock. No production code implements it; the planner's time arithmetic is pinned by its unit tests. |
| BackupRetention | Start(c,k,b) | An environment action: a capture starting, under a fresh id or a republished one. Its id derivation is a pure function pinned by `BackupIdentityTests`' 10.2.0 vectors, and what a republish may do is seeded at `Acquire(c)`; its one other production check, that the base's sink manifest exists, is shared by every incremental capture and pinned by the incremental capture suite, outside retention. |
| BackupRetention | WriteArt(c) | The artifact write is the capture engine's, common to every capture; an edit that drops it fails every capture test, which measures the capture engine rather than retention. |
| BackupRetention | WriteMan(c) | The manifest write is the capture engine's, common to every capture; dropping it fails every capture test, not the retention detectors specifically. |
| BackupRetention | Abandon(c) | Its safety half is the publish check, which `IncrementPublishesWithoutBaseRecheck` seeds; withdrawing the sink manifest is a tidy-up whose loss leaves an unlisted manifest, which `BackupRetentionConcurrencyTests.A_pass_before_the_increment_wrote_its_manifest_makes_the_increment_fall_back_to_a_full_backup` detects by listing the sink directly. |
| BackupRetention | CaptureCrash(c) | An environment action: a crash is injected by the detectors' stores, so no production code implements it. |
| BackupRetention | RemoveRow(p,m) | Removing the row later than the scan is the same production reordering as the scan finding nothing for an in-flight increment; `PassKeepsNothingBuiltOn` seeds that outcome against the same detectors. |
| BackupRetention | RestoreRow(p,m) | Restoring the row and still deleting is the scan's outcome discarded; `PassKeepsNothingBuiltOn` seeds it against the same detectors. |
| BackupRetention | DelMan(p,m) | The delete itself is the sink's; the order around it is the row-first and tip-first decision the seeded rows pin, the tip-first order through `PassDeletesInListingOrder`. |
| BackupRetention | DelArt(p,m) | The delete itself is the sink's; which artifacts are kept is `BackupRetentionPlanner.RetainedArtifacts`, defensive because production artifact ids are unique per capture. |
| BackupRetention | PruneCrash(p) | An environment action: a fault is injected by the detectors' stores. The guard's release after a fault is a `finally`, whose removal leaves the guard set and fails `BackupSchedulerGrainUnitTests.A_faulted_retention_pass_does_not_wedge_the_scope`. |
| BackupRetention | RetainedRestorable | Its production edits are the seeded rows' edits: the pass's scan and the increment's re-check. |
| BackupRetention | InFlightBaseSurvives | Its production edit is the publish check, which `IncrementPublishesWithoutBaseRecheck` seeds against the same detector. |
| BackupRetention | ExpiredEventuallyPruned | Liveness. A wedged scope is the guard's `finally` missing; a never-pruning plan yields an assertion only against a named keep set, which the safety rows' detectors already make. |