# Production perturbations: schema

Each `.perturbation` file here is a production-perturbation experiment for one
behaviour row of a refinement note in this area: an anchored edit to one file
under `src/` that breaks the behaviour the row describes, and the detectors the
row cites, at least one of which must go red when the edit is applied. The
format, the gate and the runner are described in the
[spec README](../../README.md#module-layout).

Run one, or every one in this area, with:

```text
python3 tools/perturbations/run-perturbations.py spec/schema/perturbations/<Name>.perturbation
python3 tools/perturbations/run-perturbations.py --area schema --output result.json
```

## Seeded rows

`SchemaPolicyConfig` ([`SchemaPolicyConfigRefinement.md`](../SchemaPolicyConfigRefinement.md))
and `SchemaStageOrdering` ([`SchemaStageOrderingRefinement.md`](../SchemaStageOrderingRefinement.md))
have one perturbation for every behaviour row. `SchemaVersionFamily`
([`SchemaVersionFamilyRefinement.md`](../SchemaVersionFamilyRefinement.md)),
`SchemaDurableOperation`
([`SchemaDurableOperationRefinement.md`](../SchemaDurableOperationRefinement.md))
`SchemaComplianceScan`
([`SchemaComplianceScanRefinement.md`](../SchemaComplianceScanRefinement.md)),
`SchemaCrossRegion` ([`SchemaCrossRegionRefinement.md`](../SchemaCrossRegionRefinement.md))
and `SchemaTenantScope` ([`SchemaTenantScopeRefinement.md`](../SchemaTenantScopeRefinement.md))
seed every behaviour row except those listed below. The gate requires every
behaviour row of a module that has any perturbation to be either seeded here or
listed below, never both.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| SchemaVersionFamily | ConfigNeverRewritesData | The version admins and the runtime registry hold no path to a tree's stored values, so no edit to one production file makes a config change or a registration rewrite them; the detectors pin the contract end to end against any future such path. |
| SchemaVersionFamily | MigrationTerminates | A migration that fails to terminate does so by faulting on a value it cannot migrate and staying in flight, which is exactly the edit the `AbortLeavesDataUntouched` perturbation seeds; a second copy of that edit would add no evidence. |
| SchemaDurableOperation | Crash(s) | Silo death is an environment step with no production code path to edit; its detectors are the silo-loss chaos fixtures, which kill a real silo rather than run an edited one. |
| SchemaDurableOperation | HandleHasRecord | A handle is built only from the record `BeginAsync` returns; no single-file edit makes a handle without a record that is not already the `StartRejected(o,s)` or `Begin(o,s,p)` perturbation. |
| SchemaDurableOperation | OperationTerminates | Liveness: every edit that stops an operation terminating is one of the seeded `Finish(o)`, `DetectLoss(o)` or `LeaseExpire(o)` perturbations, whose detectors this row cites. |
| SchemaComplianceScan | Cancel | Cancellation reaches the scan through the token the enumeration and the runner also observe, so removing the scan's own check is not observable by a deterministic detector. |
| SchemaComplianceScan | Write(k,v) | An environment step: writes are ordinary data-plane writes with no scan code to edit. |
| SchemaComplianceScan | StableKeysCounted | Every edit that skips a stable key is the seeded `Step` or `CountsAddUp` perturbation over the same loop. |
| SchemaComplianceScan | ReportOnlyWhenSucceeded | The report is the work's return value, recorded only on success by the shared runner; the runner's success path is the `Finish(o)` row of `SchemaDurableOperation`. |
| SchemaComplianceScan | ScanTerminates | Liveness: every edit that stops a scan terminating is one of the seeded `Count` or `Step` perturbations, whose detectors this row cites. |
| SchemaStrictIngest | ChangePolicy | The same production edit as `SchemaPolicyConfig`'s `Change(s,p)` row, which `ChangeNeverStored` seeds against the same detector; a second copy adds no evidence. |
| SchemaStrictIngest | Propagate | The same production edit as `SchemaPolicyConfig`'s `Propagate(s)` row, which `PropagateIgnoresChangeFeed` seeds against the same detector; a second copy adds no evidence. |
| SchemaCrossRegion | Deliver(e) | Every single-file edit to the receiver's ingest path is one of the seeded `StrictAdmitsOnlyServable`, `CrdtLiftedAtIngest`, `UncheckedStoredVerbatim` or `AtomicAllOrNothing` perturbations, whose detectors this row cites. |
| SchemaCrossRegion | Decide | Applying a batch's terminal is the atomic-commit protocol's step, with no schema code on it; the schema-specific part, that a diverted entry leaves nothing staged, is the `AtomicAllOrNothing` perturbation's edit. |
| SchemaCrossRegion | Read(e) | The same production edit as the `ReadsServeOrFail` perturbation, against the same detector; a second copy adds no evidence. |
| SchemaCrossRegion | Register | The same production edit as `SchemaVersionFamily`'s `Register(a)` row, which `RegisterNeverPersists` seeds; a second copy adds no evidence. |
| SchemaCrossRegion | Advance | The same production edit as `SchemaVersionFamily`'s `Advance(t)` row, which `AdvanceSkipsCatalogCheck` seeds; a second copy adds no evidence. |
| SchemaCrossRegion | StoredStampFixed | The version admins, the runtime registry and the fold hold no path that rewrites a stored value's envelope, so no edit to one production file introduces one; the detectors pin the contract end to end against any future such path. |
| SchemaCrossRegion | NothingAdmittedWhileUnavailable | The same production edit as `SchemaStrictIngest`'s `Judge(r)` row, which `IngestJudgeTrustsFailedLookup` seeds against the same detector; a second copy adds no evidence. |
| SchemaCrossRegion | AdmittedNeverDropped | No receiver code aborts a replicated batch's other entries on a diversion; an edit that did would have to add the abort path, not perturb an existing one. The detectors pin the contract against any future such path. |
| SchemaCrossRegion | EveryEntryResolved | Liveness: every edit that stops an entry resolving is one of the seeded `StrictAdmitsOnlyServable` or `AtomicAllOrNothing` perturbations, or the applier's re-throw that `SchemaStrictIngest`'s `Settle` row seeds. |
| SchemaTenantScope | EffectsStayInTenant | Every single-file edit that lands an effect outside the caller's tenant is one of the seeded `Resolve`, `Act`, `UnattributableDenied` or `EscapeRefusedAtResolution` perturbations against the same facade seam this row's detectors drive. |
