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
have one perturbation for every behaviour row. `SchemaDurableOperation`
([`SchemaDurableOperationRefinement.md`](../SchemaDurableOperationRefinement.md))
and `SchemaComplianceScan`
([`SchemaComplianceScanRefinement.md`](../SchemaComplianceScanRefinement.md))
seed every row except those listed below. The gate requires every
behaviour row of a module that has any perturbation to be either seeded here or
listed below, never both.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| SchemaDurableOperation | Crash(s) | Silo death is an environment step with no production code path to edit; its detectors are the silo-loss chaos fixtures, which kill a real silo rather than run an edited one. |
| SchemaDurableOperation | HandleHasRecord | A handle is built only from the record `BeginAsync` returns; no single-file edit makes a handle without a record that is not already the `StartRejected(o,s)` or `Begin(o,s,p)` perturbation. |
| SchemaDurableOperation | OperationTerminates | Liveness: every edit that stops an operation terminating is one of the seeded `Finish(o)`, `DetectLoss(o)` or `LeaseExpire(o)` perturbations, whose detectors this row cites. |
| SchemaComplianceScan | Cancel | Cancellation reaches the scan through the token the enumeration and the runner also observe, so removing the scan's own check is not observable by a deterministic detector. |
| SchemaComplianceScan | Write(k,v) | An environment step: writes are ordinary data-plane writes with no scan code to edit. |
| SchemaComplianceScan | StableKeysCounted | Every edit that skips a stable key is the seeded `Step` or `CountsAddUp` perturbation over the same loop. |
| SchemaComplianceScan | ReportOnlyWhenSucceeded | The report is the work's return value, recorded only on success by the shared runner; the runner's success path is the `Finish(o)` row of `SchemaDurableOperation`. |
| SchemaComplianceScan | ScanTerminates | Liveness: every edit that stops a scan terminating is one of the seeded `Count` or `Step` perturbations, whose detectors this row cites. |