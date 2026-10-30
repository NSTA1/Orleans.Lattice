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
have one perturbation for every behaviour row. `SchemaStrictIngest`
([`SchemaStrictIngestRefinement.md`](../SchemaStrictIngestRefinement.md)) has one
for every behaviour row but the two it shares with `SchemaPolicyConfig`, listed
below. The gate requires every
behaviour row of a module that has any perturbation to be either seeded here or
listed below, never both.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| SchemaStrictIngest | ChangePolicy | The same production edit as `SchemaPolicyConfig`'s `Change(s,p)` row, which `ChangeNeverStored` seeds against the same detector; a second copy adds no evidence. |
| SchemaStrictIngest | Propagate | The same production edit as `SchemaPolicyConfig`'s `Propagate(s)` row, which `PropagateIgnoresChangeFeed` seeds against the same detector; a second copy adds no evidence. |
