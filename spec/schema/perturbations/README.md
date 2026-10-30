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
([`SchemaVersionFamilyRefinement.md`](../SchemaVersionFamilyRefinement.md)) has
one for every behaviour row except the two below. The gate requires every
behaviour row of a module that has any perturbation to be either seeded here or
listed below, never both.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| SchemaVersionFamily | ConfigNeverRewritesData | The version admins and the runtime registry hold no path to a tree's stored values, so no edit to one production file makes a config change or a registration rewrite them; the detectors pin the contract end to end against any future such path. |
| SchemaVersionFamily | MigrationTerminates | A migration that fails to terminate does so by faulting on a value it cannot migrate and staying in flight, which is exactly the edit the `AbortLeavesDataUntouched` perturbation seeds; a second copy of that edit would add no evidence. |
