# Production perturbations: atomic commit

Each `.perturbation` file here is a production-perturbation experiment for one
behaviour row of a refinement note in this area: an anchored edit to one file
under `src/` that breaks the behaviour the row describes, and the detectors the
row cites, at least one of which must go red when the edit is applied. The
format, the gate and the runner are described in the
[spec README](../../README.md#module-layout).

Run one, or every one in this area, with:

```text
python3 tools/perturbations/run-perturbations.py spec/atomic-commit/perturbations/<Name>.perturbation
python3 tools/perturbations/run-perturbations.py --area atomic-commit --output result.json
```

## Seeded rows

`AtomicCommit` ([`Refinement.md`](../Refinement.md)) has one perturbation for
every behaviour row except those listed below. The gate requires every
behaviour row of a module that has any perturbation to be either seeded here or
listed below, never both.

## Unseeded rows

| Module | Row | Reason |
|--------|-----|--------|
| AtomicCommit | AllOrNothing / VisibilityMatchesDecision | The row's only detector, `AtomicVisibilityChaosTests.Continuous_reader_observes_zero_or_all_keys_for_every_poll`, is a Chaos-tier multi-silo test. A single-file edit that splits a fan-out view would need that cluster run to show, and the experiment would measure the chaos harness's scheduling as much as the edit. The row's narrower halves are seeded through `StrictIsolation`, `LinearizedTerminals` and `NoMixedTerminals`. |
