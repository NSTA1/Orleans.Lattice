# Tenant sampled quotas

These modules check the design of sampled footprint admission, replicated usage
slots, authoritative tree-count checks, and the resource quota evaluator. They
are not a machine-checked refinement proof of the C# implementation. Each
refinement note states which production mechanisms its detectors really reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `TenantFootprint` | 5 | 3 | 8 | 13 | 15 | 62,964 |
| `TenantTreeCount` | 4 | 2 | 3 | 7 | 8 | 636 |
| `TenantQuotaEvaluation` | 2 | 1 | 1 | 9 | 3 | 124,416 |

## Semantics and assumptions

- **Soft quotas, not reservations.** Concurrent admissions can commit after
  another write has taken usage over quota. No invariant claims that actual
  usage is at most the configured limit. Footprint admission compares the last
  selected sample, not the proposed write's size.
- **Cold admission.** An absent compiled view and an empty sample both admit
  footprint writes; the model represents both by zero sampled usage. The
  authoritative tree-count check still runs for a cold tenant.
- **Accounting.** Local commits and replication applies both increase resident
  usage. Inbound applies do not consult footprint admission. A sampled local
  usage slot belongs to its writing cluster; duplicate older slot deliveries
  cannot replace its latest stamp.
- **Scopes.** `GlobalConverged` folds every published cluster slot, including
  resident replica copies. `PerCluster` reads only the local slot. This is
  physical footprint accounting, not globally distinct logical-key accounting.
- **Progress.** Footprint convergence and refusal are conditional on finite
  completed writes, continuously available metering/publication, and fair
  delivery of the latest usage slot. The model checks both zero and nonzero
  publication hysteresis, with changed-sample refresh after a finite age.
  `ClockTick` abstracts the monotonic supplied cadence-clock time and `Refresh`
  abstracts the five-minute production suppression ceiling (#4805). A stable
  sub-threshold crossing therefore becomes publishable even with a nonzero band.
  Strong fairness on latest delivery excludes endlessly selecting old packets.
  Tree completion assumes available registry count and register operations.
- **Arithmetic.** Finite storage saturation stands for `long.MaxValue`.
  Nullable dimensions are independent, burst arithmetic multiplies before
  dividing, and the first breached dimension follows bytes/keys/memory/trees.
  `TenantQuotaEvaluation` uses the integer sentinel `Unbounded` only to avoid
  heterogeneous-value ordering in TLC; it does not turn a null quota into a
  numeric production cap.

## Bounds and deliberate gaps

`TenantFootprint` has two clusters, one locally changed resident report and one
inbound-changed resident report per cluster. These are available storage-report
inputs to accounting, not a proof of core write durability. There are no deletions, changed quotas, changed residency,
failed storage operations, or equal-clock writer ties. Its numerically finite
space is not a production overshoot guarantee. Without bounds on arrival rate,
write size, sample delay and replication delay, no unconditional overshoot bound
exists. The storage ceiling abstracts representation, not admission.

`TenantTreeCount` has three unique create requests, a base cap of one or two,
and zero or 50% burst. Check and register are separate actions, preserving the
documented race. A later probe refuses at a full cap even if earlier in-flight
checks all succeeded. It neither reserves capacity nor models registry CRDT
merging or tree deletion.

`TenantQuotaEvaluation` explores independent nullable limits and usage for all
four dimensions, zero/50%/100% burst, and deterministic refusal. It does not
model rate limits, exception transport, or permission checks.

## Refinement and mutations

- [Footprint refinement](RefinementFootprint.md)
- [Tree-count refinement](RefinementTreeCount.md)
- [Evaluator refinement](RefinementQuotaEvaluation.md)

The adjacent `.mutation` catalogues use exact anchored edits, not copied mutant
modules. Every behavioural action and property has a falsifying mutation.
There are no non-behavioural action exemptions.
