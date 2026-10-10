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

## Refinement and mutations

- [Footprint refinement](RefinementFootprint.md)
- [Tree-count refinement](RefinementTreeCount.md)

Every behavioural action and checked property has a falsifying mutation.
