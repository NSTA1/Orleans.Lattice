# Shared alias protocols

`AliasRouting` checks durable administrative alias moves with two logical aliases
sharing three physical copies. A move records intent before arming a retained
copy, publishes the physical identity and matching map in one row, releases only
its destination's logical fence, and retires intent last. Finite phase-boundary
crashes preserve intent and retry. A competing registry publication can
supersede an unpublished move, including a physical identity returning after an
intervening publication (ABA); recovery checks the captured map revision.

Repeated moves and logical-alias-specific redirects are checked together, not
as separate single-cutover instances. The source-copy map is abstracted by its
identity; sparse slot movement belongs to the tree-lifecycle and ownership
models. Cached routers may remain stale indefinitely; production routing
detectors separately exercise stale read/write retries.

`AliasRevert` interleaves administrative publications with validation, lifecycle
reservation, lineage stamping, helper revalidation and expected-identity swap.
It also binds a split/fold to a physical copy before later publications and
checks that an obsolete diff cannot change the new copy's routing. Deletion can
race admission but cannot cross a held reservation. Both possible predecessors,
including the logical tree's own copy, are enumerated.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `AliasRouting` | 4 | 6 | 11 | 12 | 19 | 25,937 |
| `AliasRevert` | 5 | 7 | 11 | 13 | 21 | 1,724 |

## Scope

The base permits three total initiations/publications across two aliases, one
fault, and three physical copies. Optional starts and competing publications are
not fair; phase progress is weakly fair. Once the finite environment budget is
spent all started moves settle. The variant permits two faults.

The revert base permits two administrative moves and its ABA variant permits
three (3,020 distinct states). Administrative moves can occur between every
service phase, including after reservation. A binding check which fails after
admission may leave lineage and reservation in place; only an already superseded
request is guaranteed a side-effect-free early refusal. A revert's physical
check does not detect an intervening move back to the same shadow. These are
explicit limits, not operation/revision guarantees borrowed from AliasRouting.
No liveness is claimed for permanently superseded requests.

This is a bounded protocol abstraction, not an automatic implementation
refinement. It does not prove arbitrary-provider-write crash safety, unbounded
redirect-chain traversal, tenant authorization, shared-copy physical deletion
semantics, or copying WAL/data. Separate cutover admission coverage checks
expected-identity fences and consumer bindings. See
[RefinementRouting.md](RefinementRouting.md) for the production seams.
The revert-specific mappings and limits are in
[RefinementRevert.md](RefinementRevert.md).

## Defect found

[#4860](https://github.com/NSTA1/Orleans.Lattice/issues/4860): an old restore's
revert used an unconditional fallback after another copy superseded its shadow.
The helper now refuses unrelated current copies, fences every swap and retains
the predecessor's available routing metadata. The backup service validates
before reservation/lineage changes; the live regression also proves that a later
operation can still take the reservation. The core/helper and live tests failed
before remediation. `RevertIgnoresExpectedCopy` and `RefusalTakesReservation`
keep the binding and early-admission properties non-vacuous.
