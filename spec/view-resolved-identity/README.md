# Materialised view resolved identity

This companion isolates #4895: a volatile resolved transaction ID belongs to
the physical WAL actually being tailed, not a mutable checkpoint binding or
an in-progress candidate rebuild. It uses two logs and one reused logical TxId,
without multiplying source-admission, topology or aggregation dimensions.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| MaterialisedViewResolvedIdentity | 6 | 2 | 10 | 16 | 16 | 196 |

## Bound and discrimination

The old log initially has prepare/terminal offsets 1/2 for T. Projection resolves
T in the current activation, retaining both the resolved set and eviction order.
A rebuild chooses that same physical log or a new one. Capture observes old
head 2 or new head 0. Only afterwards do late old-log duplicates arrive at 3/4,
and the distinct new-log transaction reusing T arrive at 1/2.

Candidate publication separately updates the mutable checkpoint binding and,
unless the write failed before commit, its durable binding. A committed but lost
response is different from an uncommitted write. Neither response nor candidate
capture changes cache ownership. Failure/lost-response paths process the
already-selected original physical tail's duplicates before entering the new
tail. Same-log rebuilds and retries retain suppression. Actual changed-tail entry
clears both the set and order; only that boundary adopts the new owner.

Every physical transaction is projected once in this activation. Checkpoint
safety requires actual projection before offset 2 can be resolved. The
bare-TxId carryover mutant consumes the new log's prepare/terminal as duplicates
and advances its checkpoint without its projection, violating CheckpointSafe.
That is a safety counterexample, not just a missing eventual result.

Distinct controls catch clearing on same-log capture, adopting a candidate or
mutated live checkpoint as owner too early, retaining only the old eviction
queue, staging from the wrong log and projecting late duplicates twice. A
separate failed-publication cache-clear mutant keeps the original tail selected
and fails ProjectionOnce when that retry projects its late duplicate again.
The original cache is populated through Read/Flush before any rebuild step;
none of these controls starts with an arbitrary prefilled set.

## Instances and limits

| Instance | Target / response | Distinct states |
|----------|-------------------|-----------------|
| Base | Either physical target; all three outcomes | 196 |
| SameLog | Original physical target; all outcomes | 52 |
| FailedCandidate | New physical target; uncommitted write | 60 |
| LostCandidate | New physical target; committed lost response | 49 |

`offsets` is a physical-tagged entitlement abstraction, not a claim that the
production checkpoint persists one dictionary per physical log. Full checkpoint
publication, retained read holds and generation visibility are Rebind's separate
obligations. Projection counts represent invocation suppression during one
activation, not a claim that backend idempotency disappears or that a process
restart cannot re-invoke an idempotent operation.

There is no crash, cache-capacity eviction, abort, shard Cartesian product,
cross-view atomicity or source revision validation in this model. Fence/Horizon
and SourceAdmission supply those independent obligations. The one-entry order
checks clearing both cache structures at a physical transition, not the complete
8192-entry eviction algorithm. All liveness claims require fair progress of the
finite source delivery, tail entry, retry and maintenance steps.

The refinement remains Pending until the production owner accepts the physical
reuse and same-log failure controls. The single-transaction Rebind model does not
represent a resolved cache; this companion must not be described as an already
proved property of that model.

## Measured evidence

Actual TlcModelCheckTests executed 20/20 passing cases, zero skipped or
inconclusive: one base, three variants and 16 paired mutations. The item span
was 43.883589 seconds; the whole no-build command was 61.385425 seconds.
The independent raw exhaustive census executed 36 JVM runs: four instances
and 16 distinct control/mutant pairs, retaining TypeOK in single-property runs.
Every mutant violated its named target. Raw instance times were base 1.860s,
FailedCandidate 1.134s, LostCandidate 1.076s and SameLog 1.667s.

The owned structural scope passed 41/41. Central index/routing and production
refinement acceptance remain parent-owned and are not included in that green.
Runs used Java 17/TLC 2.19, one JVM slot, two visible CPUs/workers, ParallelGC
and Xmx1800MiB (TLC reported 1600MB usable heap). Peak live heap was not measured.
Same-target mutation controls have distinct generated headers; no shared-control
speedup is claimed. Timings are observed local wall time, not predicted CI time.
