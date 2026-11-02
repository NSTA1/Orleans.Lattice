# Replication anti-entropy

`AntiEntropy` models digest-probe anti-entropy for one replicated tree over a
directed remediation link from the local cluster A to a peer B. It covers the
digest comparison, Merkle-walk localisation, the remediation guards, targeted
leaf re-replay, the bootstrap-snapshot fallback and the receiver apply path,
and their interaction with the probe grain's restart, topology changes, alias
swaps and the projection-digest latch. Its configuration, manifest, anchored mutations and
[refinement mapping](Refinement.md) enroll it in the automatically discovered
Formal harness without harness changes.

## Instance

Two keys, a hybrid logical clock with two stamps, a traffic budget of one
entry per window, a breaker threshold of one failure and one environment
event. Both clusters write; B's writes replicate to A, so A holds
foreign-origin rows. The opt-out is chosen at initialisation. Ordinary
shipping may lose an A-origin Set or Delete, the WAL may be trimmed past the peer's
cursor, and an own-origin row may be echoed back to B. One environment event
is drawn from a shared budget: a rejected re-ship, a probe-grain restart, a
shard-map change (reshard or resize) or a digest latch.

The `Alias` variant narrows the keys to one and allows one alias swap: a
shadow cutover, restore or alias change binds the logical tree to a new
physical tree with an empty WAL and retires the old one.

## What is checked

- The receiver never applies its own origin, its identity dedup only records
  identities its projection reflects, last-writer-wins never regresses a row
  and the per-origin high-water-mark only advances.
- A's retained WAL is exactly the journalled suffix above its trim floor, so
  a wal_trimmed skip is classified soundly.
- The probe and the walk are read-only: they change neither data nor guard
  accounting.
- Repair traffic is emitted only for a localised pass, only for localised
  ranges, never while opted out, never while the breaker is open and cooling,
  and never past an exhausted budget window.
- Re-replay ships only retained A-origin entries above the peer's cursor; the
  fallback runs only after a wal_trimmed or range_empty skip and ships only
  committed rows at their own timestamp, a tombstone as a delete.
- The digest latch is permanent, a failure at the threshold opens the
  breaker, and a walk localises against the topology generation current when
  it resolves the physical tree; re-replay reads the WAL of the physical tree
  that resolve returned, never a retired one.
- Under the stated fairness, every divergence is eventually repaired unless
  digest maintenance is latched off or remediation is opted out.

Every checked property has an anchored mutation, and every non-stuttering
action is perturbed. The base and each mutant are deadlock-free, and no
mutation disables deadlock checking or changes fairness.

## Limitations

- A tombstone reaped after the tombstone grace period leaves no row for the
  fallback to re-ship, so a lost delete is repairable only while its tombstone
  is retained. Reaping is outside the model.
- Aliasing is bounded to one swap over one key in the `Alias` variant; a
  restore that rolls the projection back is outside the model.
- The projection digest folds each cluster's checkpoint offsets, so equal
  content may still mismatch and the walk may over-localise. The model allows
  both; they cost traffic, not safety.
- The digest's per-entry contribution covers a row's committed value, not its
  observer-relative origin stamp, and the model compares rows the same way.
  Production hashed the origin until contribution version 1, so two converged
  clusters mismatched on every authored row. The model allows a mismatch on
  equal content, so it cannot show that defect as a violation; a leaf-digest
  detector pins the content-only contribution (see the refinement note).
- The fallback stamps re-shipped rows with A's origin and the row's own
  timestamp, so a foreign-origin row can raise B's cursor for origin A. That
  widens the below-cursor blind spot for later A-origin entries; the model
  includes the effect and convergence still holds within its bounds.
- The receiver dedup is a bounded record in production; the model keeps an
  unbounded set. Probe cadence and jitter are abstracted into fairness.
  A shard-map change is a generation counter: the data movement of a reshard
  or resize and the bootstrap floor are modelled elsewhere.
- Guard accounting is in memory per activation and is lost on restart, which
  the model includes.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `AntiEntropy` | 5 | 14 | 22 | 32 | 39 | 475,438 |
The `Alias` variant checks 200,460 distinct states.

## Defects found

- [#4859](https://github.com/NSTA1/Orleans.Lattice/issues/4859): repair-helper faults reset the circuit breaker instead of contributing a failure.
- [#4862](https://github.com/NSTA1/Orleans.Lattice/issues/4862): scoped fallback discarded retained tombstones, leaving trimmed-WAL and below-cursor lost deletes unrepaired.
- [#4863](https://github.com/NSTA1/Orleans.Lattice/issues/4863): targeted re-replay used the logical-name WAL instead of the physical tree resolved for localisation after an alias cutover.
- [#5530](https://github.com/NSTA1/Orleans.Lattice/issues/5530): the per-entry digest contribution folded the row's origin, which differs between the cluster that authored a row and its peers, so two converged clusters never matched. Found by the backup retention model's confirmation review (epic #5348); the contribution now covers content only, and a persisted fold carries its contribution version so a stale fold is recomputed rather than mixed.

Each repair has a production regression proved failing without the fix. The
refinement note maps the model obligations to those detectors; this is not a
mechanical implementation-refinement proof.

## CI partition

The 34 base, variant and mutation cases run in three deterministic-only TLC
groups: 18, nine and seven cases, measured at 539, 598 and 549 seconds with
Java 17, two visible CPUs and one TLC slot. The groups are selected by the
categories in `TlcCiShard` and the first-match partition in
`.github/workflows/test-shards.json`. Mutation-specific generated config headers
make the current control-cache keys distinct even when two mutants target the
same property; the split therefore uses measured item wall time, not assumed
same-target reuse.
