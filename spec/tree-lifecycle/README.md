# Tree lifecycle composition

`TreeLifecycle` checks the control-plane composition of an online resize and
undo, grow and shrink reshards, and a local shadow restore and revert. It adds
crashes at every phase boundary and recovery of the durable alias reservation,
including the completion-before-release window. The base has one crash; the
shrink variant starts with three physical shards and permits two crashes.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `TreeLifecycle` | 13 | 4 | 25 | 27 | 40 | 1,190 |
| `TreeLifecycleAdmission` | 5 | 5 | 18 | 19 | 26 | 262 |

`TreeLifecycleAdmission` separately checks the publish-before-read admission
handshake between a resize and a split/fold or legacy in-place receiver
bootstrap. Both may announce simultaneously, but cannot both enter their
active phase. It also checks after-swap undo's persisted replication clearance:
replicated trees are refused before compensation, while an already-cleared
retry must finish if replication becomes enabled later. Crash/retry preserves
both announcements and that clearance. Its `ReplicatedTwoFaults` variant starts
replicated and permits two faults (166 states); the base can enable replication
before or after clearance (262 states). It also permits a later alias cutover
before undo admission and requires the superseding copy to remain unchanged.

## Instance and interactions

Three virtual slots name sparse physical indices `0`, `2`, and optionally `3`.
A grow moves the final slot onto shard `3`; a shrink returns it to shard `2`.
The resize captures every routed index rather than treating a pinned shard
count as the routing topology. Each copy and fence acknowledgement is a
separate step, permitting a crash after any subset.

A running reshard excludes resize admission. A completed resize still excludes
reshard while its old copy mirrors shard-for-shard into its undoable replacement.
Undo arms the replacement's redirect before swapping back and releases the old
copy's fences only afterwards. Expiration ends the mirror hold. These are
different interlocks: a completed resize permits adaptive splits, which the
existing `ShardOwnership` model checks, but continues to hold folds and reshards.

Restore and revert may overtake an admitted migration. The split/fold's physical
binding prevents its old diff being written onto a different copy. A following
reshard tick plans against the newly resolved map. Both orderings are explored:
the migration can publish before the swap, or encounter the swap and replan.
A resize and a restore cannot both hold the alias reservation.

The optional initiations and undo expiration are not fair. Once an operation
starts, weak fairness applies to its progress steps and retry. Crashes are finite;
durable phase, copied indices, fences, binding and reservation survive. This is
a phase-boundary crash model, not an abstraction of arbitrary provider writes.

## Scope and companion models

This is a **control-plane composition**, not a substitute for data-plane
verification. It does not encode rows, WAL offsets, saga buckets, HLCs, CRDT
joins, view checkpoints, or replication messages. Their properties are checked
in `ShardOwnership`, `ShardOwnershipRetention`, `ShardOwnershipCrdt`,
`ShardOwnershipCutover`, `WalDurability`, and the view and anti-entropy areas.
The physical maps are moved atomically here; `BackupCutover` and the registry
detectors prove alias/map coherence and stale-router healing.

The instance admits one resize before one restore, one reshard, and a revert.
In `TreeLifecycle`, restore waits until the resize's undo window has ended.
`TreeLifecycleAdmission` adds a separate narrow check of a later alias cutover
before undo admission. Neither model claims arbitrary interleavings of a
cutover with every compensation await or with already-started undo. Snapshot streaming and the
individual split/fold sweep, freeze, merge and reclaim phases are deliberately
the companion modules' scope. A successful operation does not imply the
different features' complete Cartesian product was checked.

## Mutation controls

Every checked invariant and temporal/action property has a mutation. Every
behavioural action is perturbed. `TlcModelCheckTests` builds mutants from the
current source, checks a clean control under the same single-property cfg, and
requires the mutant to fail on that **named property**, not merely any error.
Progress mutations retain fairness and strand a real phase.

See [Refinement.md](Refinement.md) for the production seams and their detectors.

## Defect found

The supersession extension exposed #4858: resize undo unconditionally replaced
a later restore or administrative alias. The production fix refuses a third
physical identity before compensation and uses the registry's expected-state
fence. Its fresh and previously-cleared regression cases stay in the core
resize suite; the admission mutants retain the abstract counterexample.
