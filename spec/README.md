# TLA+ specification of the Orleans.Lattice atomic-commit protocol

This directory holds a TLA+ specification of the distributed atomic-commit
protocol - the multi-leaf prepare / commit / abort saga, the per-tree
transaction-registry decision, and reader visibility - together with a TLC
model configuration that checks its safety and liveness properties
exhaustively over a small bounded instance.

It is the deliverable of level-C epic #1588, Phase 7 (#1596), lever (c): a
design-level specification above the code, checked by TLC, with a refinement
note ([`Refinement.md`](Refinement.md)) mapping it to the extracted Coyote
protocol cores.

## Why this lives here (and not in the solution)

The specification is intentionally **outside** the compiled solution
(`Orleans.Lattice.slnx`). It is not C#; it is checked by TLC, which needs a
Java runtime and the TLA+ tools. TLC is **not** wired into the required
per-PR build - see the "CI decision" section below. This directory contains
only `.tla`, `.cfg`, and `.md` files; nothing here is built by `dotnet`.

## Files

| File | What it is |
|------|-----------|
| [`AtomicCommit.tla`](AtomicCommit.tla) | The specification: state, actions, safety invariants, liveness properties. |
| [`AtomicCommit.cfg`](AtomicCommit.cfg) | The TLC model: the bounded instance and the invariant / property list to check. |
| [`Refinement.md`](Refinement.md) | The refinement note: each spec variable / action mapped to its protocol counterpart in the code cores. |
| `README.md` | This file. |

## What is modelled

The specification is deliberately abstract: keys, participant leaves, and a
transaction status. There is no serialization, no timers, no HLC, no WAL - the
issue scopes those out. The abstraction is chosen so TLC can enumerate every
interleaving of the protocol's decision and broadcast steps.

- **Coordinator** (`PrepareTx`, `DecideTx`, `BroadcastStep`) - the saga:
  prepare fan-out into hidden per-leaf pending buckets, a single terminal
  decision, then the per-leaf terminal broadcast one leaf at a time.
- **Transaction registry** (`decision`, `revision`) - the single tree-wide
  commit / abort decision and its monotonic revision. Recording the decision
  *before* the broadcast is the linearization point.
- **Reader visibility** (`ObservedPrepared`, `SurfaceViaGate`) - the per-key
  gate that resolves how a read of a key carrying a pending mutation is
  answered, resolved against one decision snapshot so a saga is all-or-nothing
  visible.
- **Reshard / migration** (`ShadowForwardOrphan`, `OrphanDrain`) - an abstract
  online shard-split step that shadow-forwards a stale prepared write onto a
  leaf that already applied the saga's terminal, and the orphan guard that
  makes that late bucket fall through instead of shadowing the authoritative
  value (the #1584 class at design level).

## Properties checked

Safety invariants (checked at every reachable state):

| Invariant | Meaning |
|-----------|---------|
| `TypeOK` | State stays well-typed. |
| `AllOrNothing` | Atomicity: within one saga every key resolves identically for a snapshot reader - never a split view. |
| `VisibilityMatchesDecision` | A key is post-saga-visible exactly when the tree-wide decision is committed (sharpest safety statement; implies the two below). |
| `StrictIsolation` | An in-flight or aborted saga is never surfaced as committed. |
| `CommitIntegrity` | Commit implies every participant acked; abort implies at least one nack. |
| `LinearizedTerminals` | No leaf applies a commit / abort terminal before the registry recorded that decision (decision-before-broadcast). |
| `NoMixedTerminals` | A saga never applies commit on one leaf and abort on another. |

Liveness / temporal properties:

| Property | Meaning |
|----------|---------|
| `DecisionDurability` | Once terminal, the registry decision never flips. |
| `MonotonicVisibility` | Once a key is post-saga-visible it stays visible (even across a reshard). |
| `RevisionMonotonic` | The registry revision counter never decreases. |
| `Termination` | Every saga terminates (under weak fairness of saga progress). |
| `EveryCommittedKeyReadable` | Every committed saga's keys eventually all become readable. |

## The bounded instance

`AtomicCommit.cfg` fixes a concrete instance:

- 2 concurrent sagas (`t1`, `t2`),
- 3 keys (`k1`, `k2`, `k3`),
- `t1` writes `{k1, k2}`, `t2` writes `{k2, k3}` - 2 participants each,
  overlapping on `k2`,
- a bounded reshard orphan step per key (used-once budget).

To widen the instance, edit `TxWrites`, `Txns`, and `Keys` in
`AtomicCommit.tla` and add the matching model-value constants to
`AtomicCommit.cfg`. The state space stays small (a few thousand states) for
2-3 sagas over 3-4 keys; larger instances grow quickly.

## How to run TLC

You need a Java runtime (JDK/JRE 11+) and `tla2tools.jar` from the
[TLA+ tools releases](https://github.com/tlaplus/tlaplus/releases).

```bash
# From this directory, with tla2tools.jar on hand:
java -cp /path/to/tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

On Windows PowerShell:

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

A clean run ends with `Model checking completed. No error has been found.`
and reports zero invariant or temporal-property violations and no deadlock.

### Confirming the model is non-vacuous

The invariants are load-bearing, not trivially true. To convince yourself,
temporarily weaken `BroadcastStep` so a leaf may apply its terminal while the
saga is still in `phase = "prepared"` (i.e. before `DecideTx` records the
decision). TLC then reports `Invariant AllOrNothing is violated` with a
counterexample trace: a reader observes one key at its post-saga value while a
sibling key still shows pre-saga - exactly the split view the linearization
point exists to prevent. Revert the weakening to restore the clean run.

## Last checked

This specification was checked with **TLC 2.19** (tla2tools, rev 5a47802) on a
Temurin 21 JRE:

```
Model checking completed. No error has been found.
7649 states generated, 2809 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 17.
```

All seven invariants and all five temporal properties held; no deadlock.

## CI decision

TLC **is** run per PR, as an ordinary NUnit fixture
(`test/lattice/Formal/TlcModelCheckTests.cs`) tagged `[Category("Tlc")]`. It
therefore rides the existing test fan-out with no change to the matrix planner:
the `deterministic` tier is the complement of `Chaos` and `Coyote`, so a new
category lands in it automatically, and `test/lattice`'s last shard is a
complement shard, so a new namespace is picked up without editing the shard
config. The workflow provisions a Temurin 17 JRE and a digest-pinned
`tla2tools.jar` before the leg runs.

This reverses an earlier decision recorded here, which is worth stating plainly
rather than quietly overwriting. That decision rested on two premises: that the
.NET build image carries no Java runtime, and that the specification tracks the
protocol *design* rather than any single code change, so gating a PR on it would
buy little marginal signal. The first premise was simply wrong - the GitHub
runner image ships several JDKs, and `actions/setup-java` selects one from the
image cache in a couple of seconds. The second was right about what TLC *was*
being asked to do, and is the part that changed: the fixture no longer only
checks that the specification holds. It checks that each paired **mutant** makes
its property fire, by name. That is a claim about the specification's own
diagnostic power, and unlike the design it tracks, it regresses silently the
moment somebody weakens a property - which is exactly the failure the atomicity
audit (epic #2299) found four times over.

The local invocation documented above remains supported and is still the fast
path when iterating on the protocol design.

The dev loop does **not** run this category. The Tier 1 filter in
[`.github/instructions/testing.instructions.md`](../.github/instructions/testing.instructions.md)
excludes `Tlc` alongside `AzureStorageEmulator`, for the same reason: a
contributor without the external toolchain should not be blocked. Absence is
handled asymmetrically and deliberately - the fixture skips locally (a visible
`Skipped` count, not `Assert.Inconclusive`, which NUnit counts as neither passed
nor failed nor skipped and which has already produced a false green here) and
**fails** when `GITHUB_ACTIONS` is set, because in CI a missing toolchain is a
broken pipeline rather than a missing convenience.
