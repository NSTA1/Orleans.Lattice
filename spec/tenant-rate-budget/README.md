# Tenant request-rate budgets

`TenantRateBudget` models the existing per-silo coordinator and GCRA limiter,
not a future distributed token authority. Its configuration, manifest,
anchored mutations and [refinement mapping](Refinement.md) enroll it in the
existing automatically discovered Formal harness without harness changes.

## Instance

One tenant, two silos, rates of one or two operations per second, a 50-percent
burst allowance, and a clock with two ticks per second. The first silo begins
with its bootstrap bucket; the second may join with no bucket. Each silo can
run one additional lease cycle. One departure, one budget change, and one
coordinator restart may occur, with a delayed response or cancellation between
the allocation snapshot and its installation. One clock tick permits refill
and marks the next lease cadence. This represents a supported half-second
lease interval, not the default interval. The small demand domain includes an
idle silo, a sole demander and a partial share.

## What is checked

- Shares are positive and bounded by the **captured** cluster rate, using
  static-even fallback or demand-proportional arithmetic with reserve zero.
- The snapshot uses the live count and the admitted demand that it resets.
- Joined silos bootstrap locally; departed silos leave the membership input.
- A canceled delayed grant cannot replace a bucket or reset its debt.
- Identical refreshes and same-silo coordinator recreation preserve GCRA debt.
- Admission advances TAT from the later of the old TAT and the current clock,
  with one emission interval per successful operation. Refusals do not count
  as admitted demand.
- Cadence expiry does not expire the enforcing bucket. Registry budget changes
  are deferred until the next completed refresh.
- A pending cycle settles under eventual response/exception completion. The
  fairness premise and its limits are explicit in the refinement note.

Every checked property has an anchored mutation; every non-stuttering action
is perturbed. The base and each mutant are deadlock-free; no mutation disables
deadlock checking. `LeaseNeverSettles` preserves the base fairness but changes
the protocol's response-completion step.

## Bounds, not an exact cluster ceiling

Production floors each share to one operation per second. A positive burst
percent floors additional burst tokens to at least one, and a GCRA bucket
admits `tolerance / emission + 1` immediate operations. New or changed
parameters install a fresh bucket. Existing silos may retain a pre-join,
pre-departure or pre-budget-change share until they refresh.

Consequently neither the current registry rate nor the sum of freshly divided
static shares is a strict instantaneous admission ceiling. The invariant is
per allocation snapshot, and the operation bound is per unchanged bucket.
General production timestamp division also quantizes sustained rates:
the effective rate is timestamp frequency divided by the floored emission
interval (itself at least one tick). The chosen model rates divide its
frequency exactly; it does not claim to verify all machine-integer extremes.

There is no expiring distributed rate lease in production.
`LocalTenantClusterDemandExchange` returns no aggregate, so the default follows
static-even fallback. A custom aggregate may engage demand apportionment.
The reserve-zero instance does not establish that independent asynchronous
aggregate snapshots sum to a strict cluster ceiling.

## Defect found

A delayed collaborator could return after the cycle token had been canceled,
and the coordinator would still install its response. An enumeration ending
after cancellation could also prune an existing bucket, making that tenant
unthrottled. The coordinator now checks cancellation at the synchronous
application boundaries as well as after its asynchronous reads.
`TenantRateBudgetCancellationTests` reproduced both cases before the fix;
`CancelledGrantInstalled` and `CancelDropsBucket` expose the corresponding
abstract effects. This does not make an uncooperative collaborator terminate:
it rejects its response when it finally returns.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `TenantRateBudget` | 2 | 12 | 11 | 14 | 23 | 383,624 |
