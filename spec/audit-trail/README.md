# Admin audit-trail completeness

This module checks the design of the durable admin audit trail (issue #6056,
epic #6046) against the audit event schema approved on #3638: every protected
mutation of the tenancy, auth and tenant-admin facades, and every refusal of
an authenticated caller, leaves exactly the audit record the schema says it
should, including when the caller crashes between its commit and its
finalize, when another writer moves the same record, and when replication
applies a revision or delivers another region's intent. It is not a
machine-checked refinement proof of the C# implementation; the refinement note
states which production seams its detectors reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `AdminAuditTrail` | 9 | 1 | 10 | 24 | 15 | 105,489 |

## AdminAuditTrail

**Protected actions.** Each protected action of the schema has one of four
shapes, and the model's calls take each shape. A `put` writes its anchor
record: rule upserts, group and member additions, admin-set and member-set
additions, tenant create, suspend, resume, delete and quota changes, grant
offer, approve, reject and revoke, region changes, app consent and role-binding
updates, and alias sets. A `del` removes its anchor and may match nothing:
rule, group, member, admin-set and alias removals and app uninstall. An
`activate` is an app install, upgrade or enable whose activation is accepted
and may still land after the facade returns. A `read` is a list, get or
explain call, audited only when it is refused.

**Actors and steps.** Call `a` is made by an administrator or by a refused
caller; it can crash, its audit writes can fail, and its intent ages for the
reconciler. Call `b` is a bootstrap administrator's concurrent write of the
same anchor that always completes. A call is authorized (`Authorize`),
writes its intent (`BeginIntent`), commits (`Commit`) and finalizes
(`Finalize`); the reconciler (`Reconcile`) resolves an intent the caller left.
The environment applies one replicated revision of the anchor
(`ReplicateApply`), holds another region's replicated intent from the start,
crashes the caller (`Crash`), settles an accepted activation (`Settle`), ages
intents past the lease and then the maximum pending age (`Tick`), and fixes
per behaviour whether the anchor's history is complete. An incomplete history stands for a
trimmed write-ahead-log window, or one whose first surviving entry is an
old-clock replay: the movement check the caller and the reconciler share reads
it as unproven, never unmoved. A call's change comes from the store's merge at
the commit, so of two concurrent removals of one entry only one is committed.

**Properties.**

- `EveryCommitAudited`: a committed call always has its intent pending or its
  final record, never more than one final record, and every record of an
  authorized call attributes the principal the facade authorized, with its
  actor kind.
- `NoPhantomAudit`: a record claims a change only for a call that made one.
- `NoFalseAbsence`: a record saying a call changed nothing (no change, a
  failure, not committed) is written only for a call that did not, and stays
  true.
- `NeverNotCommittedWhileLive`: not committed is recorded only for a call that
  can no longer commit.
- `ReconcilerNeverClaimsCommitted`: the reconciler never claims a commit.
- `DenialsAudited`: every refusal of a read or a mutation has one Denied
  record attributing its caller, unless the write failed and was counted, and
  only refusals are denials.
- `ReplicatedNotReAudited`: every record this region writes is for one of its
  own facade calls; a replicated revision is audited nowhere and another
  region's intent is never resolved here.
- `NoSecretMaterial`: no record carries the request's payload or its
  unbounded caller text.
- `IntentsResolve` (liveness): every local intent is eventually finalized.

**Bounds.** Two local calls on one anchor that is present or absent at the
start, one replicated revision (a write or a delete), one replicated intent,
three intent ages and a history that is complete or not. The liveness
property assumes weak fairness of each call's next step, the reconciler,
ageing and activations.

## Refinement and mutations

- [Refinement note](Refinement.md), including the deliberate abstraction gaps.
- [Mutations](mutations/): every property and every behavioural action of
  `Next` has a falsifying mutation, and so does `Crash`, which carries the
  crash-path mutation. `Crash`, `Settle`, `Tick` and `Stutter` are
  environment steps with no protocol behaviour of their own.
