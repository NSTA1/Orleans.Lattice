# Replication receiver admission

These modules check the design of the replication receiver's admission (issue
#6053, epic #6046): what a cluster accepts from honest and malicious peers on
the replication gRPC host (`ReplicationOriginBinding`), and what its applier
does with the runs that reach it (`ReplicationAdmission`). They share no
variable - the host decides who may call, the applier what a run may change -
so each is checked alone. They are not machine-checked refinement proofs of the
C# implementation; each refinement note states which production seams its
detectors reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `ReplicationOriginBinding` | 2 | 0 | 1 | 10 | 2 | 1,250 |
| `ReplicationAdmission` | 4 | 0 | 2 | 7 | 4 | 1,740 |

## ReplicationOriginBinding

**The admitted-peer seam.** Every method bound on the replication gRPC host -
the six push-transport verbs, the two remote-snapshot verbs and the five saga
control verbs - is registered with one seam, `ReplicationAdmittedPeer`, under an
admission class. The shared-secret interceptor refuses a method of the
replication services the seam does not register, before any credential and
with the status an unbound method gets, so a new cross-cluster service cannot
be bound without naming how it admits its caller. With
`BindCredentialToOriginCluster` on, the interceptor then binds the presented
credential to the transport-stamped origin. An origin-bound verb (`Push`,
`ExchangeContentManifest`, `GetPeerHighWaterMark`) requires a present stamped
origin equal to the origin its body declares; a control verb acts only for the
credential-bound origin of an authorized peer of the current topology, binds
the body's coordinator to it and overwrites the body's requester with it; a
peer-read verb consumes no body-declared origin.

**Actors and actions.** A peer calls any bound method with any stamped origin,
credential and body-declared origin (`Call`): a current peer, a former peer
whose secret is still accepted, and a stranger with no secret. Credential
binding is on or off for the whole run.

**Property.** `OriginAuthenticated`: every accepted call passed the seam. Its
method is registered, its credential accepted and, with binding on, bound to
the stamped origin; a body-declared origin is accepted only when the stamped
origin is present and equal to it; a control verb acts only for a
credential-bound current peer; and a missing, unknown or mismatched origin
draws one indistinguishable refusal.

**Bounds.** Three clusters, three credentials and every bound method. The
property is a safety property over the last call, so each counterexample is
one step long.

## ReplicationAdmission

**The applier.** A run reaches the applier from a push or from a replay (the
causal-buffer drain or a dead-letter replay) and is classified against the
local enrollment: a tree not enrolled here is dropped, an enrolled tree whose
locally resolved mode does not admit the run's write class is dead-lettered,
and an admitted run is applied and records inbound contact under the row cap.
The merge mode is an open set of write classes: `Admits(mode, class)` is
abstract and the cfg binds it to `ModeAdmits`, so a future consensus-governed
tree that admits only decided writes inherits the property unchanged.

**Actors and actions.** A run for an enrolled tree, or for a peer-chosen tree
id never enrolled here, of any write class and from any origin, reaches the
applier along either path (`ApplyRun`). An operator enrolls, re-modes or
disables a tree (`SetEnrollment`).

**Properties.**

- `NonEnrolledDropped`: a run for a tree not enrolled here is dropped, never
  dead-lettered, whichever path delivers it, so dead-letter activations stay
  within the enrolled trees under any peer behaviour.
- `ModeReResolvedLocally`: an enrolled tree's run is applied only when the
  locally resolved mode admits its write class, and a disagreement is
  dead-lettered. The wire mode is never trusted.
- `ContactOnlyWhenAdmitted`: inbound per-peer contact is recorded only for an
  admitted run, and the inbound rows stay capped.

**Bounds.** Three trees of which two are enrolled, two merge modes and three
write classes, two origins, two receive paths and an inbound-row cap of one.
Every property is a safety property over the last run and the accumulated
dead-letter and contact state. There are no liveness properties in either
module.

## Refinement and mutations

- [ReplicationOriginBinding refinement note](RefinementOriginBinding.md) and
  [mutations](mutations-origin-binding/).
- [ReplicationAdmission refinement note](RefinementAdmission.md) and
  [mutations](mutations-admission/).

Every property and every behavioural action of each module's `Next` has a
falsifying mutation. `SetEnrollment` is an environment step with no protocol
behaviour of its own.
