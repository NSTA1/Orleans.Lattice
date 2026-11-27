# Replication receiver admission

These modules check the design of the replication receiver's admission (issue
#6053, epic #6046): what a cluster accepts from honest and malicious peers on
the replication gRPC host (`ReplicationOriginBinding`), and what its applier
does with the runs that reach it (`ReplicationAdmission`). `RestorePeerAuthority`
(issue #6192) checks the authority a coordinated-restore participant acts under
once a peer's saga control call is admitted. They share no
variable - the host decides who may call, the applier what a run may change -
so each is checked alone. They are not machine-checked refinement proofs of the
C# implementation; each refinement note states which production seams its
detectors reach.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
| --- | --- | --- | --- | --- | --- | --- |
| `ReplicationOriginBinding` | 2 | 0 | 1 | 10 | 2 | 1,250 |
| `ReplicationAdmission` | 4 | 0 | 2 | 7 | 4 | 1,740 |
| `RestorePeerAuthority` | 4 | 0 | 8 | 17 | 6 | 45,828 |

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

## RestorePeerAuthority

**The coordinated-restore participant's authority.** A coordinated restore is
authorized once, on the coordinator cluster. A participant acting on a remote
coordinator's saga has no user identity of its own, so its restore steps run
with peer authority - inside a system-origin scope - only where the
authenticated peer's replication stream already reaches. Every other step runs
as the ambient caller, except a commit or abort of this cluster's own saga that
its coordinator has durably decided, which borrows the authority of the prepare
this participant ran, on the shadow that prepare built. The coordinator a step
names comes from the admitted-peer seam (`ReplicationOriginBinding`): over gRPC
it is the credential-bound origin of a current peer, overwriting any
body-declared value; locally it is this cluster or the coordinator the
participant grain persisted at prepare.

**Actors and actions.** A participant receives a prepare (`Prepare`), a commit
(`Commit`) or an abort (`Abort`) over gRPC from any admitted peer, with any
body-declared coordinator, or locally. The environment decides this cluster's
own saga (`Decide`), loses the participant's shadow cache (`LoseCache`),
expires the write fence (`FenceExpires`), enrolls or disables a tree
(`SetEnrollment`) and changes the topology (`SetTopology`); a cutover can fail
part-way and the decision read can fail.

**Properties.**

- `PeerAuthorityBounded`: a step runs with peer authority only when every seam
  is wired, the credential-bound coordinator is a remote cluster of the current
  topology and every tree it restores or swaps is enrolled here, a swap only
  inside the write fence - so a set commit never cuts over a member that
  stopped being replicated since prepare; a build under peer authority
  replays only the target or trees enrolled here, and an empty captured-source
  list refuses; and the elevated probe is not an existence oracle - a missing
  backup and one the peer may not restore draw the same refusal.
- `OwnSagaCommitNeedsDurableDecision`: a commit of this cluster's own saga
  without a readable, durable Commit decision is refused before any fence or
  swap, an abort of it without a durable Abort decision runs as the ambient
  caller, and the prepare's authority is borrowed only for the shadow this
  participant built.
- `NoStrandedFence`: a cutover that fails unblocks local writes before the
  failure propagates and keeps the shadow, so a redelivered decision can finish.

**Bounds.** Two peers, either of which can leave or join the topology, two
trees of which one starts enrolled, a single-tree or two-member set target, four
backups (of the restored tree, of the other tree, naming no source, and
missing), one saga, and every seam wired or not.
Every property is a safety property over the last step.

## Refinement and mutations

- [ReplicationOriginBinding refinement note](RefinementOriginBinding.md) and
  [mutations](mutations-origin-binding/).
- [ReplicationAdmission refinement note](RefinementAdmission.md) and
  [mutations](mutations-admission/).
- [RestorePeerAuthority refinement note](RefinementRestoreAuthority.md) and
  [mutations](mutations-restore-authority/).

Every property and every behavioural action of each module's `Next` has a
falsifying mutation. `ReplicationAdmission`'s `SetEnrollment`, and `RestorePeerAuthority`'s `Decide`,
`LoseCache`, `FenceExpires`, `SetEnrollment` and `SetTopology`, are environment
steps with no protocol behaviour of their own.
