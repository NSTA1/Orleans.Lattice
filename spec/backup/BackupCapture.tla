--------------------------- MODULE BackupCapture ---------------------------
(***************************************************************************)
(* An abstract TLA+ specification of a backup capture racing in-flight     *)
(* atomic sagas: the per-tree capture under a lease-fenced decision gate,  *)
(* and the cross-tree backup set's fence, drain gate, in-flight re-check   *)
(* and post-capture validation (LatticeBackupCaptureService.CaptureSetAsync*)
(* and CaptureFencedSetAsync).                                             *)
(*                                                                         *)
(* It models the protocol DESIGN, not the code. Keys are shards and a      *)
(* write is identified by the saga that made it. See Refinement.md for the *)
(* mapping to production symbols, and README.md for the instance.         *)
(*                                                                         *)
(* The decision gate is the INTENDED design of issue #4485, which found    *)
(* that production captures each shard at its own moment and serves a     *)
(* still-pending bucket pre-saga. Two mutations keep current production    *)
(* standing as a reproduction (BackupSagaConsistentPendingReadsPre and     *)
(* BackupSagaConsistentShardsCapturedApart).                               *)
(*                                                                         *)
(* Epic #4430, issue #4440.                                                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* The bounded instance. Tree T1 has shards a and b, tree T2 has shard c.  *)
(* Saga s is a single-tree batch over {a, b}. Saga x is a cross-tree batch *)
(* over {a, b, c}: two shards on T1, so a single-tree capture of T1 can    *)
(* tear it, and one on T2, so a set capture can tear it across trees.      *)
(* kind is fixed at Init: a standalone single-tree capture of T1, or a     *)
(* cross-tree-consistent set capture of both trees. MaxAttempts bounds the *)
(* capture's attempts (LatticeBackupOptions.MaxCrossTreeFenceAttempts).    *)
(***************************************************************************)
Trees == {"T1", "T2"}
Shards == {"a", "b", "c"}
TreeOf == [sh \in Shards |-> IF sh = "c" THEN "T2" ELSE "T1"]
Sagas == {"s", "x"}
MaxAttempts == 2
Outcomes == {"none", "committed", "aborted"}

(***************************************************************************)
(* State.                                                                  *)
(*  reg[t][T]     cross-tree saga t registered its decision authority with *)
(*                tree T (RegisterExternalDecisionAuthorityAsync, or the   *)
(*                receiver's RegisterReceiverDecisionAuthorityAsync).      *)
(*  deleg[t][T]   tree T holds t's delegation row: its in-flight count.    *)
(*  epoch[T]      T's monotonic cross-tree registration epoch.             *)
(*  prep[t][sh]   t's prepare has staged its bucket on shard sh.           *)
(*  dec[t]        t's outcome as its coordinator recorded it.             *)
(*  loc[t][T]     T's LOCAL decision record for t (a Mark: the saga's own  *)
(*                decision for a single-tree saga, the tree's finalize or  *)
(*                a cached delegated verdict for a cross-tree saga).       *)
(*  term[t][sh]   the terminal shard sh applied for t.                     *)
(*  lease[T]      the lease T's registry holds: none, fence or gate.       *)
(*  held[T]       the lease's token has been held continuously since it    *)
(*                was taken (release-with-validation's answer).            *)
(*  gated[T]      T's gate was acquired in this attempt.                   *)
(*  d0[T]         the LOCAL decision map T's registry snapshotted at gate  *)
(*                acquire.                                                 *)
(*  kind          single (standalone capture of T1) or set.               *)
(*  phase         fence -> drain -> gating -> capture -> done, or failed.  *)
(*  attempt       the attempt in progress.                                 *)
(*  before[T]     the epoch the drain gate observed at its drained moment.*)
(*  capd[sh]      shard sh has been captured in this attempt.             *)
(*  img[sh][t]    what the capture holds of t on shard sh.                 *)
(***************************************************************************)
VARIABLES reg, deleg, epoch, prep, dec, loc, term,
          lease, held, gated, d0,
          kind, phase, attempt, before, capd, img

sagaVars == <<reg, deleg, epoch, prep, dec, loc, term>>
leaseVars == <<lease, held, gated, d0>>
capVars == <<kind, phase, attempt, before, capd, img>>
vars == <<reg, deleg, epoch, prep, dec, loc, term, lease, held, gated, d0,
          kind, phase, attempt, before, capd, img>>

\* The instance's writes depend on the capture kind, which Init fixes.
Writes(t) == IF t = "s" THEN {"a", "b"} ELSE IF kind = "single" THEN {"a", "b", "c"} ELSE {"b", "c"}
TreesOf(t) == {TreeOf[sh] : sh \in Writes(t)}
CrossTree(t) == t = "x"
\* A set capture runs only the cross-tree saga: the single-tree saga's
\* consistency within a tree is the single-tree capture's to check, and the
\* set re-checks it per member regardless.
Active(t) == kind = "single" \/ t = "x"

TypeOK ==
    /\ reg \in [Sagas -> [Trees -> BOOLEAN]]
    /\ deleg \in [Sagas -> [Trees -> BOOLEAN]]
    /\ epoch \in [Trees -> 0..1]
    /\ prep \in [Sagas -> [Shards -> BOOLEAN]]
    /\ dec \in [Sagas -> {"inflight", "committed", "aborted"}]
    /\ loc \in [Sagas -> [Trees -> Outcomes]]
    /\ term \in [Sagas -> [Shards -> {"none", "commit", "abort"}]]
    /\ lease \in [Trees -> {"none", "fence", "gate"}]
    /\ held \in [Trees -> BOOLEAN]
    /\ gated \in [Trees -> BOOLEAN]
    /\ d0 \in [Trees -> [Sagas -> Outcomes]]
    /\ kind \in {"single", "set"}
    /\ phase \in {"fence", "drain", "gating", "capture", "done", "failed"}
    /\ attempt \in 1..MaxAttempts
    /\ before \in [Trees -> 0..1]
    /\ capd \in [Shards -> BOOLEAN]
    /\ img \in [Shards -> [Sagas -> {"none", "pre", "post"}]]

Members == IF kind = "single" THEN {"T1"} ELSE Trees
MemberShards == {sh \in Shards : TreeOf[sh] \in Members}
InFlight(T) == \E t \in Sagas : deleg[t][T]
Gated(T) == lease[T] = "gate"
\* A tree whose lease refuses a NEW cross-tree delegation registration.
Fenced(T) == lease[T] # "none"

(***************************************************************************)
(* What the capture holds of saga t on shard sh: an applied terminal as    *)
(* applied; a still-pending bucket resolved against the tree's gate        *)
(* snapshot d0, as SelectDecidingPrepare and AtomicVisibilityGate.ResolveKey*)
(* resolve it - a LOCAL committed decision serves post-saga, anything else *)
(* (none, an abort, a delegated txid with no local decision) pre-saga.     *)
(***************************************************************************)
Image(t, sh) ==
    IF sh \notin Writes(t) THEN "none"
    ELSE IF term[t][sh] = "commit" THEN "post"
    ELSE IF term[t][sh] = "abort" THEN "pre"
    ELSE IF prep[t][sh]
         THEN (IF d0[TreeOf[sh]][t] = "committed" THEN "post" ELSE "pre")
    ELSE "pre"

Init ==
    /\ reg = [t \in Sagas |-> [T \in Trees |-> FALSE]]
    /\ deleg = [t \in Sagas |-> [T \in Trees |-> FALSE]]
    /\ epoch = [T \in Trees |-> 0]
    /\ prep = [t \in Sagas |-> [sh \in Shards |-> FALSE]]
    /\ dec = [t \in Sagas |-> "inflight"]
    /\ loc = [t \in Sagas |-> [T \in Trees |-> "none"]]
    /\ term = [t \in Sagas |-> [sh \in Shards |-> "none"]]
    /\ lease = [T \in Trees |-> "none"]
    /\ held = [T \in Trees |-> FALSE]
    /\ gated = [T \in Trees |-> FALSE]
    /\ d0 = [T \in Trees |-> [t \in Sagas |-> "none"]]
    /\ kind \in {"single", "set"}
    /\ phase = IF kind = "single" THEN "gating" ELSE "fence"
    /\ attempt = 1
    /\ before = [T \in Trees |-> 0]
    /\ capd = [sh \in Shards |-> FALSE]
    /\ img = [sh \in Shards |-> [t \in Sagas |-> "none"]]

(***************************************************************************)
(* SAGA ACTIONS (restating the atomic-commit saga; see Refinement.md).     *)
(*                                                                         *)
(* Register(t, T): a cross-tree saga registers its decision authority with *)
(* tree T, installing a delegation row and advancing T's epoch. Admitted   *)
(* only while T's registry holds no lease.                                 *)
(***************************************************************************)
Register(t, T) ==
    /\ CrossTree(t)
    /\ T \in TreesOf(t)
    /\ ~reg[t][T]
    /\ dec[t] = "inflight"
    /\ ~Fenced(T)
    /\ reg' = [reg EXCEPT ![t][T] = TRUE]
    /\ deleg' = [deleg EXCEPT ![t][T] = TRUE]
    /\ epoch' = [epoch EXCEPT ![T] = epoch[T] + 1]
    /\ UNCHANGED <<prep, dec, loc, term>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* RegisterRefused(t, T): a lease (fence or gate) refuses the              *)
(* registration. The refusal is NOT retried: the sub-saga compensates and  *)
(* votes Failed, so its coordinator aborts.                                *)
(***************************************************************************)
RegisterRefused(t, T) ==
    /\ CrossTree(t)
    /\ T \in TreesOf(t)
    /\ ~reg[t][T]
    /\ dec[t] = "inflight"
    /\ Fenced(T)
    /\ dec' = [dec EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<reg, deleg, epoch, prep, loc, term>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* Prepare(t, sh): one shard of the prepare fan-out stages its hidden      *)
(* bucket, one shard per step. A cross-tree saga prepares on a tree only   *)
(* after registering there.                                                *)
(***************************************************************************)
Prepare(t, sh) ==
    /\ Active(t)
    /\ sh \in Writes(t)
    /\ ~prep[t][sh]
    /\ dec[t] = "inflight"
    /\ CrossTree(t) => reg[t][TreeOf[sh]]
    /\ prep' = [prep EXCEPT ![t][sh] = TRUE]
    /\ UNCHANGED <<reg, deleg, epoch, dec, loc, term>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* Decide(t): the coordinator's decision once every participant prepared.  *)
(* The outcome is nondeterministic (CommitIntegrity is the atomic-commit   *)
(* module's). A single-tree saga's decision IS a local Mark on its tree,   *)
(* so it is refused while that tree is gated (TxRegistryWriteRetry waits). *)
(***************************************************************************)
Decide(t) ==
    /\ Active(t)
    /\ dec[t] = "inflight"
    /\ \A sh \in Writes(t) : prep[t][sh]
    /\ ~CrossTree(t) => ~Gated("T1")
    /\ \E outcome \in {"committed", "aborted"} :
         /\ dec' = [dec EXCEPT ![t] = outcome]
         /\ loc' = IF CrossTree(t) THEN loc
                   ELSE [loc EXCEPT ![t] = [T \in Trees |-> IF T \in TreesOf(t) THEN outcome ELSE "none"]]
    /\ UNCHANGED <<reg, deleg, epoch, prep, term>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* Finalize(t, T): tree T records a cross-tree saga's decision locally     *)
(* (MarkCommittedAsync / MarkAbortedAsync), dropping its delegation row.   *)
(* Allowed under a fence, refused under a gate.                            *)
(***************************************************************************)
Finalize(t, T) ==
    /\ CrossTree(t)
    /\ dec[t] # "inflight"
    /\ deleg[t][T]
    /\ ~Gated(T)
    /\ loc' = [loc EXCEPT ![t][T] = dec[t]]
    /\ deleg' = [deleg EXCEPT ![t][T] = FALSE]
    /\ UNCHANGED <<reg, epoch, prep, dec, term>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* CacheVerdict(t, T): a reader resolving a delegated txid dials the       *)
(* coordinator and caches the terminal verdict into T's decisions,         *)
(* dropping the delegation row (TxRegistryGrain.ResolveDelegatedAsync).    *)
(* The cache is a local decision record, so a gate suppresses it: a live   *)
(* reader still gets the verdict, uncached.                                *)
(***************************************************************************)
CacheVerdict(t, T) ==
    /\ CrossTree(t)
    /\ dec[t] # "inflight"
    /\ deleg[t][T]
    /\ ~Gated(T)
    /\ loc' = [loc EXCEPT ![t][T] = dec[t]]
    /\ deleg' = [deleg EXCEPT ![t][T] = FALSE]
    /\ UNCHANGED <<reg, epoch, prep, dec, term>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* Broadcast(t, sh): the saga's terminal fan-out reaches shard sh, after   *)
(* its tree's local decision record (invariant I1: a terminal is applied   *)
(* only after a durable local decision for its txid).                      *)
(***************************************************************************)
Broadcast(t, sh) ==
    /\ sh \in Writes(t)
    /\ prep[t][sh]
    /\ term[t][sh] = "none"
    /\ loc[t][TreeOf[sh]] # "none"
    /\ term' = [term EXCEPT ![t][sh] = IF loc[t][TreeOf[sh]] = "committed" THEN "commit" ELSE "abort"]
    /\ UNCHANGED <<reg, deleg, epoch, prep, dec, loc>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* Sweep(t, sh): a leaf's self-terminalise sweep, or a split's retroactive *)
(* sweep, applies a terminal to a pending bucket from a terminal-intent    *)
(* status read. That read answers from LOCAL decisions only - outside a    *)
(* gate it may dial and cache first, which is CacheVerdict, and returns a  *)
(* verdict only once the cache write is durable - so it satisfies I1 too.  *)
(***************************************************************************)
Sweep(t, sh) ==
    /\ sh \in Writes(t)
    /\ prep[t][sh]
    /\ term[t][sh] = "none"
    /\ loc[t][TreeOf[sh]] # "none"
    /\ term' = [term EXCEPT ![t][sh] = IF loc[t][TreeOf[sh]] = "committed" THEN "commit" ELSE "abort"]
    /\ UNCHANGED <<reg, deleg, epoch, prep, dec, loc>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED capVars

(***************************************************************************)
(* CAPTURE ACTIONS.                                                        *)
(*                                                                         *)
(* Fence: a set capture takes a FENCE lease on every member tree. New      *)
(* delegation registrations are refused; decisions are still allowed.     *)
(***************************************************************************)
Fence ==
    /\ phase = "fence"
    /\ lease' = [T \in Trees |-> IF T \in Members THEN "fence" ELSE lease[T]]
    /\ held' = [T \in Trees |-> IF T \in Members THEN TRUE ELSE held[T]]
    /\ phase' = "drain"
    /\ UNCHANGED <<gated, d0>>
    /\ UNCHANGED <<kind, attempt, before, capd, img>>
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* DrainGate: the drain, run outside the gate. It passes at a moment when  *)
(* no member tree holds a delegation row, and records each tree's epoch    *)
(* at that drained moment.                                                 *)
(***************************************************************************)
DrainGate ==
    /\ phase = "drain"
    /\ \A T \in Members : ~InFlight(T)
    /\ before' = epoch
    /\ phase' = "gating"
    /\ UNCHANGED <<kind, attempt, capd, img>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* GateAcquire(T): the capture takes the GATE on member tree T's registry. *)
(* At acquire the registry snapshots d0, its LOCAL decision map (no        *)
(* coordinator is dialled). From here no decision can be recorded on T.    *)
(***************************************************************************)
GateAcquire(T) ==
    /\ phase = "gating"
    /\ T \in Members
    /\ ~gated[T]
    /\ lease' = [lease EXCEPT ![T] = "gate"]
    /\ held' = [held EXCEPT ![T] = TRUE]
    /\ gated' = [gated EXCEPT ![T] = TRUE]
    /\ d0' = [d0 EXCEPT ![T] = [t \in Sagas |-> loc[t][T]]]
    /\ UNCHANGED capVars
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* Release: every lease the capture took is released, and the per-attempt  *)
(* state cleared. Used by every exit from an attempt.                      *)
(***************************************************************************)
Released ==
    /\ lease' = [T \in Trees |-> "none"]
    /\ held' = [T \in Trees |-> FALSE]
    /\ gated' = [T \in Trees |-> FALSE]
    /\ d0' = [T \in Trees |-> [t \in Sagas |-> "none"]]

\* An attempt that cannot be accepted: retry from the start, or fail
\* explicitly once the attempts run out.
RetryOrFail ==
    /\ Released
    /\ IF attempt < MaxAttempts
       THEN /\ attempt' = attempt + 1
            /\ phase' = IF kind = "single" THEN "gating" ELSE "fence"
            /\ capd' = [sh \in Shards |-> FALSE]
            /\ img' = [sh \in Shards |-> [t \in Sagas |-> "none"]]
       ELSE /\ phase' = "failed"
            /\ UNCHANGED <<attempt, capd, img>>
    /\ UNCHANGED <<kind, before>>

(***************************************************************************)
(* Recheck: once every member is gated, a set capture re-checks that no    *)
(* member holds a delegation row. Under the gate a row can only stay or    *)
(* fall, so a pass holds for the whole capture. A single-tree capture does *)
(* not re-check: a cross-tree saga not finalized on the tree at the gate   *)
(* reads pre-saga there.                                                   *)
(***************************************************************************)
Recheck ==
    /\ phase = "gating"
    /\ \A T \in Members : gated[T]
    /\ IF kind = "single" \/ \A T \in Members : ~InFlight(T)
       THEN /\ phase' = "capture"
            /\ UNCHANGED <<kind, attempt, before, capd, img>>
            /\ UNCHANGED leaseVars
       ELSE RetryOrFail
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* CaptureShard(sh): one member shard's frozen baseline, captured at any   *)
(* moment while the capture holds its gate - the per-shard fan-out is not  *)
(* one instant.                                                            *)
(***************************************************************************)
CaptureShard(sh) ==
    /\ phase = "capture"
    /\ sh \in MemberShards
    /\ ~capd[sh]
    /\ capd' = [capd EXCEPT ![sh] = TRUE]
    /\ img' = [img EXCEPT ![sh] = [t \in Sagas |-> Image(t, sh)]]
    /\ UNCHANGED <<kind, phase, attempt, before>>
    /\ UNCHANGED leaseVars
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* Validate: once every member shard is captured, release with validation. *)
(* The attempt is accepted only if every member's token was held          *)
(* continuously and, for a set, no member's epoch moved since the drained  *)
(* moment; otherwise retry or fail.                                        *)
(***************************************************************************)
Validate ==
    /\ phase = "capture"
    /\ \A sh \in MemberShards : capd[sh]
    /\ IF /\ \A T \in Members : held[T]
          /\ kind = "set" => \A T \in Members : epoch[T] = before[T]
       THEN /\ phase' = "done"
            /\ Released
            /\ UNCHANGED <<kind, attempt, before, capd, img>>
       ELSE RetryOrFail
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* LeaseLapse(T): a lease expires before it is released (its holder        *)
(* stalled past the expiry). Not fair. The lapse is reported to the        *)
(* holder's validation.                                                    *)
(***************************************************************************)
LeaseLapse(T) ==
    /\ lease[T] # "none"
    /\ lease' = [lease EXCEPT ![T] = "none"]
    /\ held' = [held EXCEPT ![T] = FALSE]
    /\ UNCHANGED <<gated, d0>>
    /\ UNCHANGED capVars
    /\ UNCHANGED sagaVars

(***************************************************************************)
(* CaptureFault: the capture throws at any step before it is accepted - a  *)
(* drain timeout, a high-water move, an unreachable coordinator, a member  *)
(* fault, cancellation, or the capturing silo dying. Not fair. Nothing is  *)
(* accepted on this path, and its leases lapse or are released.            *)
(***************************************************************************)
CaptureFault ==
    /\ phase \in {"fence", "drain", "gating", "capture"}
    /\ phase' = "failed"
    /\ Released
    /\ UNCHANGED <<kind, attempt, before, capd, img>>
    /\ UNCHANGED sagaVars

SagaDone(t) ==
    \/ ~Active(t)
    \/ /\ \A sh \in Writes(t) : prep[t][sh] => term[t][sh] # "none"
       /\ \A T \in Trees : ~deleg[t][T]
       /\ dec[t] # "inflight"

FullyQuiesced ==
    /\ \A t \in Sagas : SagaDone(t)
    /\ phase \in {"done", "failed"}

Stutter == FullyQuiesced /\ UNCHANGED vars

Next ==
    \/ \E t \in Sagas : \E T \in Trees : Register(t, T)
    \/ \E t \in Sagas : \E T \in Trees : RegisterRefused(t, T)
    \/ \E t \in Sagas : \E sh \in Shards : Prepare(t, sh)
    \/ \E t \in Sagas : Decide(t)
    \/ \E t \in Sagas : \E T \in Trees : Finalize(t, T)
    \/ \E t \in Sagas : \E T \in Trees : CacheVerdict(t, T)
    \/ \E t \in Sagas : \E sh \in Shards : Broadcast(t, sh)
    \/ \E t \in Sagas : \E sh \in Shards : Sweep(t, sh)
    \/ Fence
    \/ DrainGate
    \/ \E T \in Trees : GateAcquire(T)
    \/ Recheck
    \/ \E sh \in Shards : CaptureShard(sh)
    \/ Validate
    \/ \E T \in Trees : LeaseLapse(T)
    \/ CaptureFault
    \/ Stutter

(***************************************************************************)
(* Fairness: every saga makes progress, and the capture keeps taking its   *)
(* steps while they are enabled. CacheVerdict and Sweep are optional       *)
(* (readers and sweeps need not run); LeaseLapse and CaptureFault are not  *)
(* fair.                                                                   *)
(***************************************************************************)
TxProgress(t) ==
    \/ \E T \in Trees : Register(t, T) \/ RegisterRefused(t, T) \/ Finalize(t, T)
    \/ \E sh \in Shards : Prepare(t, sh) \/ Broadcast(t, sh)
    \/ Decide(t)

CaptureProgress ==
    \/ Fence
    \/ DrainGate
    \/ \E T \in Trees : GateAcquire(T)
    \/ Recheck
    \/ \E sh \in Shards : CaptureShard(sh)
    \/ Validate

Spec == Init /\ [][Next]_vars
        /\ \A t \in Sagas : WF_vars(TxProgress(t))
        /\ WF_vars(CaptureProgress)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

\* An accepted capture never holds part of a saga within one tree: no shard
\* holds it post-saga while another shard of the same tree holds it
\* pre-saga. This is the guarantee every single-tree CaptureAsync makes.
BackupSagaConsistent ==
    phase = "done" =>
        \A t \in Sagas : \A a, b \in Writes(t) \cap MemberShards :
            TreeOf[a] = TreeOf[b] => ~(img[a][t] = "post" /\ img[b][t] = "pre")

\* An accepted cross-tree-consistent set never holds part of a saga across
\* its members.
SetSagaConsistent ==
    (phase = "done" /\ kind = "set") =>
        \A t \in Sagas : \A a, b \in Writes(t) :
            ~(img[a][t] = "post" /\ img[b][t] = "pre")

\* An accepted capture holds every member shard.
SetComplete ==
    phase = "done" => \A sh \in MemberShards : capd[sh]

\* A capture never holds a saga's writes unless the saga committed.
CaptureStrictIsolation ==
    \A sh \in Shards : \A t \in Sagas :
        img[sh][t] = "post" => dec[t] = "committed"

(***************************************************************************)
(* Liveness: every capture reaches a verdict - accepted, or failed         *)
(* explicitly when its attempts run out - by the protocol's own steps,     *)
(* without leaning on the fault path the drain timeout belongs to.         *)
(***************************************************************************)
SetCaptureCompletes == <>(phase \in {"done", "failed"})
=============================================================================
