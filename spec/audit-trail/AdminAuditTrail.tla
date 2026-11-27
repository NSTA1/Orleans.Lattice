--------------------------- MODULE AdminAuditTrail ---------------------------
(***************************************************************************)
(* Audit-trail completeness for the privileged administration surfaces    *)
(* (issue #6056, epic #6046): every protected mutation of the tenancy,     *)
(* auth and tenant-admin facades, together with the durable admin audit   *)
(* record the #3638 schema gives it.                                      *)
(*                                                                         *)
(* A facade call is authorized first; a refusal writes one Denied record   *)
(* before it is returned. An authorized mutation writes a Pending intent   *)
(* (and is refused, before any change, when it cannot), commits through    *)
(* the store, and finalizes the intent with the outcome it observed. A    *)
(* call that cannot prove its outcome, or crashes before finalizing,       *)
(* leaves the intent to the reconciler, which resolves it from the anchor  *)
(* record's evidence and never claims a commit. The final record's key is  *)
(* derived from the intent alone and written create-only, so the caller    *)
(* and the reconciler together write it at most once.                      *)
(*                                                                         *)
(* Two local calls share one anchor record, so each can move the anchor    *)
(* under the other. The environment applies a replicated revision, holds   *)
(* another region's replicated intent, fails the first call's audit writes *)
(* and fixes, per behaviour, whether the anchor's history is complete.     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

\* ---- Calls and principals ----
\* Two local facade calls, and one call made in another region whose intent
\* reaches this region by replication.
LocalOps == {"a", "b"}
RemoteOp == "r"
AllOps == LocalOps \cup {RemoteOp}

\* "admin" holds the administration grant, "boot" is a bootstrap
\* administrator, "mallory" is authenticated but refused. Asserted is the
\* subject id a client puts in its request, which is never the actor.
Callers == {"admin", "boot", "mallory"}
Authorized == {"admin", "boot"}
Bootstrap == {"boot"}
Asserted == "asserted"
NoCaller == "none"
ActorKind(a) == IF a \in Bootstrap THEN "BootstrapAdministrator" ELSE "Subject"

\* The shapes of a protected call. "put" writes the anchor (rule, group,
\* member edge, admin set, tenant lifecycle, grant, quota, region and alias
\* writes); "del" removes it and may match nothing; "activate" is an app
\* install, upgrade or enable whose activation is accepted and may still
\* land after the facade returns; "read" is a list, get or explain call,
\* which is audited only when refused.
CallerKinds(o) == IF o = "a" THEN {"put", "del", "activate", "read"} ELSE {"put", "del"}
CallerSet(o) == IF o = "a" THEN {"admin", "mallory"} ELSE {"boot"}
CommitResults(o, k) ==
    CASE o = "b" -> {"ok"}
      [] k = "activate" -> {"undecided", "domain"}
      [] OTHER -> {"ok", "domain", "infra"}
\* Call "a" can crash, its audit writes can fail and its intent ages for the
\* reconciler; "b" is a concurrent writer that always completes.
Faulty == {"a"}
Writes(o) == IF o \in Faulty THEN BOOLEAN ELSE {TRUE}

Outcomes == {"Committed", "NoChange", "Failed", "NotCommitted", "Indeterminate", "Denied"}
NegativeClaims == {"NoChange", "Failed", "NotCommitted"}
Pcs == {"idle", "authorized", "intent", "returned", "done", "crashed", "refused", "remote"}
Results == {"none", "ok", "domain", "infra", "undecided", "unavailable"}
Ages == {"young", "leased", "expired"}
Writers == {"caller", "reconciler", "replication"}

Idle == [caller |-> NoCaller, kind |-> "none", pc |-> "idle", base |-> 0, mark |-> 0,
         landed |-> FALSE, live |-> FALSE, result |-> "none", age |-> "young", meter |-> FALSE,
         actor |-> NoCaller, secret |-> FALSE, long |-> FALSE]
\* The remote region's call: authorized there, its fate known only there.
Remote == [caller |-> "admin", kind |-> "put", pc |-> "remote", base |-> 0, mark |-> 0,
           landed |-> FALSE, live |-> FALSE, result |-> "none", age |-> "expired", meter |-> FALSE,
           actor |-> "admin", secret |-> FALSE, long |-> FALSE]

\* A final audit row: whose call, what it claims, who it attributes, which
\* component wrote it, and whether it carries the request's secret payload or
\* its unbounded caller-supplied text.
Row(o, outcome, actor, writer, secret, long) ==
    [op |-> o, outcome |-> outcome, actor |-> actor, actorKind |-> ActorKind(actor),
     by |-> writer, secret |-> secret, long |-> long]

VARIABLES op, present, hist, pending, finals, viewComplete, remoteBudget
vars == <<op, present, hist, pending, finals, viewComplete, remoteBudget>>
env == <<viewComplete, remoteBudget>>

\* The anchor as AdminAuditAnchorReader observes it: its version (the
\* index of its newest revision) when present, and absent (0) otherwise.
Obs == IF present THEN Len(hist) ELSE 0

HasFinal(o) == \E r \in finals : r.op = o
RowsOf(o) == {r \in finals : r.op = o}

-----------------------------------------------------------------------------
Init ==
    /\ present \in BOOLEAN
    /\ hist = IF present THEN <<[origin |-> "local", op |-> "seed", kind |-> "put"]>> ELSE <<>>
    /\ op = [o \in AllOps |-> IF o = RemoteOp THEN Remote ELSE Idle]
    /\ pending = {RemoteOp}
    /\ finals = {}
    /\ viewComplete \in BOOLEAN
    /\ remoteBudget = 1

\* LatticeAdminAudit.AuthorizeAsync: an authorized call proceeds (a read
\* returns, a mutation goes on to its intent); a refused one writes one
\* Denied record attributing the resolved subject before the refusal is
\* returned, and a failed denial write is counted instead.
Authorize(o, c, k, w) ==
    /\ op[o].pc = "idle"
    /\ IF c \in Authorized
       THEN /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.caller = c, !.kind = k, !.pc = IF k = "read" THEN "done" ELSE "authorized"]]
            /\ finals' = finals
       ELSE /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.caller = c, !.kind = k, !.pc = "refused", !.meter = ~w]]
            /\ finals' = IF w THEN finals \cup {Row(o, "Denied", c, "caller", FALSE, FALSE)} ELSE finals
    /\ UNCHANGED <<present, hist, pending, env>>

\* DurableLatticeAdminAuditTrail.BeginAsync: the intent records the actor the
\* caller context resolves to, the anchor's base and only bounded,
\* allow-listed fields. A failed intent write refuses the call before any
\* change (fail closed).
BeginIntent(o, w) ==
    /\ op[o].pc = "authorized"
    /\ IF w
       THEN /\ pending' = pending \cup {o}
            /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.pc = "intent", !.live = TRUE, !.base = Obs, !.mark = Len(hist), !.actor = op[o].caller, !.secret = FALSE, !.long = FALSE]]
       ELSE /\ pending' = pending
            /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.pc = "done", !.result = "unavailable"]]
    /\ UNCHANGED <<present, hist, finals, env>>

\* The facade's own validate-and-commit step, unchanged by the audit. A
\* removal of an absent anchor writes nothing; a domain refusal writes
\* nothing; an infrastructure fault strikes after the write, which the
\* facade cannot tell from one before it; an accepted activation returns
\* while it is still running.
Commit(o, r) ==
    LET lands == r \in {"ok", "infra"} /\ (op[o].kind = "put" \/ present)
    IN /\ op[o].pc = "intent"
       /\ hist' = IF lands THEN Append(hist, [origin |-> "local", op |-> o, kind |-> op[o].kind]) ELSE hist
       /\ present' = IF lands THEN op[o].kind = "put" ELSE present
       /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.pc = "returned", !.landed = lands, !.live = (r = "undecided"), !.result = r]]
       /\ UNCHANGED <<pending, finals, env>>

\* LatticeAdminAudit.RunAsync and Classify, then
\* DurableLatticeAdminAuditTrail.CompleteAsync: a fault that may follow the
\* commit and an undecided result leave the intent Pending; a domain
\* refusal is Failed. A normal return is Committed only when the call's own
\* result reports that it wrote (landed); one that wrote nothing is NoChange
\* only when nothing at all has written the anchor since the intent, and
\* otherwise Indeterminate, because another writer's revision cannot be told
\* apart from the call's own. AdminAuditStore.WriteFinalAsync writes the
\* final row create-only and removes the intent; a failed write is counted
\* and left to the reconciler.
Finalize(o, w) ==
    LET outcome == CASE op[o].result \in {"infra", "undecided"} -> "Pending"
                     [] op[o].result = "domain" -> "Failed"
                     [] op[o].landed -> "Committed"
                     [] Len(hist) = op[o].mark -> "NoChange"
                     [] OTHER -> "Indeterminate"
    IN /\ op[o].pc = "returned"
       /\ IF outcome = "Pending" \/ ~w
          THEN /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.pc = "done"]]
               /\ UNCHANGED <<pending, finals>>
          ELSE /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.pc = "done"]]
               /\ finals' = IF HasFinal(o) THEN finals ELSE finals \cup {Row(o, outcome, op[o].actor, "caller", op[o].secret, op[o].long)}
               /\ pending' = pending \ {o}
       /\ UNCHANGED <<present, hist, env>>

\* The caller stops between its intent and its commit, or between its commit
\* and its finalize. Its intent stays for the reconciler; an accepted
\* activation keeps running without it.
Crash(o) ==
    /\ op[o].pc \in {"intent", "returned"}
    /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.pc = "crashed", !.live = @ /\ op[o].result = "undecided"]]
    /\ UNCHANGED <<present, hist, pending, finals, env>>

\* AdminAuditReconciler.SweepAsync with AdminAuditEvidence.ResolveAsync. Only
\* this region's intents older than the lease are swept. One whose final row
\* already exists is only removed. Otherwise: an anchor proven unmoved (a
\* present base at the same version, or an absent base with complete history
\* holding no revision since the intent) proves no commit only once the
\* intent is older than the maximum pending age; a local revision since the
\* base is Indeterminate; complete history holding only replicated revisions
\* proves no commit once the intent is past the maximum pending age; anything
\* else waits, then is Indeterminate. Never Committed.
Reconcile(o) ==
    LET expired == op[o].age = "expired"
        unmoved == IF op[o].base # 0 THEN Obs = op[o].base ELSE Obs = 0 /\ viewComplete /\ Len(hist) = op[o].mark
        localSince == \E i \in (op[o].mark + 1)..Len(hist) : hist[i].origin = "local"
        verdict == CASE unmoved -> IF expired THEN "NotCommitted" ELSE "wait"
                     [] viewComplete /\ localSince -> "Indeterminate"
                     [] viewComplete -> IF expired THEN "NotCommitted" ELSE "wait"
                     [] OTHER -> IF expired THEN "Indeterminate" ELSE "wait"
    IN /\ o \in pending
       /\ o \in LocalOps
       /\ op[o].age # "young"
       /\ IF HasFinal(o)
          THEN finals' = finals
          ELSE /\ verdict # "wait"
               /\ finals' = finals \cup {Row(o, verdict, op[o].actor, "reconciler", op[o].secret, op[o].long)}
       /\ pending' = pending \ {o}
       /\ UNCHANGED <<op, present, hist, env>>

\* A replicated revision of the anchor is applied as data: it never passes
\* through a facade, so it writes no audit row.
ReplicateApply(k) ==
    /\ remoteBudget > 0
    /\ hist' = Append(hist, [origin |-> "remote", op |-> RemoteOp, kind |-> k])
    /\ present' = (k = "put")
    /\ remoteBudget' = remoteBudget - 1
    /\ UNCHANGED <<op, pending, finals, viewComplete>>

\* ---- Environment ----

\* An accepted activation finishes after its facade returned: it lands, or
\* it does not.
Settle(o, lands) ==
    /\ op[o].live
    /\ op[o].result = "undecided"
    /\ hist' = IF lands THEN Append(hist, [origin |-> "local", op |-> o, kind |-> "put"]) ELSE hist
    /\ present' = IF lands THEN TRUE ELSE present
    /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.landed = @ \/ lands, !.live = FALSE]]
    /\ UNCHANGED <<pending, finals, env>>

\* Time passes for a pending intent: past the lease, then past the maximum
\* pending age. The maximum pending age exceeds the longest an operation can
\* still be running, so an intent reaches it only once its call has settled.
Tick(o) ==
    /\ o \in pending
    /\ o \in LocalOps
    /\ op[o].age # "expired"
    /\ op[o].age = "leased" => ~op[o].live
    /\ op' = [op EXCEPT ![o] = [@ EXCEPT !.age = IF @ = "young" THEN "leased" ELSE "expired"]]
    /\ UNCHANGED <<present, hist, pending, finals, env>>

Quiesced ==
    \A o \in LocalOps : op[o].pc \in {"done", "crashed", "refused"} /\ ~op[o].live /\ o \notin pending

Stutter == Quiesced /\ UNCHANGED vars

Next ==
    \/ \E o \in LocalOps : \E c \in CallerSet(o), k \in CallerKinds(o), w \in Writes(o) : Authorize(o, c, k, w)
    \/ \E o \in LocalOps : \E w \in Writes(o) : BeginIntent(o, w)
    \/ \E o \in LocalOps : \E r \in CommitResults(o, op[o].kind) : Commit(o, r)
    \/ \E o \in LocalOps : \E w \in Writes(o) : Finalize(o, w)
    \/ \E o \in Faulty : Crash(o)
    \/ \E o \in AllOps : Reconcile(o)
    \/ \E k \in {"put", "del"} : ReplicateApply(k)
    \/ \E o \in LocalOps, lands \in BOOLEAN : Settle(o, lands)
    \/ \E o \in Faulty : Tick(o)
    \/ Stutter

\* A running call goes on to its next step, the reconciler sweeps, time
\* passes, activations settle and faults heal.
Fairness ==
    /\ \A o \in LocalOps : WF_vars(\E w \in Writes(o) : BeginIntent(o, w))
    /\ \A o \in LocalOps : WF_vars(\E r \in CommitResults(o, op[o].kind) : Commit(o, r))
    /\ \A o \in LocalOps : WF_vars(\E w \in Writes(o) : Finalize(o, w))
    /\ \A o \in AllOps : WF_vars(Reconcile(o))
    /\ \A o \in Faulty : WF_vars(Tick(o))
    /\ \A o \in LocalOps : WF_vars(\E lands \in BOOLEAN : Settle(o, lands))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
RowRecs ==
    [op : AllOps, outcome : Outcomes, actor : Callers \cup {Asserted, NoCaller}, actorKind : {"Subject", "BootstrapAdministrator"},
     by : Writers, secret : BOOLEAN, long : BOOLEAN]

TypeOK ==
    /\ \A o \in AllOps : op[o].pc \in Pcs /\ op[o].result \in Results /\ op[o].age \in Ages
    /\ \A o \in AllOps : op[o].landed \in BOOLEAN /\ op[o].live \in BOOLEAN /\ op[o].meter \in BOOLEAN
    /\ present \in BOOLEAN
    /\ Len(hist) <= 5
    /\ pending \subseteq AllOps
    /\ finals \subseteq RowRecs
    /\ viewComplete \in BOOLEAN

\* EveryCommitAudited: a call that committed always has its intent still
\* pending or its final row; no call ever has more than one final row; and
\* every row of an authorized call attributes the principal the facade
\* authorized, with its actor kind, never an id the request asserted.
EveryCommitAudited ==
    \A o \in LocalOps :
        /\ Cardinality(RowsOf(o)) <= 1
        /\ op[o].landed => (o \in pending \/ HasFinal(o))
        /\ op[o].caller \in Authorized => \A r \in RowsOf(o) : r.actor = op[o].caller /\ r.actorKind = ActorKind(op[o].caller)

\* NoPhantomAudit: a row claims a change only for a call that made one.
NoPhantomAudit ==
    \A r \in finals : r.outcome = "Committed" => (r.op \in LocalOps /\ op[r.op].landed)

\* NoFalseAbsence: a row that says the call changed nothing (no change, a
\* failure, not committed) is never written for a call that did, and stays
\* true: the call never lands afterwards.
NoFalseAbsence ==
    \A r \in finals : r.outcome \in NegativeClaims => ~op[r.op].landed

\* NeverNotCommittedWhileLive: not-committed is recorded only for a call that
\* can no longer commit.
NeverNotCommittedWhileLive ==
    \A r \in finals : r.outcome = "NotCommitted" => ~op[r.op].live

\* ReconcilerNeverClaimsCommitted: the reconciler has no proof that a given
\* revision is its call's, so it never claims a commit.
ReconcilerNeverClaimsCommitted ==
    \A r \in finals : r.by = "reconciler" => r.outcome # "Committed"

\* DenialsAudited: every refusal returned to an authenticated caller, of a
\* mutation or a read, has one Denied row attributing that caller, unless the
\* audit write itself failed and was counted; a refused call never changes
\* anything; and only a refusal is recorded as a denial.
DenialsAudited ==
    /\ \A o \in LocalOps : op[o].pc = "refused" => ~op[o].landed
    /\ \A o \in LocalOps : op[o].pc = "refused" => op[o].meter \/ \E r \in RowsOf(o) : r.outcome = "Denied" /\ r.actor = op[o].caller /\ r.actorKind = ActorKind(op[o].caller)
    /\ \A r \in finals : r.outcome = "Denied" => op[r.op].pc = "refused"

\* ReplicatedNotReAudited: every row this region writes is for a call made
\* through one of its own facades. A replicated revision writes no row, and
\* another region's intent is never resolved here by local evidence.
ReplicatedNotReAudited ==
    \A r \in finals : r.op \in LocalOps /\ r.by \in {"caller", "reconciler"}

\* NoSecretMaterial: no row carries the request's secret payload (a rule
\* body, a credential, a manifest) or the caller's unbounded text.
NoSecretMaterial ==
    \A r \in finals : ~r.secret /\ ~r.long

\* IntentsResolve: every local intent is eventually finalized and removed.
IntentsResolve ==
    \A o \in LocalOps : (o \in pending) ~> (o \notin pending)
=============================================================================
