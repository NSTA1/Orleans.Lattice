-------------------------- MODULE BackupRetention --------------------------
(***************************************************************************)
(* An abstract TLA+ specification of backup retention pruning racing      *)
(* backup captures (issue #5350, epic #5348): a retention pass lists the   *)
(* catalog, computes what the policy keeps (keep-last, max-age, or the     *)
(* unbounded no-op) and the base closure of what it keeps, and deletes the *)
(* rest one storage call at a time, while full and incremental captures    *)
(* for the same scope run concurrently, any of them may crash at any step, *)
(* and the clock advances.                                                 *)
(*                                                                         *)
(* It models the protocol DESIGN, not the code. Each backup id is one      *)
(* capture's manifest, with its own artifact. A manifest is resolvable     *)
(* while its catalog row (which holds the whole manifest) or its sink      *)
(* manifest exists, and a backup is restorable while it is resolvable, its *)
(* artifact is in the sink and, for an increment, its base is restorable.  *)
(*                                                                         *)
(* The design checked here is the fix of the defect this module found: a   *)
(* pass deleted a manifest an in-flight increment had already chosen as    *)
(* its base, so the increment was published with a base that no longer    *)
(* existed. The fix is a two-sided fence: a pass removes a doomed          *)
(* manifest's catalog row FIRST, then scans the sink for an increment      *)
(* built on it and restores the row if it finds one; an increment writes   *)
(* its sink manifest, then re-reads its base's catalog row, and publishes  *)
(* only if the row is still there. See RefinementRetention.md for the      *)
(* mapping to production symbols.                                          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

MaxIds == 3
Ids == 1..MaxIds
NumCaps == 2
Caps == 1..NumCaps
NumPasses == 1
Passes == 1..NumPasses
MaxTime == 2
MaxCrashes == 1
MaxKeepLast == 1

Kinds == {"full", "incr"}
Meta == [kind : Kinds, base : 0..MaxIds, t : 0..MaxTime]
NoMeta == [kind |-> "full", base |-> 0, t |-> 0]
Policy == [keepLast : 0..MaxKeepLast, maxAge : 0..1]

(***************************************************************************)
(* State.                                                                  *)
(*  now      the wall clock.                                               *)
(*  pol      the scope's retention policy: keep the newest keepLast        *)
(*           backups (0 = unset) and those younger than maxAge (0 =        *)
(*           unset); both unset retains everything.                        *)
(*  next     the next backup id a capture allocates.                       *)
(*  meta     each allocated backup's manifest: full or incremental, its   *)
(*           base, and its creation time.                                  *)
(*  arts     the backups whose artifact is in the sink.                    *)
(*  snk      the backups whose manifest is in the sink.                    *)
(*  cat      the backups with a catalog row.                               *)
(*  cap      each capture slot: its phase and the backup it is writing.   *)
(*  prPh, prDoomed, prGone, prBlocked, prManGone                           *)
(*           each retention pass: its phase, the manifests it is pruning,  *)
(*           the rows it has removed, the ones it found depended on, and   *)
(*           the ones whose sink manifest it has deleted.                  *)
(*  crashes  the crash budget spent.                                       *)
(*  pub      ghost: the backups whose catalog row has ever been registered. *)
(***************************************************************************)
VARIABLES now, pol, next, meta, arts, snk, cat, cap,
          prPh, prDoomed, prGone, prBlocked, prManGone, crashes, pub

vars == <<now, pol, next, meta, arts, snk, cat, cap,
          prPh, prDoomed, prGone, prBlocked, prManGone, crashes, pub>>
storeVars == <<arts, snk, cat, pub>>
passVars == <<prPh, prDoomed, prGone, prBlocked, prManGone>>

CapPhases == {"idle", "drain", "art", "man", "ok", "stamp", "abandon"}
PassPhases == {"idle", "rows", "restore", "delete"}

TypeOK ==
    /\ now \in 0..MaxTime
    /\ pol \in Policy
    /\ next \in 1..(MaxIds + 1)
    /\ meta \in [Ids -> Meta]
    /\ arts \subseteq Ids
    /\ snk \subseteq Ids
    /\ cat \subseteq Ids
    /\ cap \in [Caps -> [ph : CapPhases, id : 0..MaxIds]]
    /\ prPh \in [Passes -> PassPhases]
    /\ prDoomed \in [Passes -> SUBSET Ids]
    /\ prGone \in [Passes -> SUBSET Ids]
    /\ prBlocked \in [Passes -> SUBSET Ids]
    /\ prManGone \in [Passes -> SUBSET Ids]
    /\ crashes \in 0..MaxCrashes
    /\ pub \subseteq Ids

Init ==
    /\ now = 0
    /\ pol \in Policy
    /\ next = 1
    /\ meta = [i \in Ids |-> NoMeta]
    /\ arts = {}
    /\ snk = {}
    /\ cat = {}
    /\ cap = [c \in Caps |-> [ph |-> "idle", id |-> 0]]
    /\ prPh = [p \in Passes |-> "idle"]
    /\ prDoomed = [p \in Passes |-> {}]
    /\ prGone = [p \in Passes |-> {}]
    /\ prBlocked = [p \in Passes |-> {}]
    /\ prManGone = [p \in Passes |-> {}]
    /\ crashes = 0
    /\ pub = {}

(***************************************************************************)
(* What the policy keeps of a listing L at the current time, as            *)
(* BackupRetentionPlanner.ComputeKeepSet decides it: the newest keepLast   *)
(* (creation time, then id, descending), and every backup created within   *)
(* maxAge; with neither set, everything. Then the base closure             *)
(* (ExpandBaseClosure): the base of a kept increment is kept, but only     *)
(* when the base is itself in the listing.                                 *)
(***************************************************************************)
Newer(j, i) == meta[j].t > meta[i].t \/ (meta[j].t = meta[i].t /\ j > i)

KeepSet(L) ==
    IF pol.keepLast = 0 /\ pol.maxAge = 0
    THEN L
    ELSE {i \in L :
            \/ pol.keepLast > 0 /\ Cardinality({j \in L : Newer(j, i)}) < pol.keepLast
            \/ pol.maxAge > 0 /\ meta[i].t + pol.maxAge >= now}

CloseStep(S, L) ==
    S \cup {meta[i].base : i \in {j \in S : meta[j].kind = "incr" /\ meta[j].base \in L}}

RetainedIn(L) == CloseStep(CloseStep(CloseStep(KeepSet(L), L), L), L)

\* A backup whose manifest names i as its base, in the sink or the catalog.
Depended(i) == \E j \in snk \cup cat : meta[j].kind = "incr" /\ meta[j].base = i

RECURSIVE Restorable(_)
Restorable(i) ==
    /\ i \in Ids
    /\ i \in cat \cup snk
    /\ i \in arts
    /\ meta[i].kind = "incr" => Restorable(meta[i].base)

(***************************************************************************)
(* The clock (environment). It only advances: a backwards step is outside *)
(* the model (see the refinement note's gaps).                             *)
(***************************************************************************)
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<pol, next, meta, cap, crashes>>
    /\ UNCHANGED storeVars
    /\ UNCHANGED passVars

(***************************************************************************)
(* A capture (environment start). Any free slot starts a full or an        *)
(* incremental capture at any time; the scheduler's own captures, the      *)
(* backup API's and another silo's are all such slots. Every capture gets  *)
(* a fresh id (BackupIdentity folds the capture's own artifact id into     *)
(* it), so no capture ever republishes an existing backup's id (#5418);   *)
(* a set member's second sink-manifest write, by the stamp, precedes its   *)
(* only registration (#5449).                                              *)
(* An increment names any base whose sink manifest it can read - the       *)
(* scheduler names the newest catalog row, the API whatever its caller     *)
(* supplies - and the read is its only check of the base before it drains. *)
(***************************************************************************)
Start(c, k, b) ==
    /\ cap[c].ph = "idle"
    /\ next <= MaxIds
    /\ k = "full" => b = 0
    /\ k = "incr" => b \in snk
    /\ meta' = [meta EXCEPT ![next] = [kind |-> k, base |-> b, t |-> now]]
    /\ cap' = [cap EXCEPT ![c] = [ph |-> "drain", id |-> next]]
    /\ next' = next + 1
    /\ UNCHANGED <<now, pol, crashes>>
    /\ UNCHANGED storeVars
    /\ UNCHANGED passVars

\* The capture streams its artifact to the sink.
WriteArt(c) ==
    /\ cap[c].ph = "drain"
    /\ arts' = arts \cup {cap[c].id}
    /\ cap' = [cap EXCEPT ![c].ph = "art"]
    /\ UNCHANGED <<now, pol, next, meta, snk, cat, crashes, pub>>
    /\ UNCHANGED passVars

\* The capture writes its manifest to the sink.
WriteMan(c) ==
    /\ cap[c].ph = "art"
    /\ snk' = snk \cup {cap[c].id}
    /\ cap' = [cap EXCEPT ![c].ph = "man"]
    /\ UNCHANGED <<now, pol, next, meta, arts, cat, crashes, pub>>
    /\ UNCHANGED passVars

(***************************************************************************)
(* Check: after its sink manifest is written, an increment re-reads its    *)
(* base's catalog row. A row still present means no pass has begun to      *)
(* prune the base, and any pass that begins later will find this sink      *)
(* manifest when it scans; a row gone means a pass is pruning the base,    *)
(* and the increment abandons its manifest instead of publishing it. A     *)
(* full capture has no base and passes.                                    *)
(***************************************************************************)
Check(c) ==
    /\ cap[c].ph = "man"
    /\ LET m == meta[cap[c].id]
       IN cap' = [cap EXCEPT ![c].ph =
                    IF m.kind = "full" \/ m.base \in cat THEN "ok" ELSE "abandon"]
    /\ UNCHANGED <<now, pol, next, meta, crashes>>
    /\ UNCHANGED storeVars
    /\ UNCHANGED passVars

(***************************************************************************)
(* The capture publishes its manifest: it registers the catalog row. A     *)
(* full capture may instead be a member of a multi-tree backup set, whose  *)
(* row is deferred to the set's membership stamp (Stamp) and registered    *)
(* there, once (#5449): until then the member is in the sink but not       *)
(* listed, so no pass can doom it. The choice is nondeterministic, so a    *)
(* full capture is checked both ways.                                      *)
(***************************************************************************)
Register(c) ==
    /\ cap[c].ph = "ok"
    /\ \/ /\ cat' = cat \cup {cap[c].id}
          /\ pub' = pub \cup {cap[c].id}
          /\ cap' = [cap EXCEPT ![c] = [ph |-> "idle", id |-> 0]]
       \/ /\ meta[cap[c].id].kind = "full"
          \* A set member: its row waits for the stamp.
          /\ UNCHANGED <<cat, pub>>
          /\ cap' = [cap EXCEPT ![c].ph = "stamp"]
    /\ UNCHANGED <<now, pol, next, meta, arts, snk, crashes>>
    /\ UNCHANGED passVars

\* The set's membership stamp (LatticeBackupCaptureService.
\* StampSetMembershipAsync), once every member is captured: it re-writes the
\* member's sink manifest, stamped with the set id, and then registers the
\* member's catalog row.
Stamp(c) ==
    /\ cap[c].ph = "stamp"
    /\ snk' = snk \cup {cap[c].id}
    /\ cat' = cat \cup {cap[c].id}
    /\ pub' = pub \cup {cap[c].id}
    /\ cap' = [cap EXCEPT ![c] = [ph |-> "idle", id |-> 0]]
    /\ UNCHANGED <<now, pol, next, meta, arts, crashes>>
    /\ UNCHANGED passVars

\* An increment whose base was being pruned deletes its own sink manifest
\* and then its artifact, as two storage calls, and falls back to a full
\* capture, which the model starts as a new capture. The artifact delete is
\* chosen nondeterministically so that a crash between the two calls - the
\* manifest gone, the artifact left unreferenced - is covered too.
Abandon(c) ==
    /\ cap[c].ph = "abandon"
    /\ snk' = snk \ {cap[c].id}
    /\ arts' \in {arts, arts \ {cap[c].id}}
    /\ cap' = [cap EXCEPT ![c] = [ph |-> "idle", id |-> 0]]
    /\ UNCHANGED <<now, pol, next, meta, cat, crashes, pub>>
    /\ UNCHANGED passVars

\* A capture crashes at any step (environment): its activation-local
\* progress is lost, and whatever it already wrote stays.
CaptureCrash(c) ==
    /\ cap[c].ph # "idle"
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ cap' = [cap EXCEPT ![c] = [ph |-> "idle", id |-> 0]]
    /\ UNCHANGED <<now, pol, next, meta>>
    /\ UNCHANGED storeVars
    /\ UNCHANGED passVars

(***************************************************************************)
(* A retention pass. PruneList is one ListScopeAsync and plan, taken under *)
(* the activation-local guard: the guard is checked and set first and the  *)
(* listing is read after it (issue #5412), so the plan is never older than *)
(* the guard. It lists the catalog, keeps what the policy retains, and     *)
(* dooms the rest; a pass with nothing to prune does nothing. The keep set *)
(* is computed here as BackupRetentionPlanner computes it, separately from *)
(* the RetainedIn the properties judge by, so that a defect in the plan is *)
(* a defect the properties can see. The pass then removes every doomed row *)
(* (RemoveRow), scans the sink once for increments built on a doomed       *)
(* manifest (Scan), restores the row of every one it found depended on     *)
(* (RestoreRow), and deletes each remaining doomed manifest's sink         *)
(* manifest (DelMan) and then its artifact (DelArt). Every delete is its   *)
(* own storage call, in any order across manifests.                        *)
(***************************************************************************)
PruneList(p) ==
    /\ prPh[p] = "idle"
    \* The activation-local guard, checked before the listing is read: a pass
    \* is skipped while another runs.
    /\ \A q \in Passes : prPh[q] = "idle"
    \* The listing, read under the guard.
    /\ LET L == cat
           Keep ==
             IF pol.keepLast = 0 /\ pol.maxAge = 0
             THEN L
             ELSE {i \in L :
                     \/ pol.keepLast > 0 /\ Cardinality({j \in L : Newer(j, i)}) < pol.keepLast
                     \/ pol.maxAge > 0 /\ meta[i].t + pol.maxAge >= now}
           D == L \ CloseStep(CloseStep(CloseStep(Keep, L), L), L)
       IN /\ D # {}
          /\ prPh' = [prPh EXCEPT ![p] = "rows"]
          /\ prDoomed' = [prDoomed EXCEPT ![p] = D]
    /\ prGone' = [prGone EXCEPT ![p] = {}]
    /\ prBlocked' = [prBlocked EXCEPT ![p] = {}]
    /\ prManGone' = [prManGone EXCEPT ![p] = {}]
    /\ UNCHANGED <<now, pol, next, meta, cap, crashes>>
    /\ UNCHANGED storeVars

RemoveRow(p, m) ==
    /\ prPh[p] = "rows"
    /\ m \in prDoomed[p] \ prGone[p]
    /\ cat' = cat \ {m}
    /\ prGone' = [prGone EXCEPT ![p] = @ \cup {m}]
    /\ UNCHANGED <<prPh, prDoomed, prBlocked, prManGone>>
    /\ UNCHANGED <<now, pol, next, meta, cap, crashes, arts, snk, pub>>

(***************************************************************************)
(* The doomed manifests the scan finds built on (BackupRetentionPlanner.   *)
(* DependedOn): those an increment in the sink outside the pass builds on, *)
(* and, transitively, the base of every one of them. An increment the pass *)
(* is itself pruning does not keep its base.                               *)
(***************************************************************************)
BuiltOn(m, S) == \E j \in S : meta[j].kind = "incr" /\ meta[j].base = m

Blocked(p) ==
    LET D == prDoomed[p]
        B0 == {m \in D : BuiltOn(m, snk \ D)}
        Grow(B) == B \cup {m \in D : BuiltOn(m, B)}
    IN Grow(Grow(Grow(B0)))

Scan(p) ==
    /\ prPh[p] = "rows"
    /\ prGone[p] = prDoomed[p]
    /\ prBlocked' = [prBlocked EXCEPT ![p] = Blocked(p)]
    /\ prPh' = [prPh EXCEPT ![p] = "restore"]
    /\ UNCHANGED <<prDoomed, prGone, prManGone>>
    /\ UNCHANGED <<now, pol, next, meta, cap, crashes>>
    /\ UNCHANGED storeVars

\* Ends the pass once nothing is left to delete.
Settle(p, d) ==
    IF d = {} THEN "idle" ELSE "delete"

RestoreRow(p, m) ==
    /\ prPh[p] = "restore"
    /\ m \in prBlocked[p]
    /\ cat' = cat \cup {m}
    /\ prBlocked' = [prBlocked EXCEPT ![p] = @ \ {m}]
    /\ prDoomed' = [prDoomed EXCEPT ![p] = @ \ {m}]
    /\ prPh' = [prPh EXCEPT ![p] =
         IF prBlocked[p] = {m} THEN Settle(p, prDoomed[p] \ {m}) ELSE "restore"]
    /\ UNCHANGED <<prGone, prManGone>>
    /\ UNCHANGED <<now, pol, next, meta, cap, crashes, arts, snk, pub>>

DelMan(p, m) ==
    /\ \/ prPh[p] = "delete"
       \/ prPh[p] = "restore" /\ prBlocked[p] = {}
    /\ m \in prDoomed[p] \ prManGone[p]
    \* Tip first: a manifest goes only after every doomed increment on it.
    /\ ~BuiltOn(m, snk \cap prDoomed[p])
    /\ snk' = snk \ {m}
    /\ prManGone' = [prManGone EXCEPT ![p] = @ \cup {m}]
    /\ prPh' = [prPh EXCEPT ![p] = "delete"]
    /\ UNCHANGED <<prDoomed, prGone, prBlocked>>
    /\ UNCHANGED <<now, pol, next, meta, cap, crashes, arts, cat, pub>>

DelArt(p, m) ==
    /\ prPh[p] = "delete"
    /\ m \in prManGone[p]
    /\ arts' = arts \ {m}
    /\ prManGone' = [prManGone EXCEPT ![p] = @ \ {m}]
    /\ prDoomed' = [prDoomed EXCEPT ![p] = @ \ {m}]
    /\ prPh' = [prPh EXCEPT ![p] = Settle(p, prDoomed[p] \ {m})]
    /\ UNCHANGED <<prGone, prBlocked>>
    /\ UNCHANGED <<now, pol, next, meta, cap, crashes, snk, cat, pub>>

\* A pass crashes at any step (environment): its plan is activation-local
\* and is lost, and whatever it already deleted stays deleted.
PruneCrash(p) ==
    /\ prPh[p] # "idle"
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ prPh' = [prPh EXCEPT ![p] = "idle"]
    /\ prDoomed' = [prDoomed EXCEPT ![p] = {}]
    /\ prGone' = [prGone EXCEPT ![p] = {}]
    /\ prBlocked' = [prBlocked EXCEPT ![p] = {}]
    /\ prManGone' = [prManGone EXCEPT ![p] = {}]
    /\ UNCHANGED <<now, pol, next, meta, cap>>
    /\ UNCHANGED storeVars

\* Natural termination once every id is used and nothing is left to do.
Stutter == UNCHANGED vars

PassStep(p) ==
    \/ PruneList(p)
    \/ \E m \in Ids : RemoveRow(p, m)
    \/ Scan(p)
    \/ \E m \in Ids : RestoreRow(p, m)
    \/ \E m \in Ids : DelMan(p, m)
    \/ \E m \in Ids : DelArt(p, m)

Next ==
    \/ Tick
    \/ \E c \in Caps, k \in Kinds, b \in 0..MaxIds : Start(c, k, b)
    \/ \E c \in Caps : WriteArt(c)
    \/ \E c \in Caps : WriteMan(c)
    \/ \E c \in Caps : Check(c)
    \/ \E c \in Caps : Register(c)
    \/ \E c \in Caps : Stamp(c)
    \/ \E c \in Caps : Abandon(c)
    \/ \E c \in Caps : CaptureCrash(c)
    \/ \E p \in Passes : PruneList(p)
    \/ \E p \in Passes, m \in Ids : RemoveRow(p, m)
    \/ \E p \in Passes : Scan(p)
    \/ \E p \in Passes, m \in Ids : RestoreRow(p, m)
    \/ \E p \in Passes, m \in Ids : DelMan(p, m)
    \/ \E p \in Passes, m \in Ids : DelArt(p, m)
    \/ \E p \in Passes : PruneCrash(p)
    \/ Stutter

(***************************************************************************)
(* Fairness is on the retention pass only: a pass, once invoked, keeps     *)
(* taking its storage steps (storage eventually completes), and passes     *)
(* keep being invoked (the schedule keeps firing, an environment premise). *)
(* Nothing is fair about captures, crashes or the clock, and no fairness   *)
(* is placed on the keep decision: PruneList is deterministic in the state *)
(* it reads.                                                               *)
(***************************************************************************)
Spec == Init /\ [][Next]_vars /\ \A p \in Passes : WF_vars(PassStep(p))

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* Every backup the policy retains from the catalog is restorable: its
\* manifest resolves, its artifact is in the sink, and so is its whole
\* base chain.
RetainedRestorable == \A i \in RetainedIn(cat) : Restorable(i)

\* Every backup the catalog lists is restorable, retained or not: a pass
\* never leaves a listed row - a row it restored included - whose manifest,
\* artifact or base chain is gone.
ListedRestorable == \A i \in cat : Restorable(i)

\* An increment is published only while its base is restorable: a pass never
\* prunes the base an in-flight increment chose before that increment
\* completes, or the increment does not publish.
InFlightBaseSurvives ==
    [][\A i \in Ids :
         (i \notin cat /\ i \in cat' /\ meta[i].kind = "incr")
            => (Restorable(meta[i].base))']_vars

\* Every listed backup's manifest is in the sink: a catalog scrub never finds
\* a listed backup orphaned, and a cold restore, which reads the sink alone,
\* resolves every backup the catalog lists. A pass removes a row before it
\* deletes the sink manifest, and a capture writes its sink manifest before it
\* registers the row; a capture that republished an id a pass had doomed
\* would register a row whose sink manifest the pass then deleted (#5418).
ListedInSink == cat \subseteq snk

\* A published increment still in the sink has a restorable base: a pass
\* deletes an increment's sink manifest before its base's (tip first), so a
\* pass interrupted part-way never leaves one behind without its base - which
\* a catalog rebuild would otherwise list again as an unrestorable backup.
SinkChainIntact ==
    \A j \in snk \cap pub : meta[j].kind = "incr" => Restorable(meta[j].base)

\* Nothing the policy keeps is pruned: an artifact of a published backup is
\* deleted only for a backup the policy would not retain even if it were
\* still listed. An increment that never published withdraws its own
\* artifact (Abandon); the policy does not apply to it.
KeepPolicyHonoured ==
    [][\A i \in pub :
         (i \in arts /\ i \notin arts') => i \notin RetainedIn(cat \cup {i})]_vars

\* A listed backup outside the policy and outside every retained closure,
\* which no manifest builds on, is eventually no longer listed. A manifest
\* that builds on it includes the sink manifest a crashed increment left
\* behind: such a leftover keeps its base listed for as long as it stays in
\* the sink, which is the safe choice and is recorded as a deliberate gap in
\* the refinement note rather than hidden here.
Expired(i) == i \in cat /\ i \notin RetainedIn(cat) /\ ~Depended(i)

ExpiredEventuallyPruned ==
    \A i \in Ids : Expired(i) ~> (i \notin cat \/ Depended(i))
=============================================================================
