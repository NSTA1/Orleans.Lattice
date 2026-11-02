--------------------------- MODULE BackupIdClaim ---------------------------
(***************************************************************************)
(* An abstract TLA+ specification of who owns a full backup's bare        *)
(* content-addressed id across scopes (issues #5620 and #5627, epic        *)
(* #5348). A full backup's id is the hash of its payload, which names no   *)
(* scope, so identical content captured in two scopes derives one bare id. *)
(* A capture asks the id's claim grain (BackupIdClaimGrain) whether its    *)
(* scope may publish under the bare id; when another scope owns the id, or *)
(* holds a claim on it, the capture takes its scope-qualified id instead,  *)
(* which is its scope's alone and so outside this model.                   *)
(*                                                                         *)
(* A publish of the bare id then waits for its scope's per-id hold (the    *)
(* scope's BackupSchedulerGrain), which a retention pass of that scope     *)
(* holds while it retires the id: it removes the catalog row, then the     *)
(* sink manifest. An operator delete of the id - two of them here, so they *)
(* overlap as a retried or double-submitted delete does (issue #5631) -    *)
(* removes the row under the hold, takes a lease of its own that keeps the *)
(* scope's publishes and rebuilds out, deletes the artifacts outside the   *)
(* grain while that lease is live, and ends under the hold again: it       *)
(* deletes the sink manifest only while no row lists the id and the        *)
(* manifest is still its own scope's, then drops its lease. A lease can    *)
(* run out at any point, and a catalog rebuild of scope A then re-lists    *)
(* the sink manifest - only when every artifact it references is still in  *)
(* the sink (issue #5637). The other scope's publishes take the other      *)
(* scope's hold, so nothing but the claim keeps the two scopes apart.      *)
(*                                                                         *)
(* The design checked here is the fix of #5627: a claim is recorded for    *)
(* every claimant, the owning scope included, and released one at a time,  *)
(* so an id a scope is still publishing - through a retire of its own -    *)
(* never becomes another scope's; and the fix of #5631: each operator      *)
(* delete holds a lease of its own, and its sink-manifest delete is fenced *)
(* by the hold; and the fix of #5637: a delete deletes artifacts only      *)
(* under a live lease, and a rebuild re-lists only a whole backup. The     *)
(* claim grain's claims are persisted (issue #5632), so they are durable   *)
(* state here. See RefinementClaim.md for the mapping to  *)
(* production symbols.                                                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

Scopes == {"A", "B"}
\* Two captures in scope A and one in scope B, all of identical content.
Caps == {1, 2, 3}
ScopeOf(c) == IF c = 3 THEN "B" ELSE "A"
\* Two operator deletes of scope A's backup under the bare id.
Dels == {1, 2}
Owners == Scopes \cup {"none"}

(***************************************************************************)
(* State.                                                                  *)
(*  row     the scope of the bare id's catalog row, or "none".             *)
(*  man     the scope of the bare id's sink manifest, or "none".           *)
(*  pend    the claim grain's live claims on the bare id, per scope.       *)
(*  held    each scope's hold on the bare id: free, held by a publish, or  *)
(*          held by a retire.                                              *)
(*  cap     each capture's progress and the id it resolved: the bare id    *)
(*          "X", its scope-qualified id "Q", or none yet.                  *)
(*  ret     scope A's retention pass over the bare id.                     *)
(*  del     each operator delete's progress.                               *)
(*  lease   each operator delete's lease on the id: none, live, or run out *)
(*          (lapsed) while its delete is still in progress.                *)
(*  art     whether every artifact the bare id's current sink manifest     *)
(*          references is still in the sink.                               *)
(*  gen     how many times the bare id's sink manifest has been written:   *)
(*          every write streams artifacts of its own.                      *)
(*  dgen    the manifest write each operator delete began from, so its     *)
(*          artifact deletes touch only that write's artifacts.            *)
(***************************************************************************)
VARIABLES row, man, pend, held, cap, ret, del, lease, art, gen, dgen

vars == <<row, man, pend, held, cap, ret, del, lease, art, gen, dgen>>

CapPhases == {"idle", "wait", "man", "row", "done"}
RetPhases == {"idle", "held", "rowgone", "done"}
\* begun: row removed, artifacts not yet deleted; art: artifacts deleted.
DelPhases == {"idle", "begun", "art", "done"}

\* Whether some operator delete's lease keeps scope A's publishes out.
Leased == \E d \in Dels : lease[d] = "live"

TypeOK ==
    /\ row \in Owners
    /\ man \in Owners
    /\ pend \in [Scopes -> 0..Cardinality(Caps)]
    /\ held \in [Scopes -> {"free", "pub", "ret"}]
    /\ cap \in [Caps -> [ph : CapPhases, id : {"none", "X", "Q"}]]
    /\ ret \in RetPhases
    /\ del \in [Dels -> DelPhases]
    /\ lease \in [Dels -> {"none", "live", "lapsed"}]
    /\ art \in BOOLEAN
    /\ gen \in 0..Cardinality(Caps)
    /\ dgen \in [Dels -> 0..Cardinality(Caps)]

\* Scope A has already published the bare id.
Init ==
    /\ row = "A"
    /\ man = "A"
    /\ pend = [s \in Scopes |-> 0]
    /\ held = [s \in Scopes |-> "free"]
    /\ cap = [c \in Caps |-> [ph |-> "idle", id |-> "none"]]
    /\ ret = "idle"
    /\ del = [d \in Dels |-> "idle"]
    /\ lease = [d \in Dels |-> "none"]
    /\ art = TRUE
    /\ gen = 0
    /\ dgen = [d \in Dels |-> 0]

\* The id's owner, read from durable state as BackupIdOwnership reads it: the
\* catalog row's scope, or else the sink manifest's.
Owner == IF row # "none" THEN row ELSE man

(***************************************************************************)
(* Resolve: one atomic call to the claim grain (its calls do not           *)
(* interleave). A claim is refused while another scope holds a claim, or   *)
(* while another scope owns the id; otherwise one claim is recorded for    *)
(* this capture - also when its scope already owns the id - and the        *)
(* capture goes on to publish the bare id. A refused capture takes its     *)
(* scope-qualified id and is done, as far as the bare id is concerned.     *)
(***************************************************************************)
Resolve(c) ==
    LET s == ScopeOf(c)
    IN /\ cap[c].ph = "idle"
       /\ IF \/ \E o \in Scopes \ {s} : pend[o] > 0
             \/ Owner \notin {"none", s}
          THEN /\ cap' = [cap EXCEPT ![c] = [ph |-> "done", id |-> "Q"]]
               /\ UNCHANGED pend
          ELSE /\ pend' = [pend EXCEPT ![s] = @ + 1]
               /\ cap' = [cap EXCEPT ![c] = [ph |-> "wait", id |-> "X"]]
       /\ UNCHANGED <<row, man, held, ret, del, lease, art, gen, dgen>>

\* The publish waits for its scope's hold on the id, which a retire of that
\* scope holds while it removes the id, and for every live lease of an
\* operator delete of that scope.
Acquire(c) ==
    LET s == ScopeOf(c)
    IN /\ cap[c].ph = "wait"
       /\ held[s] = "free"
       /\ s = "A" => ~Leased
       /\ held' = [held EXCEPT ![s] = "pub"]
       /\ cap' = [cap EXCEPT ![c].ph = "man"]
       /\ UNCHANGED <<row, man, pend, ret, del, lease, art, gen, dgen>>

\* The publish writes the sink manifest under the bare id, with artifacts of
\* its own that it has streamed whole ...
PubMan(c) ==
    /\ cap[c].ph = "man"
    /\ man' = ScopeOf(c)
    /\ art' = TRUE
    /\ gen' = gen + 1
    /\ cap' = [cap EXCEPT ![c].ph = "row"]
    /\ UNCHANGED <<row, pend, held, ret, del, lease, dgen>>

\* ... and then registers the catalog row, releases the hold and releases one
\* claim of its scope (a release with none is a no-op, as in production).
PubRow(c) ==
    LET s == ScopeOf(c)
    IN /\ cap[c].ph = "row"
       /\ row' = s
       /\ held' = [held EXCEPT ![s] = "free"]
       /\ pend' = [pend EXCEPT ![s] = IF @ > 0 THEN @ - 1 ELSE 0]
       /\ cap' = [cap EXCEPT ![c].ph = "done"]
       /\ UNCHANGED <<man, ret, del, lease, art, gen, dgen>>

(***************************************************************************)
(* Scope A's retention pass dooms its own backup under the bare id once -  *)
(* never while a publish or a live delete lease holds the id - holding A's *)
(* hold on the id: it removes the catalog row, then the sink manifest,     *)
(* then lets go.                                                           *)
(***************************************************************************)
RetireStart ==
    /\ ret = "idle"
    /\ row = "A"
    /\ held["A"] = "free"
    /\ ~Leased
    /\ held' = [held EXCEPT !["A"] = "ret"]
    /\ ret' = "held"
    /\ UNCHANGED <<row, man, pend, cap, del, lease, art, gen, dgen>>

RetireRow ==
    /\ ret = "held"
    /\ row' = "none"
    /\ ret' = "rowgone"
    /\ UNCHANGED <<man, pend, held, cap, del, lease, art, gen, dgen>>

RetireMan ==
    /\ ret = "rowgone"
    /\ man' = "none"
    /\ held' = [held EXCEPT !["A"] = "free"]
    /\ ret' = "done"
    /\ UNCHANGED <<row, pend, cap, del, lease, art, gen, dgen>>

(***************************************************************************)
(* An operator delete of scope A's backup under the bare id (issues #5628  *)
(* and #5631). It begins in one call under A's hold: when the backup is    *)
(* still A's - its row, or the sink manifest an interrupted delete left -  *)
(* it removes the row and takes a lease of its own; otherwise there is     *)
(* nothing of A's to delete. It then deletes the artifacts of the manifest *)
(* it began from outside the grain, which it does only while its lease is  *)
(* live: the caller renews the lease throughout and stops at a refused     *)
(* renewal (issue #5637). It ends in one call under A's hold: it deletes   *)
(* the sink manifest only while no row lists the id and the manifest is    *)
(* still A's, then drops its own lease.                                    *)
(***************************************************************************)
DelBegin(d) ==
    /\ del[d] = "idle"
    /\ held["A"] = "free"
    /\ IF Owner = "A"
       THEN /\ row' = IF row = "A" THEN "none" ELSE row
            /\ del' = [del EXCEPT ![d] = "begun"]
            /\ lease' = [lease EXCEPT ![d] = "live"]
            /\ dgen' = [dgen EXCEPT ![d] = gen]
            /\ UNCHANGED man
       ELSE /\ del' = [del EXCEPT ![d] = "done"]
            /\ UNCHANGED <<row, man, lease, dgen>>
    /\ UNCHANGED <<pend, held, cap, ret, art, gen>>

\* The artifact deletes, outside the grain, under a live lease. They touch
\* only the artifacts of the manifest write the delete began from.
DelArt(d) ==
    /\ del[d] = "begun"
    /\ lease[d] = "live"
    /\ art' = IF gen = dgen[d] THEN FALSE ELSE art
    /\ del' = [del EXCEPT ![d] = "art"]
    /\ UNCHANGED <<row, man, pend, held, cap, ret, lease, gen, dgen>>

DelEnd(d) ==
    /\ del[d] = "art"
    /\ held["A"] = "free"
    /\ man' = IF row = "none" /\ man = "A" THEN "none" ELSE man
    /\ del' = [del EXCEPT ![d] = "done"]
    /\ lease' = [lease EXCEPT ![d] = "none"]
    /\ UNCHANGED <<row, pend, held, cap, ret, art, gen, dgen>>

\* A delete's lease runs out: its caller was lost, or could not reach the
\* grain to renew it for a whole lease. The delete may still end later, but
\* deletes no artifact after this. Weakly fair: a lost caller's lease does
\* run out, so it never keeps the id reserved for good.
DelLapse(d) ==
    /\ del[d] \in {"begun", "art"}
    /\ lease[d] = "live"
    /\ lease' = [lease EXCEPT ![d] = "lapsed"]
    /\ UNCHANGED <<row, man, pend, held, cap, ret, del, art, gen, dgen>>

(***************************************************************************)
(* A catalog rebuild re-lists scope A's sink manifest under A's hold,      *)
(* waiting while a delete's lease is live, and only when every artifact    *)
(* the manifest references is still in the sink (issue #5637).             *)
(***************************************************************************)
Rebuild ==
    /\ held["A"] = "free"
    /\ ~Leased
    /\ row = "none"
    /\ man = "A"
    /\ art
    /\ row' = "A"
    /\ UNCHANGED <<man, pend, held, cap, ret, del, lease, art, gen, dgen>>

\* Natural termination once every capture and the retire are done.
Stutter == UNCHANGED vars

Next ==
    \/ \E c \in Caps : Resolve(c)
    \/ \E c \in Caps : Acquire(c)
    \/ \E c \in Caps : PubMan(c)
    \/ \E c \in Caps : PubRow(c)
    \/ RetireStart
    \/ RetireRow
    \/ RetireMan
    \/ \E d \in Dels : DelBegin(d)
    \/ \E d \in Dels : DelArt(d)
    \/ \E d \in Dels : DelEnd(d)
    \/ \E d \in Dels : DelLapse(d)
    \/ Rebuild
    \/ Stutter

Spec == Init /\ [][Next]_vars /\ \A d \in Dels : WF_vars(DelLapse(d))

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* Every listed backup's manifest is in the sink: a retire never deletes the
\* manifest of a row a publish of its own scope registered meanwhile.
ListedInSink == row # "none" => man # "none"

\* Every listed backup restores: its manifest is in the sink with every
\* artifact it references (issue #5637).
ListedRestorable == row # "none" => (man # "none" /\ art)

\* The catalog row and the sink manifest under the bare id name one scope, so
\* a restore of the id, from the catalog or from the sink, targets the scope
\* that published it.
ScopeAgreement == (row # "none" /\ man # "none") => row = man

\* While an operator delete of the id is in progress and its lease has not
\* run out, no publish of the id in its scope is in flight: one delete
\* ending never releases another's fence (issue #5631).
DeleteFence ==
    (\E d \in Dels : del[d] \in {"begun", "art"} /\ lease[d] # "lapsed")
        => \A c \in Caps : ScopeOf(c) = "A" => cap[c].ph \notin {"man", "row"}

\* No publish replaces another scope's backup under the bare id: its row and
\* its manifest change scope only by first being removed.
NoForeignOverwrite ==
    [][/\ (row # "none" /\ row' # "none") => row' = row
       /\ (man # "none" /\ man' # "none") => man' = man]_vars

\* Scope A's retires and deletes never remove scope B's backup under the bare
\* id: B never retires it here, so once written B's row and manifest stay.
\* Every delete's lease ends: a delete whose caller is lost does not keep the
\* id reserved, and captures of identical content out, for good.
LeaseEventuallyEnds == \A d \in Dels : lease[d] = "live" ~> lease[d] # "live"

NoForeignDelete ==
    [][/\ row = "B" => row' = "B"
       /\ man = "B" => man' = "B"]_vars
=============================================================================
