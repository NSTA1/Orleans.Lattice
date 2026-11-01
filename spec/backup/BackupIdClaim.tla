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
(* scope's BackupSchedulerGrain), which a retention pass or an operator    *)
(* delete of that scope holds while it retires the id: it removes the      *)
(* catalog row, then the sink manifest. The other scope's publishes take   *)
(* the other scope's hold, so nothing but the claim keeps the two scopes   *)
(* apart.                                                                  *)
(*                                                                         *)
(* The design checked here is the fix of #5627: a claim is recorded for    *)
(* every claimant, the owning scope included, and released one at a time,  *)
(* so an id a scope is still publishing - through a retire of its own -    *)
(* never becomes another scope's. See RefinementClaim.md for the mapping   *)
(* to production symbols.                                                  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

Scopes == {"A", "B"}
\* Two captures in scope A and one in scope B, all of identical content.
Caps == {1, 2, 3}
ScopeOf(c) == IF c = 3 THEN "B" ELSE "A"
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
(*  ret     scope A's retire of the bare id: a retention pass or an        *)
(*          operator delete.                                               *)
(***************************************************************************)
VARIABLES row, man, pend, held, cap, ret

vars == <<row, man, pend, held, cap, ret>>

CapPhases == {"idle", "wait", "man", "row", "done"}
RetPhases == {"idle", "held", "rowgone", "done"}

TypeOK ==
    /\ row \in Owners
    /\ man \in Owners
    /\ pend \in [Scopes -> 0..Cardinality(Caps)]
    /\ held \in [Scopes -> {"free", "pub", "ret"}]
    /\ cap \in [Caps -> [ph : CapPhases, id : {"none", "X", "Q"}]]
    /\ ret \in RetPhases

\* Scope A has already published the bare id.
Init ==
    /\ row = "A"
    /\ man = "A"
    /\ pend = [s \in Scopes |-> 0]
    /\ held = [s \in Scopes |-> "free"]
    /\ cap = [c \in Caps |-> [ph |-> "idle", id |-> "none"]]
    /\ ret = "idle"

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
       /\ UNCHANGED <<row, man, held, ret>>

\* The publish waits for its scope's hold on the id, which a retire of that
\* scope holds while it removes the id.
Acquire(c) ==
    LET s == ScopeOf(c)
    IN /\ cap[c].ph = "wait"
       /\ held[s] = "free"
       /\ held' = [held EXCEPT ![s] = "pub"]
       /\ cap' = [cap EXCEPT ![c].ph = "man"]
       /\ UNCHANGED <<row, man, pend, ret>>

\* The publish writes the sink manifest under the bare id ...
PubMan(c) ==
    /\ cap[c].ph = "man"
    /\ man' = ScopeOf(c)
    /\ cap' = [cap EXCEPT ![c].ph = "row"]
    /\ UNCHANGED <<row, pend, held, ret>>

\* ... and then registers the catalog row, releases the hold and releases one
\* claim of its scope (a release with none is a no-op, as in production).
PubRow(c) ==
    LET s == ScopeOf(c)
    IN /\ cap[c].ph = "row"
       /\ row' = s
       /\ held' = [held EXCEPT ![s] = "free"]
       /\ pend' = [pend EXCEPT ![s] = IF @ > 0 THEN @ - 1 ELSE 0]
       /\ cap' = [cap EXCEPT ![c].ph = "done"]
       /\ UNCHANGED <<man, ret>>

(***************************************************************************)
(* Scope A retires its own backup under the bare id - a retention pass     *)
(* that dooms it, or an operator delete - once, holding A's hold on the    *)
(* id: it removes the catalog row, then the sink manifest, then lets go.   *)
(***************************************************************************)
RetireStart ==
    /\ ret = "idle"
    /\ row = "A"
    /\ held["A"] = "free"
    /\ held' = [held EXCEPT !["A"] = "ret"]
    /\ ret' = "held"
    /\ UNCHANGED <<row, man, pend, cap>>

RetireRow ==
    /\ ret = "held"
    /\ row' = "none"
    /\ ret' = "rowgone"
    /\ UNCHANGED <<man, pend, held, cap>>

RetireMan ==
    /\ ret = "rowgone"
    /\ man' = "none"
    /\ held' = [held EXCEPT !["A"] = "free"]
    /\ ret' = "done"
    /\ UNCHANGED <<row, pend, cap>>

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
    \/ Stutter

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* Every listed backup's manifest is in the sink: a retire never deletes the
\* manifest of a row a publish of its own scope registered meanwhile.
ListedInSink == row # "none" => man # "none"

\* The catalog row and the sink manifest under the bare id name one scope, so
\* a restore of the id, from the catalog or from the sink, targets the scope
\* that published it.
ScopeAgreement == (row # "none" /\ man # "none") => row = man

\* No publish replaces another scope's backup under the bare id: its row and
\* its manifest change scope only by first being removed.
NoForeignOverwrite ==
    [][/\ (row # "none" /\ row' # "none") => row' = row
       /\ (man # "none" /\ man' # "none") => man' = man]_vars
=============================================================================
