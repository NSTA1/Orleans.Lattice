--------------------------- MODULE ShardOwnershipCrdt ---------------------------
(***************************************************************************)
(* Ownership of a CRDT-mode key across a leaf split, a shard split and an   *)
(* online resize, against an atomic write that stages a CRDT mutation and   *)
(* a non-atomic CRDT write to the same key.                                 *)
(*                                                                         *)
(* ShardOwnership and ShardOwnershipRetention model last-writer-wins keys,  *)
(* where installing the newer of two values is correct. A CRDT key is       *)
(* different: two copies of it can each hold a contribution the other       *)
(* lacks, so a path that installs one copy over the other loses whatever    *)
(* the overwritten copy alone held, whatever the stamps say. This module    *)
(* checks that every path that brings two copies of a CRDT key together     *)
(* joins them: the terminal's drain and its backstop (#4611), a split's     *)
(* imports (#4613) and a resize's mirror and snapshot drain (#4618).        *)
(*                                                                         *)
(* The value is a grow-only set of contributions, which is enough to tell   *)
(* a join from an overwrite: the saga stages "a", the non-atomic write adds *)
(* "b". The saga's staged value is the owner's row at staging time with "a" *)
(* added, the merged state a CRDT accessor's Stage method mints.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANT MaxStamp   \* bound on the per-location HLC, to keep the state space finite

Locs == {"src", "dst"}   \* the migration's source and destination (split target or resized copy)
Elems == {"a", "b"}      \* "a": the saga's contribution; "b": the non-atomic write's

Max(x, y) == IF x >= y THEN x ELSE y

VARIABLES
    row,      \* row[l]: the contributions the key's committed row at l holds
    stamp,    \* stamp[l]: the row's HLC stamp (0: no row)
    clk,      \* clk[l]: the leaf clock at l
    migd,     \* migd[l]: the row at l was last written by a cross-shard migration import
    owner,    \* the location that owns the key
    leaf,     \* the leaf of the source shard that declares the key: "L1", or "L2" after a leaf split
    sg,       \* saga phase: "idle", "prepared", "decided", "done"
    staged,   \* the saga's staged merged state
    pend,     \* pend[l]: l holds the saga's prepared bucket for the key
    bleaf,    \* the source leaf the source's bucket was prepared on
    told,     \* locations the terminal broadcast has visited
    mg,       \* migration phase: "none", "window", "copied", "done"
    kind,     \* the migration: "none", "split" (a shard split) or "resize" (an online resize)
    aAck,     \* the saga's caller has been acknowledged
    bAck      \* the non-atomic write has been acknowledged

vars == <<row, stamp, clk, migd, owner, leaf, sg, staged, pend, bleaf, told, mg, kind, aAck, bAck>>

-----------------------------------------------------------------------------
(* Derived state                                                           *)

\* The window in which the source forwards each accepted write to the destination.
Forwarding == mg \in {"window", "copied"}

\* The next stamp a write at l takes: strictly above the leaf clock and the row.
Tick(l) == Max(clk[l], stamp[l]) + 1

\* Every location the terminal broadcast must visit: the touched source, and,
\* once a migration has begun, the destination (the split's closure, or the
\* resize's mirrored terminal).
TermTargets == {"src"} \cup (IF mg # "none" THEN {"dst"} ELSE {})

\* Join the value v, stamped t, into the row at l, as a CRDT join does: the
\* result holds both, stamped above the leaf clock, the row and the incoming
\* stamp (BPlusLeafGrain.TryJoinMigratedCrdtRow), so the joined row is never
\* lost to a later last-writer-wins comparison.
JoinAt(l, v, t) ==
    LET s == Max(Max(clk[l], stamp[l]), t) + 1
    IN /\ s <= MaxStamp
       /\ row' = [row EXCEPT ![l] = @ \cup v]
       /\ stamp' = [stamp EXCEPT ![l] = s]
       /\ clk' = [clk EXCEPT ![l] = s]

\* A cross-shard import of the source's row into the destination: joined.
Import ==
    /\ JoinAt("dst", row["src"], stamp["src"])
    /\ migd' = [migd EXCEPT !["dst"] = TRUE]

-----------------------------------------------------------------------------
Init ==
    /\ row = [l \in Locs |-> {}]
    /\ stamp = [l \in Locs |-> 0]
    /\ clk = [l \in Locs |-> 0]
    /\ migd = [l \in Locs |-> FALSE]
    /\ owner = "src"
    /\ leaf = "L1"
    /\ sg = "idle"
    /\ staged = {}
    /\ pend = [l \in Locs |-> FALSE]
    /\ bleaf = "L1"
    /\ told = {}
    /\ mg = "none"
    /\ kind = "none"
    /\ aAck = FALSE
    /\ bAck = FALSE

-----------------------------------------------------------------------------
(* The atomic write (AtomicWriteGrain), staging one CRDT mutation.         *)

\* The caller stages the mutation against the owner's row (the minted merged
\* state) and the saga prepares it on the owner's declaring leaf. In the
\* migration window the prepare is also forwarded to the destination: the
\* split's hot-path forward, or the resize mirror's prepare.
Stage ==
    /\ sg = "idle"
    /\ sg' = "prepared"
    /\ staged' = row[owner] \cup {"a"}
    /\ pend' = [l \in Locs |-> l = owner \/ (owner = "src" /\ l = "dst" /\ Forwarding)]
    /\ bleaf' = leaf
    /\ UNCHANGED <<row, stamp, clk, migd, owner, leaf, told, mg, kind, aAck, bAck>>

\* Record the commit decision.
Decide ==
    /\ sg = "prepared"
    /\ sg' = "decided"
    /\ UNCHANGED <<row, stamp, clk, migd, owner, leaf, staged, pend, bleaf, told, mg, kind, aAck, bAck>>

\* One location of the terminal broadcast. A bucket on the leaf that declares
\* the key drains by folding the staged delta into the row. Otherwise the
\* terminal's backstop applies the staged state: to a bucket stranded on a
\* source leaf a leaf split narrowed (forwarded to the declaring sibling), or
\* to a location with no bucket (the coordinator's committed-values backstop).
\* Both join, as the drain does.
Terminal(l) ==
    /\ sg = "decided"
    /\ l \in TermTargets \ told
    /\ told' = told \cup {l}
    /\ pend' = [pend EXCEPT ![l] = FALSE]
    /\ IF pend[l] /\ (l = "dst" \/ bleaf = leaf)
       THEN JoinAt(l, {"a"}, 0)
       ELSE JoinAt(l, staged, 0)
    /\ migd' = [migd EXCEPT ![l] = FALSE]
    /\ UNCHANGED <<owner, leaf, sg, staged, bleaf, mg, kind, aAck, bAck>>

\* The broadcast has visited every target: the saga completes and its caller
\* is acknowledged.
Complete ==
    /\ sg = "decided"
    /\ TermTargets \subseteq told
    /\ sg' = "done"
    /\ aAck' = TRUE
    /\ UNCHANGED <<row, stamp, clk, migd, owner, leaf, staged, pend, bleaf, told, mg, kind, bAck>>

-----------------------------------------------------------------------------
(* The non-atomic CRDT write                                               *)

\* A typed CRDT delta applied to the key on its owner, folded into the row.
\* In the migration window the source forwards the post-fold row to the
\* destination, which joins it: the split's CRDT forward (#4613) or the
\* resize mirror (#4618).
WriteB ==
    /\ ~bAck
    /\ LET t == Tick(owner)
           newRow == row[owner] \cup {"b"}
       IN /\ t <= MaxStamp
          /\ IF owner = "src" /\ Forwarding
             THEN LET s == Max(Max(clk["dst"], stamp["dst"]), t) + 1
                  IN /\ s <= MaxStamp
                     /\ row' = [row EXCEPT !["src"] = newRow, !["dst"] = @ \cup newRow]
                     /\ stamp' = [stamp EXCEPT !["src"] = t, !["dst"] = s]
                     /\ clk' = [clk EXCEPT !["src"] = t, !["dst"] = s]
                     /\ migd' = [migd EXCEPT !["src"] = FALSE, !["dst"] = kind = "split"]
             ELSE /\ row' = [row EXCEPT ![owner] = newRow]
                  /\ stamp' = [stamp EXCEPT ![owner] = t]
                  /\ clk' = [clk EXCEPT ![owner] = t]
                  /\ migd' = [migd EXCEPT ![owner] = FALSE]
    /\ bAck' = TRUE
    /\ UNCHANGED <<owner, leaf, sg, staged, pend, bleaf, told, mg, kind, aAck>>

-----------------------------------------------------------------------------
(* Ownership changes                                                       *)

\* A leaf split narrows the source leaf that declares the key: the key's row
\* moves to the new sibling, and a prepared bucket stays on the donor,
\* stranded. Environment action: a leaf splits whenever it fills.
LeafSplit ==
    /\ owner = "src"
    /\ leaf = "L1"
    /\ leaf' = "L2"
    /\ UNCHANGED <<row, stamp, clk, migd, owner, sg, staged, pend, bleaf, told, mg, kind, aAck, bAck>>

\* A shard split or an online resize opens its window: from here the source
\* forwards every write it accepts to the destination.
Begin(k) ==
    /\ mg = "none"
    /\ mg' = "window"
    /\ kind' = k
    /\ UNCHANGED <<row, stamp, clk, migd, owner, leaf, sg, staged, pend, bleaf, told, aAck, bAck>>

\* The drain passes the key: the source's row is imported into the
\* destination (the split's background drain, or the resize's snapshot drain),
\* and the source's in-flight bucket is replayed there (the prepared-bucket
\* sweep).
Copy ==
    /\ mg = "window"
    /\ mg' = "copied"
    /\ Import
    /\ pend' = [pend EXCEPT !["dst"] = @ \/ (pend["src"] /\ sg = "prepared")]
    /\ UNCHANGED <<owner, leaf, sg, staged, bleaf, told, kind, aAck, bAck>>

\* The migration commits: the destination becomes the owner. A split first
\* runs its final authoritative drain under the source's freeze, importing the
\* source's row again; a resize swaps its alias, its mirror having carried
\* every write since the drain.
Commit ==
    /\ mg = "copied"
    /\ mg' = "done"
    /\ owner' = "dst"
    /\ IF kind = "split"
       THEN Import
       ELSE UNCHANGED <<row, stamp, clk, migd>>
    /\ UNCHANGED <<leaf, sg, staged, pend, bleaf, told, kind, aAck, bAck>>

\* Nothing is in flight.
Quiescent ==
    /\ sg \in {"idle", "done"}
    /\ mg \in {"none", "done"}

Stutter ==
    /\ Quiescent
    /\ UNCHANGED vars

-----------------------------------------------------------------------------
Next ==
    \/ Stage
    \/ Decide
    \/ \E l \in Locs : Terminal(l)
    \/ Complete
    \/ WriteB
    \/ LeafSplit
    \/ \E k \in {"split", "resize"} : Begin(k)
    \/ Copy
    \/ Commit
    \/ Stutter

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties                                                              *)

TypeOK ==
    /\ row \in [Locs -> SUBSET Elems]
    /\ stamp \in [Locs -> 0..MaxStamp]
    /\ clk \in [Locs -> 0..MaxStamp]
    /\ migd \in [Locs -> BOOLEAN]
    /\ owner \in Locs
    /\ leaf \in {"L1", "L2"}
    /\ sg \in {"idle", "prepared", "decided", "done"}
    /\ staged \in SUBSET Elems
    /\ pend \in [Locs -> BOOLEAN]
    /\ bleaf \in {"L1", "L2"}
    /\ told \subseteq Locs
    /\ mg \in {"none", "window", "copied", "done"}
    /\ kind \in {"none", "split", "resize"}
    /\ aAck \in BOOLEAN
    /\ bAck \in BOOLEAN

\* The owner's row holds every contribution acknowledged to a writer: the
\* saga's once its caller is acknowledged, the non-atomic write's once it is.
NoLostContribution ==
    /\ aAck => "a" \in row[owner]
    /\ bAck => "b" \in row[owner]

=============================================================================
