---- MODULE SchemaCrossRegion ----
EXTENDS Naturals, FiniteSets

\* A versioned tree replicated from a source region to a receiver region, each
\* with its own policy, version target and upcaster catalog: schema definitions
\* are region-local, so nothing the stream carries changes the receiver's
\* catalog. The source ships each entry stamped at the version it held: a plain
\* last-writer-wins write, a typed-CRDT delta, or one entry of one atomic
\* batch. Under strict ingest the receiver judges every replicated entry, a
\* plain write included (#5684): an entry its catalog cannot serve, or whose
\* body its own policy refuses, is diverted to one dead letter, and an admitted
\* one keeps the stamp it carried. Without strict ingest nothing is judged. The
\* stream, the receiver's catalog, policy and dead-letter collaborators, and
\* its physical copy (which a coordinated restore can hold receive-closed) can
\* each be unavailable for a while. The receiver's operator registers the
\* source's newer version and later advances to it, or never does. A row can
\* also reach the receiver through anti-entropy repair or a re-seed import;
\* both apply the same judgement, so a body the receiver refused is never
\* re-introduced.
\*
\* Rely (restated, no INSTANCE): the source judged each entry by its own
\* policy before it shipped (SchemaStageOrdering), one delivery is judged
\* once and diverted only with a durable record (SchemaStrictIngest), the
\* facade advances a target only to a registered, reachable version
\* (SchemaVersionFamily), and a batch's commit decision reaches the receiver
\* after every one of its entries and reveals the staged ones together
\* (AtomicCommitCrossCluster). Each assumption is named in the refinement note
\* with the module whose checked properties discharge it.

CONSTANTS N, MaxFaults

Entries == 1..N
Kinds == {"Lww", "Crdt", "Atomic"}
\* "Local" is a body the source's policy accepts and the receiver's refuses.
Bodies == {"Good", "Local"}
Versions == {1, 2}
States == {"Pending", "Stored", "Staged", "Diverted"}
Collaborators == {"Link", "Deps", "Copy"}

VARIABLES kind, body, stamp, strict, tgt, hop, st, tag, retried, decided,
          up, faults, lastRead,
          dl        \* dl[e]: whether the receiver holds a dead letter for entry e
vars == <<kind, body, stamp, strict, tgt, hop, st, tag, retried, decided,
          up, faults, lastRead, dl>>

\* The receiver's read decoder serves a stamp at its target, or an older one
\* along a registered hop; a newer stamp, or an older one with no hop, is an
\* explicit failure.
Servable(v) == v = tgt \/ (v < tgt /\ hop)

\* Strict ingest judges every replicated entry: a typed-CRDT delta, an
\* atomic-batch entry, and a plain last-writer-wins write (#5684). Without it,
\* nothing is judged.
Checked(e) == strict

AtomicEntries == {e \in Entries : kind[e] = "Atomic"}
Admitted(e) == st[e] \in {"Stored", "Staged"}
Visible(e) == st[e] = "Stored" \/ (st[e] = "Staged" /\ decided)

TypeOK ==
    /\ kind \in [Entries -> Kinds]
    /\ body \in [Entries -> Bodies]
    /\ stamp \in [Entries -> Versions]
    /\ strict \in BOOLEAN
    /\ tgt \in Versions /\ hop \in BOOLEAN
    /\ st \in [Entries -> States]
    /\ tag \in [Entries -> {0} \cup Versions]
    /\ retried \in [Entries -> BOOLEAN]
    /\ decided \in BOOLEAN
    /\ up \in [Collaborators -> BOOLEAN]
    /\ faults \in 0..MaxFaults
    /\ lastRead \in [e : {0} \cup Entries, out : {"None", "Body", "Fail"}]
    /\ dl \in [Entries -> 0..1]

\* Converged modulo dead letters: the receiver lacks an entry the source holds
\* for good only where it diverted it, and every such divergence is accounted
\* for by at least one dead-letter record, whichever path delivered the row.
DivergenceAccounted ==
    \A e \in Entries : st[e] = "Diverted" => dl[e] = 1

\* Under strict ingest every judged entry the receiver admitted carries a body
\* its own policy accepts and a stamp its own catalog serves.
StrictAdmitsOnlyServable ==
    \A e \in Entries :
        (Checked(e) /\ Admitted(e)) => (body[e] = "Good" /\ Servable(tag[e]))

\* A reader sees none of a replicated batch, or every entry of it the receiver
\* admitted, and only once every entry was admitted or diverted.
AtomicAllOrNothing ==
    LET vis == {e \in AtomicEntries : Visible(e)}
    IN vis # {} =>
        \A e \in AtomicEntries : st[e] # "Pending" /\ (Admitted(e) => e \in vis)

\* A stored stamp is fixed when the entry lands: registering a definition,
\* advancing the target, reading or folding never rewrites it.
StoredStampFixed ==
    [][\A e \in Entries : st[e] # "Pending" => tag'[e] = tag[e]]_vars

\* Strict ingest lifts a typed-CRDT delta to the receiver's target once, at the
\* boundary, so every later fold strips the same stored stamp.
CrdtLiftedAtIngest ==
    [][\A e \in Entries :
          (st[e] = "Pending" /\ st'[e] = "Stored" /\ Checked(e) /\ kind[e] = "Crdt")
              => tag'[e] = tgt]_vars

\* Without strict ingest, every entry is stored with the stamp it carried,
\* never re-stamped or diverted.
UncheckedStoredVerbatim ==
    [][\A e \in Entries :
          (~strict /\ st[e] = "Pending" /\ st'[e] # "Pending")
              => (Admitted(e)' /\ tag'[e] = stamp[e])]_vars

\* Nothing is judged, admitted or diverted while the receiver's catalog,
\* policy or dead-letter collaborator is unavailable.
NothingAdmittedWhileUnavailable ==
    [][\A e \in Entries :
          (Checked(e) /\ st[e] = "Pending" /\ st'[e] # "Pending") => up["Deps"]]_vars

\* No entry of any kind lands on, or is diverted from, a physical copy a
\* coordinated restore holds receive-closed: it is deferred and re-shipped.
NothingLandsOnFencedCopy ==
    [][\A e \in Entries : (st[e] = "Pending" /\ st'[e] # "Pending") => up["Copy"]]_vars

\* A read serves a body exactly when the receiver's catalog can decode the
\* stored stamp at its target, and otherwise fails explicitly.
ReadsServeOrFail ==
    [][(lastRead' # lastRead /\ lastRead'.e # 0)
       => (lastRead'.out = "Body" <=> Servable(tag[lastRead'.e]))]_vars

\* An entry the receiver admitted stays admitted: a diverted sibling leaves the
\* rest of its batch to commit.
AdmittedNeverDropped ==
    [][\A e \in Entries : Admitted(e) => Admitted(e)']_vars

\* Every entry is eventually admitted and visible, or diverted with its dead
\* letter, once the stream and collaborators stop failing: the replicas
\* converge modulo dead-lettered entries.
EveryEntryResolved ==
    <>[](\A e \in Entries : (st[e] = "Diverted" /\ dl[e] = 1) \/ Visible(e))

\* Conditional convergence: if the receiver's operator eventually holds the
\* source's newer version as its target, every entry not diverted becomes
\* readable there. A diverted entry stays diverted.
ConvergesWhenAligned ==
    <>[](tgt = 2) =>
        <>[](\A e \in Entries : st[e] = "Diverted" \/ (Visible(e) /\ Servable(tag[e])))

Init ==
    /\ kind \in [Entries -> Kinds]
    /\ body \in [Entries -> Bodies]
    /\ stamp \in [Entries -> Versions]
    /\ strict \in BOOLEAN
    /\ tgt = 1 /\ hop = FALSE
    /\ st = [e \in Entries |-> "Pending"]
    /\ tag = [e \in Entries |-> 0]
    /\ retried = [e \in Entries |-> FALSE]
    /\ decided = FALSE
    /\ up = [c \in Collaborators |-> TRUE]
    /\ faults = 0
    /\ lastRead = [e |-> 0, out |-> "None"]
    /\ dl = [e \in Entries |-> 0]

\* The stream delivers an entry to the receiver. An entry routed to a copy the
\* restore holds closed, or a judged entry whose collaborators are
\* unavailable, fails and stays pending for re-delivery.
Deliver(e) ==
    /\ up["Link"] /\ st[e] = "Pending"
    /\ IF ~up["Copy"]
       THEN /\ ~retried[e]
            /\ retried' = [retried EXCEPT ![e] = TRUE]
            /\ UNCHANGED <<st, tag>>
       ELSE IF Checked(e)
       THEN IF up["Deps"]
            THEN /\ IF Servable(stamp[e]) /\ body[e] = "Good"
                    THEN /\ st' = [st EXCEPT ![e] = IF kind[e] = "Atomic" THEN "Staged" ELSE "Stored"]
                         /\ tag' = [tag EXCEPT ![e] = IF kind[e] = "Crdt" THEN tgt ELSE stamp[e]]
                    ELSE /\ st' = [st EXCEPT ![e] = "Diverted"]
                         /\ UNCHANGED tag
                 /\ UNCHANGED retried
            ELSE /\ ~retried[e]
                 /\ retried' = [retried EXCEPT ![e] = TRUE]
                 /\ UNCHANGED <<st, tag>>
       ELSE /\ st' = [st EXCEPT ![e] = IF kind[e] = "Atomic" THEN "Staged" ELSE "Stored"]
            /\ tag' = [tag EXCEPT ![e] = stamp[e]]
            /\ UNCHANGED retried
    /\ dl' = [dl EXCEPT ![e] = IF st'[e] = "Diverted" THEN 1 ELSE @]
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, decided, up, faults, lastRead>>

\* The batch's commit decision arrives after every entry of the batch was
\* admitted or diverted, and reveals the staged ones.
Decide ==
    /\ up["Link"] /\ up["Copy"] /\ ~decided /\ AtomicEntries # {}
    /\ \A e \in AtomicEntries : st[e] # "Pending"
    /\ decided' = TRUE
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, retried, up, faults, lastRead>>
    /\ UNCHANGED dl

\* A client reads a visible entry through the receiver's decoder.
Read(e) ==
    /\ Visible(e)
    /\ lastRead' = [e |-> e, out |-> IF Servable(tag[e]) THEN "Body" ELSE "Fail"]
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, retried, decided, up, faults>>
    /\ UNCHANGED dl

\* The receiver's operator registers the source's newer version and the hop to
\* it in the receiver's own catalog.
Register ==
    /\ ~hop
    /\ hop' = TRUE
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, st, tag, retried, decided, up, faults, lastRead>>
    /\ UNCHANGED dl

\* The receiver's operator advances its target, which the facade admits only
\* to a version registered and reachable from the current one.
Advance ==
    /\ tgt = 1 /\ hop
    /\ tgt' = 2
    /\ UNCHANGED <<kind, body, stamp, strict, hop, st, tag, retried, decided, up, faults, lastRead>>
    /\ UNCHANGED dl

\* Environment: the stream, or the receiver's collaborators, become unavailable.
Fault(c) ==
    /\ up[c] /\ faults < MaxFaults
    /\ up' = [up EXCEPT ![c] = FALSE]
    /\ faults' = faults + 1
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, retried, decided, lastRead>>
    /\ UNCHANGED dl

\* Environment: an unavailable collaborator comes back.
Recover(c) ==
    /\ ~up[c]
    /\ up' = [up EXCEPT ![c] = TRUE]
    /\ retried' = [e \in Entries |-> FALSE]
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, decided, faults, lastRead>>
    /\ UNCHANGED dl

\* The receiver's ingest of a row reaching it by another path than the stream,
\* with the judgement Deliver applies: under strict ingest a refused row is
\* diverted to a dead letter, and an admitted one keeps its stamp - a
\* delta, and a row a re-seed drains (lift), are stamped at the target, which
\* the read decoder serves - and without it the row is stored verbatim.
Landing(e) == IF kind[e] = "Atomic" THEN "Staged" ELSE "Stored"
Refused(e) == body[e] # "Good" \/ ~Servable(stamp[e])
Ingest(e, lift) ==
    IF strict
    THEN IF body[e] = "Good" /\ Servable(stamp[e])
         THEN /\ st' = [st EXCEPT ![e] = Landing(e)]
              /\ tag' = [tag EXCEPT ![e] = IF kind[e] = "Crdt" \/ lift THEN tgt ELSE stamp[e]]
              /\ UNCHANGED dl
         ELSE /\ st' = [st EXCEPT ![e] = "Diverted"]
              /\ dl' = [dl EXCEPT ![e] = 1]
              /\ UNCHANGED tag
    ELSE /\ st' = [st EXCEPT ![e] = Landing(e)]
         /\ tag' = [tag EXCEPT ![e] = stamp[e]]
         /\ UNCHANGED dl

\* Anti-entropy repair finds a committed row the source holds and the receiver
\* lacks, a diverted one included, and re-ships it through the receiver's
\* judgement, which diverts a refused row again. A staged batch entry is not a
\* committed row. The re-ship of a diverted row the receiver's catalog has
\* since come to serve is not modelled (a deliberate abstraction gap).
Repair(e) ==
    /\ up["Link"] /\ up["Copy"] /\ kind[e] # "Atomic"
    /\ ~Admitted(e)
    /\ strict => up["Deps"]
    /\ dl[e] = 1 => Refused(e)
    /\ IF dl[e] = 1 THEN UNCHANGED <<st, tag, dl>> ELSE Ingest(e, FALSE)
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, retried, decided, up, faults, lastRead>>

\* A re-seed import carries every row the source holds, a batch's staged rows
\* included, and applies each through the receiver's judgement, which diverts
\* a refused row again. As for Repair, the re-import of a diverted row the
\* receiver's catalog has since come to serve is not modelled.
Import(e) ==
    /\ up["Link"] /\ up["Copy"]
    /\ ~Admitted(e)
    /\ strict => up["Deps"]
    /\ dl[e] = 1 => Refused(e)
    /\ IF dl[e] = 1 THEN UNCHANGED <<st, tag, dl>> ELSE Ingest(e, TRUE)
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, retried, decided, up, faults, lastRead>>

Stutter == UNCHANGED vars

Next ==
    \/ \E e \in Entries : Deliver(e)
    \/ Decide
    \/ \E e \in Entries : Read(e)
    \/ Register
    \/ Advance
    \/ \E c \in Collaborators : Fault(c)
    \/ \E c \in Collaborators : Recover(c)
    \/ \E e \in Entries : Repair(e)
    \/ \E e \in Entries : Import(e)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A e \in Entries : WF_vars(Deliver(e))
    /\ WF_vars(Decide)
    /\ \A c \in Collaborators : WF_vars(Recover(c))
====
