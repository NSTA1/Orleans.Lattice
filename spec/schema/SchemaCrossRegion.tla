---- MODULE SchemaCrossRegion ----
EXTENDS Naturals, FiniteSets

\* A versioned tree replicated from a source region to a receiver region, each
\* with its own policy, version target and upcaster catalog: schema definitions
\* are region-local, so nothing the stream carries changes the receiver's
\* catalog. The source ships each entry stamped at the version it held: a plain
\* last-writer-wins write, a typed-CRDT delta, or one entry of one atomic
\* batch. The receiver ingests plain writes below the write check, and judges
\* deltas and batch entries only under strict ingest, where an entry its
\* catalog cannot serve, or whose body its own policy refuses, is diverted. The
\* stream, the receiver's catalog, policy and dead-letter collaborators, and
\* its physical copy (which a coordinated restore can hold receive-closed) can
\* each be unavailable for a while. The receiver's operator registers the
\* source's newer version and later advances to it, or never does.
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
          up, faults, lastRead
vars == <<kind, body, stamp, strict, tgt, hop, st, tag, retried, decided,
          up, faults, lastRead>>

\* The receiver's read decoder serves a stamp at its target, or an older one
\* along a registered hop; a newer stamp, or an older one with no hop, is an
\* explicit failure.
Servable(v) == v = tgt \/ (v < tgt /\ hop)

\* Strict ingest judges what reaches the write choke point as a system-origin
\* write: typed-CRDT deltas and atomic-batch entries. A plain write merges
\* below it.
Checked(e) == strict /\ kind[e] \in {"Crdt", "Atomic"}

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

\* What is not judged (a plain write in any mode, or anything without strict
\* ingest) is stored with the stamp it carried, never re-stamped or diverted.
UncheckedStoredVerbatim ==
    [][\A e \in Entries :
          (~Checked(e) /\ st[e] = "Pending" /\ st'[e] # "Pending")
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

\* Every entry is eventually admitted and visible, or diverted, once the
\* stream and collaborators stop failing.
EveryEntryResolved ==
    <>[](\A e \in Entries : st[e] = "Diverted" \/ Visible(e))

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
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, decided, up, faults, lastRead>>

\* The batch's commit decision arrives after every entry of the batch was
\* admitted or diverted, and reveals the staged ones.
Decide ==
    /\ up["Link"] /\ up["Copy"] /\ ~decided /\ AtomicEntries # {}
    /\ \A e \in AtomicEntries : st[e] # "Pending"
    /\ decided' = TRUE
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, retried, up, faults, lastRead>>

\* A client reads a visible entry through the receiver's decoder.
Read(e) ==
    /\ Visible(e)
    /\ lastRead' = [e |-> e, out |-> IF Servable(tag[e]) THEN "Body" ELSE "Fail"]
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, retried, decided, up, faults>>

\* The receiver's operator registers the source's newer version and the hop to
\* it in the receiver's own catalog.
Register ==
    /\ ~hop
    /\ hop' = TRUE
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, st, tag, retried, decided, up, faults, lastRead>>

\* The receiver's operator advances its target, which the facade admits only
\* to a version registered and reachable from the current one.
Advance ==
    /\ tgt = 1 /\ hop
    /\ tgt' = 2
    /\ UNCHANGED <<kind, body, stamp, strict, hop, st, tag, retried, decided, up, faults, lastRead>>

\* Environment: the stream, or the receiver's collaborators, become unavailable.
Fault(c) ==
    /\ up[c] /\ faults < MaxFaults
    /\ up' = [up EXCEPT ![c] = FALSE]
    /\ faults' = faults + 1
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, retried, decided, lastRead>>

\* Environment: an unavailable collaborator comes back.
Recover(c) ==
    /\ ~up[c]
    /\ up' = [up EXCEPT ![c] = TRUE]
    /\ retried' = [e \in Entries |-> FALSE]
    /\ UNCHANGED <<kind, body, stamp, strict, tgt, hop, st, tag, decided, faults, lastRead>>

Stutter == UNCHANGED vars

Next ==
    \/ \E e \in Entries : Deliver(e)
    \/ Decide
    \/ \E e \in Entries : Read(e)
    \/ Register
    \/ Advance
    \/ \E c \in Collaborators : Fault(c)
    \/ \E c \in Collaborators : Recover(c)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A e \in Entries : WF_vars(Deliver(e))
    /\ WF_vars(Decide)
    /\ \A c \in Collaborators : WF_vars(Recover(c))
====
