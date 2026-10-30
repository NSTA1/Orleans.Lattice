---- MODULE SchemaStageOrdering ----
EXTENDS Naturals, Sequences

\* The schema write stages at the LatticeGrain write choke point: which stages
\* each registration of the two add-ons resolves, the system-origin opt-in,
\* per-stage per-tree strict flags, the version envelope in and out, the batch
\* semantics of a refusal, and writes that land below the check.

CONSTANTS BatchSize

Registrations == {"None", "Enforcement", "Versioning", "Composed", "Reversed"}
Origins == {"Local", "System"}
Bodies == {"Good", "Bad"}
Tags == {"Target", "Old", "Orphan"}
Upcastable == {"Target", "Old"}
Raw(b) == [form |-> "Raw", body |-> b, tag |-> "None"]
Env(b, t) == [form |-> "Env", body |-> b, tag |-> t]
Values == {Raw(b) : b \in Bodies} \cup {Env(b, t) : b \in Bodies, t \in Tags}
Entries == 1..BatchSize
BoundedSeq(S) == UNION {[1..n -> S] : n \in 0..BatchSize}
Range(s) == {s[i] : i \in DOMAIN s}

\* The stage chain the DI wiring resolves. "Reversed" is versioning registered
\* before enforcement: enforcement re-composes with the registered versioning
\* stage instead of replacing it (#5241), so it resolves the same chain.
Chain(r) ==
    CASE r = "None" -> << >>
      [] r = "Enforcement" -> <<"E">>
      [] r = "Versioning" -> <<"V">>
      [] r = "Composed" -> <<"E", "V">>
      [] r = "Reversed" -> <<"E", "V">>
HasStage(r, s) == \E i \in 1..Len(Chain(r)) : Chain(r)[i] = s

VARIABLES reg, gStrictE, gStrictV, policyOn, versionOn, tStrictE, tStrictV,
          phase, origin, atomic, req, ei, idx, val, kept, path, outcome,
          refusedBy, dropped, dlq, stored
config == <<reg, gStrictE, gStrictV, policyOn, versionOn, tStrictE, tStrictV>>
vars == <<config, phase, origin, atomic, req, ei, idx, val, kept, path, outcome,
          refusedBy, dropped, dlq, stored>>

\* The interceptor opts into system origin when any registered stage's global
\* strict flag is on; the core skips it otherwise.
Intercepts(r) == (HasStage(r, "E") /\ gStrictE) \/ (HasStage(r, "V") /\ gStrictV)
Skips(r, o) == r = "None" \/ (o = "System" /\ ~Intercepts(r))

\* Enforcement judges the bytes a reader is served. Registering versioning
\* installs the read decoder, which strips the envelope and upcasts the body;
\* a value it cannot decode (newer than the target, or no upcast path) has no
\* judgeable body. With no decoder the whole value, envelope included, is
\* judged, and no body policy accepts the envelope header.
DecoderInstalled(r) == r \in {"Versioning", "Composed", "Reversed"}
Decodable(v) ==
    v.form = "Raw" \/ (DecoderInstalled(reg) /\ ~(versionOn /\ v.tag = "Orphan"))

TypeOK ==
    /\ reg \in Registrations
    /\ gStrictE \in BOOLEAN /\ gStrictV \in BOOLEAN
    /\ policyOn \in BOOLEAN /\ versionOn \in BOOLEAN
    /\ tStrictE \in BOOLEAN /\ tStrictV \in BOOLEAN
    /\ phase \in {"Idle", "Submitted", "Chain", "Ready", "Done"}
    /\ origin \in Origins /\ atomic \in BOOLEAN
    /\ req \in [Entries -> Values]
    /\ ei \in 1..(BatchSize + 1) /\ idx \in 1..2
    /\ val \in Values /\ kept \in BoundedSeq(Values)
    /\ path \in {"None", "Intercepted", "Skipped", "Below"}
    /\ outcome \in {"Pending", "Applied", "Rejected", "DeadLettered"}
    /\ refusedBy \in {"None", "E", "V"}
    /\ dropped \in 0..BatchSize /\ dlq \in 0..BatchSize
    /\ stored \in BoundedSeq(Values)

\* The enforcement stage judges the entry as it was submitted, before any stage
\* adds the version envelope.
EnforcementSeesSubmittedValue ==
    (phase = "Chain" /\ Chain(reg)[idx] = "E") => val = req[ei]

CommitStep == phase = "Ready" /\ phase' = "Done"

\* An intercepted local write is committed only when every entry is judgeable
\* and its body satisfies the policy of a registration that includes
\* enforcement.
ValidationBeforeCommit ==
    [][(CommitStep /\ path = "Intercepted" /\ origin = "Local"
        /\ HasStage(reg, "E") /\ policyOn)
       => \A e \in Range(stored') :
            /\ e.body = "Good"
            /\ (e.form = "Env" => (DecoderInstalled(reg) /\ ~(versionOn /\ e.tag = "Orphan")))]_vars

\* A rejected or aborted write applies nothing.
RefusalAppliesNothing ==
    [][(phase # "Done" /\ phase' = "Done" /\ outcome' \in {"Rejected", "DeadLettered"})
       => stored' = stored]_vars

\* Every intercepted entry is either applied or dropped as dead-lettered, and an
\* atomic batch never drops one.
BatchAccounting ==
    [][(CommitStep /\ path = "Intercepted")
       => /\ Len(stored') + dropped = BatchSize
          /\ (atomic => dropped = 0)]_vars

\* Enforcement refuses only an entry whose served body breaks the policy or
\* cannot be decoded: an entry inside a version envelope that decodes to a
\* valid body is never refused.
ValidBodyNeverRefusedByEnforcement ==
    [][((dlq' # dlq \/ outcome' = "Rejected") /\ refusedBy' = "E")
       => \/ val.body = "Bad"
          \/ (val.form = "Env" /\ ~(DecoderInstalled(reg) /\ ~(versionOn /\ val.tag = "Orphan")))]_vars

\* Under every registration with versioning, in either order, every intercepted
\* entry is stored self-describing. The guard is the registration (the decoder
\* is installed exactly when versioning is), not the resolved chain, so a
\* registration that silently loses the stamping stage violates it.
StampedWhenVersioned ==
    [][(CommitStep /\ path = "Intercepted"
        /\ DecoderInstalled(reg) /\ versionOn)
       => \A e \in Range(stored') : e.form = "Env"]_vars

\* An entry that arrived enveloped is stored with its own tag, never re-stamped.
EnvelopedValuesVerbatim ==
    [][(CommitStep /\ path = "Intercepted")
       => \A e \in Range(stored') :
            e.form = "Env" =>
                (e \in Range(req) \/ (e.tag = "Target" /\ Raw(e.body) \in Range(req)))]_vars

\* A system-origin write is intercepted exactly when a registered stage's global
\* strict flag opts the interceptor in.
SystemOriginGate ==
    [][(phase = "Submitted" /\ phase' # phase)
       => (path' = "Intercepted" <=>
             /\ reg # "None"
             /\ (origin = "Local"
                 \/ (HasStage(reg, "E") /\ gStrictE)
                 \/ (HasStage(reg, "V") /\ gStrictV)))]_vars

\* A stage diverts an entry to the dead-letter tree only under its own per-tree
\* strict flag.
StrictFlagsPerStage ==
    [][dlq' # dlq =>
         /\ (refusedBy' = "E" => tStrictE)
         /\ (refusedBy' = "V" => tStrictV)]_vars

\* Strict ingest stores no entry its stage would refuse.
StrictIngestDiverts ==
    [][(CommitStep /\ path = "Intercepted" /\ origin = "System")
       => \A e \in Range(stored') :
            /\ (HasStage(reg, "E") /\ policyOn /\ tStrictE =>
                    e.body = "Good" /\ (e.form = "Env" => (DecoderInstalled(reg) /\ ~(versionOn /\ e.tag = "Orphan"))))
            /\ (HasStage(reg, "V") /\ versionOn /\ tStrictV => e.tag \in Upcastable)]_vars

\* Only system-origin ingest is dead-lettered; a local refusal is a rejection.
LocalRefusalRejects == [][dlq' # dlq => origin = "System"]_vars

\* Every admitted write reaches a decision.
WriteDecides == (phase \in {"Submitted", "Chain", "Ready"}) ~> (phase = "Done")

Init ==
    /\ reg \in Registrations
    /\ gStrictE \in BOOLEAN /\ gStrictV \in BOOLEAN
    /\ policyOn \in BOOLEAN /\ versionOn \in BOOLEAN
    /\ tStrictE \in BOOLEAN /\ tStrictV \in BOOLEAN
    /\ phase = "Idle" /\ origin = "Local" /\ atomic = FALSE
    /\ req = [i \in Entries |-> Raw("Good")]
    /\ ei = 1 /\ idx = 1 /\ val = Raw("Good") /\ kept = << >>
    /\ path = "None" /\ outcome = "Pending" /\ refusedBy = "None"
    /\ dropped = 0 /\ dlq = 0 /\ stored = << >>

\* Move to the next entry, keeping k as the entries applied so far.
NextEntry(k) ==
    /\ kept' = k
    /\ ei' = ei + 1
    /\ idx' = 1
    /\ val' = IF ei < BatchSize THEN req[ei + 1] ELSE val
    /\ phase' = IF ei < BatchSize THEN "Chain" ELSE "Ready"

\* Hand v to the next stage, or keep it when this was the last stage.
Advance(v) ==
    IF idx < Len(Chain(reg))
    THEN /\ val' = v /\ idx' = idx + 1
         /\ UNCHANGED <<phase, ei, kept>>
    ELSE NextEntry(Append(kept, v))

\* A client or peer write reaches the choke point: a point write is a batch of one.
Submit(o, a, r) ==
    /\ phase = "Idle"
    /\ phase' = "Submitted" /\ origin' = o /\ atomic' = a /\ req' = r /\ val' = r[1]
    /\ UNCHANGED <<config, ei, idx, kept, path, outcome, refusedBy, dropped, dlq, stored>>

\* The core gate: skip the null interceptor, and system origin unless opted in.
Admit ==
    /\ phase = "Submitted"
    /\ IF Skips(reg, origin)
       THEN /\ path' = "Skipped" /\ kept' = req /\ phase' = "Ready"
       ELSE /\ path' = "Intercepted" /\ phase' = "Chain"
            /\ UNCHANGED kept
    /\ UNCHANGED <<config, origin, atomic, req, ei, idx, val, outcome, refusedBy, dropped, dlq, stored>>

\* The enforcement stage judges the entry's served body, failing closed on an
\* entry it cannot decode.
RunEnforcement ==
    /\ phase = "Chain" /\ Chain(reg)[idx] = "E"
    /\ IF ~policyOn \/ (val.body = "Good" /\ Decodable(val))
       THEN /\ Advance(val)
            /\ UNCHANGED <<outcome, refusedBy, dropped, dlq>>
       ELSE IF origin = "Local"
       THEN /\ phase' = "Done" /\ outcome' = "Rejected" /\ refusedBy' = "E"
            /\ UNCHANGED <<ei, idx, val, kept, dropped, dlq>>
       ELSE IF ~tStrictE
       THEN /\ Advance(val)
            /\ UNCHANGED <<outcome, refusedBy, dropped, dlq>>
       ELSE IF atomic
       THEN /\ dlq' = dlq + 1
            /\ phase' = "Done" /\ outcome' = "DeadLettered" /\ refusedBy' = "E"
            /\ UNCHANGED <<ei, idx, val, kept, dropped>>
       ELSE /\ dlq' = dlq + 1 /\ dropped' = dropped + 1 /\ refusedBy' = "E"
            /\ NextEntry(kept)
            /\ UNCHANGED outcome
    /\ UNCHANGED <<config, origin, atomic, req, path, stored>>

\* The versioning stage stamps a raw entry and checks an enveloped one on strict ingest.
RunVersioning ==
    /\ phase = "Chain" /\ Chain(reg)[idx] = "V"
    /\ IF ~versionOn
       THEN /\ Advance(val)
            /\ UNCHANGED <<outcome, refusedBy, dropped, dlq>>
       ELSE IF val.form = "Raw"
       THEN /\ Advance(Env(val.body, "Target"))
            /\ UNCHANGED <<outcome, refusedBy, dropped, dlq>>
       ELSE IF origin = "Local" \/ ~tStrictV \/ val.tag \in Upcastable
       THEN /\ Advance(val)
            /\ UNCHANGED <<outcome, refusedBy, dropped, dlq>>
       ELSE IF atomic
       THEN /\ dlq' = dlq + 1
            /\ phase' = "Done" /\ outcome' = "DeadLettered" /\ refusedBy' = "V"
            /\ UNCHANGED <<ei, idx, val, kept, dropped>>
       ELSE /\ dlq' = dlq + 1 /\ dropped' = dropped + 1 /\ refusedBy' = "V"
            /\ NextEntry(kept)
            /\ UNCHANGED outcome
    /\ UNCHANGED <<config, origin, atomic, req, path, stored>>

\* The choke point appends the entries the chain kept.
Commit ==
    /\ phase = "Ready"
    /\ stored' = kept /\ phase' = "Done" /\ outcome' = "Applied"
    /\ UNCHANGED <<config, origin, atomic, req, ei, idx, val, kept, path, refusedBy, dropped, dlq>>

\* Environment: plain LWW replication apply, backup restore or tree merge
\* writes below the choke point, unchecked and unstamped.
Bypass(r) ==
    /\ phase = "Idle"
    /\ stored' = r /\ req' = r /\ path' = "Below" /\ origin' = "System"
    /\ phase' = "Done" /\ outcome' = "Applied"
    /\ UNCHANGED <<config, atomic, ei, idx, val, kept, refusedBy, dropped, dlq>>

Stutter == UNCHANGED vars

Next ==
    \/ \E o \in Origins, a \in BOOLEAN, r \in [Entries -> Values] : Submit(o, a, r)
    \/ Admit
    \/ RunEnforcement
    \/ RunVersioning
    \/ Commit
    \/ \E r \in [Entries -> Values] : Bypass(r)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Admit) /\ WF_vars(RunEnforcement)
    /\ WF_vars(RunVersioning) /\ WF_vars(Commit)
====
