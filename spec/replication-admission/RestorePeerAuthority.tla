----------------------- MODULE RestorePeerAuthority -----------------------
(***************************************************************************)
(* The authority a coordinated-restore participant acts under (issue       *)
(* #6192, epic #6046): when its restore steps are elevated to system       *)
(* origin, when a commit or abort of this cluster's own saga borrows the   *)
(* authority of the prepare it ran, and what a failed cutover leaves       *)
(* behind.                                                                 *)
(*                                                                         *)
(* A step reaches the participant over the replication gRPC host, whose    *)
(* admitted-peer seam (ReplicationOriginBinding) admits only the           *)
(* credential-bound origin of a current peer and overwrites the request's  *)
(* coordinator with it, or locally, from this cluster's own coordinator or *)
(* from the participant grain redelivering the coordinator it persisted at *)
(* prepare. Peer authority holds only when every seam is wired, the        *)
(* coordinator is a remote current peer, the target is enrolled here, and  *)
(* a build replays only the target or trees enrolled here. Otherwise a     *)
(* step runs as the ambient caller, except a commit or abort of this       *)
(* cluster's own saga that its coordinator has durably decided, acting on  *)
(* the shadow this participant built.                                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

\* ---- Clusters ----
\* "peerA" and "peerB" are replication peers; either can join or leave the
\* topology. A credential is bound to at most one of them.
Self == "self"
Peers == {"peerA", "peerB"}
NoCluster == "none"
Clusters == {Self} \cup Peers
Channels == {"grpc", "local"}

\* ---- Trees and backups ----
Trees == {"t1", "t2"}
Targets == SUBSET Trees \ {{}}
NoShadow == {}
\* The trees a backup's chain was captured from, for each restored tree. "bSelf"
\* is a backup of the restored tree itself, "bOther" of t2, "bEmpty" a report naming no source;
\* "bMissing" does not exist, so its admission probe fails.
Backups == {"bSelf", "bOther", "bEmpty", "bMissing"}
Exists(b) == b # "bMissing"
Sources(b, t) == CASE b = "bSelf" -> {t} [] b = "bOther" -> {"t2"} [] OTHER -> {}

Decisions == {"none", "commit", "abort"}

VARIABLES wired, enrolled, topology, decision, prepCoord, cache, fence, obs
vars == <<wired, enrolled, topology, decision, prepCoord, cache, fence, obs>>

NoObs == [kind |-> "none", swapped |-> {}, step |-> "none", ch |-> "none", coord |-> NoCluster, bound |-> NoCluster,
          target |-> {}, backup |-> "none", auth |-> "none", durable |-> "none", readOk |-> FALSE,
          cached |-> FALSE, built |-> FALSE, engaged |-> FALSE, reply |-> "none"]

Obs(kind, sw, step, ch, coord, bound, t, b, auth, durable, readOk, cached, built, engaged, reply) ==
    [kind |-> kind, swapped |-> sw, step |-> step, ch |-> ch, coord |-> coord, bound |-> bound, target |-> t,
     backup |-> b, auth |-> auth, durable |-> durable, readOk |-> readOk, cached |-> cached,
     built |-> built, engaged |-> engaged, reply |-> reply]

-----------------------------------------------------------------------------
\* The admitted-peer seam, abstractly (ReplicationOriginBinding): a gRPC step
\* is admitted only for the credential-bound origin of a current peer, with any
\* body-declared coordinator equal to it. A local step carries no transport
\* origin and names this cluster or the coordinator persisted at prepare.
LocalCoords(step) == IF step = "prepare" THEN {Self} ELSE {Self} \cup ({prepCoord} \ {NoCluster})

Admitted(step, ch, bound, claimed) ==
    IF ch = "grpc"
    THEN bound # NoCluster /\ bound \in topology /\ claimed \in {NoCluster, bound}
    ELSE bound = NoCluster /\ claimed \in LocalCoords(step)

\* SagaControlRequest.CoordinatorClusterId as the participant reads it: the
\* gRPC saga service overwrites it with the credential-bound origin.
Coord(ch, bound, claimed) == IF ch = "grpc" THEN bound ELSE claimed

\* The requested trees this cluster hosts: RestoreParticipant.IsHostedTarget for
\* a single tree and RestoreParticipant.FilterHostedMembers for a set, each a
\* no-op when the membership seam is not wired.
Members(ts) == IF wired THEN ts \cap enrolled ELSE ts
Hosted(t) == t # {}

\* RestoreParticipant.HasPeerAuthority.
HasPeerAuthority(c) == wired /\ c \notin {Self, NoCluster} /\ c \in topology

\* RestoreParticipant.SourcesEnrolled, for every restored tree: an empty source
\* list refuses.
SourcesEnrolled(b, t) == \A m \in t : Sources(b, m) # {} /\ Sources(b, m) \subseteq enrolled \cup {m}

\* Whether the step names this cluster's own saga (needs the options seam).
Own(c) == wired /\ c = Self

-----------------------------------------------------------------------------
Init ==
    /\ wired \in BOOLEAN
    /\ enrolled = {"t1"}
    /\ topology = {"peerA"}
    /\ decision = "none"
    /\ prepCoord = NoCluster
    /\ cache = NoShadow
    /\ fence = "off"
    /\ obs = NoObs

\* A participant receives a prepare: the hosting gate, then the admission probe
\* and the captured-source confinement, then the unfenced shadow build of every
\* hosted tree.
Prepare(ch, bound, claimed, ts, b) ==
    LET t == Members(ts)
        c == Coord(ch, bound, claimed)
        up == HasPeerAuthority(c)
        auth == IF up THEN "peer" ELSE "ambient"
    IN /\ Admitted("prepare", ch, bound, claimed)
       /\ prepCoord' = c
       /\ IF ~Hosted(t)
          THEN /\ obs' = Obs("refused", {}, "prepare", ch, c, bound, t, b, "none", "none", FALSE, FALSE, FALSE, FALSE, "notReplicated")
               /\ UNCHANGED cache
          ELSE IF ~Exists(b)
          THEN /\ obs' = Obs("refused", {}, "prepare", ch, c, bound, t, b, auth, "none", FALSE, FALSE, FALSE, FALSE, IF up THEN "peerRefusal" ELSE "probeFailed")
               /\ UNCHANGED cache
          ELSE IF up /\ ~SourcesEnrolled(b, t)
          THEN /\ obs' = Obs("refused", {}, "prepare", ch, c, bound, t, b, auth, "none", FALSE, FALSE, FALSE, FALSE, "peerRefusal")
               /\ UNCHANGED cache
          ELSE /\ obs' = Obs("build", {}, "prepare", ch, c, bound, t, b, auth, "none", FALSE, FALSE, TRUE, FALSE, "commit")
               /\ cache' = t
       /\ UNCHANGED <<wired, enrolled, topology, decision, fence>>

\* A participant receives a commit: the hosting gate, the step's authority,
\* a rebuild when the cached shadows were lost, then the fenced cutover of the
\* hosted trees. A commit names the targets the shadows were built for, and
\* swaps a cached shadow only for a tree it still hosts.
Commit(ch, bound, claimed, ts, b, readOk, cutOk) ==
    LET t == Members(ts)
        c == Coord(ch, bound, claimed)
        up == HasPeerAuthority(c)
        read == IF readOk THEN decision ELSE "unreadable"
        auth == IF up THEN "peer"
                ELSE IF ~Own(c) THEN "ambient"
                ELSE IF read # "commit" THEN "refused"
                ELSE IF cache # NoShadow THEN "decided"
                ELSE "ambient"
        cached == cache # NoShadow
        dur == IF Own(c) /\ ~up THEN decision ELSE "none"
        rok == readOk /\ Own(c) /\ ~up
        sw == IF cached THEN cache \cap t ELSE t
    IN /\ Admitted("commit", ch, bound, claimed)
       /\ cached => cache \subseteq ts /\ b = "bSelf"
       /\ sw = {} => cutOk
       /\ IF ~Hosted(t)
          THEN /\ fence' = "off"
               /\ obs' = Obs("refused", {}, "commit", ch, c, bound, t, b, "none", dur, rok, cached, FALSE, FALSE, "notReplicated")
               /\ UNCHANGED cache
          ELSE IF auth = "refused"
          THEN /\ obs' = Obs("refused", {}, "commit", ch, c, bound, t, b, auth, dur, rok, cached, FALSE, FALSE, "undecided")
               /\ UNCHANGED <<cache, fence>>
          ELSE IF ~cached /\ up /\ ~SourcesEnrolled(b, t)
          THEN /\ fence' = "off"
               /\ obs' = Obs("refused", {}, "commit", ch, c, bound, t, b, auth, dur, rok, cached, FALSE, FALSE, "notReplicated")
               /\ UNCHANGED cache
          ELSE IF ~cached /\ ~Exists(b)
          THEN /\ obs' = Obs("failed", {}, "commit", ch, c, bound, t, b, auth, dur, rok, cached, FALSE, FALSE, "buildFailed")
               /\ UNCHANGED <<cache, fence>>
          ELSE IF cutOk
          THEN /\ fence' = "unblocked"
               /\ cache' = sw
               /\ obs' = Obs("swap", sw, "commit", ch, c, bound, t, b, auth, dur, rok, cached, ~cached, TRUE, "committed")
          ELSE /\ fence' = "unblocked"
               /\ cache' = sw
               /\ obs' = Obs("cutoverFailed", sw, "commit", ch, c, bound, t, b, auth, dur, rok, cached, ~cached, TRUE, "cutoverFailed")
       /\ UNCHANGED <<wired, enrolled, topology, decision, prepCoord>>

\* A participant receives an abort, from the coordinator or from its own grain
\* after a fence expiry: the hosting gate, the step's authority, then the
\* compensation (revert, shadow garbage collection, fence lift).
Abort(ch, bound, claimed, ts, readOk) ==
    LET t == Members(ts)
        c == Coord(ch, bound, claimed)
        up == HasPeerAuthority(c)
        read == IF readOk THEN decision ELSE "unreadable"
        cached == cache # NoShadow
        auth == IF up THEN "peer"
                ELSE IF ~Own(c) THEN "ambient"
                ELSE IF read = "abort" /\ cached THEN "decided"
                ELSE "ambient"
        dur == IF Own(c) /\ ~up THEN decision ELSE "none"
        rok == readOk /\ Own(c) /\ ~up
    IN /\ Admitted("abort", ch, bound, claimed)
       /\ cached => cache \subseteq ts
       /\ fence' = "off"
       /\ IF ~Hosted(t)
          THEN /\ obs' = Obs("refused", {}, "abort", ch, c, bound, t, "none", "none", dur, rok, cached, FALSE, FALSE, "notReplicated")
               /\ UNCHANGED cache
          ELSE /\ obs' = Obs("compensate", {}, "abort", ch, c, bound, t, "none", auth, dur, rok, cached, FALSE, FALSE, "aborted")
               /\ cache' = NoShadow
       /\ UNCHANGED <<wired, enrolled, topology, decision, prepCoord>>

\* Environment: this cluster's own coordinator durably decides its saga.
Decide(d) ==
    /\ decision = "none"
    /\ decision' = d
    /\ obs' = NoObs
    /\ UNCHANGED <<wired, enrolled, topology, prepCoord, cache, fence>>

\* Environment: a reactivation loses the in-memory shadow cache.
LoseCache ==
    /\ cache # NoShadow
    /\ cache' = NoShadow
    /\ obs' = NoObs
    /\ UNCHANGED <<wired, enrolled, topology, decision, prepCoord, fence>>

\* Environment: the fence grain self-lifts on its bounded deadline.
FenceExpires ==
    /\ fence # "off"
    /\ fence' = "off"
    /\ obs' = NoObs
    /\ UNCHANGED <<wired, enrolled, topology, decision, prepCoord, cache>>

\* Environment: an operator enrolls or disables a tree here.
SetEnrollment(t) ==
    /\ enrolled' = IF t \in enrolled THEN enrolled \ {t} ELSE enrolled \cup {t}
    /\ obs' = NoObs
    /\ UNCHANGED <<wired, topology, decision, prepCoord, cache, fence>>

\* Environment: a peer joins or leaves the replication topology.
SetTopology(p) ==
    /\ topology' = IF p \in topology THEN topology \ {p} ELSE topology \cup {p}
    /\ obs' = NoObs
    /\ UNCHANGED <<wired, enrolled, decision, prepCoord, cache, fence>>

Next ==
    \/ \E ch \in Channels, bound \in Peers \cup {NoCluster}, claimed \in Clusters \cup {NoCluster}, ts \in Targets, b \in Backups : Prepare(ch, bound, claimed, ts, b)
    \/ \E ch \in Channels, bound \in Peers \cup {NoCluster}, claimed \in Clusters \cup {NoCluster}, ts \in Targets, b \in Backups, r \in BOOLEAN, ok \in BOOLEAN : Commit(ch, bound, claimed, ts, b, r, ok)
    \/ \E ch \in Channels, bound \in Peers \cup {NoCluster}, claimed \in Clusters \cup {NoCluster}, ts \in Targets, r \in BOOLEAN : Abort(ch, bound, claimed, ts, r)
    \/ \E d \in {"commit", "abort"} : Decide(d)
    \/ LoseCache
    \/ FenceExpires
    \/ \E t \in Trees : SetEnrollment(t)
    \/ \E p \in Peers : SetTopology(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ wired \in BOOLEAN
    /\ enrolled \subseteq Trees
    /\ topology \subseteq Peers
    /\ decision \in Decisions
    /\ prepCoord \in Clusters \cup {NoCluster}
    /\ cache \subseteq Trees
    /\ fence \in {"off", "engaged", "unblocked"}
    /\ obs.auth \in {"none", "peer", "decided", "ambient", "refused"}

\* PeerAuthorityBounded: a step runs with peer authority only when every seam
\* is wired, its coordinator is a remote cluster of the current topology and,
\* over gRPC, the credential-bound origin, and every tree it restores or swaps
\* is enrolled here, a swap only inside the write fence over those trees; a
\* build under peer authority replays only the target or trees enrolled here,
\* and an empty source list refuses; and an elevated prepare refuses a missing
\* backup and one it may not restore with one answer, so the elevated probe is
\* not an existence oracle.
PeerAuthorityBounded ==
    /\ obs.auth = "peer" =>
         /\ wired
         /\ obs.coord \notin {Self, NoCluster}
         /\ obs.coord \in topology
         /\ (obs.ch = "grpc" => obs.coord = obs.bound)
         /\ obs.target \subseteq enrolled
         /\ obs.swapped \subseteq enrolled
         /\ obs.swapped \subseteq obs.target
    /\ (obs.auth = "peer" /\ obs.built) =>
         \A m \in obs.target : Sources(obs.backup, m) # {} /\ Sources(obs.backup, m) \subseteq enrolled \cup {m}
    /\ (obs.step = "prepare" /\ obs.auth = "peer" /\ obs.kind = "refused") => obs.reply = "peerRefusal"

\* OwnSagaCommitNeedsDurableDecision: a commit of this cluster's own saga
\* without a readable, durable Commit decision is refused before any fence or
\* swap; an abort of it without a durable Abort decision runs as the ambient
\* caller; and the prepare's authority is borrowed only for the shadow this
\* participant built.
OwnSagaCommitNeedsDurableDecision ==
    /\ (obs.step = "commit" /\ Own(obs.coord) /\ obs.auth # "peer" /\ ~(obs.readOk /\ obs.durable = "commit"))
         => obs.kind = "refused" /\ ~obs.engaged
    /\ (obs.step = "abort" /\ Own(obs.coord) /\ ~(obs.readOk /\ obs.durable = "abort")) => obs.auth \in {"ambient", "none"}
    /\ obs.auth = "decided" => Own(obs.coord) /\ obs.cached /\ obs.readOk
         /\ obs.durable = (IF obs.step = "commit" THEN "commit" ELSE "abort")

\* NoStrandedFence: a cutover that fails has unblocked local writes before the
\* failure propagates, and leaves the shadow cached, so a redelivered decision
\* can finish.
NoStrandedFence ==
    obs.kind = "cutoverFailed" => fence # "engaged" /\ cache = obs.swapped
=============================================================================
