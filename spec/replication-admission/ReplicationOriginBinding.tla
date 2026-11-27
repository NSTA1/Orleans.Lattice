--------------------- MODULE ReplicationOriginBinding ---------------------
(***************************************************************************)
(* The admitted-peer seam of the replication gRPC host (issue #6053, epic  *)
(* #6046): what a receiver accepts from honest and malicious peers on      *)
(* every method bound on the host.                                         *)
(*                                                                         *)
(* Every bound method passes through one seam. The shared-secret           *)
(* interceptor refuses a method the seam does not register and, with       *)
(* credential binding on, binds the presented credential to the            *)
(* transport-stamped origin. The method's admission class then binds what  *)
(* its body declares about its caller to that origin: an origin-bound verb *)
(* requires the stamped origin to equal the body's, and a control verb     *)
(* acts only for the credential-bound origin of a current peer.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

\* ---- Clusters, secrets and the topology ----
\* "peerA" is a current peer. "peerB" was removed from the topology but its
\* secret is still configured (and accepted). "stranger" has no secret here.
Clusters == {"peerA", "peerB", "stranger"}
Topology == {"peerA"}
NoSecret == "none"
Secret(c) == CASE c = "peerA" -> "sA" [] c = "peerB" -> "sB" [] OTHER -> NoSecret
Accepted == {"sA", "sB"}
Credentials == {"sA", "sB", "bogus"}
NoHeader == "none"
Headers == {NoHeader} \cup Clusters
NoBody == "none"
Bodies == {NoBody} \cup Clusters

\* ---- Every method bound on the replication gRPC host ----
PushMethods == {"Push", "ProbeDigest", "ExchangeContentManifest", "PullCompressionDictionary", "ProbeMerkleWalk", "GetPeerHighWaterMark"}
SnapshotMethods == {"GetMetadata", "RequestSnapshot"}
SagaMethods == {"Prepare", "Commit", "Abort", "GetStatus", "GetDecision"}
HostMethods == PushMethods \cup SnapshotMethods \cup SagaMethods

\* The admitted-peer seam's registration and each method's admission class.
Registered == {"Push", "ExchangeContentManifest", "GetPeerHighWaterMark", "ProbeDigest", "ProbeMerkleWalk", "PullCompressionDictionary", "GetMetadata", "RequestSnapshot", "Prepare", "Commit", "Abort", "GetStatus", "GetDecision"}
OriginBound == {"Push", "ExchangeContentManifest", "GetPeerHighWaterMark"}
ControlPeer == {"Prepare", "Commit", "Abort", "GetStatus", "GetDecision"}
\* GetDecision's caller names this cluster, the coordinator it asks, in the
\* body; its requester identity is the stamped origin, never a body value.
RequesterQuery == {"GetDecision"}

VARIABLES binding, obs
vars == <<binding, obs>>

NoObs == [kind |-> "none"]
CallObsRecs ==
    [kind : {"call"}, m : HostMethods, hdr : Headers, cred : Credentials, body : Bodies,
     binding : BOOLEAN, status : STRING, acted : Headers]

-----------------------------------------------------------------------------
\* LatticeReplicationGrpcAuthInterceptor.EnforceOriginBindingAsync: the
\* presented credential equals the secret this cluster uses for the claimed
\* origin.
Bound(hdr, cred) == hdr # NoHeader /\ Secret(hdr) # NoSecret /\ Secret(hdr) = cred

\* The interceptor: an unregistered method is refused before any credential;
\* an unaccepted credential is refused; with binding on, a credential not
\* bound to the stamped origin draws one refusal whatever the cause.
Interceptor(m, hdr, cred) ==
    IF m \notin Registered THEN "unimplemented"
    ELSE IF cred \notin Accepted THEN "denied"
    ELSE IF binding /\ ~Bound(hdr, cred) THEN "unbound"
    ELSE "pass"

\* ReplicationAdmittedPeer.CredentialBoundOrigin: the stamped origin, when the
\* interceptor bound it to the credential.
BoundOrigin(hdr) == IF binding /\ hdr # NoHeader THEN hdr ELSE NoHeader

\* The method's own admission class: the status, and the identity the method
\* acts for.
Verb(m, hdr, body) ==
    IF m \in OriginBound
    THEN IF body = NoBody THEN <<"invalid", NoHeader>>
         ELSE IF hdr = NoHeader \/ hdr # body THEN <<"denied", NoHeader>>
         ELSE <<"ok", body>>
    ELSE IF m \in ControlPeer
    THEN IF BoundOrigin(hdr) = NoHeader THEN <<"denied", NoHeader>>
         ELSE IF m \notin RequesterQuery /\ body # NoBody /\ body # hdr THEN <<"denied", NoHeader>>
         ELSE IF hdr \notin Topology THEN <<"denied", NoHeader>>
         ELSE <<"ok", BoundOrigin(hdr)>>
    ELSE <<"ok", NoHeader>>

-----------------------------------------------------------------------------
Init ==
    /\ binding \in BOOLEAN
    /\ obs = NoObs

\* A peer, honest or not, calls method m with a stamped origin header, a
\* credential and a body-declared origin.
Call(m, hdr, cred, body) ==
    LET gate == Interceptor(m, hdr, cred)
        verb == Verb(m, hdr, body)
        status == IF gate # "pass" THEN gate ELSE verb[1]
        acted == IF gate # "pass" THEN NoHeader ELSE verb[2]
    IN /\ obs' = [kind |-> "call", m |-> m, hdr |-> hdr, cred |-> cred, body |-> body,
                  binding |-> binding, status |-> status, acted |-> acted]
       /\ UNCHANGED binding

Next ==
    \/ \E m \in HostMethods, hdr \in Headers, cred \in Credentials, body \in Bodies : Call(m, hdr, cred, body)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
TypeOK ==
    /\ binding \in BOOLEAN
    /\ obs \in {NoObs} \cup CallObsRecs

Ok == obs.kind = "call" /\ obs.status = "ok"

\* OriginAuthenticated: every call the host accepts passed the admitted-peer
\* seam. Its method is registered and its credential accepted; with binding
\* on, the credential is the one this cluster uses for the stamped origin; a
\* method consuming a body-declared origin acts only for a present stamped
\* origin equal to it; a control method acts only for the credential-bound
\* origin of a current peer, with every body-declared identity bound to it;
\* and a missing, unknown or mismatched origin draws one refusal.
OriginAuthenticated ==
    /\ Ok => obs.m \in Registered /\ obs.cred \in Accepted
    /\ (Ok /\ obs.binding) => Bound(obs.hdr, obs.cred)
    /\ (Ok /\ obs.m \in OriginBound) => obs.hdr # NoHeader /\ obs.hdr = obs.body /\ obs.acted = obs.body
    /\ (Ok /\ obs.m \in ControlPeer) => obs.binding /\ Bound(obs.hdr, obs.cred) /\ obs.hdr \in Topology /\ obs.acted = obs.hdr
    /\ (Ok /\ obs.m \in ControlPeer \ RequesterQuery /\ obs.body # NoBody) => obs.body = obs.hdr
    /\ (obs.kind = "call" /\ obs.binding /\ obs.m \in Registered /\ obs.cred \in Accepted /\ ~Bound(obs.hdr, obs.cred)) => obs.status = "unbound"
=============================================================================
