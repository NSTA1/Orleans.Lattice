---------------------------- MODULE AliasRouting ----------------------------
\* Shared registry alias routing, not snapshot copying or data-plane sagas.
EXTENDS Naturals

Aliases == {"x", "y"}
Copies == {"a", "b", "c"}
MaxMoves == 3
CONSTANT MaxFaults
None == "none"
VARIABLES alias, map, version, phase, source, destination, expected, token,
          redirect, owner, moves, route, running, faults, overwritten
vars == <<alias, map, version, phase, source, destination, expected, token,
          redirect, owner, moves, route, running, faults, overwritten>>

Init ==
    /\ alias = [l \in Aliases |-> "a"]
    /\ map = alias
    /\ version = [l \in Aliases |-> 0]
    /\ phase = [l \in Aliases |-> "idle"]
    /\ source = alias /\ destination = alias
    /\ expected = version /\ token = version
    /\ redirect = [l \in Aliases |-> [p \in Copies |-> None]]
    /\ owner = [l \in Aliases |-> [p \in Copies |-> 0]]
    /\ moves = 0 /\ route = alias
    /\ running = TRUE /\ faults = 0 /\ overwritten = FALSE

\* Intent is durable before any shard fence is changed. Tokens are unique
\* within the bounded instance, not reusable physical-copy identities.
Begin ==
    \E l \in Aliases, p \in Copies :
        /\ running /\ phase[l] = "idle" /\ p # alias[l] /\ moves < MaxMoves
        /\ phase' = [phase EXCEPT ![l] = "intent"]
        /\ source' = [source EXCEPT ![l] = alias[l]]
        /\ destination' = [destination EXCEPT ![l] = p]
        /\ expected' = [expected EXCEPT ![l] = version[l]]
        /\ token' = [token EXCEPT ![l] = moves + 1]
        /\ moves' = moves + 1
        /\ UNCHANGED <<alias, map, version, redirect, owner, route, running, faults, overwritten>>

Arm ==
    \E l \in Aliases :
        /\ running /\ phase[l] = "intent"
        /\ alias[l] = source[l] /\ version[l] = expected[l]
        /\ redirect' = [redirect EXCEPT ![l][source[l]] = destination[l]]
        /\ owner' = [owner EXCEPT ![l][source[l]] = token[l]]
        /\ phase' = [phase EXCEPT ![l] = "armed"]
        /\ UNCHANGED <<alias, map, version, source, destination, expected, token,
                       moves, route, running, faults, overwritten>>

Publish ==
    \E l \in Aliases :
        /\ running /\ phase[l] = "armed"
        /\ alias[l] = source[l] /\ version[l] = expected[l]
        /\ alias' = [alias EXCEPT ![l] = destination[l]]
        /\ map' = [map EXCEPT ![l] = destination[l]]
        /\ version' = [version EXCEPT ![l] = @ + 1]
        /\ phase' = [phase EXCEPT ![l] = "published"]
        /\ overwritten' = (overwritten \/ alias[l] # source[l] \/ version[l] # expected[l])
        /\ UNCHANGED <<source, destination, expected, token, redirect, owner,
                       moves, route, running, faults>>

\* Another registry writer can supersede a durable intent. It changes the
\* revision even when the same physical identity returns (the ABA case).
Supersede ==
    \E l \in Aliases, p \in Copies :
        /\ running /\ phase[l] \in {"intent", "armed"} /\ p # alias[l]
        /\ moves < MaxMoves
        /\ alias' = [alias EXCEPT ![l] = p]
        /\ map' = [map EXCEPT ![l] = p]
        /\ version' = [version EXCEPT ![l] = @ + 1]
        /\ moves' = moves + 1
        /\ redirect' = [redirect EXCEPT ![l][p] = None]
        /\ owner' = [owner EXCEPT ![l][p] = moves + 1]
        /\ UNCHANGED <<phase, source, destination, expected, token, route, running, faults, overwritten>>

\* Only the owning operation may remove its fence. A newer fence, and every
\* other logical alias's fence on that same physical copy, must survive.
Abandon ==
    \E l \in Aliases :
        /\ running /\ phase[l] \in {"intent", "armed"}
        /\ alias[l] # source[l] \/ version[l] # expected[l]
        /\ redirect' = IF alias[l] = source[l] /\ owner[l][source[l]] = token[l]
                        THEN [redirect EXCEPT ![l][source[l]] = None] ELSE redirect
        /\ phase' = [phase EXCEPT ![l] = "idle"]
        /\ UNCHANGED <<alias, map, version, source, destination, expected, token,
                       owner, moves, route, running, faults, overwritten>>

Release ==
    \E l \in Aliases :
        /\ running /\ phase[l] = "published"
        /\ redirect' = [redirect EXCEPT ![l][destination[l]] = None]
        /\ phase' = [phase EXCEPT ![l] = "released"]
        /\ UNCHANGED <<alias, map, version, source, destination, expected, token,
                       owner, moves, route, running, faults, overwritten>>

Complete ==
    \E l \in Aliases :
        /\ running /\ phase[l] = "released"
        /\ phase' = [phase EXCEPT ![l] = "idle"]
        /\ UNCHANGED <<alias, map, version, source, destination, expected, token,
                       redirect, owner, moves, route, running, faults, overwritten>>

Refresh ==
    \E l \in Aliases :
        /\ route' = [route EXCEPT ![l] = alias[l]]
        /\ UNCHANGED <<alias, map, version, phase, source, destination, expected,
                       token, redirect, owner, moves, running, faults, overwritten>>

Crash ==
    /\ running /\ faults < MaxFaults
    /\ \E l \in Aliases : phase[l] # "idle"
    /\ running' = FALSE /\ faults' = faults + 1
    /\ UNCHANGED <<alias, map, version, phase, source, destination, expected,
                   token, redirect, owner, moves, route, overwritten>>

Retry ==
    /\ ~running /\ running' = TRUE
    /\ UNCHANGED <<alias, map, version, phase, source, destination, expected,
                   token, redirect, owner, moves, route, faults, overwritten>>

Stutter == UNCHANGED vars
Next ==
    \/ Begin
    \/ Arm
    \/ Publish
    \/ Supersede
    \/ Abandon
    \/ Release
    \/ Complete
    \/ Refresh
    \/ Crash
    \/ Retry
    \/ Stutter
Progress == Arm \/ Publish \/ Abandon \/ Release \/ Complete \/ Retry
Spec == Init /\ [][Next]_vars /\ WF_vars(Progress)

TypeOK ==
    /\ alias \in [Aliases -> Copies] /\ map \in [Aliases -> Copies]
    /\ version \in [Aliases -> 0..MaxMoves]
    /\ phase \in [Aliases -> {"idle", "intent", "armed", "published", "released"}]
    /\ source \in [Aliases -> Copies] /\ destination \in [Aliases -> Copies]
    /\ expected \in [Aliases -> 0..MaxMoves] /\ token \in [Aliases -> 0..MaxMoves]
    /\ redirect \in [Aliases -> [Copies -> Copies \cup {None}]]
    /\ owner \in [Aliases -> [Copies -> 0..MaxMoves]]
    /\ route \in [Aliases -> Copies] /\ moves \in 0..MaxMoves
    /\ running \in BOOLEAN /\ faults \in 0..MaxFaults /\ overwritten \in BOOLEAN
CoherentRow == alias = map
NoStalePublication == ~overwritten
FinishedCopyUnfenced ==
    \A l \in Aliases : phase[l] = "released" => redirect[l][alias[l]] = None
FenceBeforePublication ==
    [][\A l \in Aliases :
       (alias'[l] # alias[l] /\ phase'[l] = "published")
          => redirect[l][source[l]] = destination[l]]_vars
RevisionAdvances ==
    [][\A l \in Aliases : alias'[l] # alias[l] => version'[l] > version[l]]_vars
IndependentAliases ==
    [][\A l \in Aliases : alias'[l] = alias[l] /\ phase'[l] = phase[l]
                             => redirect'[l] = redirect[l]]_vars
RefreshedBinding ==
    [][\A l \in Aliases : route'[l] # route[l] => route'[l] = alias[l]]_vars
DurableMove ==
    [][(~running \/ ~running') =>
         /\ phase' = phase /\ source' = source /\ destination' = destination
         /\ expected' = expected /\ token' = token]_vars
MovesSettle == (\E l \in Aliases : phase[l] # "idle") ~> (\A l \in Aliases : phase[l] = "idle")
=============================================================================
