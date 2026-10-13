---------------------------- MODULE AntiEntropy ----------------------------
(***************************************************************************)
(* Replication anti-entropy for one tree and one directed remediation link *)
(* from the local cluster A to a peer B: the digest probe, Merkle-walk     *)
(* localisation, the remediation guards (opt-out, circuit breaker, traffic *)
(* budget), targeted leaf re-replay above the peer's high-water-mark       *)
(* cursor, the bootstrap-snapshot fallback, and the receiver apply path    *)
(* (own-origin drop, identity dedup, last-writer-wins, cursor advance).    *)
(* B writes too, and its writes replicate to A, so A holds foreign-origin  *)
(* rows. Restart, topology change (reshard), an alias swap (resize, shadow *)
(* cutover, restore, alias change: the logical tree moves to a fresh       *)
(* physical tree with its own WAL) and the digest latch are environment    *)
(* events drawn from one shared budget.                                    *)
(* See README.md and Refinement.md for scope and correspondence.           *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Keys, MaxTs, Budget, Threshold, MaxEnv, MaxSwap

Origins == {"A", "B"}
Empty == [ts |-> 0, del |-> TRUE, org |-> "none"]
Rec == [ts : 0..MaxTs, del : BOOLEAN, org : Origins \cup {"none"}]
Msg == [k : Keys, ts : 1..MaxTs, del : BOOLEAN, org : Origins,
        via : {"ship", "repair"}, to : Origins]
Ident == [k : Keys, ts : 1..MaxTs, del : BOOLEAN, org : Origins]
WalRec == [k : Keys, ts : 1..MaxTs, del : BOOLEAN]
Phases == {"idle", "mismatch", "localised"}
MaxConsumed == Budget - 1 + MaxTs + Cardinality(Keys)

VARIABLES
    aStore,     \* A's committed projection per key
    bStore,     \* B's committed projection per key
    inflight,   \* replication and repair messages in transit
    applied,    \* B's receiver identity-dedup record
    hwm,        \* B's high-water-mark for origin A (A's re-replay cursor)
    wal,        \* A's retained WAL of A-origin entries
    journal,    \* ghost: every A-origin entry ever journalled
    walFloor,   \* highest trimmed timestamp of A's WAL
    clock,      \* hybrid logical clock (globally unique stamps)
    phase,      \* probe pass phase
    loc,        \* key ranges the Merkle walk localised
    locEpoch,   \* topology generation the walk localised against
    latch,      \* projection-digest registry latched off
    epoch,      \* shard-map / physical-tree generation
    optOut,     \* AutoRemediateOnDigestMismatch is off
    env,        \* environment events (fault, restart, reshard, latch) used
    failures,   \* consecutive remediation failures for (tree, peer)
    open,       \* circuit breaker open
    cooled,     \* breaker cooldown elapsed (half-open)
    consumed,   \* entries shipped in the current budget window
    swapEpoch,  \* topology generation at which the alias swap happened (0 = none)
    swapTs,     \* clock at the alias swap: the live WAL starts above it
    retired,    \* the retired physical tree's WAL, frozen at the swap
    retFloor    \* the retired WAL's trim floor, frozen at the swap

vars == <<aStore, bStore, inflight, applied, hwm, wal, journal, walFloor, clock,
          phase, loc, locEpoch, latch, epoch, optOut, env,
          failures, open, cooled, consumed, swapEpoch, swapTs, retired, retFloor>>

dataVars == <<aStore, bStore, inflight, applied, hwm, wal, journal, walFloor, clock>>
probeVars == <<phase, loc, locEpoch>>
aliasVars == <<swapEpoch, swapTs, retired, retFloor>>
envVars == <<latch, epoch, optOut, env, aliasVars>>
guardVars == <<failures, open, cooled, consumed>>

AsRec(m) == [ts |-> m.ts, del |-> m.del, org |-> m.org]
IdOf(m) == [k |-> m.k, ts |-> m.ts, del |-> m.del, org |-> m.org]
Blocking == open /\ ~cooled
\* The projection digest covers the committed value, not the origin stamp.
SameValue(r1, r2) == r1.ts = r2.ts /\ r1.del = r2.del
Diverged == {k \in Keys : ~SameValue(aStore[k], bStore[k])}
Synced == Diverged = {}
Max(a, b) == IF a > b THEN a ELSE b
RepairMsg(k, t, d) == [k |-> k, ts |-> t, del |-> d, org |-> "A",
                       via |-> "repair", to |-> "B"]
\* The pass reads the WAL of the physical tree its walk resolved: the live
\* one, or - for a walk localised before a later alias swap - the retired one.
LiveResolved == swapEpoch = 0 \/ locEpoch >= swapEpoch
ResolvedLog == IF LiveResolved THEN wal ELSE retired
ResolvedFloor == IF LiveResolved THEN walFloor ELSE retFloor
\* The re-replay selection: retained A-origin entries of the localised
\* ranges above the peer's cursor, read from the resolved physical tree's
\* WAL. That WAL is trimmed past the cursor when its floor exceeds it.
Selection == {r \in ResolvedLog : r.k \in loc /\ r.ts > hwm}
WalTrimmed == ResolvedFloor > hwm
\* The fallback rows: every committed row of the localised ranges, values and
\* tombstones alike (a never-written key has no row).
FallbackRows == {k \in loc : aStore[k].ts > 0}
Admitted == ~optOut /\ ~Blocking /\ consumed < Budget

Init ==
    /\ aStore = [k \in Keys |-> Empty]
    /\ bStore = [k \in Keys |-> Empty]
    /\ inflight = {}
    /\ applied = {}
    /\ hwm = 0
    /\ wal = {}
    /\ journal = {}
    /\ walFloor = 0
    /\ clock = 0
    /\ phase = "idle"
    /\ loc = {}
    /\ locEpoch = 0
    /\ latch = FALSE
    /\ epoch = 0
    /\ optOut \in BOOLEAN
    /\ env = 0
    /\ failures = 0
    /\ open = FALSE
    /\ cooled = FALSE
    /\ consumed = 0
    /\ swapEpoch = 0
    /\ swapTs = 0
    /\ retired = {}
    /\ retFloor = 0

-----------------------------------------------------------------------------
(* Writes and ordinary replication.                                        *)

\* A local write on A: journalled in A's WAL stamped with A's origin and
\* handed to the shipper.
AWrite(k, d) ==
    /\ clock < MaxTs
    /\ aStore' = [aStore EXCEPT ![k] = [ts |-> clock + 1, del |-> d, org |-> "A"]]
    /\ wal' = wal \cup {[k |-> k, ts |-> clock + 1, del |-> d]}
    /\ journal' = journal \cup {[k |-> k, ts |-> clock + 1, del |-> d]}
    /\ inflight' = inflight \cup {[k |-> k, ts |-> clock + 1, del |-> d, org |-> "A",
                                   via |-> "ship", to |-> "B"]}
    /\ clock' = clock + 1
    /\ UNCHANGED <<bStore, applied, hwm, walFloor>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* A write on B, replicated to A by B's own shipper.
BWrite(k) ==
    /\ clock < MaxTs
    /\ bStore' = [bStore EXCEPT ![k] = [ts |-> clock + 1, del |-> FALSE, org |-> "B"]]
    /\ inflight' = inflight \cup {[k |-> k, ts |-> clock + 1, del |-> FALSE, org |-> "B",
                                   via |-> "ship", to |-> "A"]}
    /\ clock' = clock + 1
    /\ UNCHANGED <<aStore, applied, hwm, wal, journal, walFloor>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* An environment echo of one of B's own-origin rows held at A back to B
\* (mesh relay or redelivery). The receiver must drop it.
Reflect(k) ==
    /\ aStore[k].org = "B"
    /\ inflight' = inflight \cup {[k |-> k, ts |-> aStore[k].ts, del |-> aStore[k].del,
                                   org |-> "B", via |-> "ship", to |-> "B"]}
    /\ inflight' # inflight
    /\ UNCHANGED <<aStore, bStore, applied, hwm, wal, journal, walFloor, clock>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* Drift: ordinary shipping loses an A-origin Set or Delete.
Drop(m) ==
    /\ m \in inflight
    /\ m.via = "ship" /\ m.to = "B" /\ m.org = "A"
    /\ inflight' = inflight \ {m}
    /\ UNCHANGED <<aStore, bStore, applied, hwm, wal, journal, walFloor, clock>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* B's receiver: own-origin drop, identity dedup, last-writer-wins apply,
\* then the per-origin high-water-mark advance.
DeliverB(m) ==
    /\ m \in inflight /\ m.to = "B"
    /\ inflight' = inflight \ {m}
    /\ IF m.org = "B" \/ IdOf(m) \in applied
          THEN UNCHANGED <<bStore, applied, hwm>>
          ELSE /\ bStore' = IF m.ts > bStore[m.k].ts
                               THEN [bStore EXCEPT ![m.k] = AsRec(m)]
                               ELSE bStore
               /\ applied' = applied \cup {IdOf(m)}
               /\ hwm' = Max(hwm, m.ts)
    /\ UNCHANGED <<aStore, wal, journal, walFloor, clock>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* A's receiver for B's writes: last-writer-wins apply.
DeliverA(m) ==
    /\ m \in inflight /\ m.to = "A"
    /\ inflight' = inflight \ {m}
    /\ aStore' = IF m.ts > aStore[m.k].ts
                    THEN [aStore EXCEPT ![m.k] = AsRec(m)]
                    ELSE aStore
    /\ UNCHANGED <<bStore, applied, hwm, wal, journal, walFloor, clock>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* WAL garbage collection past a floor.
Trim ==
    /\ \E f \in (walFloor + 1)..clock :
          /\ walFloor' = f
          /\ wal' = {r \in wal : r.ts > f}
    /\ UNCHANGED <<aStore, bStore, inflight, applied, hwm, journal, clock>>
    /\ UNCHANGED probeVars /\ UNCHANGED envVars /\ UNCHANGED guardVars

-----------------------------------------------------------------------------
(* Digest probe and Merkle-walk localisation (read-only).                  *)

\* A scheduled digest comparison against the peer. Content divergence always
\* mismatches; equal content may still mismatch, because the digest folds
\* each cluster's own checkpoint offsets (a conservative trigger).
ProbeCompare ==
    /\ ~latch /\ phase = "idle"
    /\ IF Diverged # {} THEN phase' = "mismatch" ELSE phase' \in {"idle", "mismatch"}
    /\ UNCHANGED dataVars
    /\ UNCHANGED <<loc, locEpoch>>
    /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* The peer reports the digest unavailable, the versions skew, or the local
\* projection digest is not maintained: no comparison this tick.
ProbeUnavailable ==
    /\ ~latch /\ phase = "idle"
    /\ UNCHANGED dataVars
    /\ UNCHANGED probeVars
    /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* The Merkle walk, against a freshly resolved physical tree, localises a
\* non-empty set of ranges covering every diverged key (it may over-localise).
Localise ==
    /\ phase = "mismatch"
    /\ \E S \in SUBSET Keys :
          /\ S # {}
          /\ Diverged \subseteq S
          /\ loc' = S
    /\ phase' = "localised"
    /\ locEpoch' = epoch
    /\ UNCHANGED dataVars
    /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* The walk aborts (depth cap, byte budget, peer unavailable, version skew,
\* fault) or localises nothing: no remediation this pass.
LocaliseAbort ==
    /\ phase = "mismatch"
    /\ phase' = "idle"
    /\ loc' = {}
    /\ UNCHANGED dataVars
    /\ UNCHANGED locEpoch
    /\ UNCHANGED envVars /\ UNCHANGED guardVars
-----------------------------------------------------------------------------
(* Remediation: guards, re-replay, bootstrap fallback.                     *)

\* The master opt-out, an open breaker still cooling, or an exhausted budget
\* window skips the pass with zero traffic.
RemediationGated ==
    /\ phase = "localised"
    /\ optOut \/ Blocking \/ consumed >= Budget
    /\ phase' = "idle" /\ loc' = {}
    /\ UNCHANGED dataVars
    /\ UNCHANGED locEpoch
    /\ UNCHANGED envVars /\ UNCHANGED guardVars

\* Targeted leaf re-replay: re-ship the selection as A-origin repair traffic.
\* A shipped re-ship is a success and resets the breaker.
ReReplay ==
    /\ phase = "localised"
    /\ ~optOut
    /\ ~Blocking
    /\ consumed < Budget
    /\ ~WalTrimmed
    /\ Selection # {}
    /\ inflight' = inflight \cup {RepairMsg(r.k, r.ts, r.del) : r \in Selection}
    /\ consumed' = consumed + Cardinality(Selection)
    /\ phase' = "idle" /\ loc' = {}
    /\ failures' = 0 /\ open' = FALSE /\ cooled' = FALSE
    /\ UNCHANGED <<aStore, bStore, applied, hwm, wal, journal, walFloor, clock>>
    /\ UNCHANGED locEpoch
    /\ UNCHANGED envVars

\* Bootstrap-snapshot fallback after a wal_trimmed or range_empty re-replay
\* skip: re-ship the committed rows of the localised ranges from the live
\* projection, stamped with A's origin and the row's own timestamp; a
\* tombstone row ships as a delete. An empty plan ships nothing and is not a
\* failure.
Fallback ==
    /\ phase = "localised"
    /\ ~optOut
    /\ ~Blocking
    /\ consumed < Budget
    /\ WalTrimmed \/ Selection = {}
    /\ inflight' = inflight \cup {RepairMsg(k, aStore[k].ts, aStore[k].del) : k \in FallbackRows}
    /\ consumed' = consumed + Cardinality(FallbackRows)
    /\ phase' = "idle" /\ loc' = {}
    /\ failures' = 0 /\ open' = FALSE /\ cooled' = FALSE
    /\ UNCHANGED <<aStore, bStore, applied, hwm, wal, journal, walFloor, clock>>
    /\ UNCHANGED locEpoch
    /\ UNCHANGED envVars

\* The peer rejects the re-ship (zero shipped of a non-empty attempt), or the
\* re-ship faults (a transport error the repair helper swallows): either way a
\* failure is recorded, never a success, and the breaker opens at the threshold or, already
\* open, refreshes its cooldown. Draws on the environment budget.
ShipRejected ==
    /\ phase = "localised"
    /\ Admitted
    /\ env < MaxEnv
    /\ IF ~WalTrimmed /\ Selection # {} THEN TRUE ELSE FallbackRows # {}
    /\ env' = env + 1
    /\ failures' = IF failures < Threshold THEN failures + 1 ELSE failures
    /\ open' = (open \/ failures + 1 >= Threshold)
    /\ cooled' = FALSE
    /\ phase' = "idle" /\ loc' = {}
    /\ UNCHANGED dataVars
    /\ UNCHANGED locEpoch
    /\ UNCHANGED <<latch, epoch, optOut, consumed>>
    /\ UNCHANGED aliasVars

\* The breaker's cooldown elapses: the next pass is a half-open trial.
Cooldown ==
    /\ open /\ ~cooled
    /\ cooled' = TRUE
    /\ UNCHANGED dataVars /\ UNCHANGED probeVars /\ UNCHANGED envVars
    /\ UNCHANGED <<failures, open, consumed>>

\* The traffic-budget window rolls over.
WindowRoll ==
    /\ consumed > 0
    /\ consumed' = 0
    /\ UNCHANGED dataVars /\ UNCHANGED probeVars /\ UNCHANGED envVars
    /\ UNCHANGED <<failures, open, cooled>>

-----------------------------------------------------------------------------
(* Environment: restart, topology change, digest latch.                    *)

\* The probe grain re-activates: in-memory guard accounting and the pass in
\* flight are lost; the latched registry is durable.
Restart ==
    /\ env < MaxEnv
    /\ env' = env + 1
    /\ failures' = 0 /\ open' = FALSE /\ cooled' = FALSE /\ consumed' = 0
    /\ phase' = "idle" /\ loc' = {}
    /\ UNCHANGED dataVars
    /\ UNCHANGED locEpoch
    /\ UNCHANGED <<latch, epoch, optOut>>
    /\ UNCHANGED aliasVars

\* A reshard changes the shard map a walk must resolve, within the same
\* physical tree and WAL.
Reshard ==
    /\ env < MaxEnv
    /\ env' = env + 1
    /\ epoch' = epoch + 1
    /\ UNCHANGED dataVars /\ UNCHANGED probeVars /\ UNCHANGED guardVars
    /\ UNCHANGED <<latch, optOut>>
    /\ UNCHANGED aliasVars

\* An alias swap (resize, shadow-cutover restore, schema remediation or an
\* operator alias change) repoints the logical tree at a fresh physical tree.
\* The live projection is carried over; the new physical tree journals into
\* its own, initially empty WAL, and the retired tree's WAL is left frozen.
\* The swap is a new topology generation a forced routing resolve observes.
AliasSwap ==
    /\ env < MaxEnv
    /\ swapEpoch = 0
    /\ MaxSwap > 0
    /\ env' = env + 1
    /\ swapEpoch' = epoch + 1
    /\ epoch' = swapEpoch'
    /\ swapTs' = clock
    /\ retired' = wal
    /\ retFloor' = walFloor
    /\ wal' = {}
    /\ walFloor' = 0
    /\ UNCHANGED <<aStore, bStore, inflight, applied, hwm, journal, clock>>
    /\ UNCHANGED probeVars /\ UNCHANGED guardVars
    /\ UNCHANGED <<latch, optOut>>

\* A non-domain digest fault latches projection-digest maintenance off.
LatchOff ==
    /\ ~latch
    /\ env < MaxEnv
    /\ env' = env + 1
    /\ latch' = TRUE
    /\ UNCHANGED dataVars /\ UNCHANGED probeVars /\ UNCHANGED guardVars
    /\ UNCHANGED <<epoch, optOut>>
    /\ UNCHANGED aliasVars

Stutter == UNCHANGED vars

Remediation == RemediationGated \/ ReReplay \/ Fallback \/ ShipRejected

Next ==
    \/ \E k \in Keys, d \in BOOLEAN : AWrite(k, d)
    \/ \E k \in Keys : BWrite(k)
    \/ \E k \in Keys : Reflect(k)
    \/ \E m \in inflight : Drop(m)
    \/ \E m \in inflight : DeliverB(m)
    \/ \E m \in inflight : DeliverA(m)
    \/ Trim
    \/ ProbeCompare
    \/ ProbeUnavailable
    \/ Localise
    \/ LocaliseAbort
    \/ RemediationGated
    \/ ReReplay
    \/ Fallback
    \/ ShipRejected
    \/ Cooldown
    \/ WindowRoll
    \/ Restart
    \/ Reshard
    \/ AliasSwap
    \/ LatchOff
    \/ Stutter

\* Delivery is fair per message; the probe cadence keeps firing, so a
\* comparison and a walk that stay possible eventually happen; a localised
\* pass is eventually resolved; cooldowns and budget windows elapse.
Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A m \in Msg : WF_vars(DeliverB(m)) /\ WF_vars(DeliverA(m))
    /\ SF_vars(ProbeCompare /\ phase' = "mismatch")
    /\ SF_vars(Localise)
    /\ WF_vars(Remediation)
    /\ WF_vars(Cooldown)
    /\ WF_vars(WindowRoll)

-----------------------------------------------------------------------------
(* Properties.                                                             *)

TypeOK ==
    /\ aStore \in [Keys -> Rec]
    /\ bStore \in [Keys -> Rec]
    /\ inflight \subseteq Msg
    /\ applied \subseteq Ident
    /\ hwm \in 0..MaxTs
    /\ wal \subseteq WalRec
    /\ journal \subseteq WalRec
    /\ walFloor \in 0..MaxTs
    /\ clock \in 0..MaxTs
    /\ phase \in Phases
    /\ loc \subseteq Keys
    /\ locEpoch \in 0..MaxEnv
    /\ latch \in BOOLEAN
    /\ epoch \in 0..MaxEnv
    /\ optOut \in BOOLEAN
    /\ env \in 0..MaxEnv
    /\ failures \in 0..Threshold
    /\ open \in BOOLEAN
    /\ cooled \in BOOLEAN
    /\ consumed \in 0..MaxConsumed
    /\ swapEpoch \in 0..MaxEnv
    /\ swapTs \in 0..MaxTs
    /\ retired \subseteq WalRec
    /\ retFloor \in 0..MaxTs

\* The receiver never applies an entry stamped with its own origin.
NoOwnOriginApplied == \A id \in applied : id.org # "B"

\* An identity the dedup recorded is reflected in B's projection, so a later
\* identical re-ship may be suppressed without losing data.
DedupSound == \A id \in applied : bStore[id.k].ts >= id.ts

\* A's live WAL is exactly the journalled suffix above its trim floor and
\* above the alias swap (the live physical tree journals only what it saw),
\* which is what makes the wal_trimmed classification sound.
WalComplete == wal = {r \in journal : r.ts > Max(walFloor, swapTs)}

\* An alias swap is a topology generation the forced routing resolve
\* observes, so a walk resolved after it never reads the retired tree.
SwapObserved == swapEpoch <= epoch

\* Repair traffic emitted by a step.
NewRepairs == {m \in inflight' \ inflight : m.via = "repair"}

HwmMonotone == [][hwm' >= hwm]_vars

LwwNoRegress ==
    [][\A k \in Keys : aStore'[k].ts >= aStore[k].ts /\ bStore'[k].ts >= bStore[k].ts]_vars

ProbeReadOnly ==
    [][(ProbeCompare \/ ProbeUnavailable \/ Localise \/ LocaliseAbort)
         => UNCHANGED <<dataVars, guardVars>>]_vars

RepairOnlyLocalised ==
    [][NewRepairs # {} => phase = "localised" /\ \A m \in NewRepairs : m.k \in loc]_vars

OptOutSilent == [][optOut => NewRepairs = {}]_vars

BreakerBlocks == [][Blocking => NewRepairs = {}]_vars

BudgetGate == [][NewRepairs # {} => consumed < Budget]_vars

ReplayAboveCursor ==
    [][ReReplay =>
         \A m \in NewRepairs :
            /\ m.ts > hwm
            /\ m.org = "A"
            /\ [k |-> m.k, ts |-> m.ts, del |-> m.del] \in ResolvedLog]_vars

FallbackOnlyOnSkip ==
    [][Fallback /\ NewRepairs # {} => WalTrimmed \/ Selection = {}]_vars

\* The fallback ships every committed row of the localised ranges, value or
\* tombstone, at its own timestamp and with its own tombstone flag.
FallbackCommittedOnly ==
    [][Fallback =>
         /\ \A m \in NewRepairs :
               /\ aStore[m.k].ts > 0
               /\ m.ts = aStore[m.k].ts
               /\ m.del = aStore[m.k].del
         /\ \A k \in FallbackRows : RepairMsg(k, aStore[k].ts, aStore[k].del) \in inflight']_vars

LatchPermanent == [][latch => latch']_vars

BreakerOpensAtThreshold == [][failures' >= Threshold => open']_vars

LocaliseFresh == [][phase' = "localised" /\ phase # "localised" => locEpoch' = epoch]_vars

\* Every divergence is eventually repaired unless the digest is latched off
\* or remediation is opted out.
Converges == <>[](Synced \/ latch \/ optOut)

=============================================================================