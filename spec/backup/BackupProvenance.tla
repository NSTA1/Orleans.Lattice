------------------------- MODULE BackupProvenance -------------------------
(***************************************************************************)
(* An abstract TLA+ specification of what a backup chain records about     *)
(* the writes it captured: the per-origin provenance high-water            *)
(* (BackupOriginProvenance), the empty-origin rule of #2621, and the       *)
(* chain's HLC frontier (BackupConsistencyCut.HlcTimestamp, #3758).        *)
(*                                                                         *)
(* It models the DESIGN of the bookkeeping, not the capture's isolation    *)
(* (BackupCapture.tla and, for increments, BackupIncremental.tla own       *)
(* that). Each write is its own key, so a capture                          *)
(* holds every write it covers, and a chain is a full capture followed by  *)
(* increments that each cover the writes appended since the last link.     *)
(* See Refinement.md for the mapping to production symbols.                *)
(*                                                                         *)
(* Epic #4430, issue #4440.                                                *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

(***************************************************************************)
(* The bounded instance. Origin "" is a locally authored write on a host   *)
(* with no cluster identity (DefaultLatticeOriginClusterIdResolver stamps  *)
(* string.Empty); "o1" is a replicated origin. HLC wall-clock values run   *)
(* over 1..MaxHlc, the log holds at most MaxLog writes, and the chain at    *)
(* most MaxChain links.                                                    *)
(***************************************************************************)
Origins == {"", "o1"}
MaxHlc == 3
MaxLog == 3
MaxChain == 3
Hlcs == 1..MaxHlc

(***************************************************************************)
(* State.                                                                  *)
(*  log      the tree's writes in arrival order. author is the origin that *)
(*           wrote it; stamp is the origin the row carries                 *)
(*           (LwwEntry.OriginClusterId); hlc its wall-clock HLC.           *)
(*  clock    the local HLC, which a local write advances.                  *)
(*  chain    the backup chain: a full link, then increments. Each link     *)
(*           records the log range it covers (from..to), its cut HLC and   *)
(*           its per-origin provenance (a function from origin to          *)
(*           high-water).                                                  *)
(***************************************************************************)
VARIABLES log, clock, chain

vars == <<log, clock, chain>>

Write == [author : Origins, stamp : Origins, hlc : Hlcs]

Link == [from : 1..(MaxLog + 1), to : 0..MaxLog, cut : 0..MaxHlc,
         prov : UNION {[O -> Hlcs] : O \in SUBSET Origins}]

TypeOK ==
    /\ log \in Seq(Write)
    /\ Len(log) <= MaxLog
    /\ clock \in 0..MaxHlc
    /\ chain \in Seq(Link)
    /\ Len(chain) <= MaxChain

Range(lo, hi) == {i \in 1..Len(log) : lo <= i /\ i <= hi}

Max(S) == CHOOSE m \in S : \A n \in S : n <= m

\* The highest HLC over the writes at the given positions, or 0 when none.
HighHlc(I) == IF I = {} THEN 0 ELSE Max({log[i].hlc : i \in I})

(***************************************************************************)
(* The collector's per-origin high-water over the writes at positions I    *)
(* (RawEntryCollector.RecordEntry, IncrementalDeltaCollector.OnEntry). An   *)
(* unstamped write - origin "" - is normalised away and contributes        *)
(* nothing: the #2621 decision, because inventing an id for it would put a *)
(* fabricated origin into a wire-format manifest.                          *)
(***************************************************************************)
StampedOrigins(I) == {log[i].stamp : i \in I} \ {""}

HighWater(I) ==
    [o \in StampedOrigins(I) |-> Max({log[i].hlc : i \in {j \in I : log[j].stamp = o}})]

Init ==
    /\ log = << >>
    /\ clock = 0
    /\ chain = << >>

(***************************************************************************)
(* LocalWrite: a write authored on this cluster. On a host with no         *)
(* replication it carries no origin stamp, and its HLC is the local clock, *)
(* advanced.                                                               *)
(***************************************************************************)
LocalWrite ==
    /\ Len(log) < MaxLog
    /\ clock < MaxHlc
    /\ clock' = clock + 1
    /\ log' = Append(log, [author |-> "", stamp |-> "", hlc |-> clock + 1])
    /\ UNCHANGED chain

(***************************************************************************)
(* ReplicatedApply: a peer origin's write applied here, stamped with its   *)
(* origin and carrying the peer's HLC - which may be below writes already  *)
(* in the log, because peers' clocks are not ordered with this one. The    *)
(* local clock merges it (HLC receive rule).                               *)
(***************************************************************************)
ReplicatedApply ==
    /\ Len(log) < MaxLog
    /\ \E h \in Hlcs :
         /\ log' = Append(log, [author |-> "o1", stamp |-> "o1", hlc |-> h])
         /\ clock' = IF h > clock THEN h ELSE clock
    /\ UNCHANGED chain

(***************************************************************************)
(* CaptureFull: a full capture covers every write so far and starts a new  *)
(* chain. It is always enabled, which is also why the model needs no     *)
(* stuttering action: a full capture can always be taken. Its cut HLC is the larger of the registry-snapshot anchor        *)
(* (always 0 in production) and the highest HLC it captured               *)
(* (LatticeBackupCaptureService.BuildConsistencyCut, #3758).               *)
(***************************************************************************)
CaptureFull ==
    /\ LET I == Range(1, Len(log))
       IN chain' = << [from |-> 1, to |-> Len(log),
                       cut |-> IF 0 > HighHlc(I) THEN 0 ELSE HighHlc(I),
                       prov |-> HighWater(I)] >>
    /\ UNCHANGED <<log, clock>>

(***************************************************************************)
(* CaptureIncremental: an increment on the chain's last link covers the    *)
(* writes appended since it (the forward WAL drain from the base's resume  *)
(* offsets). Its provenance is the delta's own high-water; its cut is the  *)
(* delta's highest HLC, never below its base's                             *)
(* (LatticeBackupCaptureService.BuildIncrementalCut).                      *)
(***************************************************************************)
CaptureIncremental ==
    /\ chain # << >>
    /\ Len(chain) < MaxChain
    /\ LET last == chain[Len(chain)]
           I == Range(last.to + 1, Len(log))
           high == HighHlc(I)
       IN chain' = Append(chain, [from |-> last.to + 1, to |-> Len(log),
                                  cut |-> IF high < last.cut THEN last.cut ELSE high,
                                  prov |-> HighWater(I)])
    /\ UNCHANGED <<log, clock>>

Next ==
    \/ LocalWrite
    \/ ReplicatedApply
    \/ CaptureFull
    \/ CaptureIncremental

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* No link's provenance names the empty origin. Production's
\* BackupOriginProvenance constructor rejects one, so in production this
\* defect presents as every capture of a tree with locally authored rows
\* throwing - the #2621 outage - rather than as a bad manifest.
ProvenanceNoEmptyOrigin ==
    \A k \in 1..Len(chain) : "" \notin DOMAIN chain[k].prov

\* No link silently drops a real origin: every captured write authored by a
\* real origin is attributed to it, at a high-water at or above its HLC.
ProvenanceCoversCaptured ==
    \A k \in 1..Len(chain) : \A i \in Range(chain[k].from, chain[k].to) :
        log[i].author # "" =>
            /\ log[i].author \in DOMAIN chain[k].prov
            /\ chain[k].prov[log[i].author] >= log[i].hlc

\* Every link's cut HLC is at or above every write it captured - the
\* frontier an incremental pins the WAL at while it drains forward (#3758).
FrontierCoversCaptured ==
    \A k \in 1..Len(chain) : \A i \in Range(chain[k].from, chain[k].to) :
        chain[k].cut >= log[i].hlc

\* The chain's frontier never regresses from one link to the next.
ChainFrontierMonotonic ==
    \A k \in 1..(Len(chain) - 1) : chain[k + 1].cut >= chain[k].cut
=============================================================================
