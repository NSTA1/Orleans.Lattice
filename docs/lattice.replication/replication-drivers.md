# Replication drivers

This document describes the **production drivers** that turn the dormant
replication primitives - the tree's partitioned write-ahead log, the WAL storage provider, the
WAL garbage collector, and the fall-off-the-log detector - into a running
end-to-end pipeline. Without these drivers, calling `AddLatticeReplication`
yields the seam set but emits nothing on the wire, and the WAL is trimmed
only by the core library's own per-silo garbage-collection scheduler
(see [`WalGcInterval`](../lattice/configuration.md#walgcinterval)): every
metric on `LatticeReplicationMetrics` other than `dead_letter.*` stays at
zero.

The drivers are wired automatically when the host calls
`siloBuilder.AddLatticeReplication(...)`. There is no separate registration
step.

---

## Architecture

Two Orleans-native grain types, both registered as cluster singletons via
their grain key. Cluster-singleton placement gives automatic activation
migration on silo loss without leader election.

| Driver | Key | Cadence | Purpose |
|---|---|---|---|
| Per-peer shipper | `{treeName}/{peerClusterId}` | 100 ms phase timer + 90 s reminder backstop + writer-side doorbell | Drains the tree's WAL partitions from the per-peer partition cursors, applies producer-side filters and the local-origin-only cycle-break, calls `IReplicationTransport.SendAsync`, advances the cursor on ack, applies exponential backoff on transient failure, parks batches whose framing header cannot be built on the per-tree DLQ. |
| Per-tree maintenance | `{treeName}` | 5 s phase timer + 60 s reminder backstop | Schedules WAL garbage collection (`ILatticeWalGc.RunOnceAsync`) and per-peer fall-off-the-log probes (`ILatticeFallOffLogDetector.CheckAndTriggerAsync`) on independent cadences. |

The shipper is per-peer because per-peer back-pressure isolation must not
couple peers to each other; one slow peer cannot block any other.
The maintenance grain is per-tree because GC and fall-off probing are
tree-scoped, not peer-scoped - running them once per tree avoids
N-fold-redundant work that scales with peer count.

### Activation

A hosted background service (a `BackgroundService`) activates the driver
grains for every replicated tree on startup: the per-tree maintenance
grain, one shipper per current peer, and - only when `DigestProbeEnabled`
is set for the tree - the per-tree
[anti-entropy digest probe](anti-entropy-digest-probe.md) scheduler.
Activation is idempotent - Orleans deduplicates concurrent activations
via grain identity, and a grain whose keepalive reminder and phase timer
are already wired treats a repeat activation as a no-op.

The replicated-tree set comes from `IReplicatedTreeMembership` rather
than the raw `ReplicatedTrees` map. On a host that opts into
[runtime replication configuration](runtime-config.md) it is the union
of the static map and the trees enabled at runtime, and a lightweight
poll of the compiled configuration snapshot (every 2 seconds, doing work
only when the snapshot has rebuilt) enrols the driver grains of any tree
enabled after startup without a silo restart. Enrolment is additive
only: disabling a tree at runtime does not tear its driver grains down,
and the shipper keeps shipping the tree - it does not consult the
merge-mode resolver to decide whether to run (see
[Fail-closed ambiguity](runtime-config.md#fail-closed-ambiguity) for what
a peer does with those entries).

The activation loop is **retry-with-backoff**: a freshly-started silo may
race the Orleans runtime's own `IHostedService` ordering, so the first
`EnsureActiveAsync` call can throw transiently. The service starts with a
250 ms inter-attempt delay, doubles on each consecutive miss up to a 30 s
cap, and resets the delay on the first successful activation. The loop
only exits when every pending grain is active or the host's
`stoppingToken` is cancelled.

```csharp verify
// Hosts opt into the drivers transparently - registration is part
// of AddLatticeReplication.
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
    {
        ["catalog"] = LatticeMergeMode.LwwRegister,
    };
    opts.ReplicationPeers = new[] { "site-b", "site-c" };
});
```

### Runtime topology changes

Peer membership is sourced from `IReplicationTopology`, the
runtime-observable peer topology seam - not snapshotted once at silo
startup. The default implementation projects
`LatticeReplicationOptions.ReplicationPeers` via
`IOptionsMonitor<LatticeReplicationOptions>.OnChange` and diffs each
reload against the last-seen set so callers see one `PeerChanged` event
per net add and per net remove.

A consumer that needs to react to peer arrivals - for example, a
custom dashboard or a metrics publisher - takes a dependency on
`IReplicationTopology` and calls `Subscribe`:

```csharp verify
// Resolve the topology and observe peer membership at runtime.
// In real code this would be injected via constructor parameter.
var topology = client.ServiceProvider.GetRequiredService<IReplicationTopology>();

// Read the current snapshot before subscribing so the consumer does
// not need to replay arrivals it already knows about.
IReadOnlyCollection<string> initialPeers = topology.CurrentPeers;

// Subscribe receives one PeerChanged event per net membership change.
// The returned IDisposable unsubscribes when disposed.
using IDisposable subscription = topology.Subscribe(change =>
{
    if (change.Kind == PeerChangeKind.Added)
    {
        // React to a peer arriving at runtime.
    }
    else if (change.Kind == PeerChangeKind.Removed)
    {
        // React to a peer being removed.
    }
});
```

Hosts that source their topology from a service registry, configuration
provider, or any other dynamic surface can replace the registration by
pre-registering their own `IReplicationTopology` singleton before
`AddLatticeReplication` runs - the default registration uses
`TryAddSingleton`, so a pre-registered implementation wins. The custom
topology only needs to implement the two-member surface:
`IReadOnlyCollection<string> CurrentPeers` and
`IDisposable Subscribe(Action<PeerChanged>)`.

The driver activation background service subscribes for the lifetime of the
silo: when a peer arrives at runtime (`PeerChangeKind.Added`), one
shipper grain is activated per replicated tree under the same
retry-with-backoff loop as the startup pass, without a silo restart.
Removal events (`PeerChangeKind.Removed`) intentionally do not tear
down the shipper grain - it stays activated to drain its remaining
backlog.

#### Peer configuration: topology vs. `ReplicationPeers`

`IReplicationTopology` is the **single source of truth** for peer
membership inside the replication pipeline. There is no priority
resolution between topology and `LatticeReplicationOptions.ReplicationPeers`
because no membership-sensitive consumer reads the options collection
directly - the question of "who wins on mismatch" collapses by
construction. `ReplicationPeers` is the canonical *configuration*
surface, not a runtime input: it is one of several possible feeds into
an `IReplicationTopology` implementation, and the default
options-backed topology is the only thing in the pipeline that
reads it.

##### Membership-sensitive consumers

These are the drivers whose behaviour depends on which peers
are currently reachable. Every one of them reads
`IReplicationTopology` and nothing else (other membership-sensitive
paths - the anti-entropy digest probe, the source-identity rebind, the
coordinated-restore saga's dispatcher and write fence, and the
cross-cluster backup sink-sharing probe - also read `CurrentPeers` live
on each pass):

| Consumer | Source it reads | Effect of a topology change |
|---|---|---|
| Driver activation background service (startup pass + runtime adds and removes) | `CurrentPeers` at startup + `Subscribe(...)` for the silo's lifetime | On `Added`, activates one per-peer shipper per replicated tree under a retry-with-backoff loop. On `Removed`, detaches each of the removed peer's shippers from the write-ahead log (see *Shipper-lifetime asymmetry*). No silo restart required. |
| Commit-time doorbell sink (doorbell fan-out per commit) | `CurrentPeers` (live read per commit) | The next commit rings doorbells for exactly the current snapshot. A peer added 1 ms ago is rung; a peer removed 1 ms ago is not. |
| Per-tree fall-off probe (per-cadence) | `CurrentPeers` (live read per cadence tick) | The next cadence tick probes exactly the current snapshot - a removed peer is dropped from the probe set; an added peer joins it on the next tick. |
| Per-peer shipper pump | The grain key it was activated under - neither topology nor options is re-read | The shipper is bound to a specific `(tree, peer)` for its activation lifetime. See *Shipper-lifetime asymmetry* below. |

##### What `LatticeReplicationOptions.ReplicationPeers` does

`ReplicationPeers` is read by exactly one component:
the options-backed default topology. That component is the
`TryAddSingleton`-registered default `IReplicationTopology` and it
turns each `IOptionsMonitor<LatticeReplicationOptions>.OnChange`
reload into a diff against the last-projected set (dropping empty,
whitespace-only, and duplicate peer ids), and emits one `PeerChanged`
event per net add and net remove. Hosts that take no action see the same behaviour the
options surface used to provide - peers configured in
`ReplicationPeers` are the peers the pipeline ships to - because the
default topology is a faithful projection of those options.

Non-membership replication knobs - `ShipDoorbellEnabled`,
`MaintenanceGcInterval`, `MaintenanceFallOffCheckInterval`,
`ShipBatchSize`, the backoff triple, etc. - continue to flow through
options independent of the topology seam. They are configuration, not
membership.

##### Custom topologies replace `ReplicationPeers` entirely

A host that pre-registers its own `IReplicationTopology` (typically a
service-registry-backed source) before `AddLatticeReplication` runs
displaces the default registration - `TryAddSingleton` is a no-op when
the key is already present. In this mode
`LatticeReplicationOptions.ReplicationPeers` is **inert for
membership**: nothing reads it, and leaving it unset (or stale) has no
effect on which peers the pipeline ships to. The custom topology is
the authority for every membership-sensitive consumer in the table
above. Hosts running in this mode usually leave `ReplicationPeers`
empty so that a future revert to the default registration produces an
empty topology rather than a surprising re-emergence of a stale list.

##### Lifecycle rules

1. **Add (peer appears in topology).** The activation service activates
   one shipper per replicated tree. The next commit rings the new
   shipper's doorbell. The next fall-off cadence tick probes the new
   peer.
2. **Remove (peer disappears from topology).** The doorbell loop stops
   ringing the removed peer on the next commit. The next fall-off
   cadence tick excludes it from the probe set. The activation service
   does *not* tear down the existing shipper - see the asymmetry rule
   below - but it does detach it from the write-ahead log, so the
   removed peer no longer holds the log's trims or the tree's saga
   decision purges.
3. **Re-add (peer disappears and reappears).** If the original shipper
   activation is still in memory, it is reused - there is no fresh
   activation, and the durable cursor on that activation continues
   from where the previous run left off. Activating it re-attaches it
   to the write-ahead log. It is still off the log, so it withholds saga
   records until the peer is re-seeded; that re-seed's drain also clears
   any pending bucket the peer staged before the removal whose decision
   the source purged meanwhile.
4. **Replace (host swaps the topology implementation).** Possible only
   at silo startup, before `AddLatticeReplication` registers the
   default. After registration the `TryAddSingleton` slot is occupied
   for the silo's lifetime.

##### Shipper-lifetime asymmetry (load-bearing)

A shipper grain bound at activation time to `(tree, peer)` **stays
bound for its activation lifetime**, even if the peer is removed from
the topology. Removal events deliberately do not tear down the shipper
so it can drain its remaining backlog, but nothing retires it
afterwards either. The backpressure path is:

- Doorbells and fall-off probes immediately stop firing for the
  removed peer (those consumers read live topology snapshots).
- The shipper grain does not read the topology, so it keeps pumping
  the tree's WAL - its existing backlog and every later local write -
  through the configured transport on its phase timer. If the transport
  can no longer reach the peer, the shipper's exponential backoff
  handles the failure the same way it handles any other transient
  outage.
- The shipper treats itself as always in progress, so its 90 s
  keepalive reminder is never unregistered: Orleans may collect an
  idle activation, but the reminder reactivates it and re-arms the
  phase timer, so a removed peer's shipper keeps running.

This is intentional: tearing down the shipper on `Removed` would lose
any in-flight batch and any cursor advance that had not yet been
persisted. The cost is that a peer removed from the topology is not
the same as a peer disconnected from the wire - reachability is the
transport's responsibility, not the topology's.

What `Removed` does change is what the shipper *holds*
([#4534](https://github.com/NSTA1/Orleans.Lattice/issues/4534)). A
running shipper holds the tree's write-ahead log at its durable read
position (the WAL GC trims below it), and a trim the `WalRetention`
ceiling forces past it records a saga decision-purge hold (*Forced
gaps* below). A peer that is gone for good must hold neither, so on
`Removed` the activation service calls
`IReplicationShipperGrain.DetachFromLogAsync` for each replicated
tree. The shipper durably marks itself detached and takes the peer off
the log (*Forced gaps* below) in the same write, withdraws from the
log's offset consumers, and releases its purge hold. The GC no longer
waits for it, so a trim can pass a prepare it has not read; it
therefore keeps shipping only plain writes and withholds every saga
record, and it keeps the re-seed marker for as long as it is detached.
Topology removal is the escape hatch for a peer that will never
re-seed. A detach that loses
a race with the peer being added back is skipped. `EnsureActiveAsync`
(the `Added` path and the startup pass) re-attaches the shipper. A
peer removed while no silo was running stays attached until it is
removed again with the cluster up.

##### Mismatch scenarios at a glance

| Scenario | Membership-sensitive behaviour | Notes |
|---|---|---|
| Default topology; peer added to `ReplicationPeers` | Activated + ringed + probed on next tick | Standard option-driven flow. |
| Default topology; peer removed from `ReplicationPeers` | Doorbell + probe stop on next tick; shipper keeps shipping | The shipper is not retired; its keepalive reminder keeps it running (see above). |
| Custom topology emits `Added`; `ReplicationPeers` unchanged | Activated + ringed + probed on next tick | `ReplicationPeers` is inert; the topology is authoritative. |
| Custom topology emits `Removed`; `ReplicationPeers` still lists the peer | Doorbell + probe stop on next tick; shipper keeps shipping | The options list does not resurrect the peer. |
| `ReplicationPeers` lists a peer the custom topology never publishes | No activation, no doorbell, no probe | The options list is read only by the default topology. |

#### Why not `IObservable<PeerChanged>`?

The seam is intentionally a callback (`IDisposable Subscribe(Action<PeerChanged>)`)
rather than `IObservable<PeerChanged>`. Full Rx semantics
(`OnCompleted`, `OnError`, schedulers, replay buffering) buy nothing for
membership diffs, and an `IObservable<T>`-shaped seam tempts callers to
pull in `System.Reactive` for what is otherwise a one-line lambda. An
`IObservable<PeerChanged>` adapter can be added later as a thin
extension method (`topology.AsObservable()`) without breaking the
primary surface; the reverse change would be a breaking one.

---

## Shipper grain

### Pump loop

Every phase tick (default 100 ms, `LatticeReplicationOptions.ShipPhaseTimerPeriod`) the shipper:

1. Returns immediately while a coordinated restore saga has paused
   shipping for the pair, or while the backoff deadline set by a
   previous failed attempt (or extended by a receiver `PauseForMs`
   hint) has not yet passed.
2. Drains a batch of up to the effective batch cap - `ShipBatchSize`,
   lowered by the adaptive controller and by any receiver
   `SuggestedBatchSize` hint - from the tree's WAL partitions, resuming
   each partition from its durable sequence cursor and merging the
   partitions by HLC (see [Partition resume cursor](#partition-resume-cursor)).
3. Filters each entry through the producer-side filter chain:
   - **Durability-only and maintenance entries:** skip entries with no
     `OriginClusterId` and tombstone-reap envelopes, neither of which has
     receiver-side meaning.
   - **Cycle-break (local origin only):** skip every entry whose
     `OriginClusterId` is not the local `ClusterId`. Entries this cluster
     applied on behalf of another origin are never re-shipped, which
     subsumes "a peer never receives its own writes back".
   - **`KeyFilter`:** skip entries whose key fails the configured
     predicate.
   - **`KeyPrefixes`:** skip entries whose key does not start with any
     configured prefix.

   Saga terminal marks (`TxCommit` / `TxAbort`) bypass `KeyFilter` and
   `KeyPrefixes`, so a saga whose prepared keys passed the filters always
   receives its terminal.
4. Coalesces redundant same-key versions and, when opted in, elides
   payloads the receiver already holds (see [Pre-ship coalescing](#pre-ship-coalescing)
   and [Content-hash dedup measurement](#content-hash-dedup-measurement)),
   then stamps a framing header over the pre-encoded WAL entry segments -
   no entry is re-encoded (see "Buffer reuse" below).
5. Calls `IReplicationTransport.SendAsync` with the framed batch in
   `ReplicationBatch.EncodedEnvelope`.
6. On an accepted ack, advances the cursor to the ack's
   `HighestAppliedHlc` - or to the last shipped entry's HLC when that
   frontier does not pass the current cursor (a fully deduplicated
   batch) - folds the per-partition resume cursors forward, and reports
   the durable cursor through `IWalCursorRegistry` once it has been
   persisted (see [Deferred cursor persistence](#deferred-cursor-persistence)).
   A rejected ack (`Accepted = false`) is treated as transient: the
   cursor stays put and the shipper backs off.

A successful round-trip resets `ConsecutiveFailures` to `0` and clears
the backoff budget.

**Continuous drain within a tick.** A tick does not stop after shipping a
single batch. Once a partition has been primed (step 2 onwards), steps 2-6
repeat in a loop, shipping batch after batch back-to-back until the backlog
is exhausted - a merge that yields fewer entries than the tick's effective
batch cap (`ShipBatchSize`, lowered by the adaptive controller or a receiver
hint; the final short tail) ends the tick, as does a receiver flow-control signal (a
positive `SuggestedBatchSize` hint or a just-applied `PauseForMs`) or a
transport / ack failure. This mirrors the pipelined path (`ShipMaxInFlight
> 1`), which already drained to exhaustion per tick, and prevents a backlog
larger than one batch from draining one batch per 100 ms tick - the defect
that made a large write burst trickle across the link over many seconds even
though the receiver had headroom. The single per-tick partition prime (below)
is amortised across every batch shipped in the loop; the cursor is advanced
and durably folded per shipped batch (deferred-persisted on the
`ShipCursorWriteInterval` cadence), so incremental progress survives a crash
mid-tick.

At the start of every tick the pump primes one shipping page per WAL
partition before the k-way HLC merge (the tree's WAL is sharded into
`ReplogPartitions` partitions). These priming reads are issued
**concurrently** and awaited once, not serialized one partition at a
time: each read is an independent WAL-shard grain call that writes only
its own partition's scratch slot, so an N-partition tree primes in a
single read latency. Serializing them would pay N cross-silo/durable WAL
round-trips every tick - even for partitions that turn out to be idle -
which is the dominant per-pump cost on a multi-partition tree whose WAL
shards are activated on a different silo, and is enough on its own to
collapse steady-state throughput under a write burst.

### Source-identity rebind

A shipper tails the **physical** WAL of its logical source tree, and the
persisted per-partition resume cursors are absolute offsets into *that*
physical log. When the logical tree's registry alias changes - a shadow-cutover
restore or its revert, a resize or its undo, a schema remediation, or an
operator alias change - the shipper must reset those cursors and re-ship from
the new physical log start, or it would keep tailing the retired identity's
WAL. An online reshard changes the tree's shard map, not its alias, so it
does not trigger a rebind.
A resize of a replicated tree cannot be undone after its alias swap
([#4518](https://github.com/NSTA1/Orleans.Lattice/issues/4518)): writes the
resized copy took may already have shipped, and last-writer-wins shipping
never retracts them, so the rebind back to the old copy would leave them on
the peer while this cluster discards them. An undo before the swap rebinds
nothing, because the shipper never left the old copy. To return a replicated tree to its old
shape, resize it again, back to its previous sizing: unlike an undo, a
resize discards no write, so the clusters stay in agreement.

Detection is **event-driven, not polled**. The alias swap is performed by an
identifiable producer that writes the repoint into the tree registry; the
registry fires the core `ITreeAliasObserver` hook from inside its single
alias-mutation choke point (`SetAliasAsync` / `RemoveAliasAsync`), after the
new alias is durably persisted and **only** when the effective physical id
actually changed. The replication package registers an observer that fans the
`TreeAliasChange` to the affected per-`(tree, peer)` shipper grains via
`IReplicationShipperGrain.NotifySourceIdentityChangedAsync`, which rebinds
immediately - the new physical id travels in the notification itself, so the
rebind reads the registry **zero** times. Because the observer runs on the
source silo as an ordinary grain call, it reaches the shipper even while the
inter-site delivery edge is partitioned, so the rebind is applied the moment
the swap commits rather than after the edge heals.

A coarse backstop resolve
(`LatticeReplicationOptions.ShipSourceIdentityBackstopInterval`, default 30 s)
is the only path that still reads the registry, and it runs solely as a
safety net: it re-resolves when the binding has never been established for the
activation, or when the interval has elapsed since the last resolve or rebind.
A lost notification (observer fault, or a shipper deactivated across the swap)
therefore degrades to poll-driven detection bounded by that interval rather
than a permanent mis-binding - it never reintroduces a per-tick registry read
on an idle tree.

The backstop is not enough after a coordinated restore. The restore saga
pauses shipping, cuts the alias over to the restored copy, and resumes
shipping at global completion. A paused shipper does not re-resolve, and the
backstop interval counts from the last resolve, which may be moments before
the pause. If the push was lost, a resumed shipper would keep draining the
retired log and carry a pre-restore write onto the peer's restored copy,
re-advancing the restored cut for good (issue #4490). So `ResumeShippingAsync`
forces the first tick after the resume to resolve the source identity, and to
rebind with a cursor reset if it moved, before that tick reads or sends
anything. A shipper reactivated during the pause resolves first anyway.

A rebind also decides what happens to the saga terminals the shipper holds
from the retired log (see [Saga terminal hold](#saga-terminal-hold)):

- **A rebind made while shipping was paused by a saga**, or found by the
  resolve that follows the resume, is a coordinated restore. Such a rebind
  **drops** the holds. Both clusters were reset to the cut: a saga before the
  cut is settled by each side's restored copy, a terminal after it must not
  cross the cut, and every bucket the peer staged from the retired log went
  with the copy its own cutover replaced.
- **Any other rebind** (a resize, its undo, a schema remediation, an operator
  alias change) **carries** each hold forward by transaction. A carried hold
  waits for the new log's prepare tally or tail barrier, so it cannot overtake
  a prepare the new copy mirrored. It also cannot strand the buckets the peer
  already staged from the retired log.

Two residuals apply to carried holds:

- A prepare written to the retired copy before an online resize began
  forwarding is not in the new copy's log (#4455). A hold carried past it
  still releases on the new log's tail barrier.
- A resize undo discards the writes the new copy accepted after the swap. A
  terminal held from that copy for a saga bound to it is released on the old
  copy's log, which the peer then applies (#4474).

This event-driven inversion closes two problems the former per-tick registry
resolve had at once: the idle-only registry read load (an otherwise-quiet link
performed a steady stream of `_lattice_trees` reads purely to notice a swap
that rarely happens), and a correctness sharp edge - the detection window in
which a still-live shipper kept tailing the retired physical WAL and could
ship keys confined to the retired identity (for example the keys a
restore-to-drop-keys cutover meant to discard, which plain last-writer-wins
cross-cluster shipping never retracts). Pushing the rebind synchronously with
the swap shrinks that window to the notification latency.

The other per-tick metadata resolutions are memoised on the same principle -
recompute only when an input changed, not every tick. Peer wire-version
negotiation and shared-dictionary negotiation both key off the receiver's
advertised capability on `ReplicationAck`, so their results are cached and
recomputed only when a new ack changes the peer's advertised capability (or
the shipper's options instance or effective dictionary id changes), not on
every pump tick. Together with the source-identity rebind this removes
steady-state idle registry/metadata resolutions, so an idle shipper's
only per-tick work is the WAL-tail poll, cursor-flush, and liveness probe.

### Doorbell

The shipper grain is the log-first replication producer: it tails the
partitioned per-tree write-ahead log (the leaf commit-log writer is the
sole WAL appender) from a durable per-partition cursor and is the only
ship driver. The commit-time doorbell sink does not append to the
WAL and does not ship; it maintains no producer-side vector clock state
and is reduced to a low-latency tree-id nudge that rings shipper
doorbells so an idle or deactivated shipper is woken to drain the fresh
append.

In addition to the phase timer, the shipper exposes
`OnDoorbellAsync(CancellationToken)`. The producer-side doorbell sink
rings the doorbell after every commit for the affected
`(tree, peer)` activations. The doorbell is a **cheap, edge-triggered
wake**: it does not run the drain+ship pump inline. Its sole effect is to
(re)activate the shipper if it had been deactivated - and the shipper's
phase timer, which is armed on every activation (`OnActivateCoreAsync`)
and re-armed by the keepalive reminder, is the single authority that
drains and ships. Running the pump inline on the doorbell turn would hold
the non-reentrant activation for a full cross-cluster ship round-trip,
head-of-line-block the phase timer and every queued doorbell, and - under
receiver back-pressure - time out at the sink and be dropped, starving the
very wake it was meant to deliver. Because the timer is the drain driver,
steady-state ship latency stays sub-second regardless of the doorbell, and
the doorbell is best-effort: a missed call only delays the next ship by one
timer tick (default 100 ms).

#### Writer-side coalescing

A doorbell is an idempotent, edge-triggered "there is work" signal, so the
sink does not dispatch one grain call per commit. Because the shipper
activation is non-reentrant, an unbounded per-commit fan-out under a write
burst would grow its turn queue without bound: fresh doorbells would be
dropped as expired before they run, and the keepalive reminder that drives
shipping would be starved behind the backlog - so shipping stalls exactly
when the peer has the most to ship.

The commit-time doorbell sink therefore coalesces at the source. It keeps a per-`(tree,
peer)` coalescer and collapses a burst of ring requests into **at most one
in-flight ring plus one pending follow-up**:

- The first request transitions the coalescer from idle to in-flight and
  starts a ring loop that calls `OnDoorbellAsync`.
- A request that arrives while a ring is in flight sets a single pending
  flag instead of dispatching its own grain call, and is counted as
  coalesced.
- When the in-flight ring completes, the loop fires exactly one trailing
  ring if the pending flag was set (then clears it), and otherwise settles
  to idle.

The trailing ring guarantees the last write in a burst still wakes the
shipper (no missed wake), while the doorbell message rate the shipper sees
is bounded to a small constant regardless of write throughput. The base
phase-timer and keepalive-reminder safety net still drive shipping
independently of doorbells, so a coalesced (elided) ring never delays
delivery beyond one timer tick. The coalescing ratio is observable via the
`orleans.lattice.replication.doorbell.rung` (rings dispatched) and
`orleans.lattice.replication.doorbell.coalesced` (rings elided) counters,
both tagged by tree and peer.

### Backoff schedule

Transient failures (drain throw, transport throw, ack rejected) feed an
exponential backoff sized by:

- `ShipBackoffInitial` (default 100 ms) - base delay on the first failure.
- `ShipBackoffMax` (default 30 s) - cap on the doubled delay regardless of
  consecutive failure count. Jitter is applied after the cap, so a jittered
  delay can exceed it by up to the jitter fraction (36 s at the defaults).
- `ShipBackoffJitter` (default 0.2) - symmetric `[1 - jitter, 1 + jitter]`
  multiplier applied to the computed delay so a fleet of shippers sharing
  a transient outage does not resynchronise on retry.

`Random.Shared` is the jitter source - sufficient for distribution
purposes, not cryptographic.

### Unencodable batches

When building the outbound framing header throws an `ArgumentException`
or `InvalidOperationException` - a schema-shaped failure the batch can
never recover from in its current form - the batch cannot reach the peer
through the log. Parking it on this cluster's dead-letter queue would not
help: a replay applies a parked entry here, where it is a no-op, and never
sends it to the peer. So the shipper treats the batch like a
[forced gap](#forced-gap-a-peer-taken-off-the-log) (#4614):

1. It takes the peer off the log: it takes the replay hold, records the
   tree's current export epoch as the re-seed marker, withholds saga
   records, and asks the peer to re-seed on every push and liveness probe.
   On the first failure it also drops its terminal holds, as the forced gap
   does.
2. It quarantines the batch: per partition, the hull from that partition's
   cursor through the batch's last read sequence, merged with any earlier
   quarantine. The marker and the hull are written durably **before** the
   cursor moves.
3. It advances the cursor past the batch, so plain writes keep shipping.

Every further failure raises the marker to the current export epoch: an
export already opened past the old epoch predates the new batch and must
not clear the marker. The peer re-bootstraps from an export after the
marker, which carries the quarantined writes (committed rows, prepared rows
of sagas still in flight, and decision rows), encoded by the snapshot path
rather than the batch framing. After the echo, the rewind consumes every
quarantined position without shipping it - every record in the hull was
appended before the marker, so the export already carried it - and the
quarantine clears once no re-seed is outstanding and every partition's
cursor has passed it. A rebind to a new source log clears it too, because
its sequences belong to the retired log. The quarantine is one sequence
range per partition, so it needs no capacity bound and never stalls the
link.

The shipper logs a warning naming the batch size and the epoch, and the
link reports `Stalled` (with `ReseedRequiredSeconds` set) until the peer
has re-seeded. Nothing is written to the dead-letter queue.

A shipper whose state still carries a poison list from an earlier build
(#4494, which parked the batch and poisoned its sagas) takes the peer off
the log on activation and forgets the list: the re-seed delivers each of
those sagas whole.

### Buffer reuse

The shipper maintains activation-scoped buffers reused across pump
ticks:

- `_drainBuffer` (`List<WalRecord>`) - cleared in place before every
  drained batch (a tick can drain several - see *Continuous drain within
  a tick* above). The list drives the shipper's own filtering,
  coalescing, and cursor bookkeeping and is never handed to the
  transport, which receives only the framing header and the pre-encoded
  segments below, so reuse is safe (no aliasing past the call).
- `_drainEncodedSegments` (`List<ArraySegment<byte>>`) - cleared in
  lockstep with `_drainBuffer`. Holds the pre-encoded payload bytes
  the shard grain returned from `ReadShippingAsync`; the segments are
  owned by the WAL grain's page DTOs and are safe to wrap because
  Orleans serialises grain turns and `SendAsync` awaits inline.

Net effect: zero per-tick heap allocation on the steady-state path
modulo Orleans-internal serializer wrappers, and zero producer-side
re-encode of WAL bytes - the framing header is the only thing the
shipper writes per tick.

### Partition resume cursor

The ship loop never uses `IChangeFeed`: it reads each WAL
partition directly via the partition grain's sequence-ranged read (from a sequence lower bound)
starting at a durable per-partition resume cursor stored on
the shipper's persisted partition-cursor state. Per pump tick the shipper
fetches up to `ShipPartitionPageSize` (default 256) entries from each
partition and merges them by HLC ascending via a heap-free O(P) linear
scan-for-min over partition heads. The merge collapses to O(1) for the
canonical single-partition case.

Sequence-based (not HLC-based) resume converts every pump tick from an
O(N) rescan-from-zero walk over the WAL into an O(page) read past the
last successfully shipped offset. `IChangeFeed` remains a public seam
for tests and host-built consumers that have no notion of partition
routing; neither the drivers nor the bootstrap path consume it.

The durable per-partition sequence cursor is the exactly-once resume
token: the merge presents each WAL sequence once, and a partition
cursor moves past a sequence only on an accepted ack. The shipper
therefore does **not** drop an entry merely because its HLC is at or
below the scalar HLC cursor. Source HLCs are stamped per leaf and a
partition interleaves many leaves, so a genuinely new write routinely
sits below the running maximum, and dropping it would silently strand
it. A scalar-HLC drop survives only for the one-time legacy migration
tick - state persisted by a build that predates partition cursors, with
a non-zero HLC cursor but an empty partition-cursor map - so an
upgraded shipper does not re-ship its whole already-shipped prefix; even
then zero-HLC range deletes and prepared atomic-batch entries are never
dropped. The receiver's shadow-forward identity cache and per-key
last-writer-wins guard make any re-shipped duplicate a no-op.

### Saga terminal hold

A saga writes its prepares to the partitions their keys hash to, decides,
and then writes one terminal (`TxCommit` / `TxAbort`) per touched shard to
the partition its shard index hashes to. The receiver commits a saga once
every shard's terminal has arrived. A receiver leaf that applies a terminal
with no pending bucket for the saga records the terminal, and then refuses
any prepare that arrives after it. So a terminal that reaches the peer ahead
of one of its prepares leaves the saga split on the receiver for good: one
key post-saga, the other never written (issue #4480).

The HLC merge cannot prevent that. A partition is not HLC-ordered in append
order, because leaf clocks are independent and skew across silos. So an
unrelated entry with a higher HLC can sit ahead of a prepare and sort after
the terminal. A partition read empty early in a tick is not read again that
tick. And with `ShipMaxInFlight` above one, a failed batch is re-shipped
after a later batch has applied.

So whenever the shipper reads more than one partition, or pipelines, it
pulls every terminal it reaches out of the merge into an in-memory hold.
The terminal's partition keeps draining behind it. A held terminal is
shipped, at the head of a later batch, only once the peer has acknowledged
every prepare of its saga. The shipper knows that in one of two ways:

- **A complete tally.** The shipper counts the prepared records it reads
  for each transaction: every `AtomicBatchIndex` up to the transaction's
  `AtomicBatchSize`, and the highest sequence it read in each partition.
  Once every index has been read, the terminal ships when each partition's
  acknowledged frontier has passed the saga's last prepare in it. The tally
  is saga-wide: it never consults the terminal's shard count, so the split
  coordinator's unstamped (count 0) sweep terminal is held exactly like any
  other.
- **A tail barrier**, when no complete tally exists. That happens when the
  prepares were acknowledged before the shipper restarted, when they are
  unsized legacy prepares, or when the tally was evicted (at most 1,024
  tallies are kept). The shipper reads each partition's tail after it has
  read the terminal, and ships the terminal once the acknowledged frontier
  reaches that tail in every partition. This is sound because a prepare's
  append completes before the saga decides, and the decision precedes every
  terminal append.

Release is gated on acknowledgements, not on reads, so a failed batch that
carries a prepare cannot be overtaken by its terminal. A batch carrying a
released terminal that is not accepted, for example one the receiver
defers, re-arms the hold for the next tick.

While a terminal is held, its partition's durable cursor stops at the
terminal's sequence, and the HLC cursor reported to the WAL GC stays below
the terminal's clock. So a restart re-reads the terminal and the GC cannot
trim it. The in-memory resume point is not capped, so the entries after a
held terminal are not re-shipped on every tick. After a restart, a held
terminal whose prepares were acknowledged earlier releases on the tail
barrier.

A held terminal is never stranded. A batch that could not be encoded takes
the peer off the log and drops every hold (see
[Unencodable batches](#unencodable-batches)), so no terminal of a saga that
lost a prepare in it is released before the peer has re-seeded:

- A prepare trimmed before it shipped (a peer that fell off the log) is
  passed by the acknowledged frontier like any other sequence.
- The tail barrier needs only acknowledgements of entries that exist.
- A rebind to a new source log (an alias swap) stops reading the retired
  log. After a coordinated restore the holds are dropped, and otherwise they
  are carried forward to the new log (see
  [Source-identity rebind](#source-identity-rebind)).

One case releases without the guarantee:

- The hold assumes every key of the saga is replicated. A `KeyFilter` or
  `KeyPrefixes` that drops some of a saga's prepares yields an
  all-or-nothing view over the replicated subset only. That is the filter's
  semantics.

With one partition and a window of one, the stream already delivers each
partition in append order and applies each batch before the next ships,
so no hold is taken.

Wire-compat is additive: the new `[Id(2)]` partition-cursor slot on
the shipper's persisted state decodes as the empty dictionary for legacy
persisted state, which the cold-start path treats identically to a
fresh activation. Setting `ReplogPartitions=1` reduces the merge to a
single read per tick; the shipping default is `8`, and the value must
equal `LatticeOptions.WalPartitions` so the shipper reads every
partition the commit-log writer fans across (see
[`ReplogPartitions`](configuration.md#replogpartitions)).

### Forced gap: a peer taken off the log

A `WalRetention` ceiling trims the write-ahead log past a lagging consumer by design, so it can remove records the shipper has not yet delivered to its peer. Skipping the trimmed prefix is harmless for plain writes, but not for a saga: if the trimmed record was one of a saga's prepares and its terminal is still retained, the terminal reaches the peer without it, the receiver commits the saga and drains its other keys, and the lost key is missing - a torn saga ([#4534](https://github.com/NSTA1/Orleans.Lattice/issues/4534)). A shipper cannot even name the transactions it lost.

The shipper therefore treats a shipping read whose first entry is above the requested sequence as a **forced gap** when the source WAL's trim watermark has reached the requested sequence. Offsets are not dense: a flush abandoned at its deadline that never lands leaves a hole, and a hole directly above a trim point leaves the lowest stored offset above the requested sequence although nothing the peer needs was trimmed. The trim watermark tells the two apart (issue #4621; see [the WAL](../lattice/wal.md#abandoned-flushes-holes-and-the-trim-watermark)). When the source cannot report a trusted watermark - a provider that keeps none, or a silo in the cluster that predates it - every jump is treated as a forced gap, which re-seeds a peer needlessly at worst and never skips a trim. On the first one it durably records the tree's current snapshot export epoch in `ReplicationShipperState.ReseedRequiredEpoch`, before it consumes past the gap, drops every terminal it was holding, and from then on:

- **withholds every saga record** - prepares, `TxCommit` and `TxAbort` - from that peer, while plain writes keep shipping. Nothing the peer already holds can tear: a staged bucket with no terminal stays invisible. The withheld records stay in the log for the rewind: the shipper records each partition's durable cursor (or its lowest retained entry, past a trim) in `ReplicationShipperState.ReseedRetainFrom`, and its published read positions never pass it, so the WAL GC keeps every record it withholds. If the retention ceiling trims past that point anyway, the shipper does not rewind on the next echo, because the echoed export may predate a withheld saga's decision: it takes the peer off the log again, at the current export epoch ([#4533](https://github.com/NSTA1/Orleans.Lattice/issues/4533));
- **asks the peer to re-seed** on every push and liveness probe. The gRPC transport sends the recorded epoch in the `x-lattice-replication-reseed-after` call header. The receiver, having verified the caller's origin, starts a full bootstrap from that sender when it has not completed one from an export with a greater epoch and none is running (governed by `AutoBootstrapOnFallOffLog`), and echoes the epoch of its last completed one in `ReplicationAck.BootstrapEpoch`.

Every full snapshot export takes a fresh export epoch before its registry snapshot, so an echoed epoch greater than the recorded one proves the peer was re-seeded from an export taken after the gap. Any ack can carry the echo: a push from a serial or a pipelined (`ShipMaxInFlight` > 1) window, or a liveness probe on a quiet link. At the end of the pump tick, after every batch of the tick has folded its cursors, the shipper clears the marker, rewinds every partition to its lowest retained entry, and resumes ([#4606](https://github.com/NSTA1/Orleans.Lattice/issues/4606)). Rewinding before a batch's fold would let that fold raise the partition past the retained saga records the rewind exists to re-ship. The export carried every stored saga's decision and committed values, and the re-shipped saga records settle against them; the [replay filter](#replay-filter-a-non-contiguous-stream-over-purged-sagas) withholds any saga whose decision the origin has purged. A range-scoped re-replay never advances the echoed epoch.

While a re-seed is outstanding the peer's outbound status row reports how long it has waited, and `ILatticeReplicationStatus` classifies the link as `Stalled`, whatever its backlog and contact counters say.

A custom `IReplicationTransport` does not carry the re-seed request, so a peer behind one stays withheld until it is bootstrapped by other means. The shipper logs a warning when it takes a peer off the log.

#### Decision-purge holds

The re-seed can only settle a saga whose decision the origin's transaction registry still stores, but a trimmed saga's decision becomes purgeable once its records are gone from the log. So a trim never passes a shipper silently: each WAL GC pass reads every registered shipper's durable read position, and before it trims an offset at or past one (only the `WalRetention` ceiling admits such a trim) it durably records a hold for that shipper in the tree's `IWalPurgeHoldGrain`, keyed by the physical tree and the shipper's grain id. A failed hold write skips the trim, and so does a pass that could not read the set of registered shippers; a shipper whose own position could not be read counts as position 0, so a forced trim holds it. While any hold is outstanding the registry purges no decision on the tree.

The shipper releases its own hold, conditionally in the hold grain so a hold a concurrent trim widened is never released by an older read:

- when the peer acknowledges the re-seed, since every partition then re-ships from its lowest retained entry, past whatever was trimmed;
- on its phase timer (at most every 30 s), when no re-seed is outstanding and its durable read position is past every trimmed offset, as when the trimmed records were already in flight and were acknowledged after the trim;
- when its peer is removed from the topology (*Shipper-lifetime asymmetry* above).

An old silo never records a hold, and a host that has none behaves exactly as before.

### Replay filter: a non-contiguous stream over purged sagas

A host that ran without replication, or before the decision-purge guard ([#4508](https://github.com/NSTA1/Orleans.Lattice/issues/4508)) existed, purged saga decisions on retention alone while the write-ahead log kept their records. So does a registry activation on a silo that predates the guard, during a rolling upgrade. Re-shipping such a saga to a peer strands it there: nothing can settle it ([#4533](https://github.com/NSTA1/Orleans.Lattice/issues/4533)). A stream that delivers the log in order delivers every saga whole, prepares before terminals, and is never filtered. Only a **replay** is: after a re-seed rewinds the shipper to the lowest retained entry, or after a source-identity rebind restarts it on a new log.

A replay records a horizon, `ReplicationShipperState.ReplayFilterHorizon`: every partition's next sequence when it began. While it is set, the shipper decides once per saga, on the first record of it that it reads, whether the origin proves the saga forgotten and its decision purged:

1. It reads the saga's **participant row**. A saga registers its participants durably before it appends any prepare (the shard root awaits the registration and fails the write if it fails), and only `ForgetAsync`, after the decision, removes them. A row means the saga is live, or decided and not yet forgotten: it ships.
2. With no row, it reads the **stored decision**. A decision means the saga was forgotten but its decision is still stored, which the peer's re-seed carried: it ships. No decision means it was purged: the decision is recorded before the forget and purged after it, so "no participants, then no decision" proves the purge.

The verdict is cached for the replay and applies to every record of the saga, terminals included, so a saga is shipped whole or withheld whole. A failed read fails the tick, so the partition holds and retries rather than guessing. Every record of a purged saga was appended before its purge, so a purged verdict raises the horizon to the log's current next sequences, and the filter clears only once every partition's cursor has passed it. The cache lives only while the filter is set: it holds at most one entry per saga with a record between the cursors and the horizon, which the retained log bounds, and it is cleared when the filter starts, when it clears, and with the activation.

A saga in flight when the peer is taken off the log can be decided, forgotten and, its records having been trimmed, purged while the replay runs; read as purged, its terminals would be withheld from a peer that the re-seed's export re-staged it on. So the shipper takes a **replay hold** on the tree's decision purges (`IWalPurgeHoldGrain`, the registry purges nothing while any hold is outstanding) before it reads the export epoch for the re-seed marker, or before a rebind replay begins, and releases it once the filter has cleared with no re-seed outstanding. A purged verdict is then only ever a saga purged before the hold, which no export can carry either. The hold is only honoured by silos that host `IWalPurgeHoldGrain`, so during a rolling upgrade the shipper reads the cluster manifest and, while any active silo lacks it, does not rewind for a re-seed: it stays off the log, withholding saga records and shipping plain writes. If such a silo joins while a replay runs, it takes the peer off the log again.

A withheld saga must still converge on the peer, so the shipper withholds one only when the replay carries its effects. A re-seed's snapshot carries a committed saga's values as committed rows (an aborted saga has none), and the receiver clears the source's stale pending buckets during that drain (see [Snapshot bootstrap](snapshot-bootstrap.md#re-seed-stale-pending-clear)). Otherwise the verdict takes the peer off the log instead, and the re-seed that follows restarts the replay with a carrier:

- a replay that a **rebind** started has no snapshot;
- a replay that an **earlier activation** started may have shipped part of the saga under a verdict that died with that activation.

A rebind that meets no purged saga, which is the normal case, costs nothing beyond one registry read per saga in the replayed region. Every saga record in every released build's write-ahead log was appended after a durable participant registration, because both shipped together in lattice 4.0.0, so the predicate holds across rolling upgrades.

### Deferred cursor persistence

Cursor advances are amortised across `ShipCursorWriteInterval`
(default 16) successful acks rather than persisted per-ack. The
`_pendingCursorWrites` counter increments on every advance and the
durable `WriteStateAsync` fires whenever **either** of two thresholds
is reached - whichever comes first:

- **Batch count** - the counter reaches `ShipCursorWriteInterval`.
- **Elapsed time** - more than `ShipCursorWriteMaxDelay` (default 2 s)
  of wall-clock time has passed since the first un-flushed advance.

The time dimension bounds how stale the durable cursor can become on a
low-throughput or bursty stream that ships fewer than
`ShipCursorWriteInterval` batches and then quiesces: a pure batch-count
rule would leave those last few advances un-flushed indefinitely while
the stream is idle, widening the crash-replay window and pinning the
WAL GC trim frontier at the last reported cursor. The elapsed check is
evaluated both when a new advance is booked and on idle pump ticks (the
empty-drain path), so a stream that goes completely silent still
checkpoints within the time bound. (A graceful deactivation also
flushes - see below.)

Re-shipping is safe because the receiver absorbs repeats without relying
on its per-origin high-water mark or any snapshot floor, neither of
which drops point writes: a recently applied `(origin, HLC, key, op)`
identity is suppressed by the shadow-forward identity cache, and
anything else re-applies idempotently under per-key last-writer-wins. A
silo crash inside the deferred-persist window
therefore costs at most `ShipCursorWriteInterval x ShipBatchSize`
entries of wasteful re-shipping and no data is lost. Lowering
`ShipCursorWriteMaxDelay` only ever makes the durable cursor fresher; it
can never widen that bound.

Setting `ShipCursorWriteInterval=1` recovers the persist-every-ack
behaviour for hosts that prefer the smaller replay window over the
amortised storage cost. Setting `ShipCursorWriteMaxDelay` to
`Timeout.InfiniteTimeSpan` disables the time dimension and coalesces
purely by batch count.

### Persist-then-report ordering (load-bearing)

`IWalCursorRegistry.ReportCursorAsync` is called
strictly **after** `WriteStateAsync` completes. The WAL GC consumes
the reported cursor to compute the trim frontier, so reporting before
persistence would risk trimming entries the shipper cannot recover
after a crash. This ordering is preserved across the deferred-persist
change: only flushes that durably advance the HLC cursor produce a new
registry report.

A registry-side failure during the report does not unwind the durable
cursor advance and does not retry - the shipper updates
`_lastReportedCursor` to the durable value regardless. The
suppression check inside `FlushCursorAsync` then skips the next
report attempt until the durable cursor moves further forward, at
which point the next flush re-reports the new frontier through the
recovered registry. Operators monitoring the WAL GC trim frontier
should expect this lag to clear on the next post-outage ack rather
than immediately when the registry recovers.

### Per-partition read positions hold the WAL (issue #4579)

The HLC cursor alone does not protect what the shipper has not read. It is
the HLC of the last entry shipped in merge order, and a WAL partition is not
HLC-ordered in offset: a silo whose clock trails, or a merge that keeps its
source stamp, can put an entry the shipper
has not read at an HLC at or below the cursor it has already reported. Once
the owning leaf checkpoints past such an entry, nothing else holds it, so a
GC pass could trim it unshipped.

The shipper is therefore also an offset-reading WAL consumer:

- Before its first read of a physical log it registers with that log's
  durable consumer set, so a GC pass on any silo, and after a restart, asks
  it where it is.
- It answers with its durable `PartitionCursors`, which a held saga terminal
  already caps. A position is raised only after the write that made it
  durable. It is lowered before the next read when the in-memory cursors drop
  (an alias rebind, a rewind).
- A registered shipper that has acknowledged nothing answers 0 for every
  partition, so it holds the whole log instead of racing the GC.
- On an alias rebind it registers with the new physical log first and only
  then withdraws from the old one. It answers nothing for a log it no longer
  reads.

The GC refuses every entry at or above the lowest position any registered
consumer reports for that partition, however the HLC clauses read. Only the
`WalRetention` TTL ceiling trims past it, and the shipper then sees the gap
on its next read. A stalled or removed peer's shipper therefore holds the WAL
at its last durable position until the TTL ceiling applies. A peer whose
shipper has never activated is not yet a consumer; it starts from a snapshot
bootstrap.

### Graceful deactivation

`OnDeactivateCoreAsync` flushes any pending cursor advance before the
activation tears down so a clean shutdown (e.g. operator silo drain)
eliminates the deferred-persist replay window entirely. A storage
failure during the flush is logged and swallowed - deactivation must
not block - and the next activation recovers by re-shipping at most
`ShipCursorWriteInterval x ShipBatchSize` entries, which the receiver
absorbs idempotently.

### Content-hash dedup measurement

`LatticeReplicationOptions.ContentHashDedupEnabled` (default `true`)
measures the **payload re-send rate**: how often the shipper
ships a `Set` whose value bytes are byte-identical to the value most
recently shipped for the same key. This is the idempotent-re-write rate
that decides whether a sender-manifest / receiver-pull-missing dedup
round trip would pay for its extra latency. The measurement is on out of
the box so the re-send-rate signal is available without a config change;
a host that wants the historical zero-overhead path sets
`ContentHashDedupEnabled = false`.

When enabled, the shipper keeps a per-activation, per-key bounded LRU
of the last-shipped content hash - FNV-1a 64-bit over the op, key,
range end-key, and value bytes - sized by
`ContentHashDedupCacheSize` (default `4096`, validated `>= 64`). As
each redundant entry drains onto the wire the shipper increments the
two observability counters
`orleans.lattice.replication.ship.redundant_payloads` and
`orleans.lattice.replication.ship.redundant_payload_bytes` (tagged
`tree` + `peer`; see [Observability](observability.md#content-hash-payload-re-send-rate-shipredundant_payloads--shipredundant_payload_bytes)).
When the flag is set to `false` the shipper does no extra work and never
touches the cache or the counters.

The measurement is **observability-only**: it never elides, reorders,
or alters the bytes the sender ships, so the wire output is byte-for-byte
identical whether or not the flag is set. Actually skipping a
byte-identical re-set that carries a newer HLC would be unsafe without
receiver consent: the receiver tracks a per-origin high-water mark by
HLC and the sender advances its durable cursor to
`ack.HighestAppliedHlc`, so dropping the newer-HLC entry would strand
the receiver's stored timestamp behind the sender's cursor and change
LWW/HLC convergence against concurrent foreign-origin writes. Eliding
safely requires the receiver to report which writes it already holds -
exactly, by content hash, origin and source HLC, with its leaf still at
that version or newer (#4585), never by bytes alone.
That is the separate opt-in `ContentHashDedupElisionEnabled` (default
`false`, and it requires this master switch): before each batch ships
the shipper runs a content-manifest exchange over the digest-probe
transport (`IReplicationDigestProbeTransport.ExchangeContentManifestAsync`),
and the exchange composes with the bounded-pipelining window - see
[Content-manifest payload elision](observability.md#content-manifest-payload-elision).
The measurement itself needs no wire-format, serialization, `[Id]`, or
`[Alias]` change. Because the counters fire as entries are framed onto
the wire, a batch re-shipped after a transient transport failure counts
its entries again - correct, since a re-ship is itself a redundant wire
payload.

### Pre-ship coalescing

`LatticeReplicationOptions.PreShipCoalescingEnabled` (default `true`)
collapses redundant per-key versions out of a freshly-drained
batch **before** they cross the cross-cluster link. A hot key rewritten
several times within a single ship window otherwise ships every
intermediate version a last-writer-wins receiver would overwrite anyway;
coalescing drops those intermediate versions from the wire. This runs in
a default build; a host that wants the historical verbatim drain/ship
path sets `PreShipCoalescingEnabled = false`. This is
distinct from the content-hash dedup measurement above, which never
alters the bytes shipped - coalescing actually elides entries.

The pass handles both last-writer-wins and recognised CRDT trees, by
different mechanics. For a tree whose declared `LatticeMergeMode` is
`LwwRegister` the receiver applies each entry by last-writer-wins
ordered by HLC (an exact HLC tie falls to the replica-invariant
tombstone, expiry, and value-byte fields before the observer-relative
origin id), so within
one drained batch only the highest-HLC version per key survives
convergence and the earlier ones are invisible after apply. Because the
shipper only ever drains its own cluster's authored writes (the
cycle-break filters to `options.ClusterId`), every coalescing candidate
shares one origin and the drain buffer is already HLC-ascending, so the
last occurrence of a key is the highest-HLC one - the version the receiver
converges to. The LWW path therefore keeps only that last version and
drops the earlier ones outright.

For a recognised CRDT tree, the receiver applies each entry by folding its
per-entry typed delta into the loaded state, so dropping an intermediate
version would lose its contribution rather than merely hide it. The CRDT
path instead **folds** a same-key run's typed deltas into a single combined
delta - a join over the primitive's own semilattice using the registered
combine semantics for that tree shape - re-encodes it onto the kept
(highest-HLC) entry, and elides the earlier
same-key entries. Each combine is commutative, associative, and
idempotent, so the combined delta's receiver-side apply effect is
identical to applying the source deltas in sequence: a coalesced CRDT run
converges to the **identical** state as shipping every delta individually.
The kept entry inherits the last contributing entry's HLC and causal
metadata.

An `OrMap` tree whose concrete `(TKey, TValue)` shape is **unregistered**
(no shape descriptor resolves for the tree) and any CRDT entry carrying no
typed delta (`WalRecord.Delta == null`, an opaque or legacy payload) fall
back to shipping individually - loss-free; only the bandwidth saving is
forgone. A registered OR-Map tree folds through the value-shape descriptor
exactly like the closed shapes, because the descriptor binds the concrete
value CRDT and can recurse into its own join.

Only plain point `Set` / `Delete` writes are eligible. Range deletes,
saga terminal marks, prepared atomic-batch (saga) entries, tombstone-reap
envelopes, and entries carrying `HybridLogicalClock.Zero` are never
coalesced and never participate, so atomic-batch boundaries, causal
dependencies, per-origin FIFO, and the no-cross-origin-reorder invariant
all hold unchanged. The coalescing pass runs after the merge loop has
already folded every drained entry's per-partition sequence into the
resume bookkeeping, so the durable cursor still advances past every
elided entry and nothing is re-shipped or stranded.

As the shipper compacts a batch it increments these counters - tagged
`tree` + `peer`; see
[Observability](observability.md#pre-ship-coalescing-coalesceentries_elided--coalescebytes_elided--coalescedeltas_merged):

- `orleans.lattice.replication.coalesce.entries_elided` - one per dropped
  entry (both paths).
- `orleans.lattice.replication.coalesce.bytes_elided` - the sum of the
  pre-encoded wire-segment lengths of the dropped entries (both paths).
- `orleans.lattice.replication.coalesce.deltas_merged` - on the CRDT path
  only, one per source delta folded into a combined delta.

The coalesced output converges identically on an unmodified receiver - a
strict **subset** of the verbatim batch on LWW trees, an
**effect-equivalent merge** on CRDT trees. The change is purely additive:
no new frame type, no wire-format, serialization, `[Id]`, or `[Alias]`
change, and no wire-version bump (fewer / merged entries of the existing
shape). When the flag is off the drain/ship path is byte-identical to
before and none of the counters fire.

### Bounded pipelining (`ShipMaxInFlight`)

`ShipMaxInFlight` (default `1`, validated `>= 1`) is live. At `1` the
shipper is strictly serial per `(tree, peer)`; above `1` it keeps up to
that many shipped-but-unacknowledged batches in flight, consumes acks in
strict FIFO order, and advances the durable cursor past a batch only
once every lower-HLC batch before it has been acknowledged. A receiver
`SuggestedBatchSize` hint collapses the window back to `1` for the tick,
and the content-manifest elision exchange runs inline before each batch
without collapsing it. A window above `1` issues concurrent
`IReplicationTransport.SendAsync` calls for one pair, so the transport
must tolerate that. The live depth is the `peer.ship_in_flight` gauge;
see [Sender-side pipelining](receiver-flow-control.md#sender-side-pipelining).

---

## Maintenance grain

### Independent cadences

The two scheduled passes run on independent cadences with their own
last-run timestamps in persistent state:

- **GC pass** - calls `ILatticeWalGc.RunOnceAsync(treeName)` every
  `MaintenanceGcInterval` (default 5 s). The GC consults the
  cursor registry for the slowest-ack frontier across registered
  consumers - every per-peer shipper reports its durable cursor there -
  and trims the WAL up to that frontier (or the `WalRetention` TTL
  ceiling, whichever is later).
- **Fall-off-the-log probe** - every `MaintenanceFallOffCheckInterval`
  (default 30 s) reads a bounded window at the head of each local WAL
  partition, takes the oldest entry each data origin authored in that
  window (`ILatticeWalIntrospection.GetOldestAvailableHlcByOriginAsync`)
  and, for each current topology peer that authored at least one
  entry in the window, calls
  `ILatticeFallOffLogDetector.CheckAndTriggerAsync(treeName, peer, oldestHlc)`
  with that peer's own oldest local HLC. A peer with no authored entry
  in the window is skipped - probing it against another origin's entries
  was the source of a false-positive re-bootstrap loop. This probe only
  compares local readings and guards local seal gaps; it is not the
  cross-cluster source-WAL trim detector. Source trims are detected by
  the sender shipper when a shipping read returns a first sequence above
  the requested sequence, and the request is carried on
  `ReplicationBatch.ReseedAfterEpoch`. On positive local detection, the
  detector drives the bootstrap kickoff itself - the maintenance grain
  is a pure scheduler.

### Failure handling

The cadence stamp advances **only on a successful pass**. A thrown
`RunOnceAsync` or probe pass is logged as a warning and retried on the
next phase tick rather than waiting a full cadence interval; a failure
probing one peer is logged and skipped without failing the pass, so that
peer is retried on the next cadence. The keepalive
reminder (60 s) is the backstop against a deterministically-failing pass
so the activation cannot stall indefinitely.

This is the opposite of "log and skip" - a steady-state maintenance error
is visible in logs immediately and recovers as soon as the underlying
condition clears.

---

## Options

| Option | Default | Validator | Purpose |
|---|---|---|---|
| `ShipBatchSize` | 256 | `>= 1` | Maximum entries per ship loop iteration. |
| `ShipMaxInFlight` | 1 | `>= 1` | Shipped-but-unacknowledged batches per `(tree, peer)`; `1` is strictly serial. See [Bounded pipelining](#bounded-pipelining-shipmaxinflight). |
| `AdaptiveBatchSizingEnabled` | `true` | - | Sender-side AIMD controller that lowers the per-batch cap below `ShipBatchSize` on rising ack latency or send errors, tuned by `AdaptiveBatchIncrement`, `AdaptiveBatchDecreaseFactor`, `AdaptiveBatchLatencyThreshold`, and `AdaptiveBatchWindowLength`. See [Sender-side adaptive batch sizing](receiver-flow-control.md#sender-side-adaptive-batch-sizing). |
| `ShipBackoffInitial` | 100 ms | `> TimeSpan.Zero` | Base delay on first transient failure. |
| `ShipBackoffMax` | 30 s | `>= ShipBackoffInitial` | Cap on the doubled backoff delay regardless of consecutive failure count; jitter is applied after the cap. |
| `ShipBackoffJitter` | 0.2 | `[0.0, 1.0]` | Symmetric jitter multiplier. |
| `MaintenanceGcInterval` | 5 s | `> TimeSpan.Zero` | Cadence between WAL GC passes. |
| `MaintenanceFallOffCheckInterval` | 30 s | `> TimeSpan.Zero` | Cadence between per-peer fall-off-the-log probes. |
| `ShipDoorbellEnabled` | `true` | - | Master switch for the writer-side doorbell. |
| `PreShipCoalescingEnabled` | `true` | - | On by default; set to `false` per tree to opt out. Collapse a drained batch's redundant per-key versions before they ship: latest-wins elision on LWW trees, delta-merge folding on recognised CRDT trees (an unregistered OR-Map shape or an opaque delta ships individually). |
| `ShipPartitionPageSize` | 256 | `>= 1` | Entries read from each WAL partition per pump tick before the HLC merge. See [Partition resume cursor](#partition-resume-cursor). |
| `ShipCursorWriteInterval` | 16 | `>= 1` | Successful acks per durable cursor write; `1` persists every ack. See [Deferred cursor persistence](#deferred-cursor-persistence). |
| `ShipCursorWriteMaxDelay` | 2 s | `> TimeSpan.Zero` or `Timeout.InfiniteTimeSpan` | Longest an un-flushed cursor advance waits for a durable write; `Timeout.InfiniteTimeSpan` coalesces purely by batch count. |
| `ShipPhaseTimerPeriod` | 100 ms | `> TimeSpan.Zero` | Cadence of the shipper's phase timer, the single authority that drains and ships. |
| `ShipSourceIdentityBackstopInterval` | 30 s | `> TimeSpan.Zero` | Backstop re-resolve of the source tree's physical identity. See [Source-identity rebind](#source-identity-rebind). |
| `LivenessProbeInterval` | 30 s | `> TimeSpan.Zero` or `Timeout.InfiniteTimeSpan` | When a tick finds nothing to ship and this long has passed since the last successful contact, the shipper sends an empty batch as a liveness probe, so the outbound `peer.last_contact_seconds` gauge keeps resetting on a healthy idle link. `Timeout.InfiniteTimeSpan` disables the probe. |
| `ContentHashDedupEnabled` | `true` | - | Measures the payload re-send rate. See [Content-hash dedup measurement](#content-hash-dedup-measurement). |
| `ContentHashDedupCacheSize` | 4096 | `>= 64` | Per-activation, per-key LRU size for the last-shipped content hash. |
| `ContentHashDedupElisionEnabled` | `false` | Requires `ContentHashDedupEnabled` | Opt-in content-manifest exchange that elides payloads the receiver already holds. |

Every option in the table except `ShipDoorbellEnabled` and `ShipPhaseTimerPeriod` resolves via
`IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`, so per-tree
overrides are honoured. `ShipDoorbellEnabled` (read by the commit-time
doorbell sink) and `ShipPhaseTimerPeriod` (read when a shipper
activation arms its timer) come from the cluster-wide options instance
only.

### Receiver-side flow control

Receiver-side WAL back-pressure is on by default: `AddLatticeReplication`
installs `WalSaturationReceiverFlowControlPolicy`, which translates the local
WAL's saturation state into the sender backoff hints carried on each
`ReplicationAck`. The policy looks that state up under the replicated tree's
name, while the signal records it under the id the tree's WAL is written
under, so on a tree that is aliased on the receiving cluster the lookup
reads `Healthy` and the ack carries no hint - see
[Resolution and scope](../lattice/wal-saturation-signal.md#resolution-and-scope).
The mapping is tuned with the separate
`WalSaturationReceiverFlowControlOptions` (`ThrottledBatchRatio`,
`ThrottledPauseMs`, `SaturatedBatchSize`, `SaturatedPauseMs`) via
`ISiloBuilder.AddWalSaturationReceiverFlowControl(...)`. Hosts opt out by
pre-registering `NoOpReceiverFlowControlPolicy`. See
[Receiver flow control](receiver-flow-control.md#built-in-wal-saturation-policy).

---

## Metric activation

These instruments stay at zero until the drivers light them up - except
`wal.entries_trimmed`, which the core garbage-collection scheduler also
emits; the table shows which driver is the source of each.

| Metric | Source | When it fires |
|---|---|---|
| `wal.entries_shipped` | gRPC push transport, inside the shipper's `IReplicationTransport.SendAsync` call | A `Push` call for a non-empty batch returned an ack - accepted or not, so a batch a receive fence deferred counts again when it is re-shipped (a custom transport does not emit it). |
| `wal.entries_trimmed` (on the core `orleans.lattice` meter, not `orleans.lattice.replication` - see `LatticeMetrics.WalEntriesTrimmed`) | Maintenance grain GC pass, and the core library's per-silo WAL garbage-collection scheduler, which runs without the drivers | GC trim removed at least one entry. |
| `ship.duration` | gRPC push transport, inside the shipper's `IReplicationTransport.SendAsync` call | Every `Push` call (success or failure), liveness probes included. |
| `peer.fell_off_log` | Maintenance grain fall-off probe | Detector finds the peer's HWM below the oldest entry that peer authored in the head window of the local WAL partitions. Source shipper trim gaps use the `ReplicationBatch.ReseedAfterEpoch` request path instead. |
| `apply.lag` / `apply.duration` / `apply.fifo_violations` / `apply.buffered_entries` / `apply.buffer_bytes` / `apply.dependency_wait` / `apply.causal_violations_blocked` / `apply.parallel_runs` | Receiver-side `IReplicationApplier` | Lit transitively once the peer is shipping real traffic. |
| `dead_letter.removed` | (already wired) | Operator discards / replays. The queue refuses rather than evicts, so `evicted` is no longer emitted. |
| `dead_letter.refused` | Dead-letter queue grain | A park refused because the queue is full; the shipper holds its cursor (backoff `dead-letter-refused`) and the link reports Stalled. |

---

## The local materialiser shares the cursor registry, not the scheduler

The core library's local materialiser does not run on the shipper's
scheduler. Each leaf replays the write-ahead log entries past its
projection checkpoint when it activates and, after every checkpoint it
persists, reports the highest HLC it has applied into the same
`IWalCursorRegistry` the per-peer shippers report their durable cursors
to. Absent a `WalRetention` ceiling, the WAL garbage collector
therefore trims only below the slowest consumer of either kind - a
lagging leaf checkpoint or a lagging peer. Neither the leaf materialiser
nor the shipper consumes `IChangeFeed`.
