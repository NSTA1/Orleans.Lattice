# Replication drivers

This document describes the **production drivers** that turn the dormant
replication primitives - the tree's partitioned write-ahead log, the WAL storage provider, the
WAL garbage collector, and the fall-off-the-log detector - into a running
end-to-end pipeline. Without these drivers, calling `AddLatticeReplication`
yields the seam set but emits nothing on the wire and trims nothing from
disk: every metric on `LatticeReplicationMetrics` other than
`dead_letter.*` stays at zero.

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

A hosted background service (a `BackgroundService`) calls `EnsureActiveAsync` on the cluster-singleton
grain for every replicated tree on startup. Calls are idempotent - Orleans
deduplicates concurrent activations via grain identity, and
`StartCoordinatorAsync` short-circuits when a reminder + phase timer are
already wired.

The activation loop is **retry-with-backoff**: a freshly-started silo may
race the Orleans runtime's own `IHostedService` ordering, so the first
`EnsureActiveAsync` call can throw transiently. The service starts with a
250 ms inter-attempt delay, doubles on each consecutive miss up to a 30 s
cap, and resets the delay on the first successful activation. The loop
only exits when every pending grain is active or the host's
`stoppingToken` is cancelled.

```csharp
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

These are the four drivers whose behaviour depends on which peers
are currently reachable. Every one of them reads
`IReplicationTopology` and nothing else (other membership-sensitive
paths - the anti-entropy digest probe, the source-identity rebind, and
the coordinated-restore saga's dispatcher and write fence - also read
`CurrentPeers` live on each pass):

| Consumer | Source it reads | Effect of a topology change |
|---|---|---|
| Driver activation background service (startup pass + runtime adds) | `CurrentPeers` at startup + `Subscribe(...)` for the silo's lifetime | On `Added`, activates one per-peer shipper per replicated tree under a retry-with-backoff loop. No silo restart required. |
| Commit-time doorbell sink (doorbell fan-out per commit) | `CurrentPeers` (live read per commit) | The next commit rings doorbells for exactly the current snapshot. A peer added 1 ms ago is rung; a peer removed 1 ms ago is not. |
| Per-tree fall-off probe (per-cadence) | `CurrentPeers` (live read per cadence tick) | The next cadence tick probes exactly the current snapshot - a removed peer is dropped from the probe set; an added peer joins it on the next tick. |
| Per-peer shipper pump | The grain key it was activated under - neither topology nor options is re-read | The shipper is bound to a specific `(tree, peer)` for its activation lifetime. See *Shipper-lifetime asymmetry* below. |

##### What `LatticeReplicationOptions.ReplicationPeers` does

`ReplicationPeers` is read by exactly one component:
the options-backed default topology. That component is the
`TryAddSingleton`-registered default `IReplicationTopology` and it
turns each `IOptionsMonitor<LatticeReplicationOptions>.OnChange`
reload into a diff against the last-projected set, deduplicates and
trims whitespace, and emits one `PeerChanged` event per net add and
net remove. Hosts that take no action see the same behaviour the
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
   below.
3. **Re-add (peer disappears and reappears).** If the original shipper
   activation is still in memory, it is reused - there is no fresh
   activation, and the durable cursor on that activation continues
   from where the previous run left off.
4. **Replace (host swaps the topology implementation).** Possible only
   at silo startup, before `AddLatticeReplication` registers the
   default. After registration the `TryAddSingleton` slot is occupied
   for the silo's lifetime.

##### Shipper-lifetime asymmetry (load-bearing)

A shipper grain bound at activation time to `(tree, peer)` **stays
bound for its activation lifetime**, even if the peer is removed from
the topology. Removal events deliberately do not tear down the shipper
so it can drain its remaining backlog before deactivation. The
backpressure path is:

- Doorbells and fall-off probes immediately stop firing for the
  removed peer (those consumers read live topology snapshots).
- The shipper grain continues to pump its existing backlog through the
  configured transport. If the transport can no longer reach the peer
  the shipper's exponential backoff and DLQ paths handle the failure
  the same way they handle any other transient outage.
- Orleans eventually deactivates the idle shipper via standard
  collection rules.

This is intentional: tearing down the shipper on `Removed` would lose
any in-flight batch and any cursor advance that had not yet been
persisted. The cost is that a peer removed from the topology is not
the same as a peer disconnected from the wire - reachability is the
transport's responsibility, not the topology's.

##### Mismatch scenarios at a glance

| Scenario | Membership-sensitive behaviour | Notes |
|---|---|---|
| Default topology; peer added to `ReplicationPeers` | Activated + ringed + probed on next tick | Standard option-driven flow. |
| Default topology; peer removed from `ReplicationPeers` | Doorbell + probe stop on next tick; shipper drains | The shipper drains then idles out via Orleans collection. |
| Custom topology emits `Added`; `ReplicationPeers` unchanged | Activated + ringed + probed on next tick | `ReplicationPeers` is inert; the topology is authoritative. |
| Custom topology emits `Removed`; `ReplicationPeers` still lists the peer | Doorbell + probe stop on next tick; shipper drains | The options list does not resurrect the peer. |
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
is exhausted - a merge that yields fewer than `ShipBatchSize` entries (the
final short tail) ends the tick, as does a receiver flow-control signal (a
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
physical log. When a shadow-cutover restore, a resize, or a reshard repoints
the logical tree's registry alias to a freshly minted physical tree, the
shipper must reset those cursors and re-ship from the new physical log start,
or it would keep tailing the retired identity's orphaned WAL.

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

This event-driven inversion closes two problems the former per-tick registry
resolve had at once: the idle-only registry read load (an otherwise-quiet link
performed a steady stream of `_lattice_trees` reads purely to notice a swap
that rarely happens), and a correctness sharp edge - the detection window in
which a still-live shipper kept tailing the retired physical WAL and could
ship keys confined to the retired identity (for example the keys a
restore-to-drop-keys cutover meant to discard, which plain last-writer-wins
cross-cluster shipping never retracts). Pushing the rebind synchronously with
the swap shrinks that window to the notification latency.

Two further per-tick metadata resolutions are memoised on the same principle -
recompute only when an input changed, not every tick. Peer wire-version
negotiation and shared-dictionary negotiation both key off the receiver's
advertised capability on `ReplicationAck`, so their results are cached and
recomputed only when a new ack changes the peer's advertised capability (or
the shipper's options instance or effective dictionary id changes), not on
every pump tick. Together with the source-identity rebind this removes all
three steady-state idle registry/metadata resolutions, so an idle shipper's
only per-tick work is the WAL-tail poll, cursor-flush, and liveness probe.

### Doorbell

The shipper grain is the log-first replication producer: it tails the
single per-shard leaf write-ahead log (the leaf commit-log writer is the
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
- `ShipBackoffMax` (default 30 s) - upper bound regardless of consecutive
  failure count.
- `ShipBackoffJitter` (default 0.2) - symmetric `[1 - jitter, 1 + jitter]`
  multiplier applied to the computed delay so a fleet of shippers sharing
  a transient outage does not resynchronise on retry.

`Random.Shared` is the jitter source - sufficient for distribution
purposes, not cryptographic.

### Permanent encode failure: dead-letter routing

When building the outbound framing header throws an `ArgumentException`
or `InvalidOperationException` - schema-shape failures the batch can
never recover from in its current form - the shipper:

1. Parks every entry in the offending batch on the per-tree
   dead-letter store tagged with
   `LatticeReplicationMetrics.ReasonSchema` so a single poison entry never
   stalls the stream forever.
2. Advances the cursor past the batch so the stream makes forward
   progress.
3. Logs a warning with the entry count and the new cursor position.

The DLQ enqueue is best-effort; a deterministically-failing DLQ does not
pin the ship loop. The original entries remain in the WAL until the GC
pass trims them, so an operator can still recover off the WAL even when
the DLQ is unavailable.

### Buffer reuse

The shipper maintains activation-scoped buffers reused across pump
ticks:

- `_drainBuffer` (`List<WalRecord>`) - cleared in place at the start
  of every `PumpOnceAsync`. The framing encoder consumes the list
  synchronously inside `EncodeFraming`, so reuse is safe (no aliasing
  past the call).
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
partition directly via the per-shard WAL grain's sequence-ranged read (from a sequence lower bound)
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

Wire-compat is additive: the new `[Id(2)]` partition-cursor slot on
the shipper's persisted state decodes as the empty dictionary for legacy
persisted state, which the cold-start path treats identically to a
fresh activation. Setting `ReplogPartitions=1` reduces the merge to a
single read per tick; the shipping default is `8`, and the value must
equal `LatticeOptions.WalPartitions` so the shipper reads every
partition the commit-log writer fans across (see
[`ReplogPartitions`](configuration.md#replogpartitions)).

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
on its per-origin high-water mark, which drops nothing: an entry at or
below the snapshot-pinned causal floor is dropped, a recently applied
`(origin, HLC, key, op)` identity is suppressed by the shadow-forward
identity cache, and anything else re-applies idempotently under per-key
last-writer-wins. A silo crash inside the deferred-persist window
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

### Graceful deactivation

`OnDeactivateCoreAsync` flushes any pending cursor advance before the
activation tears down so a clean shutdown (e.g. operator silo drain)
eliminates the deferred-persist replay window entirely. A storage
failure during the flush is logged and swallowed - deactivation must
not block - and the next activation recovers by re-shipping at most
`ShipCursorWriteInterval × ShipBatchSize` entries the receiver
dedupes.

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
safely requires the receiver to report which content it already holds.
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
  (default 30 s) reads the oldest retained HLC per data origin from the
  local WAL (`ILatticeWalIntrospection.GetOldestAvailableHlcByOriginAsync`)
  and, for each current topology peer that authored at least one
  retained entry, calls
  `ILatticeFallOffLogDetector.CheckAndTriggerAsync(treeName, peer, oldestHlc)`
  with that peer's own oldest HLC. A peer with no retained authored
  entries is skipped - probing it against another origin's entries was
  the source of a false-positive re-bootstrap loop. On positive
  detection, the detector drives the bootstrap kickoff itself -
  the maintenance grain is a pure scheduler.

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
| `ShipBackoffInitial` | 100 ms | `> TimeSpan.Zero` | Base delay on first transient failure. |
| `ShipBackoffMax` | 30 s | `>= ShipBackoffInitial` | Upper bound on backoff regardless of consecutive failure count. |
| `ShipBackoffJitter` | 0.2 | `[0.0, 1.0]` | Symmetric jitter multiplier. |
| `MaintenanceGcInterval` | 5 s | `> TimeSpan.Zero` | Cadence between WAL GC passes. |
| `MaintenanceFallOffCheckInterval` | 30 s | `> TimeSpan.Zero` | Cadence between per-peer fall-off-the-log probes. |
| `ShipDoorbellEnabled` | `true` | - | Master switch for the writer-side doorbell. |
| `PreShipCoalescingEnabled` | `true` | - | On by default; set to `false` per tree to opt out. Collapse a drained batch's redundant per-key versions before they ship: latest-wins elision on LWW trees, delta-merge folding on recognised CRDT trees (an unregistered OR-Map shape or an opaque delta ships individually). |

Every option in the table except `ShipDoorbellEnabled` resolves via
`IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`, so per-tree
overrides are honoured. `ShipDoorbellEnabled` (read by the commit-time
doorbell sink) and `ShipPhaseTimerPeriod` (read when a shipper
activation arms its timer) come from the cluster-wide options instance
only.

### Receiver-side flow control

Receiver-side WAL back-pressure is on by default: `AddLatticeReplication`
installs `WalSaturationReceiverFlowControlPolicy`, which translates the local
WAL's saturation state into the sender backoff hints carried on each
`ReplicationAck`. The mapping is tuned with the separate
`WalSaturationReceiverFlowControlOptions` (`ThrottledBatchRatio`,
`ThrottledPauseMs`, `SaturatedBatchSize`, `SaturatedPauseMs`) via
`ISiloBuilder.AddWalSaturationReceiverFlowControl(...)`. Hosts opt out by
pre-registering `NoOpReceiverFlowControlPolicy`. See
[Receiver flow control](receiver-flow-control.md#built-in-wal-saturation-policy).

---

## Metric activation

These instruments stay at zero until the drivers light them up; the table
shows which driver is the source of each.

| Metric | Source | When it fires |
|---|---|---|
| `wal.entries_shipped` | gRPC push transport, inside the shipper's `IReplicationTransport.SendAsync` call | Outbound batch acknowledged (a custom transport does not emit it). |
| `wal.entries_trimmed` (on the core `orleans.lattice` meter, not `orleans.lattice.replication` - see `LatticeMetrics.WalEntriesTrimmed`) | Maintenance grain GC pass | GC trim removed at least one entry. |
| `ship.duration` | gRPC push transport, inside the shipper's `IReplicationTransport.SendAsync` call | Every `Push` call (success or failure), liveness probes included. |
| `peer.fell_off_log` | Maintenance grain fall-off probe | Detector finds the peer's HWM below the oldest retained entry that peer authored in the local WAL. |
| `apply.lag` / `apply.duration` / `apply.fifo_violations` / `apply.buffered_entries` / `apply.buffer_bytes` / `apply.dependency_wait` / `apply.causal_violations_blocked` | Receiver-side `IReplicationApplier` | Lit transitively once the peer is shipping real traffic. |
| `dead_letter.enqueued` (reason=schema) | Shipper grain (framing-header construction failure) | Schema-shape failure building the outbound batch. |
| `dead_letter.removed` | (already wired) | Operator discards / replays, or FIFO capacity eviction. |

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
