# Replication drivers

This document describes the **production drivers** that turn the dormant
replication primitives - the change feed, the WAL storage provider, the
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

| Grain | Key | Cadence | Purpose |
|---|---|---|---|
| `IReplicationShipperGrain` | `{treeName}/{peerClusterId}` | 100 ms phase timer + 90 s reminder backstop + writer-side doorbell | Drains the per-tree change feed from the per-peer cursor, applies producer-side filters and the cycle-break, calls `IReplicationTransport.SendAsync`, advances the cursor on ack, applies exponential backoff on transient failure, parks malformed batches on the per-tree DLQ. |
| `IReplicationMaintenanceGrain` | `{treeName}` | 5 s phase timer + 60 s reminder backstop | Schedules WAL garbage collection (`ILatticeWalGc.RunOnceAsync`) and per-peer fall-off-the-log probes (`ILatticeFallOffLogDetector.CheckAndTriggerAsync`) on independent cadences. |

The shipper is per-peer because per-peer back-pressure isolation must not
couple peers to each other; one slow peer cannot block any other.
The maintenance grain is per-tree because GC and fall-off probing are
tree-scoped, not peer-scoped - running them once per tree avoids
N-fold-redundant work that scales with peer count.

### Activation

A hosted background service (`ReplicationDriverActivationService`,
`BackgroundService`) calls `EnsureActiveAsync` on the cluster-singleton
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
startup. The default implementation, `OptionsReplicationTopology`,
projects `LatticeReplicationOptions.ReplicationPeers` via
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

`ReplicationDriverActivationService` subscribes for the lifetime of the
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
`OptionsReplicationTopology` is the only thing in the pipeline that
reads it.

##### Membership-sensitive consumers

These are the four consumers whose behaviour depends on which peers
are currently reachable. Every one of them reads
`IReplicationTopology` and nothing else:

| Consumer | Source it reads | Effect of a topology change |
|---|---|---|
| `ReplicationDriverActivationService` (startup pass + runtime adds) | `CurrentPeers` at startup + `Subscribe(...)` for the silo's lifetime | On `Added`, activates one `IReplicationShipperGrain` per replicated tree under a retry-with-backoff loop. No silo restart required. |
| `ShardedReplogSink` (doorbell fan-out per commit) | `CurrentPeers` (live read per commit) | The next commit rings doorbells for exactly the current snapshot. A peer added 1 ms ago is rung; a peer removed 1 ms ago is not. |
| `ReplicationMaintenanceGrain.ProbeFallOffAsync` (per-cadence fall-off probe) | `CurrentPeers` (live read per cadence tick) | The next cadence tick probes exactly the current snapshot - a removed peer is dropped from the probe set; an added peer joins it on the next tick. |
| `ReplicationShipperGrain` (per-peer pump) | The grain key it was activated under - neither topology nor options is re-read | The shipper is bound to a specific `(tree, peer)` for its activation lifetime. See *Shipper-lifetime asymmetry* below. |

##### What `LatticeReplicationOptions.ReplicationPeers` does

`ReplicationPeers` is read by exactly one component:
`OptionsReplicationTopology`. That component is the
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

1. Honours the backoff budget set by the previous failed attempt - if
   `_nextRetryAtUtc > now`, the tick returns immediately.
2. Drains a batch up to `LatticeReplicationOptions.ShipBatchSize` entries
   past the persisted cursor from the change feed.
3. Filters each entry through the producer-side filter chain:
   - **Cycle-break:** skip entries whose `OriginClusterId` matches the
     peer's own cluster id (peer never receives its own writes back).
   - **`KeyFilter`:** skip entries whose key fails the configured
     predicate.
   - **`KeyPrefixes`:** skip entries whose key does not start with any
     configured prefix.
4. Encodes the batch via the configured `IReplicationBatchEncoder` into
   an activation-scoped `ArrayBufferWriter<byte>` (see "Buffer reuse"
   below).
5. Calls `IReplicationTransport.SendAsync` with the framed batch.
6. On positive ack, advances the durable cursor to
   `min(ack.HighestAppliedHlc, lastShippedHlc)` and reports the new
   cursor through the `IWalCursorRegistry`.

A successful round-trip resets `ConsecutiveFailures` to `0` and clears
the backoff budget.

### Doorbell

The shipper grain is the log-first replication producer: it tails the
single per-shard leaf write-ahead log (the leaf commit-log writer is the
sole WAL appender) from a durable per-partition cursor and is the only
ship driver. The commit-time `ShardedReplogSink` does not append to the
WAL and does not ship; it is reduced to a low-latency nudge that advances
the producer-side local vector clock cache for local-origin entries and
rings shipper doorbells so the background tailing loop drains immediately.

In addition to the phase timer, the shipper exposes
`OnDoorbellAsync(CancellationToken)`. The producer-side `ShardedReplogSink`
rings the doorbell after every commit for the affected
`(tree, peer)` activations, so steady-state ship latency is sub-second.
The doorbell is
best-effort - a missed call only delays the next ship by one timer tick (default 100 ms).

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

When `IReplicationBatchEncoder.Encode` throws an `ArgumentException` or
`InvalidOperationException` - schema-shape failures the bytes can never
recover from in their current form - the shipper:

1. Parks every entry in the offending batch on the per-tree
   `IReplicationDeadLetterGrain` tagged with
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

The steady-state ship loop bypasses `IChangeFeed` and reads each WAL
partition directly via `IWalShardGrain.ReadAsync(fromSequence, …)`
starting at a durable per-partition resume cursor stored on
`ReplicationShipperState.PartitionCursors`. Per pump tick the shipper
fetches up to `ShipPartitionPageSize` (default 256) entries from each
partition and merges them by HLC ascending via a heap-free O(P) linear
scan-for-min over partition heads. The merge collapses to O(1) for the
canonical single-partition case.

Sequence-based (not HLC-based) resume converts every pump tick from an
O(N) rescan-from-zero walk over the WAL into an O(page) read past the
last successfully shipped offset. `IChangeFeed` is retained verbatim
for bootstrap, test, and future-materialiser consumers that have no
notion of partition routing.

A defensive HLC predicate at the top of the merge loop drops any entry
whose timestamp is at-or-below the durable HLC `Cursor`. This is the
single insurance line that handles the legacy-state-decode case (an
upgraded shipper resuming with a populated HLC cursor but an empty
`PartitionCursors` dictionary), the bootstrap case (the receiver
applies a snapshot that pushes the HLC cursor past pending WAL
entries), and the cross-shipper-HWM case (another peer advanced the
receiver's frontier past ours and the next ack reflects that). Steady
state never matches the predicate because partition cursors move
strictly forward on every positive ack.

Wire-compat is additive: the new `[Id(2)] PartitionCursors` slot on
`ReplicationShipperState` decodes as the empty dictionary for legacy
persisted state, which the cold-start path treats identically to a
fresh activation. Setting `ReplogPartitions=1` reduces the merge to a
single read per tick; the shipping default is `8` (kept in lockstep
with `LatticeOptions.WalPartitions` so the shipper reads every
partition the commit-log writer fanned across).

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

Receiver-side apply is HLC-monotonic and dedupes on
`(originClusterId, originHlc)`, so a silo crash inside the
deferred-persist window costs at most `ShipCursorWriteInterval × ShipBatchSize`
entries of wasteful re-shipping - the receiver no-ops the duplicates
and no data is lost. Lowering `ShipCursorWriteMaxDelay` only ever makes
the durable cursor fresher; it can never widen that bound.

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
safely requires the receiver to advertise which content hashes it
already holds (the manifest/pull exchange), which needs an
additive-but-new request/response shape on `IReplicationTransport`.
That elision (`ContentHashDedupElisionEnabled`) is deliberately kept
opt-in and deferred until wire-version capability negotiation lands, so
the default build ships the measurement that justifies the round trip
without any wire-format, serialization, `[Id]`, or
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
`LwwRegister` the receiver applies each entry by last-writer-wins on the
value bytes ordered by `(HybridLogicalClock, OriginClusterId)`, so within
one drained batch only the highest-HLC version per key survives
convergence and the earlier ones are invisible after apply. Because the
shipper only ever drains its own cluster's authored writes (the
cycle-break filters to `options.ClusterId`), every coalescing candidate
shares one origin and the drain buffer is already HLC-ascending, so the
last occurrence of a key is the highest-HLC one - the version the receiver
converges to. The LWW path therefore keeps only that last version and
drops the earlier ones outright.

For a recognised CRDT tree (`OrSet`, `PnCounter`, `VersionVector`,
`MvRegister`, `Sequence`, and a registered `OrMap`) the receiver applies
each entry by folding its per-entry typed delta into the loaded state, so
dropping an intermediate version would lose its contribution rather than
merely hide it. The CRDT path instead **folds** a same-key run's typed
deltas into a single combined delta - a join over the primitive's own
semilattice (union for OR-Set adds / removes, pointwise-max for PN-Counter
and version-vector components, dot-dominance merge for the multi-value
register, grow-only union for the sequence CRDT, and for the OR-Map a
union of the dot-tagged adds and tombstones with same-dot value snapshots
lattice-merged through the value CRDT's own `ICrdt<TValue>.MergeFrom`) -
re-encodes it onto the kept (highest-HLC) entry, and elides the earlier
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

### `ShipMaxInFlight` is v1-inert

The validator accepts any value `>= 1`, but the shipper grain hard-codes
strict serial sends per `(tree, peer)` in this release. Multi-batch
pipelining is gated on the typed-envelope transport seam (which removes
the sender-side decode round-trip the gRPC push transport currently pays)
and on multi-batch in-flight WAL flush landing first.

---

## Maintenance grain

### Independent cadences

The two scheduled passes run on independent cadences with their own
last-run timestamps in persistent state:

- **GC pass** - calls `ILatticeWalGc.RunOnceAsync(treeName)` every
  `MaintenanceGcInterval` (default 5 s). The GC consults the
  cursor registry for the slowest-ack frontier across `IChangeFeed`
  consumers and trims the WAL up to that frontier (or the
  `WalRetention` TTL ceiling, whichever is later).
- **Fall-off-the-log probe** - calls
  `ILatticeFallOffLogDetector.CheckAndTriggerAsync(treeName, peer, oldestHlc)`
  every `MaintenanceFallOffCheckInterval` (default 30 s) for each
  configured peer. The sender-oldest HLC is computed via
  `ILatticeWalIntrospection.GetOldestAvailableHlcAsync`. On positive
  detection, the detector drives the bootstrap kickoff itself -
  the maintenance grain is a pure scheduler.

### Failure handling

The cadence stamp advances **only on a successful pass**. A thrown
`RunOnceAsync` or probe pass is logged as a warning and retried on the
next phase tick rather than waiting a full cadence interval. The keepalive
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
| `ShipMaxInFlight` | 1 | `>= 1` | **v1-inert.** Reserved for future multi-batch pipelining. |
| `ShipBackoffInitial` | 100 ms | `> TimeSpan.Zero` | Base delay on first transient failure. |
| `ShipBackoffMax` | 30 s | `>= ShipBackoffInitial` | Upper bound on backoff regardless of consecutive failure count. |
| `ShipBackoffJitter` | 0.2 | `[0.0, 1.0]` | Symmetric jitter multiplier. |
| `MaintenanceGcInterval` | 5 s | `> TimeSpan.Zero` | Cadence between WAL GC passes. |
| `MaintenanceFallOffCheckInterval` | 30 s | `> TimeSpan.Zero` | Cadence between per-peer fall-off-the-log probes. |
| `ShipDoorbellEnabled` | `true` | - | Master switch for the writer-side doorbell. |
| `PreShipCoalescingEnabled` | `false` | - | Opt-in per tree. Collapse a drained batch's redundant per-key versions before they ship: latest-wins elision on LWW trees, delta-merge folding on recognised CRDT trees (OR-Map / opaque deltas ship individually). |

All options resolve via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`,
so per-tree overrides are honoured.

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
| `wal.entries_shipped` | Shipper grain via `IReplicationTransport.SendAsync` | Outbound batch acknowledged. |
| `wal.entries_trimmed` (on the core `orleans.lattice` meter, not `orleans.lattice.replication` - see `LatticeMetrics.WalEntriesTrimmed`) | Maintenance grain GC pass | GC trim removed at least one entry. |
| `ship.duration` | Shipper grain via `IReplicationTransport.SendAsync` | Every send call (success or failure). |
| `peer.fell_off_log` | Maintenance grain fall-off probe | Detector confirms peer's HWM precedes sender's oldest HLC. |
| `apply.lag` / `apply.duration` / `apply.fifo_violations` / `apply.buffered_entries` / `apply.buffer_bytes` / `apply.dependency_wait_ms` / `apply.causal_violations_blocked` | Receiver-side `IReplicationApplier` | Lit transitively once the peer is shipping real traffic. |
| `dead_letter.enqueued` (reason=schema) | Shipper grain (permanent encode failure) | Schema-shape encode throw. |
| `dead_letter.removed` | (already wired) | Operator discards/replays. |

---

## Local-apply materialiser reuses the same scheduler

The shipper grain is the canonical scheduler skeleton for the
**local-apply materialiser** in the core library - the
same `IChangeFeed` consumer shape, the same per-consumer cursor on
`IWalCursorRegistry`, the same phase-timer + doorbell +
reminder triad. The materialiser is just another change-feed consumer
with its own cursor; the `IReplicationTransport.SendAsync` call is
replaced with a local `IReplicationApplier.ApplyAsync` call (or the
commit-log apply seam) and the rest of the scheduling skeleton is
verbatim. The maintenance grain is similarly the natural home for any
future per-tree background pass (compaction, snapshot pruning, projection
rebuild) without inventing a third scheduler shape.
