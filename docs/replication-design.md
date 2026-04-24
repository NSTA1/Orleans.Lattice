# Orleans.Lattice.Replication — Design Notes

> Scratch-pad for the future `Orleans.Lattice.Replication` package. The
> observations below are taken from building and reviewing the
> cross-cluster replication pipeline in the `MultiSiteManufacturing`
> sample (`samples/MultiSiteManufacturing/src/MultiSiteManufacturing.Host/Replication/`).
> The sample gets the shape right — per-peer cursors, HLC-ordered
> ship, replog janitor — but takes shortcuts that would be wrong to
> inherit into the library. This document captures what to upgrade
> when the feature is promoted into `Orleans.Lattice`.

## TL;DR

The sample's replication is **key-level delta, full-value-body,
last-writer-wins-by-HLC, pull-over-HTTP-JSON**. That works for the
demo but throws away the library's core property (CRDT convergence)
and has several latent correctness pitfalls (ship-time value reads,
thread-local cycle-break, reminder-cadence latency). A library-level
replacement needs a **write-time change feed carrying typed CRDT
deltas, origin-stamped HLC, pluggable push transport, and a snapshot
bootstrap protocol.**

## 1. Semantic model — the biggest gap

### 1a. Replicate CRDT deltas, not post-merge bytes

The wire payload is `byte[]` and the receiver calls `SetAsync` with
opaque bytes — effectively cross-cluster **LWW-by-HLC on raw state**.
That discards the library's defining property: CRDTs converge under
concurrent updates only if peers exchange *operations* (or semilattice
*deltas*), not post-merge snapshots. Two clusters concurrently adding
to an OR-Set will lose one side's adds.

**Library design:**

- The change feed emits *typed* deltas for the primitives the
  library ships (LWW-Register, OR-Set, PN-Counter, VersionVector-
  wrapped values).
- Opaque-byte LWW is the fallback only for schemaless / unknown value
  types.
- Remote deltas apply via the CRDT merge operation, not `SetAsync`.

### 1b. Preserve source HLC / causality on apply

The receiver's local lattice stamps a fresh HLC on apply today —
`SourceHlc` is "informational only". Breaks transitive replication
(A → B → C) and any vector-clock-based conflict resolution. The
library must carry an **origin-cluster-stamped HLC** (or per-cluster
HLC lane) through to stored metadata so a receiver can:

- Reject already-seen writes idempotently (exactly-once per
  `(origin, hlc)`).
- Resolve concurrent writes deterministically across any number of
  peers.

### 1c. Capture value at write time, not ship time

`ReplicatorGrain.ShipOneBatchAsync` calls `primary.GetAsync(origKey)`
per key when building the batch. Three bad properties:

- **Read amplification.** N extra shard round-trips per batch.
- **Lossy history.** Writes W1 → W2 between replog-append and ship
  collapse to W2 silently — fine for LWW, destructive for
  set/counter deltas.
- **False deletes.** If a key is deleted between append and ship, a
  `Set` entry is rewritten to `Delete` on the wire. A peer that
  already received a later causally-independent write from a third
  cluster now *spuriously deletes* the value.

**Fix:** change feed carries the full mutation (op + value *or*
delta) at the moment of the write. Needs a single-writer journal per
shard (WAL-style) rather than best-effort post-write append.

## 2. Loop-break and identity

- `RequestContext["lattice.replay"]` is thread-local ambient state —
  fragile across async boundaries, stream handlers, or any apply path
  that doesn't originate from the inbound HTTP call chain. Replace
  with an **origin-cluster** field that rides with the write and is
  stored in per-key metadata; the outbound feed filters
  `origin == self`.
- Shared-secret HTTP auth (`X-Replication-Token`) is demo-grade. The
  library integrates with the standard Orleans transport security
  story (mTLS, bearer tokens with rotation) — not a new auth header.

## 3. Efficiency

Current profile: full value bytes, per changed key, coalesced per
batch window, JSON-over-HTTP POST, pull-per-reminder-tick.

| Upgrade | Why |
|---|---|
| Binary framing (Orleans serializer or protobuf) | `byte[]` in JSON is base64 → ~33% inflation. |
| Compression (`Content-Encoding: zstd`/`gzip`) at batch boundary | Non-trivial on bursty replicated facts. |
| Content-hash dedupe (sender sends hashes, receiver pulls only missing) | Skip bytes the peer already has — matters when a key is re-set to the same value. |
| Delta values (per-CRDT-type delta rather than full state) | Ties to §1a; enables bounded-size deltas for large registers. |
| Batched shard read (`GetManyAsync`) on sender | Today N× round-trip per batch. Better: eliminate the read (§1c). |
| Push transport (gRPC streaming or Orleans cross-cluster streams) | Reminder-cadence pull gives tens-of-seconds tail latency. |
| Partitioned replog (N shards keyed by `hash(tree,key) % N`) with parallel scans + merge | Single replog is a hot range under fan-in. |
| Per-peer cursor coalesced (every K batches or T seconds, checkpoint before shutdown) | Today every batch does `WriteStateAsync`. |

## 4. Log lifecycle

- **GC by min acked cursor across peers**, not wall-clock TTL. The
  current TTL janitor can delete entries a lagging peer still needs,
  forcing a fall-off-the-log bootstrap. Tracking
  `min(cursor_peer_i)` lets you trim aggressively while still
  serving every peer.
- **Bootstrap / snapshot protocol** when a peer's cursor is older
  than the replog's oldest entry. Required for new peers,
  long-offline peers, bandwidth-constrained re-seeds. Design: peer
  requests snapshot for tree `T` as-of HLC `h`; sender streams a
  range scan of the primary tree with an `as-of` HLC; peer switches
  to incremental from `h` on completion.
- **Poison-entry quarantine.** Inbound apply currently `break`s on
  first exception and the sender retries the same batch on the next
  tick. If one entry is permanently bad (schema skew, oversized
  value, corrupt HLC) replication stalls forever. Move the failing
  `(origin, hlc)` to a DLQ after K retries and advance past it.

## 5. Reliability and back-pressure

- **Replog append is inline with the user write**, adding one
  lattice round-trip to every replicated mutation. Acceptable for
  the sample; for a library it should be a local WAL append (sync) +
  background ship, with the WAL being the single source of truth.
  Host-side inline append through an outgoing grain-call filter is
  the wrong long-term abstraction.
- **Writer swallows storage failures** — the filter's inline comment
  says "so a transient hiccup never propagates to the caller", but
  that silently drops replicated mutations. The library must surface
  append failure (optionally behind an opt-in "strict replication"
  mode) and/or maintain an on-disk spill so in-memory failures are
  recoverable.
- **No receiver-side flow control.** Sender always ships `BatchSize`.
  The ack should carry a `SuggestedBatchSize` / `Pause` hint so a
  struggling receiver can throttle without timing out.
- **No at-most-once on the receiver.** Two deliveries of the same
  `(origin, source_hlc)` both apply. Benign for LWW bytes, wrong for
  counters / sets (§1a). Receiver needs a small high-water-mark
  table keyed by origin cluster.

## 6. Observability

First-class per-peer metrics on the library surface:

- `entries_behind`, `bytes_behind`, `seconds_behind`
- `consecutive_errors`, `last_contact`
- Per-tree replication lag histogram (`now - source_hlc` at apply)
- Back-pressure signal ("replog growing faster than ship rate") as a
  `HealthCheck`

## 7. API shape for the library

Where the sample spreads its concerns across a host-level outgoing
filter + a custom minimal-API endpoint + a bootstrap hosted service,
a library-level design wants a cleaner seam:

```
Orleans.Lattice.Replication
├─ IChangeFeed            // subscribe to per-tree deltas (typed or opaque)
├─ IReplicationTransport  // pluggable: HTTP/gRPC/Orleans stream/custom
├─ ReplicationTopology    // dynamic, add/remove peers at runtime
├─ ISnapshotProvider      // bootstrap new peers
└─ AddLatticeReplication(builder, cfg)
```

- `IChangeFeed` replaces the outgoing-call-filter approach and is
  produced by the grain at commit time (so value capture is atomic,
  §1c).
- `IReplicationTransport` abstracts HTTP vs. stream vs. other (the
  sample's `ReplicationHttpClient` becomes one implementation; the
  gRPC push transport added to the sample is another).
- Cycle-break, idempotency, and causal-ordering logic live in the
  library's inbound applier — hosts never see
  `RequestContext["lattice.replay"]`.
- Topology is observable (`IObservable<PeerChanged>`) so peers can
  be added without a restart — today `ReplicationTopology.Load` is
  a one-shot read from `IConfiguration`.

## 8. Things the sample already gets right

Worth preserving when the library feature lands:

- **Per-peer cursor as HLC** — the right choice.
- **Advance cursor strictly by `ack.HighestAppliedHlc` on
  partial-apply.** Subtle and correct; the inline comment in
  `ReplicatorGrain` is a good warning.
- **Don't replicate the replog itself** (`ReplogTreePrefix` check).
  Obvious in hindsight, easy to forget.
- **Opt-in at the tree level** (plus per-key filter for the
  `mfg-part-crdt` split — labels replicate, operator register does
  not). The library needs the same granularity.
- **Janitor as a separate grain.** Right decomposition — keep it,
  just change the GC predicate (§4).

## 9. Suggested upgrade order (when it's time)

If we land this incrementally rather than as a big-bang:

1. **Value-at-write-time change feed** (§1c) — everything else
   gets easier once this exists.
2. **Origin-stamped HLC + idempotent apply** (§1b, §5 at-most-once)
   — makes cycle-break durable and enables exactly-once.
3. **Typed CRDT deltas in the feed** (§1a) — the real payoff;
   unblocks active-active for the primitives the library ships.
4. **Push transport + binary framing + compression** (§3) —
   bandwidth and latency wins. (The sample's gRPC push transport is
   a prototype for this.)
5. **Snapshot / bootstrap protocol** (§4) — required before we can
   tell anyone to deploy it to production.
6. **GC by min-acked-cursor, DLQ, back-pressure, metrics** (§4–§6)
   — ops polish.

## 10. What the sample's gRPC push transport demonstrates

The `MultiSiteManufacturing` sample was upgraded from pull-over-HTTP
to gRPC push streaming. The upgrade is intentionally small-scope — it
does **not** address any of the §1–§2 semantic gaps — but it
exercises several patterns the library will want:

- **Long-lived server-streaming RPC** per `(peer, tree)`. The sender
  opens `PushBatches(stream Batch)` to the receiver and keeps it
  open; batches flow as the local replog advances.
- **Reconnect-with-backoff** when the stream faults (network flap,
  peer restart, process recycle). Bounded exponential backoff with
  jitter; sender advances cursor only on acked batches so a
  mid-flight disconnect replays cleanly.
- **Push latency** drops from reminder-tick cadence (~60 s worst
  case) to sub-second, because the sender flushes when the local
  replog advances rather than waiting for the next tick.
- **HTTP fallback retained** for bootstrap + low-frequency paths
  that don't need low-latency push.

The gRPC transport in the sample is therefore a reference
implementation of what `IReplicationTransport` should look like —
when the library feature lands, the sample's gRPC code can be lifted
almost verbatim, with the wire types replaced by the library's
typed-delta contracts.
