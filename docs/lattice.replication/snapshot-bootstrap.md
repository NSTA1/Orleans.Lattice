# Snapshot / bootstrap export

`Orleans.Lattice.Replication` ships an `ISnapshotProvider` seam used by
the snapshot/bootstrap protocol to seed a newly-joining peer (or a
peer that has fallen off the WAL) before switching it to incremental
replication.

The seam is registered by `AddLatticeReplication` and resolved per
host via `TryAddSingleton`, so a host that needs a more efficient
storage-specific export can pre-register its own implementation
before calling `AddLatticeReplication`.

## Public surface

| Type | Shape | Purpose |
|------|-------|---------|
| `ISnapshotProvider` | `Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken ct)` + `Task<SnapshotStream> ExportAsync(string treeName, string sourceClusterId, HybridLogicalClock asOfHlc, CancellationToken ct)` + `Task<SnapshotStream> ExportAsync(string treeName, IReadOnlyList<LeafReReplayRange> ranges, HybridLogicalClock asOfHlc, CancellationToken ct)` | Streaming as-of-HLC export of a tree's primary state. The three-arg overload carries the sender-cluster identifier and is the one the bootstrap coordinator invokes; intra-cluster implementations inherit a default interface method that delegates to the two-arg overload after validating `sourceClusterId`. The range-scoped overload - used by the [bootstrap fallback](anti-entropy-bootstrap-fallback.md) - yields only entries inside the half-open `[StartKey, EndKey)` ranges; its default interface method filters the whole-tree export client-side. |
| `SnapshotStream` | sealed class with `TreeName`, `AsOfHlc`, `CausalStableFrontier` (`VersionVector`), `Entries` (`IAsyncEnumerable<SnapshotEntry>`) | Carries the export metadata + entry stream produced by `ExportAsync`. |
| `SnapshotEntry` | `readonly record struct` with `Key`, `Value`, `Timestamp`, `IsPrepared`, `IsTombstone`, `TransactionId`, `SourceShardIndex`, `AtomicBatchSize`, `AtomicBatchIndex`, `ExpiresAtTicks`, `Delta`, `Mode` | A single exported record stamped with its commit-time HLC so the receiver can pin the value at exactly that timestamp. A committed-projection row sets `Key`, `Value`, `Timestamp`, and `ExpiresAtTicks`; a prepared saga row additionally sets `IsPrepared`, `IsTombstone`, `TransactionId`, and the typed CRDT `Delta` / `Mode` (see [Snapshot and in-flight atomic visibility](#snapshot-and-in-flight-atomic-visibility)). `SourceShardIndex` is reserved and always `0`. |

`SnapshotEntry` is alias `olr.se`.

## Semantics

- **`asOfHlc = HybridLogicalClock.Zero`** disables the upper-bound
  filter and includes every live entry in the tree. This is the
  common case when seeding a fresh peer that has no incremental
  cursor yet.
- **`asOfHlc > Zero`** filters out entries whose stamped commit-time
  HLC is strictly greater than `asOfHlc`. After the drain the
  receiver's bootstrap handoff merges the snapshot's causal-stable
  frontier into its per-origin high-water-mark vector by pointwise
  maximum (the source cluster's own coordinate sealed at or above every
  entry the drain applied), clears any legacy pinned floor, and drains
  the durable causal-apply buffer. `IReplicationApplier`
  does not drop live incremental point writes at or below that
  frontier; duplicates across the boundary are absorbed by exact
  identity dedup and idempotent per-key merge. See "Bootstrap drain
  bypasses the high-water-mark advance" below for the receiver-side
  state machine that keeps the in-drain apply idempotent.
- **`CausalStableFrontier`** is the producer's causal-stable frontier
  at snapshot time - the pointwise minimum `VersionVector` across
  every consumer that has reported a vector through
  `IWalCursorRegistry.GetCausalStableAsync`. When no
  consumer has reported a VC-shaped cursor (single-peer cluster, fresh
  deployment, host using the legacy HLC-only overload), the provider
  falls back to the producer's per-tree local vector clock from
  the per-tree high-water-mark store's current vector - a strict superset
  of the meet that is safe as a snapshot cut-point. The receiver
  records this `(asOfHlc, frontier)` cut-point when the export opens
  and merges the frontier into the per-tree high-water-mark store only
  after every snapshot entry has been applied, so the causal
  dependency check on the first incremental entry after the handoff runs
  from a non-empty frontier.
- **Deletes ship as committed tombstone rows, then reaped source deletes
  reconcile.** The committed projection carries live keys only, so the
  default provider ends the export with a tombstone pass: every key a
  source leaf still holds as a tombstone ships as a row with
  `IsTombstone` set and `IsPrepared` clear, stamped with the tombstone's
  own HLC, and the bootstrap drain applies it as a delete. Without it a
  receiver that bootstraps in place over an existing copy - a peer that
  fell off the log and is re-bootstrapped by either the receiver-side local
  detector or the sender-side trim-gap request, or an operator re-seed over existing data - kept the old value of every key
  the source deleted while it was behind, permanently, because the
  delete's WAL record is behind the source's trim point and the
  incremental stream never delivers it (#4504).

  A source tombstone can be physically reaped after
  `TombstoneGracePeriod`, so an in-place drain also pre-captures the
  receiver's live source-origin, non-expiring rows before opening the
  export. The sender carries a source-generation tuple at export open and
  close: physical tree id, shard-map version, lineage token, soft-delete
  epoch, and deletion state. After the drain, for every pre-captured
  source-origin key that the whole-tree export did not carry as a live,
  tombstone, or prepared row, the coordinator synthesises a delete at the
  captured HLC. The HLC is not advanced: the last-writer-wins merge makes
  a tombstone win an equal-HLC tie, while any receiver write with a newer
  HLC still wins.

  The reconcile is deliberately fail-safe. It runs only for unscoped,
  last-writer-wins exports whose open and close generation match, whose
  source was not deleted or purging at either end, and whose lineage
  matches the receiver's durable aligned-lineage record for that source.
  A bootstrap records that alignment when the receiver held no row of the
  source's origin when the import began (tombstones and expiring rows
  included; the receiver's own and third-origin rows do not count), and it
  reconciles in that same import. A receiver that cannot prove alignment
  that way - it was never aligned, or the source's lineage has since
  changed (a restore, a revert, or an alias moved to another tree) -
  adopts the export's lineage when every source-origin key it held was
  carried by the export, because there is then nothing it could wrongly
  delete; otherwise it skips, counted as `skipped_never_aligned` or
  `skipped_lineage_mismatch`, and keeps every key. Every alignment - this
  one and the one a receiver that held no source row records - also
  needs a scan at the end of the drain to find no live, non-expiring
  source-origin row the export lacks (#4549). A source-origin row that
  arrived during the drain, perhaps one the source shipped under an older
  lineage before a restore, is absent from the pre-capture; aligning over
  it would let a later pass delete a value the source never deleted.

  An unknown generation (a sender that predates generations), a generation
  that moved during the export, or a deleted or purging source records a
  durable owed retry. Maintenance re-enters the normal full bootstrap path
  with a fresh pre-capture and every gate run again, so an upgraded sender
  reconciles on its first retry. Owed retries back off exponentially from
  one minute to a six-hour cap, so a sender that never reports a
  generation does not re-bootstrap the tree on every tick. Source-origin
  keys carrying an expiry are not captured because they expire on their
  own.

  Rows of any other origin - the receiver's own writes and a third
  cluster's - converge through the source's applied frontier (#4549). The
  export carries, when it opens and after its opening generation, the
  source tree's per-origin applied low watermark `S(o)` and the writes
  below it the source holds without applying `H(o)` (lost marks
  included); the source's own origin is never part of it, because its
  writes are re-shipped in offset order with their deletes. A write of
  origin `o` stamped below `S(o)` and not in `H(o)` was applied at the
  source before the export opened, so the export already reflects it:
  carried, or superseded by a later write or a delete.

  Before the drain, once the import is recorded (and once the tree is
  registered, so its lineage stamp cannot reset anything mid-drain), the
  receiver installs `(S, H)` as a durable **bootstrap drop floor** on its
  high-water-mark grain, provided the frontier was read under the
  export's opening lineage; otherwise it clears any earlier floor. The
  floor starts **provisional**: a delivery below it - a third cluster's
  write still in flight, a dead-letter replay, or a causal-buffer drain -
  is deferred (`outcome=bootstrap-floor-deferred`), not acknowledged,
  because an import that turns out unstable clears the floor and a
  dropped delivery would never be re-sent. The drain's own rows, saga
  terminals, and range deletes are exempt.

  Every install bumps the tree's **floor epoch**. The applier stamps each
  replicated write with the epoch it was admitted under, the epoch is
  raised in the tree registry
  (`TreeRegistryEntry.ReplicationFloorEpoch`), and every shard root of the
  tree is armed with it. Arming is a serial shard-root turn that returns
  only once the writes the shard admitted under an older epoch have
  finished their leaf merges; from then on the shard refuses an
  older-stamped write, which the applier defers. A shard root that
  activates later - a split or reshard target included - reads the epoch
  from the registry before it admits its first stamped write. So no write
  admitted before the floor existed can land after the reconcile scan.

  After the drain the coordinator scans the receiver's live, non-expiring
  last-writer-wins rows of every origin other than the source (a local
  row counts as this cluster's id), and deletes, at the row's own HLC and
  stamped with the source's id, each one whose key the export did not
  carry and whose write is below `S(o)` and not in `H(o)`. A pending saga
  prepare matching the same test belongs to a saga the source had already
  decided - one still open there is exported as prepared rows - so its
  bucket on that leaf is discarded, durably, and its later commit
  installs nothing. A row of another origin the export lacks whose write
  is at or above `S(o)` - or of an origin the frontier carries no
  watermark for - proves nothing: the source may have applied it during
  the export and then deleted it, or not have received it yet. It is kept
  and the reconcile is owed a retry, which settles it once the watermark
  has risen past it. The reconcile needs a stable generation like the
  source-origin reconcile, but not the aligned-lineage record: the
  frontier itself proves the source applied the write. A stable close
  then makes the floor final, and from then on a delivery below it is
  acknowledged without being merged (`outcome=bootstrap-floor-dropped`).
  An export whose generation moved clears the floor instead, so the
  deferred deliveries apply when re-shipped, and owes a retry that
  installs a fresh one. The floor belongs to the receiver's lineage of
  the tree: the tree frontier's re-stamp clears it with the tree's
  applied identities. A sender that carries no frontier installs no floor
  and reconciles no other origin.

  An in-flight saga's prepared delete ships as a prepared row with
  `IsTombstone` set (see
  [Snapshot and in-flight atomic visibility](#snapshot-and-in-flight-atomic-visibility)).
- **An unbounded export carries the source's applied frontier**
  ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)). For
  every foreign origin, it carries the applied low watermark below which
  each of that origin's writes to the tree is reflected in the export, and
  the writes the source held without applying them (parked, dead-lettered
  or lost). The values come from the source's own receiver tree frontier,
  and only when that frontier observed the open generation's lineage. The
  source's own origin is never covered: a restore, revert or alias move of
  the source's tree can lose its own writes without deleting them, and the
  source's shipped watermark, tagged with the receiver's lineage after its
  re-seed rewind, covers its writes at each receiver anyway. The value
  rides the export metadata at open, so a receiver can put a drop floor in
  force before the drain applies anything, and the end-of-stream trailer
  at close. The receiver keeps it only when the open and close generations
  match, are known, describe a live tree, and carry the lineage the
  frontier was read under. The bootstrap pin then installs it on the
  receiver's tree frontier: each origin's watermark starts from the
  export's, and the source's held writes stay held until the receiver
  applies them itself or the origin's own watermark passes them. Otherwise
  every origin starts from zero, which is sound and rises as each origin's
  own sender ships its watermark. The export reads every key of the tree
  through non-interleaving calls, so a write the watermarks cover cannot be
  overtaken by the read. A bounded or scoped export carries no frontier.
- **Expired keys are not emitted.** The committed projection reads an
  expired key as absent; the receiver's copy carries the same absolute
  expiry, so it expires there too.
- **A live key's TTL is carried.** Every exported row - a
  committed-projection row as well as a prepared saga row - carries the
  source entry's absolute `ExpiresAtTicks` (`0` for a durable key), so on
  a last-writer-wins tree a key that has a TTL on the source expires at
  the same instant on the bootstrapped peer. On a typed CRDT tree the
  receiver folds a committed row's full state through a state-based
  merge that does not apply the carried expiry, so the key is written
  there as a durable entry.

## Default implementation

The default snapshot provider enumerates the **local** tree
via the public `ILattice.EntriesAsync` surface and stamps each entry
with its commit-time HLC via `ILattice.GetWithVersionAsync`. It is
correct for **intra-cluster** seeding (snapshot-as-a-tool: an operator
snapshots a tree and restores it later in the same cluster, where the
local tree is the authoritative source) but pays a per-key version
round-trip on top of the leaf-chain enumeration. A future revision
will swap to a single-pass streaming HLC-threshold scan once the core
library exposes a version-bearing leaf-scan primitive; hosts that need a
faster export today can register their own `ISnapshotProvider` via DI.

**Cross-cluster bootstrap uses a separate receiver-side seam.**
On a receiver whose local tree is empty (e.g. a fresh cluster joining
an existing federation), the default snapshot provider would
yield zero entries because it reads the receiver's own tree rather
than the sender's. The "Cross-cluster transport contract" section
below documents the `IRemoteSnapshotTransport` abstraction and the
sender-side `LatticeRemoteSnapshotService`; the "Receiver-side
adapter" section documents `RemoteSnapshotProvider`, the
`IBootstrapSnapshotSource` implementation the bootstrap state
machine drains from when an `IRemoteSnapshotTransport` is registered.
The seam is split from `ISnapshotProvider` so a single silo can
simultaneously act as snapshot sender (its local `ISnapshotProvider`
is exported to peer receivers via `LatticeRemoteSnapshotService`) and
snapshot receiver (its `IBootstrapSnapshotSource` drains from an
upstream peer through the registered transport). Registering an
`IRemoteSnapshotTransport` alongside `AddLatticeReplication` is the
active-active signal: the receiver-side seam auto-flips to
`RemoteSnapshotProvider`, no separate opt-in call required.

## Cross-cluster transport contract

The first step of the cross-cluster bootstrap pipeline is the
transport-shaped seam that delivers a snapshot stream from a sender
cluster to a receiver cluster. It is a separate abstraction from the
live-incremental `IReplicationTransport` so a host can plug a different
binding for the bulk snapshot path (HTTP, blob-store, gRPC) without
disturbing the live tail pipeline.

| Type | Shape | Purpose |
|------|-------|---------|
| `IRemoteSnapshotTransport` | `Task<RemoteSnapshotMetadata> GetMetadataAsync(string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken ct)` + `IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken ct)` | Transport-shaped sub-interface used by a cross-cluster `ISnapshotProvider` adapter to fetch a snapshot from a sender cluster. |
| `RemoteSnapshotMetadata` | `readonly record struct` with `TreeName`, `SourceClusterId`, `AsOfHlc`, `CausalStableFrontier` | Snapshot cut-point captured atomically with the start of the entry stream; alias `olr.sm`. |

### Semantics

- **Two RPCs, one cut-point.** The receiver invokes `GetMetadataAsync`
  first to capture the sender's cut-point, then invokes
  `RequestSnapshotAsync` with the same `treeName` /
  `sourceClusterId` / `fromAsOfHlc` tuple to drain the stream. The
  metadata RPC returns the `(AsOfHlc, CausalStableFrontier)` pair the
  receiver records before the drain and pins on the per-tree
  high-water-mark store once the drain completes, so the
  snapshot/incremental handoff stays exactly-once even though
  metadata and stream travel on separate calls.
- **Point-in-time view.** Implementations MUST guarantee that entries
  committed on the sender after the metadata cut-point do not leak
  into the corresponding stream call. Receivers treat the stream as a
  point-in-time view at `metadata.AsOfHlc`; a moving-target stream
  would violate the cut-point pin and break the causal-stable handoff
  of the first incremental entry.
- **Concurrency.** Implementations are safe to invoke concurrently
  across distinct `(treeName, sourceClusterId)` pairs. Concurrent
  invocation against the same pair is implementation-defined;
  receivers serialise per pair through the bootstrap coordinator.
- **Argument validation.** Both methods throw
  `ArgumentNullException` when `treeName` or `sourceClusterId` is
  `null` and `ArgumentException` when either argument is empty or
  whitespace-only.
- **Prepared-transaction state travels in the entry stream.** The
  metadata DTO carries only the cut-point. In-flight saga state rides
  the entry stream as prepared rows (see
  [Snapshot and in-flight atomic visibility](#snapshot-and-in-flight-atomic-visibility)),
  so a transport must stream each `SnapshotEntry` verbatim -
  `IsPrepared`, `TransactionId`, `Delta`, and `Mode` included - for the
  bootstrapping peer to keep every saga all-or-nothing across the
  bootstrap.

### Contract test fixture

Implementations import `RemoteSnapshotTransportContractTests` from
the replication test project and derive a concrete fixture overriding
`CreateTransportAsync` to plug the transport in front of a
sender-side `StubSenderSnapshotProvider`. The inherited acceptance
suite pins:

- `GetMetadataAsync` returns `treeName` / `sourceClusterId` /
  `AsOfHlc` / `CausalStableFrontier` matching the staged sender
  snapshot.
- `RequestSnapshotAsync` streams every staged entry verbatim.
- `RequestSnapshotAsync` yields an empty stream when the sender has
  no entries.
- Metadata-then-stream is consistent under concurrent sender writes:
  entries staged after the metadata cut-point do not leak.
- `ArgumentNullException` / `ArgumentException` invariants hold for both RPCs.
- The stream observes cancellation tokens during enumeration.

The exemplar `InMemoryRemoteSnapshotTransport` in the replication
test project wraps a local `ISnapshotProvider` and is the smallest
reference shape for what a wire-bound implementation must preserve.

### Sender-side handler

`LatticeRemoteSnapshotService` is the canonical sender-side
implementation of `IRemoteSnapshotTransport`. It is registered as a
singleton by `AddLatticeReplication` and is the seam concrete bindings
(gRPC, in-process loopback, custom HTTP) delegate to when an inbound
metadata/stream RPC arrives on a producer silo. The handler is
deliberately binding-agnostic: the same instance is shared across every
concrete binding the host registers.

| Type | Purpose |
|------|---------|
| `LatticeRemoteSnapshotService` | Sender-side `IRemoteSnapshotTransport` handler. Validates routing arguments, refuses a tree that is not enrolled for replication on this cluster, invokes the local `ISnapshotProvider`, and returns the resulting cut-point metadata or streams the resulting entries. Stateless and safe for concurrent invocation across distinct `(treeName, sourceClusterId)` pairs. |

#### Semantics

- **Sender-side enrollment gate.** The requested tree name comes from
  the peer, so it is re-resolved against this cluster's own
  replication enrollment before anything is read. A tree that is not
  enrolled here - or a handler with no enrollment source to decide -
  is refused with `UnauthorizedAccessException`, so a peer that holds
  the mesh secret cannot stream out a tree this cluster keeps local,
  such as the `sys-` authorization and identity trees. The check sits
  on the handler, so every binding (gRPC, loopback, custom) inherits it.
- **Delegation to the local provider.** `GetMetadataAsync` calls
  `ISnapshotProvider.ExportAsync(treeName, fromAsOfHlc, ct)` and
  returns a `RemoteSnapshotMetadata` carrying the resulting
  `SnapshotStream.AsOfHlc` and `CausalStableFrontier`. A paired
  `RequestSnapshotAsync` call invokes `ExportAsync` again with the
  same `fromAsOfHlc` filter and drains `SnapshotStream.Entries`.
- **Point-in-time view.** The canonical default snapshot provider
  reads the producer's causal-stable frontier once at the start of
  the export and applies the receiver-supplied `fromAsOfHlc` filter
  at entry-emission time. When `fromAsOfHlc` is greater than `Zero`,
  writes committed on the sender after the metadata cut-point are
  excluded from the matching stream call by the per-entry HLC filter,
  so the stream remains a point-in-time view at `metadata.AsOfHlc` even
  though the metadata RPC and the stream RPC are separate calls. A
  `Zero` `fromAsOfHlc` - what a fresh bootstrap passes - disables the
  filter, so that stream can include writes committed after the
  metadata call.
- **Host-replaceable provider.** Hosts that register a custom
  `ISnapshotProvider` (for example, a storage-backend-aware export)
  drive the handler through the standard DI seam: replacing the
  provider replaces what `LatticeRemoteSnapshotService` exports, no
  per-binding configuration change required. A custom provider must
  preserve the point-in-time semantics above; otherwise the
  cross-cluster bootstrap may lose entries committed during the
  drain window.

### Receiver-side adapter

`RemoteSnapshotProvider` is the canonical receiver-side
`IBootstrapSnapshotSource` implementation. The bootstrap state
machine drains from `IBootstrapSnapshotSource`; `AddLatticeReplication`
registers a factory that resolves the seam to `RemoteSnapshotProvider`
when an `IRemoteSnapshotTransport` is registered alongside it (the
active-active default), or to a local snapshot-source wrapper
over the silo's local `ISnapshotProvider` otherwise (the
single-cluster recovery path). The silo's local `ISnapshotProvider`
is untouched in either case, so a silo that is also a snapshot
sender keeps serving outbound requests from peer receivers via
`LatticeRemoteSnapshotService` while its own bootstrap drains from
the registered transport.

| Type | Purpose |
|------|---------|
| `IBootstrapSnapshotSource` | Receiver-side seam consumed by the bootstrap state machine. Split from `ISnapshotProvider` so sender and receiver roles can coexist on a single silo without one DI slot overwriting the other. |
| `RemoteSnapshotProvider` | Cross-cluster `IBootstrapSnapshotSource`. Calls `IRemoteSnapshotTransport.GetMetadataAsync` once to capture the sender-side cut-point, then drains `RequestSnapshotAsync` and yields each entry through the existing `SnapshotStream` shape. Stateless and safe for concurrent invocation across distinct `(treeName, sourceClusterId)` pairs. |
| Local snapshot-source wrapper | Single-cluster default `IBootstrapSnapshotSource`. Forwards both `ExportAsync` overloads to the silo's local `ISnapshotProvider`. |

#### Semantics

- **Three-arg overload only.** The cross-cluster adapter implements
  only the three-arg `ExportAsync(treeName, sourceClusterId, asOfHlc, ct)`
  overload; the legacy two-arg overload throws
  `InvalidOperationException` because the adapter cannot address a
  sender peer without the sender cluster id. The bootstrap coordinator
  always invokes the three-arg overload with the value read from
  the coordinator's persisted source-cluster id, so this branch is
  unreachable in the normal flow and indicates an integration bug
  when it fires.
- **Metadata-then-stream consistency.** The adapter calls
  `GetMetadataAsync` once and pins the returned `AsOfHlc` and
  `CausalStableFrontier` on the `SnapshotStream` it returns; the
  entries on that stream come from a paired `RequestSnapshotAsync`
  call against the same `(treeName, sourceClusterId, fromAsOfHlc)`
  tuple. The transport contract guarantees the two calls describe
  the same snapshot, so the receiver-side state machine pins a
  point-in-time frontier even though the metadata and stream RPCs
  are separate.
- **Transport-agnostic.** The adapter knows nothing about the
  concrete binding (gRPC, in-process loopback, custom HTTP); the
  host wires the binding by registering an `IRemoteSnapshotTransport`
  singleton in DI alongside `AddLatticeReplication`.
- **Active-active by default.** A silo that registers an
  `IRemoteSnapshotTransport` becomes both a sender (serving outbound
  snapshot requests via `LatticeRemoteSnapshotService`, which drives
  the local `ISnapshotProvider`) and a receiver (draining inbound
  snapshots through `RemoteSnapshotProvider`, which drives the
  registered transport). The two seams do not collide: each grain
  consumes its own DI slot. Hosts that want to force the local-only
  bootstrap path even with a transport present can pre-register a
  custom `IBootstrapSnapshotSource` before `AddLatticeReplication`
  and the default factory becomes a no-op.

Sample wiring on a silo that bootstraps from a peer cluster
(registering the transport is sufficient - the bootstrap seam
auto-flips to the cross-cluster adapter):

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

siloBuilder.AddLatticeReplication(opts => opts.ClusterId = "site-b");

// The concrete transport binding the host plugs in (gRPC, custom
// HTTP, or a test-only loopback) implements IRemoteSnapshotTransport
// against the sender cluster. Registered as a singleton so the
// adapter resolves a stable instance per silo. A real implementation
// connects to the sender's binding endpoint; the no-op shown here
// stands in for the host-supplied implementation in this snippet.
// Registering the transport is sufficient: AddLatticeReplication's
// IBootstrapSnapshotSource factory observes the registration and
// resolves the seam to RemoteSnapshotProvider automatically.
siloBuilder.Services.AddSingleton<IRemoteSnapshotTransport>(_ =>
    throw new NotImplementedException("Plug in your transport binding."));
```

A reference for the receiver-side wiring lives in the replication
test project's `RemoteSnapshotProviderIntegrationTests`, which uses
an in-process transport stub to round-trip the receiver-side adapter
against the real sender-side `LatticeRemoteSnapshotService` running
on a peer cluster.

## gRPC binding

The `Orleans.Lattice.Replication.Grpc` package ships the canonical
`IRemoteSnapshotTransport` binding alongside the live-push
`IReplicationTransport`. Both transports share a single options
type, one peer-URI map, and the same TLS, shared-secret, and
channel-reuse conventions.

| Type | Role |
|------|------|
| Client-side gRPC snapshot transport | Client-side `IRemoteSnapshotTransport` that hosts the cross-cluster `GetMetadataAsync` unary call and the `RequestSnapshotAsync` server-streaming call. One gRPC channel is cached per `sourceClusterId` for the transport's lifetime, built through the same hardened channel pipeline (TLS gate and shared-secret call credentials) as the live-push transport, which keeps its own channel cache. |
| Sender-side gRPC snapshot service | Sender-side ASP.NET Core gRPC service that delegates each call to the local `LatticeRemoteSnapshotService` (which in turn drives the host-registered `ISnapshotProvider`). |
| `LatticeReplicationGrpcOptions` | Per-peer endpoint map (`Peers`), TLS-required-by-default gate (`AllowPlaintextEndpoints`), optional per-channel configuration hook (`ConfigureChannel`), and an override for the local cluster id used in outbound headers. The same options instance drives both the live-push transport and the snapshot transport. |

A silo can simultaneously serve outbound snapshot requests *and*
bootstrap its own tree from a peer; the default composition is
active-active. A single helper pair wires both directions:

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

siloBuilder.AddLatticeReplication(opts => opts.ClusterId = "site-b");

// Cross-cluster gRPC binding: registers both the live-push
// IReplicationTransport (outbound batches) and the snapshot
// IRemoteSnapshotTransport (bootstrap pulls). Registering the
// snapshot transport auto-flips the receiver-side
// IBootstrapSnapshotSource seam to the cross-cluster adapter so
// the bootstrap state machine drains through gRPC; the sender-side
// ISnapshotProvider remains the local tree.
siloBuilder.Services.AddLatticeReplicationGrpc(opts =>
{
    opts.Peers["site-a"] = new Uri("https://snap.site-a.example/");
});

// In the ASP.NET Core endpoint composition:
// app.MapLatticeReplicationGrpc();
```

A silo that only sends snapshots (it never bootstraps from a peer)
leaves `Peers` empty; a silo that only receives snapshots omits the
`MapLatticeReplicationGrpc` endpoint mapping. The active-active
default is opt-in by registration, not by a separate role flag.

The shared-secret authentication interceptor that gates the existing
`orleans.lattice.replication.LatticeReplication` push service also
recognises the `orleans.lattice.replication.LatticeRemoteSnapshot`
service (and the `orleans.lattice.replication.LatticeSaga` control
channel) and enforces on every RPC shape - unary, server-streaming,
client-streaming, and duplex - so the same shared-secret credential
(`LATTICE_REPLICATION_SECRET` on the sender, checked against the
receiver's accepted set, `LATTICE_REPLICATION_ACCEPTED_SECRETS`, or a
custom `ILatticeReplicationSecretSource`) and the same
`LatticeReplicationSecurityOptions.RequireAuthentication` switch cover
inbound snapshot calls without additional wiring. With
`LatticeReplicationSecurityOptions.BindCredentialToOriginCluster` on
(the default), the interceptor also refuses a snapshot call that
carries no `x-lattice-replication-origin` header, or whose presented
secret is not the one the exporting cluster would itself send to the
named origin - see
[Configuration](configuration.md#transport-security---latticereplicationsecurityoptions).

The client translates `RpcException(StatusCode.Cancelled)` raised while
the caller's own cancellation token is cancelled into the canonical
`OperationCanceledException`, so receivers can rely on the
same cancellation contract whether the transport is the gRPC binding,
the in-process loopback, or a host-supplied custom binding.

## Sample usage

```csharp verify
using Orleans.Lattice;

ISnapshotProvider provider = client.ServiceProvider.GetRequiredService<ISnapshotProvider>();
SnapshotStream snapshot = await provider.ExportAsync("orders", HybridLogicalClock.Zero, cancellationToken);

await foreach (SnapshotEntry entry in snapshot.Entries.WithCancellation(cancellationToken))
{
    // Apply each entry on the receiver. Use the entry's commit-time
    // Timestamp so transitive replication paths (A -> B -> C) preserve
    // the originating HLC.
    _ = entry.Key;
    _ = entry.Value;
    _ = entry.Timestamp;
}

VersionVector frontier = snapshot.CausalStableFrontier;
HybridLogicalClock asOf = snapshot.AsOfHlc;
_ = (frontier, asOf);
```

In a host the `ISnapshotProvider` is resolved from DI on the sender
side; the default snapshot provider is shown above for illustration. The
receiver merges the snapshot's `CausalStableFrontier` into its
per-tree high-water-mark store after draining the entry stream, then
drains the durable causal-apply buffer, so the causal dependency check
on the first incremental entry after the handoff runs from a non-empty
frontier.

## Receiver-side bootstrap state machine

The bootstrap state machine that drains an `ISnapshotProvider` export
on the receiver, applies every entry through the local apply seam
preserving the source HLC, and merges the snapshot's causal-stable
frontier into the per-tree high-water-mark grain ships as the public
`ILatticeBootstrapCoordinator` seam. Triggered by the receiver-side local fall-off
detector (when the per-tree maintenance pass finds a peer's per-origin
high-water mark behind the oldest entry that peer authored in the head
window of the local WAL partitions), by the source shipper's sequence-gap
re-seed request after a sender WAL trim, and by operator-driven re-seed flows.
See [Auto-Bootstrap](auto-bootstrap.md).

| Type | Shape | Purpose |
|------|-------|---------|
| `LatticeBootstrapState` | `enum` with members `Idle`, `RequestingSnapshot`, `ApplyingSnapshot`, `IncrementalHandoff`, `LiveIncremental`, `Failed` | The state machine's observable position for a single tree. |
| `BootstrapCoordinatorStatus` | `readonly record struct (LatticeBootstrapState Phase, string? SourceClusterId)` plus `ReadFenced`, `EntriesApplied` and `RedriveAttempts` | Observable status snapshot returned by `GetStatusAsync`; carries the phase, the in-flight source cluster id (or `null` when no bootstrap is in flight), whether the tree's reads are fenced, how many snapshot entries the current drain attempt has applied, and how many times a failed bootstrap has been re-driven (see [Read fence during the drain](#read-fence-during-the-drain)). Answered while a drain runs. |
| `ILatticeBootstrapCoordinator` | `Task<LatticeBootstrapState> GetStateAsync(string treeName, CancellationToken ct)` + `Task<BootstrapCoordinatorStatus> GetStatusAsync(string treeName, CancellationToken ct)` + `Task BootstrapAsync(string treeName, string sourceClusterId, CancellationToken ct)` | Public facade over the per-tree bootstrap coordinator grain. Registered as a singleton by `AddLatticeReplication`; the state machine itself lives in a per-tree internal grain whose cluster-wide single activation provides cross-silo mutual exclusion. |
| `LatticeBootstrapTransientFaultClassifier` | `public static class` exposing `bool IsTransient(Exception)` | Default classifier consumed by the bootstrap drain's bounded-retry seam. Returns `true` for `TimeoutException`, `HttpRequestException`, `SocketException`, `IOException`, `LatticeTreeBootstrappingException` (the source tree is itself mid-bootstrap), Orleans' `EnumerationAbortedException` (an expired cross-grain enumeration session), aggregate wrappers of those, and gRPC `RpcException` carrying `Unavailable`, `DeadlineExceeded`, or `Aborted`. Hosts can compose this with a custom predicate via `LatticeReplicationOptions.BootstrapTransientRetry.RetryableExceptionClassifier`. |

### State transitions

```text
Idle
  -> RequestingSnapshot     (BootstrapAsync invoked; ExportAsync issued)
     -> ApplyingSnapshot    (snapshot stream open; draining Entries)
        -> IncrementalHandoff (entries drained; merging CausalStableFrontier)
           -> LiveIncremental (terminal - incremental replication is live)

Any state -> Failed         (any thrown exception; restart is a fresh BootstrapAsync call)

Failed -> RequestingSnapshot (automatic re-drive of a drain that failed part-way
                              through an import; the tree stays read-fenced)
```

### Semantics

- **Kickoff-and-poll API.** `BootstrapAsync` is an idempotent
  kickoff: it persists the bootstrap intent, schedules the
  background phase pump on a 2-second grain timer plus a 1-minute
  keepalive reminder, and returns. Callers poll `GetStateAsync`
  for progress. This avoids the 30-second Orleans RPC timeout for
  long-running snapshot drains and decouples caller liveness from
  the bootstrap workflow.
- **One bootstrap per tree at a time, cluster-wide.** The state
  machine is hosted in an internal per-tree Orleans grain,
  so every silo's
  `BootstrapAsync` call for a given tree id routes to the same
  activation. The grain reads the persisted `InProgress` flag on
  entry: a concurrent call from the same source cluster is a no-op
  (idempotent retry); a concurrent call from a different source
  cluster throws `InvalidOperationException`. No distributed lock
  or external coordination is required - Orleans' single-activation
  invariant plus the durable in-progress flag is the synchronisation
  primitive. Concurrent bootstraps of different trees route to
  different activations and run in parallel.
- **Durable, crash-resumable state.** The grain uses the same
  keepalive-reminder plus phase-timer coordinator pattern as
  the core tree-resize coordinator. Phase, source cluster id,
  and a `LastAppliedHlc` cursor are persisted to the
  `LatticeOptions.StorageProviderName` storage provider. After a
  silo crash, Orleans reactivates the grain on a surviving silo
  within the keepalive reminder period and the phase pump resumes
  from the persisted phase. During `ApplyingSnapshot` the cursor
  (the highest source HLC applied so far) is persisted every 100
  entries. On resume the coordinator re-opens the export with no upper
  bound (`HybridLogicalClock.Zero`), exactly as a fresh drain does. The
  cursor is never passed as the export's `asOfHlc`:
  `ISnapshotProvider.ExportAsync` treats that argument as a strict upper
  bound, not as a resume point, and emits entries in leaf-chain order
  rather than HLC order, so a resume bounded at the cursor would silently
  drop every not-yet-applied entry stamped above it. The cursor feeds only
  the source-origin seal pinned at the handoff. Re-applying the entries the
  interrupted drain already applied is a correctness no-op, because the
  receiver-side LWW
  reconciliation on each leaf grain (plus the per-leaf recently-terminal
  short-circuit and the per-tx registry no-op described under "Bootstrap
  drain bypasses the high-water-mark advance" below) absorbs it. A
  fresh `BootstrapAsync` kickoff resets the cursor
  to `Zero`.
- **`Failed` is restartable.** On any thrown exception inside the
  phase pump the state transitions to `Failed` (persisted). A drain
  that failed before applying any snapshot entry tears the pump down,
  and a subsequent `BootstrapAsync` call restarts the cycle from
  `RequestingSnapshot`. A drain that failed part-way through an import
  keeps the tree read-fenced and is re-driven automatically instead
  (see [Read fence during the drain](#read-fence-during-the-drain)).
- **Bounded retry on transient transport faults.** When the
  snapshot drain throws an exception classified as transient by
  `LatticeReplicationOptions.BootstrapTransientRetry.RetryableExceptionClassifier`
  (default: `LatticeBootstrapTransientFaultClassifier.IsTransient` -
  `TimeoutException`, `HttpRequestException`, `SocketException`,
  `IOException`, `EnumerationAbortedException`, aggregate wrappers, and
  gRPC `RpcException` carrying
  `Unavailable`, `DeadlineExceeded`, or `Aborted`), the coordinator
  retries the drain in-place using a bounded exponential backoff
  (default: `DefaultBootstrapMaxAttempts = 4` attempts, initial delay
  `500 ms`, capped at `30 s`). Each retry re-opens the export with no
  upper bound (the resume rule above); receiver-side LWW reconciliation
  is what makes re-applying the entries the failed attempt already
  applied a correctness no-op. Every retry increments
  the `orleans.lattice.replication.bootstrap.transient_retries`
  counter (`LatticeReplicationMetrics.BootstrapTransientRetries`) so
  operators can dashboard the rate. Non-transient faults still pivot
  to `Failed` on the first failure exactly as they did before this
  seam landed; budget exhaustion re-throws the captured transient and
  pivots to `Failed` as the terminal outcome. Set
  `BootstrapTransientRetry.MaxAttempts = 1` to disable retries
  entirely (fail-fast).
- **A deferred entry is never skipped.** The applier can defer an entry
  (`ApplyResult.Deferred`): a cross-cluster restore saga's receive fence
  has paused inbound apply for the tree, or the entry duplicates one still
  in flight on the receiver. Nothing else re-sends a snapshot row, so the
  drain stops at the first deferred entry, without counting it or folding
  its clock into the handoff seal, and fails the attempt. The deferral
  consumes a slot of the same `BootstrapTransientRetry` budget whatever the
  classifier says, and each retry re-opens the full export; once the budget
  is spent the bootstrap fails with the import started, so the tree stays
  read-fenced and is re-driven until a drain applies every entry. The
  handoff is never pinned past a deferred entry
  ([#4604](https://github.com/NSTA1/Orleans.Lattice/issues/4604)).
- **Source HLC + origin preservation.** Every snapshot entry is
  applied through `IReplicationApplier.ApplyAsync`, the same canonical
  inbound apply seam used by live-incremental replication, carrying
  the entry's commit-time `Timestamp` and the supplied
  `sourceClusterId`. On a last-writer-wins tree the receiver keeps that
  `Timestamp`, so transitive replication paths (A -> B -> C) preserve
  the originating HLC; a typed-CRDT row folds through a state-based
  merge that the receiver writes at a fresh local HLC.
- **Bootstrap and live-incremental share the apply seam.** Routing
  the snapshot drain through `IReplicationApplier` means every
  decorator stacked on the applier - dead-letter tracking and any
  host-supplied per-key change observer - fires identically for
  bootstrap-arrived entries and live-incremental entries. (Bootstrap
  entries carry no vector clock, so the applier's causal-dependency
  gate never parks them in the causal-apply buffer.) A receiver that catches up via bootstrap
  therefore raises the same observable side-effects as a receiver
  that catches up via the WAL tail, so UI live-update hooks and
  audit observers see the bootstrap window rather than missing it.
- **Bootstrap drain bypasses the high-water-mark advance.** The
  applier reads an ambient bootstrap-apply flag on every inbound call;
  when the flag is set (the bootstrap coordinator opens one scope
  around the entire drain) the post-apply high-water-mark advance and
  the steady-state `orleans.lattice.replication.apply.fifo_violations`
  tracker are skipped. This is required because the snapshot exporter
  enumerates shards/leaves in arbitrary order rather than HLC order:
  per-shard HLCs are not globally monotonic across a single bootstrap
  stream, so advancing the high-water-mark mid-drain can suppress a
  still-pending saga key with a strictly-earlier source HLC and break
  per-saga all-or-nothing visibility on the bootstrapped peer; and
  feeding those out-of-order HLCs to the FIFO diagnostic would register
  every out-of-order shard arrival as a violation. The drain is still
  idempotent end-to-end because:
  - **Receiver-side LWW** on each leaf grain reconciles concurrent
    arrivals of the same key by HLC, with a replica-invariant
    tie-break (tombstone, expiry, then value bytes) ahead of the
    origin id, so a re-applied snapshot entry that has already been
    delivered is a no-op rather than an over-write.
  - The per-leaf recently-terminal short-circuit on
    the leaf grain suppresses a re-arriving saga terminal whose
    bucket has already drained, so saga-terminal re-delivery is
    correctness-preserving.
  - The per-tree transaction registry's "repeat-same-outcome no-op"
    drops a commit/abort mark that matches a transaction id already
    in the requested terminal state. Note the bound: that guarantee
    holds only while the decision is still recorded. Once the tree has
    forgotten the saga and its tombstone has been physically pruned,
    the registry has nothing left to recognise the repeat against and a
    late redelivery records the verdict afresh. The safe redelivery
    window is `LatticeOptions.TxDecisionRetention`, not "forever".

  The post-drain bootstrap handoff atomically raises the per-origin
  high-water-mark vector by pointwise maximum with the snapshot's
  causal-stable frontier, clears any legacy pinned floor, and drains
  the durable causal-apply buffer. The bootstrap-to-incremental
  handoff remains idempotent on the live tail because exact identity
  dedup and per-key merge absorb duplicates, while point writes at or
  below one pinned coordinate still apply when they are not duplicates.
  Range deletes, saga terminal records (which carry the saga's own
  terminal HLC), and tombstone-reap envelopes are routed before the
  point-write path, so the bootstrap scope does not change how they
  apply.
- **Live-incremental dedup is unchanged except for the removed floor.**
  The applier suppresses exact re-delivery of bootstrap-arrived entries
  through the shadow-forward identity cache, and an entry whose cache
  tuple has aged out re-applies idempotently under the leaf-level merge.
- **Snapshot/incremental handoff is idempotent.** The coordinator
  merges the snapshot's causal-stable frontier into the per-tree
  high-water-mark store *after* every snapshot entry has been
  applied, first sealing the source cluster's own coordinate at or
  above the highest HLC the drain applied and the oldest
  source-authored entry the local WAL still retains, so the receiver-side
  fall-off detector cannot read the retained baselines as a local trim gap. The
  merge takes the pointwise maximum with the vector already held,
  clears any legacy pinned floor (the `AsOfHlc` passed alongside it is
  currently ignored), and drains the durable causal-apply buffer. The
  applier has no HLC floor gate; exact identity dedup and idempotent
  per-key merge make the snapshot/incremental boundary safe regardless of
  overlap.
- **Committed tombstone rows apply as deletes.** A committed
  (non-prepared) row with `IsTombstone` set is applied as a delete at
  the row's HLC, and a prepared row with `IsTombstone` set as a prepared
  delete. A committed row whose `Value` is `null` and which does not set
  `IsTombstone` (not emitted by the default provider, but permissible
  from a host-supplied `ISnapshotProvider`) is skipped, as is a prepared
  row with an empty `TransactionId`.
- **Per-tree merge mode is honoured on bootstrap.** Every
  `WalRecord` emitted by the bootstrap drain is stamped with the
  merge mode `ILatticeMergeModeResolver` resolves for the tree - the
  `LatticeReplicationOptions.ReplicatedTrees` declaration, or the
  runtime configuration on a host that enables it. Trees declared as
  `LatticeMergeMode.OrSet` or `LatticeMergeMode.PnCounter` therefore
  merge bootstrap-arrived entries under the tree's CRDT semantics: a
  committed-projection row carries the primitive's full state, which the
  receiver folds through the primitive's state-based merge rather than
  the per-entry delta fold live-incremental entries use. A tree the
  resolver returns no mode for (and a tree declared as
  `LwwRegister`) is stamped `LatticeMergeMode.LwwRegister`; for a tree
  the resolver returns no mode for, the receiver's own enrollment gate
  then drops every stamped entry as not enrolled, so such a drain applies
  nothing. The
  mode is resolved once at the start of the drain, not per entry:
  the resolver is on the hot path's allocation budget but is
  invariant for the lifetime of a single drain.

### Read fence during the drain

The drain applies the export one row at a time. A committed atomic batch
arrives as independent committed rows: once its terminal has drained at
the source, nothing in the export identifies the rows as one saga. A reader
part-way through the drain could therefore see some of a batch's keys
post-batch and the rest pre-batch, or absent (issue #4526). To prevent that,
the coordinator **fences the tree's reads for the whole drain**:

1. Before it applies the first entry, it arms a durable read fence on every
   shard of the copy the tree routes to.
2. While the fence is up, every read of the tree is refused with
   `LatticeTreeBootstrappingException`. Readers are therefore either refused
   or see the whole import, never part of it.
3. After the last entry is applied, it lifts the fence on every shard, and
   the import becomes visible at once.

**What is refused.**

- Point and multi-key reads, existence checks, counts, and key and entry
  scans.
- The read-modify-write verbs whose outcome depends on the current value:
  `GetOrSetAsync`, version-conditional writes, and predicate writes.
- The snapshot baseline capture that backups and snapshot cursors are built
  from.

**What still applies.** Plain writes, deletes, range deletes, replication
applies (the drain itself, and live incremental replication from any
peer), saga prepares and terminals, and maintenance. Live replication
interleaved with the drain becomes visible at the lift, with the import.

`LatticeTreeBootstrappingException` derives directly from `Exception`
and carries the refused `TreeId`. It is **transient**: back off and retry.
The gRPC data and state APIs map it to `StatusCode.Unavailable`.

**How long it lasts.** The fence lasts for the duration of the drain,
which is roughly proportional to the size of the tree. On a fresh receiver
the tree holds nothing worth reading yet. On an **in-place re-bootstrap**,
where a receiver that fell off the log re-bootstraps over its existing
copy, a tree that was readable becomes **unreadable for the whole drain**.
That trade-off is deliberate: a correct, retryable refusal is better than
a torn read. A bootstrap into a shadow copy that keeps the receiver
readable is tracked as #4567.

**Watching a drain.** `ILatticeBootstrapCoordinator.GetStatusAsync`
answers while a drain runs. Its `ReadFenced` field reports the fence,
`EntriesApplied` reports the current attempt's progress, and
`RedriveAttempts` counts automatic re-drives.

**Migrations, resizes and undos are held.** The fence covers the shards
that existed when it was armed, so nothing may move the tree off them
while it is up:

- A fenced shard refuses to open a split or consolidation, so a reshard
  made of them is refused too.
- A resize of a fenced tree is refused, and so is an undo of a completed
  resize.
- If the coordinator finds a migration, resize or undo already in
  progress, it waits for it before draining, re-checking every tick. While
  it waits, it lifts a fence that hides no partial import.

Each side publishes its own state before reading the other's: the fence
first, or the migration record or resize intent first. Whichever of two
racing starts reads second therefore sees the first. A probe that cannot
answer counts as a hold.

**A failed drain keeps the fence.** A drain that fails after applying part
of an import leaves the tree holding that partial import, so the fence stays
up. The bootstrap stays in progress, reports `Failed`, and is **re-driven
automatically**: it re-exports and re-drains the whole snapshot, with a
backoff that starts at 5 seconds and doubles to a 5-minute cap. The fence
lifts when a drain completes. A bootstrap from a different source cluster
may take over a failed, fenced one. A drain that fails before applying any
entry lifts the fence and fails as before. If that lift itself fails, the
fence is kept and the bootstrap re-driven instead, so a fence is never left
up with no coordinator to lift it.

**Operator override.**
`ILatticeReplicationAdmin.ForceLiftBootstrapReadFenceAsync(treeName, reason)`
lifts the fence a failed bootstrap left up and stops its automatic re-drive.
It is an **alarmed** override, never a recovery path, and works as follows:

- **Consequence.** Reads may then observe the partial import - a committed
  batch with some keys present and others missing or stale - until a later
  bootstrap completes.
- **When it is refused.** While a drain is running.
- **Failure.** It fails closed: a shard that cannot be lifted leaves the
  fence up and the call throws.
- **Audit.** It requires a reason. Every call is audit-logged at `Warning`
  before it is dispatched, and every lift increments
  `orleans.lattice.replication.bootstrap.read_fence_force_lifted`.
- **Exposure.** Like the re-seed verbs on the same seam, it is available
  to host code only and is not exposed by any network API.

```csharp verify
ILatticeReplicationAdmin admin = client.ServiceProvider
    .GetRequiredService<ILatticeReplicationAdmin>();

BootstrapCoordinatorStatus status = await client.ServiceProvider
    .GetRequiredService<ILatticeBootstrapCoordinator>()
    .GetStatusAsync("orders", cancellationToken);

if (status.Phase == LatticeBootstrapState.Failed && status.ReadFenced)
{
    // Exposes the partial import to readers until a later bootstrap completes.
    bool lifted = await admin.ForceLiftBootstrapReadFenceAsync(
        "orders", reason: "source cluster decommissioned; re-seeding from site-b", cancellationToken);
    _ = lifted;
}
```

### Source lineage gate

When a whole-tree drain opens, the coordinator records the source lineage the export opened under for that source, together with the receiver tree frontier's epoch at the time ([#4673](https://github.com/NSTA1/Orleans.Lattice/issues/4673)). The record is durable before the first entry applies. It is written again wherever the aligned lineage is written, and it is kept when the reconcile skips.

From then on, the receiver refuses an entry stamped by that source in two cases:
- the batch is stamped with any other source lineage (see [Source lineage stamp](replication-drivers.md#source-lineage-stamp));
- the receiver's frontier epoch has moved since the drain. The receiver's own contents were then replaced, for example by a coordinated restore cutover, and the drain no longer describes the tree.

A refused batch is not accepted. Its ack sets `ReplicationAck.SourceLineageRefused`, and the batch is counted on `orleans.lattice.replication.apply.source_lineage_refused`. The sender's cursor holds, so nothing is lost:
- A sender whose binding is stale rebinds and never re-sends the old log.
- A sender whose binding is current re-seeds the peer, and the new drain records the current lineage.

The check runs at the applier's admission seam (`ReplicationSourceLineageGate.AdmitAsync`), which every apply entry passes through, so it covers more than the push that delivered a batch ([#4707](https://github.com/NSTA1/Orleans.Lattice/issues/4707)). An entry that parks in the causal-apply buffer, or is dead-lettered, keeps the stamp it arrived under. It is checked again when the buffer drains it or an operator replays it, against the lineage the tree has drained by then:
- The drain discards a refused entry. It belongs to a lineage the tree no longer replicates, as a refused push would have.
- A refused replay returns `ApplyResult.SourceLineageRefused` and leaves the entry parked for the operator to discard.
- A failure to read the record keeps a drained entry parked, and defers a replay.

The gate refuses more than the reconcile strictly needs, because a lineage cannot be ordered and a refusal only costs a re-seed. A batch with no stamp (a sender that predates the header), or from a source this tree never drained, applies as before. A failure to read the record refuses the batch for now without asking for a re-seed.

### Sample usage

```csharp verify
ILatticeBootstrapCoordinator coordinator = client.ServiceProvider
    .GetRequiredService<ILatticeBootstrapCoordinator>();

await coordinator.BootstrapAsync("orders", sourceClusterId: "site-a", cancellationToken);

LatticeBootstrapState state = await coordinator.GetStateAsync("orders", cancellationToken);
_ = state; // LatticeBootstrapState.LiveIncremental once the bootstrap completes
```


## Operator-driven re-seed

Beyond the receiver-side local auto-bootstrap path (`ILatticeFallOffLogDetector`) and the sender-side trim-gap request path, the package exposes an explicit operator-facing entry point for scheduled bootstraps - a new peer joining, a bandwidth-constrained initial sync, or a post-disaster re-bootstrap. The seam is `ILatticeReplicationAdmin.RequestSnapshotAsync`; honoured requests delegate to the same `ILatticeBootstrapCoordinator.BootstrapAsync` driving the automatic paths, so every re-seed - operator-driven, detector-driven, or sender-requested - flows through one state machine.

| Type | Shape | Purpose |
|------|-------|---------|
| `ILatticeReplicationAdmin` | `Task<OperatorReseedDecision> RequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken ct)` | Public facade that gates the request behind a per-`(tree, sourceClusterId)` rate limit before delegating to the bootstrap coordinator. |
| `ILatticeReplicationAdmin` | `Task<OperatorReseedDecision> ForceRequestSnapshotAsync(string treeName, string sourceClusterId, CancellationToken ct)` | Opt-in bypass that skips the rate-limit check entirely. Intended for disaster-recovery and scheduled re-seed scenarios where a real cross-cluster drain may exceed the configured window. Every call is audit-logged at `Information`. |
| `OperatorReseedDecision` | `readonly record struct` with `Triggered`, `LastRequestedAt`, `RetryAfter` | Diagnostic return value indicating whether the call invoked the coordinator and, when denied, how long the operator should wait before retrying. Both overloads share this return shape. |

### Semantics

- **Per-`(tree, sourceClusterId)` rate limit.** `LatticeReplicationOptions.OperatorReseedMinInterval` (default `1 minute`) bounds the minimum gap between honoured requests for the same pair. A second request inside the window returns `Triggered = false` with `RetryAfter` set to the remaining time; the coordinator is not invoked and no exception is thrown. `TimeSpan.Zero` disables the rate limit entirely (every request reaches the coordinator).
- **Process-local rate-limit table.** The default implementation tracks honoured requests in process memory only; a silo restart resets the rate-limit window for every pair. Cross-silo coordination is not required because `ILatticeBootstrapCoordinator` is itself idempotent under concurrent invocations against the same tree from the same source cluster (the per-tree internal grain absorbs the second call as a no-op) and rejects mismatched-source concurrent kickoffs as `InvalidOperationException`. The rate limit is therefore a fairness mechanism, not a correctness one.
- **Timestamp updates only on success.** The dictionary timestamp is stamped only after the coordinator call returns successfully, so a thrown coordinator exception (transport failure, conflicting in-flight bootstrap from a different source) does not consume the rate-limit budget against the operator. The same rule applies to both the rate-limited overload and the bypass overload.
- **Per-tree options resolution.** The minimum interval is resolved per-tree via `IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`, so different replicated trees can run different re-seed cadences without separate seam instances.
- **Argument validation.** `treeName` and `sourceClusterId` must be non-null and non-empty (`ArgumentNullException` when `null`, `ArgumentException` when empty); the cancellation token is observed before the rate-limit check and propagated to the underlying coordinator. The bypass overload validates identically.

### Force-bypass semantics

The rate limit was originally sized assuming the underlying snapshot drain is intra-cluster and fast. Once cross-cluster transport is in play, a real re-seed against a large tree may exceed the configured window, and a routine retry would be denied even though the previous call's drain has not completed. `ForceRequestSnapshotAsync` is the escape hatch for that case.

- **Always reaches the coordinator (on success).** When the coordinator call returns, `Triggered = true` unconditionally; the bypass overload never returns a denied decision. `RetryAfter` is always `null`.
- **Audit-logged on every call.** A `LogLevel.Information` line tagged `FORCE` and carrying both `tree` and `sourceClusterId` is emitted **before** the coordinator dispatch, so a log-tailing operator sees the bypass even if the coordinator call subsequently throws.
- **Stamps the dictionary on success.** A successful bypass updates the rate-limit dictionary timestamp so a follow-up routine `RequestSnapshotAsync` inside the window correctly observes the bypass as the last honoured request. This preserves the operator's mental model that the limiter knows about every actual re-seed, not just the rate-limited ones.
- **Failure does not consume the rate-limit budget.** A coordinator exception leaves the dictionary unchanged so a follow-up routine call is still honourable.

### Sample usage

```csharp verify
ILatticeReplicationAdmin admin = client.ServiceProvider
    .GetRequiredService<ILatticeReplicationAdmin>();

OperatorReseedDecision decision = await admin.RequestSnapshotAsync(
    "orders", sourceClusterId: "site-a", cancellationToken);

if (!decision.Triggered)
{
    // Rate-limited: the operator should wait `decision.RetryAfter` before
    // retrying. The previously honoured request is still driving the
    // bootstrap coordinator if one was kicked off recently.
    _ = decision.RetryAfter;
    _ = decision.LastRequestedAt;
    return;
}

// Triggered: poll the coordinator for state-machine progress.
LatticeBootstrapState state = await client.ServiceProvider
    .GetRequiredService<ILatticeBootstrapCoordinator>()
    .GetStateAsync("orders", cancellationToken);
_ = state;
```

### Sample usage (force bypass)

```csharp verify
ILatticeReplicationAdmin admin = client.ServiceProvider
    .GetRequiredService<ILatticeReplicationAdmin>();

// Disaster recovery: the previous routine re-seed against a large
// cross-cluster tree is still draining and we need to retry under
// a different sourceClusterId without waiting for the configured
// OperatorReseedMinInterval window. ForceRequestSnapshotAsync
// always reaches the coordinator and audit-logs at Information.
OperatorReseedDecision decision = await admin.ForceRequestSnapshotAsync(
    "orders", sourceClusterId: "site-b", cancellationToken);

// On success, Triggered is always true and RetryAfter is null.
// A coordinator exception (e.g. a conflicting in-flight bootstrap
// against a different source) propagates verbatim; catch and
// inspect at the caller.
_ = decision.Triggered;
_ = decision.LastRequestedAt;
```

## Snapshot and in-flight atomic visibility

The default snapshot provider preserves cross-cluster saga atomic
visibility across the bootstrap boundary: a saga whose prepare-commit
pair straddles the producer's snapshot cut is observed by the
bootstrapped peer either at every key or at none, never at a strict
subset.

The export operates in two passes against a single frozen view of the
producer's tree-wide transaction-registry decisions, unioned across every
registry shard of the tree, followed by the tombstone pass described under
[Semantics](#semantics):

1. **Prepared rows pass (runs first).** Walks every shard's leaf
   chain and emits a `SnapshotEntry` with `IsPrepared = true` for
   every `(transactionId, key)` pair in any leaf's per-tx pending
   bucket whose registry status in the captured snapshot is
   `InFlight`, `Indeterminate`, or absent (an `Indeterminate` saga whose
   decision is still stored is first resolved to it; see below). The emitted row carries the
   source-stamped
   prepare-time HLC verbatim, plus `IsTombstone`, `TransactionId`,
   `ExpiresAtTicks`, and the typed CRDT `Delta` / `Mode` so the
   receiver can route it identically to a steady-state prepared WAL
   record and a prepared CRDT entry folds its delta on the terminal
   commit.

2. **Committed projection pass.** Drains the source tree's entries
   via the resilient `ScanEntriesAsync` wrapper over
   `ILattice.EntriesAsync`, under the same frozen registry scope.
   Sagas the snapshot recorded as
   `Committed` surface their prepared value as the live one; sagas
   recorded as `Aborted` are dropped; sagas still `InFlight` (or
   `Indeterminate`) against
   the snapshot are hidden from the committed scan because the
   prepared rows pass above has already shipped them. Each emitted row
   carries the key's value, commit-time HLC, and absolute
   `ExpiresAtTicks` from the same per-key version read. The wrapper
   matters here because the export is long-running and latency-prone:
   a per-key version read and a (potentially proxied, cross-cluster)
   stream write interleave between pulls, so the source grain
   enumerator can idle-expire or be reclaimed by a deactivation
   mid-stream. `ScanEntriesAsync` recovers from the resulting
   `EnumerationAbortedException` and deterministically resumes from
   the last yielded key, so a reclaimed enumerator is re-opened
   instead of aborting the whole snapshot stream (which would fail the
   receiver's bootstrap drain and leave its high-water-mark pinned,
   re-triggering the fall-off detector on its next tick).

The receiver replays prepared rows through
the receiver-side prepared-set and prepared-delete apply paths
into its per-tx pending bucket. The
matching terminal record - delivered subsequently by the
post-snapshot incremental WAL stream - flips visibility atomically
via the transaction-terminal apply path and the receiver's local
transaction-registry linearization point.

**Ordering matters.** The prepared rows pass runs before the
committed projection pass because a source-side terminal that drains
a pending bucket between the two would otherwise erase the saga from
the export entirely: the committed pass under the frozen snapshot
would hide the saga (the snapshot still says `InFlight`), the
prepared pass would find the bucket already drained, and the
prepare-time WAL records - stamped at HLC <= `asOfHlc` - would never
re-arrive through the post-snapshot incremental stream (which starts
at `asOfHlc`). Capturing prepared rows first guarantees every
`InFlight` saga's per-key state is shipped to the receiver's pending
bucket, and the incremental stream delivers the terminal record to
flip visibility.

**An aged-out decision is shipped, not dropped.** A decision whose
tombstone has outlived `LatticeOptions.TxDecisionRetention` is carried
in the frozen snapshot as `Indeterminate` rather than omitted from it.
Omission used to fold two different facts onto one reading - "no
decision was ever recorded" and "a decision was recorded and the source
is no longer entitled to report it". Locally that collapse was
invisible, but in this export it was load-bearing in the wrong
direction: an aged-out `Committed` saga reached the receiver as
absence, was read as still preparing, and could never be corrected,
because the terminal that would have flipped it was already behind the
incremental stream the receiver drains after the snapshot. The source
held "committed", the receiver held "preparing", permanently, with no
repair path on either side. Carrying the row explicitly means absence
in this payload once again means only what it says.

**The verdict behind an aged-out row is exported, not the mask.** The
prepared rows pass resolves an `Indeterminate` saga whose decision the
registry still stores to that recorded verdict
(`ITxRegistryGrain.GetRecordedStatusAsync`), once per saga, before it
emits any of the saga's rows. Both passes then treat the saga as
decided. A recorded commit ships as committed rows, and a recorded abort
ships nothing. The prepared rows pass emits a recorded commit's committed
rows itself, because the committed projection pass need not enumerate a
key held only in a pending bucket. A delete the saga committed ships as a
committed tombstone row, which the bootstrap drain applies as a delete,
rather than as an absence: a bootstrap can land on a receiver copy that
still holds the key (a peer that fell off the log re-bootstraps over its
existing copy, which the drain does not clear), and an absence would leave
that older value beside the saga's other keys. Shipping such a saga as prepared rows split it on the
receiver (#4481): when its terminal had drained some keys before the
decision aged out and left another bucket stranded, the drained keys
arrived as committed rows and the stranded one as a prepared row. The
receiver's registry has no row for the saga, so it reads that prepared
row as still in flight and serves its pre-saga value beside the drained
keys' post-saga values, and nothing on either side repairs it. The
recorded verdict is what the source's own leaf sweep finishes the
stranded prepare by, so the receiver now holds the state the source
converges to. The source's read path keeps masking the row: the export
transfers state the source owns rather than disclosing an outcome to a
reader. Two cases still ship as prepared rows:

- An `Indeterminate` saga with no stored verdict, such as a cross-tree
  delegation whose coordinator could not be reached.
- A saga whose row has already been purged reads as absent. Its export
  carries the split the source itself serves (#4508).

A saga the producer's registry recorded as `Committed` before the
snapshot is naturally folded into the committed projection by the
leaf scan's pending-transaction read step - which honors
the frozen registry scope - so the receiver observes the post-saga
value directly without a separate prepared/terminal round trip. A
bucket of such a saga that the terminal has not yet drained is also
emitted as a committed row by the prepared rows pass, because the scan
need not enumerate a key held only in a pending bucket. The
same applies in reverse for `Aborted`: the prepared mutation is
correctly dropped from the committed pass and not shipped as a
prepared row. A `Committed` saga's pending **delete** is the exception
to the fold: the committed pass reads the deleted key as absent and
emits nothing, so the prepared rows pass ships it itself as a committed
tombstone row (`IsTombstone` set, `IsPrepared` clear, at the prepare's
HLC). Otherwise a receiver re-bootstrapping over a copy that still held
the key would keep its older value beside the saga's other keys (#4504).

**Decision rows: pre-cut saga records re-shipped after the bootstrap.**
The source shipper resumes from its own per-partition cursors after a
bootstrap. A peer that fell off the log resumes from the oldest entry
the source still retains, which can sit below the snapshot's cut. A
saga's prepares and its terminals live in different partitions, each
trimmed to its own floor. So the stream can re-ship a prepare from
before the cut whose terminal was already trimmed (#4482). Staged on
the receiver, that prepare would wait in a pending bucket for a
terminal that never comes.

The export therefore ends with one **decision row** per saga the
source still stores a decision for. A decision row has no key or
value; it carries the transaction id and `SettledDecision` (`true` for
a commit). An aged-out row is resolved to its recorded verdict first,
whether or not it has a resident bucket. The drain records each
outcome in the receiver's transaction registry and does not forget
it. Re-shipping a long retained tail can outlast the receiver's
decision retention, and a prepare arriving after the row was purged
would strand again. The receiver cannot yet observe the stream passing
the export's cut, which is what would make retiring the row safe, so
it retains one registry row per saga the source stored at the export
([#4524](https://github.com/NSTA1/Orleans.Lattice/issues/4524) tracks
retiring them).

The receiver then settles a replicated prepare against any decision
its registry already holds, instead of staging it (the read uses the
recorded verdict behind an aged-out row):

- A commit is applied as a committed write at the prepare's source
  clock. Last-writer-wins keeps it below any newer write on the key,
  and it is a no-op over the snapshot row.
- An abort is dropped.

A receiver that predates the decision slot sees a row with no value
that is neither prepared nor a tombstone, which its drain skips.

**Cross-tree sub-sagas.** A tree's part of a cross-tree atomic write (see
[Cross-tree terminals](replication-apply.md#cross-tree-terminals-receiver-barrier))
is settled on a receiver by a barrier that waits for every participating
tree's terminal. An import of one participant settles that tree's
sub-saga from the export instead, and its terminal may never be shipped
again: it is often trimmed, which is why the peer fell off the log. So
([#4683](https://github.com/NSTA1/Orleans.Lattice/issues/4683)):

- **The export names the operation.** The authoring tree's transaction
  registry records each sub-saga's cross-tree operation id and
  participating trees when it parks prepared, and keeps them for exactly
  as long as it stores the decision. Every decision row and prepared row
  of such a sub-saga carries them.
- **The drain records the arrival.** For a decision row that names an
  operation, the drain records the tree's arrival at the receiver's barrier
  for it, with the row's verdict, as the tree's terminal would. The wait
  set is the participants replicated here, plus the tree. If that completes
  the barrier, every participant is finalized.
- **The tree stays fenced until the barrier decides.** The imported tree
  serves the sub-saga post-saga, while a sibling whose terminal has not
  arrived serves it pre-saga. So the drain does not lift the read fence
  while any barrier it arrived at is undecided. The bootstrap stays in its
  incremental-handoff phase, re-checks on each tick, and lifts the fence
  and completes once every one has decided.
  - The cost is availability: the imported tree is unreadable until the
    sibling's own terminal reaches this receiver.
  - A re-driven drain records the same arrival again, and a terminal of the
    tree shipped later overwrites it. Both are no-ops.
- **An import that names the operation nowhere can still arrive.** The
  origin keeps a cross-tree sub-saga's decision until every peer of every
  participant has acknowledged past it (see
  [Cross-tree decision purge hold](replication-drivers.md#cross-tree-decision-purge-hold)),
  but a decision purged before that hold existed reaches an export only as
  the sub-saga's committed rows
  ([#4684](https://github.com/NSTA1/Orleans.Lattice/issues/4684)). So at the
  end of every drain from an export the source served under the hold, the
  drain records the import in the tree's barrier index: the export's epoch
  and every cross-tree operation one of its rows named. It then has every
  barrier waiting for the tree re-evaluate. A barrier does the same whenever
  it opens or records an arrival, so the order of the import and the sibling's
  terminal does not matter. A tree that has not arrived arrives with its
  siblings' verdict - one operation has one verdict - when its latest import
  named the operation nowhere and its export opened after the operation's
  decision: the export's epoch is greater than the tree's **decision stamp**,
  the tree's export epoch the origin read after the decision was durable
  (see [Cross-tree decision purge hold](replication-drivers.md#cross-tree-decision-purge-hold)).
  An export that opened before the decision can predate the sub-saga's
  prepare, so its rows can be pre-saga, and the tree stays pending. An
  operation decided by a silo that predates stamping carries no stamps; the
  source serves no export while such a silo is up, so every export it served
  opened after that decision.
- **The tree stays fenced while any barrier waits for it.** The drain does
  not lift the read fence, and the bootstrap does not complete, while any
  barrier indexed under the tree is undecided, including one that opened
  after the drain while the fence was still up.

A saga whose decision the source has already purged cannot be
exported. The source never re-ships such a saga: its shipper's
[replay filter](replication-drivers.md#replay-filter-a-non-contiguous-stream-over-purged-sagas)
withholds it whole (#4533). A pending bucket the receiver staged for it
before a re-seed is cleared by that re-seed's drain (below). Any other
full bootstrap clears one too (#4692) - a tree re-added to replication,
which must come back holding no leftover bucket from the origin, among
them - but only for a saga that was already pending from the origin when
the export opened and that the export neither carries in flight nor
decides. The sender holds nothing back during such a bootstrap, so a saga
staged after the export opened is not stale: its terminal is still to
come, and it keeps its bucket. A saga the
source still knows but cannot settle (an `Indeterminate` row with no
recorded verdict) ships as a value-less row that names it with no
`SettledDecision`, so the receiver does not take it for a purged one;
a receiver that predates it skips the row like any other value-less row.

**Late decisions.** A saga snap0 had in flight can decide while the
passes run and drain some of its keys before they are read: those keys
reach the committed pass as plain values, while the rest still ship as
prepared rows. Were its terminal then trimmed, nothing would settle the
prepared rows on the receiver, which would serve the saga split
([#4627](https://github.com/NSTA1/Orleans.Lattice/issues/4627)). So
once the passes are done the export re-reads the decision of every saga
it shipped as prepared rows, and ships a decision row for each one that
decided meanwhile.

<a id="export-completion"></a>
**Completion: sagas that decide while the export reads.** The passes
read each leaf at its own instant, so a saga can decide between two of
those reads: one of its keys is read before it prepared there (pre-saga),
another after its terminal drained there (post-saga). A saga that started
after snap0 and staged its prepares after the prepared pass read their
leaves ships no prepared row at all, so no decision row names it either,
and the receiver would serve it split until the incremental stream
delivered its last source-shard terminal
([#4685](https://github.com/NSTA1/Orleans.Lattice/issues/4685)). The
export therefore:

1. Records a decision-purge hold on the tree
   ([`IWalPurgeHoldGrain`](replication-drivers.md#replay-filter-a-non-contiguous-stream-over-purged-sagas))
   before snap0, and releases it once the completion has read the log, so a
   saga that decides during the export is still recorded at close. A crashed
   export's hold is released by the tree's next export after 24 hours.
2. Captures every write-ahead-log partition's readable head (C0) before
   snap0.
3. Once the passes and the decision rows are done, takes a second
   registry snapshot (snap1) and then every partition's head (C1). Every
   saga decided in snap1 that snap0 did not have decided ships a decision
   row, and each one that committed also ships every prepare of it the log
   holds in [C0, C1), as a committed row at the prepare's own stamp. The
   segment is read page by page, keeping only those sagas' prepares.

Each saga then reaches the receiver whole:

- A saga still undecided at snap1 drained nothing before the passes
  ended, so its keys shipped pre-saga or as buckets, and the receiver's
  tally hides it until the stream settles it.
- A prepare below C0 of a saga that decided during the export was staged
  before the passes read its key. The passes therefore captured that key as
  its bucket, which the decision row settles, or as its drained value.
- A prepare at or above C0 precedes the saga's decision, snap1 and C1, so
  it ships committed.

Last-writer-wins at the prepare's stamp keeps the committed row above any
pre-saga value the passes shipped. Over a drained value it is a no-op,
because the stamp is the same. A typed CRDT prepare ships its delta, and
CRDT deltas are joins, so a key whose drained state already includes the
delta counts it once.

The export fails with a retryable `TimeoutException`, and the bootstrap
retries it whole, when a trim reached into [C0, C1) before the completion
read it or the tree moved to another physical copy while it was exported.

<a id="re-seed-stale-pending-clear"></a>
**Re-seed: clearing stale pending buckets.** A sender that took its
peer off the log ([forced gap](replication-drivers.md#forced-gap-a-peer-taken-off-the-log))
asks for a re-seed past an export epoch. The receiver records the
request on its bootstrap coordinator before it starts (or joins) the
bootstrap, and records it even with `AutoBootstrapOnFallOffLog` off,
so an operator's bootstrap serves it. While the request is recorded
the sender withholds every saga record, so a drain whose export
postdates the request knows every pending bucket it holds from that
sender was staged before the re-seed or re-staged by the export.
After the drain has applied the export, still behind the read fence,
it walks the tree's pending buckets from that sender and settles each
one durably, through a terminal mark on the shards that hold it:

- A saga the export carried as a prepared row (in flight at the cut)
  or as a value-less row (known but unsettled) is left staged for the
  terminal that follows the re-seed. A saga undecided at the cut is
  therefore never cleared, and commits after the re-seed.
- A saga the export carried as a decision row (including one that also
  shipped as prepared rows and decided while the export ran), or that
  this receiver's registry has decided, is drained by that decision: the
  source may already have trimmed its terminal, which no rewind can
  re-ship.
- Any other saga was decided and purged by the source. Its bucket is
  discarded with an abort mark that records no outcome in the
  receiver's registry, because the source may have committed it: its
  committed values, if any, arrived as committed rows.

The drain then consumes the request.

Two rules keep the window closed:

- **Stragglers are refused.** Until the drain has consumed the
  request, the receiver refuses, with a not-accepted ack, any pushed
  batch that carries a saga record from that sender. The sender is
  withholding saga records then, so such a batch was pushed before its
  marker and could otherwise stage a purged saga's prepare after the
  clear. The sender re-ships the batch's plain records.
- **No echo without the clear.** A bootstrap that completes while a
  request it did not consume is outstanding (one recorded after its
  drain finished) does not record its export epoch for the echo, so
  the sender keeps withholding until a drain has cleared.

**Visibility while the drain runs.** The drain installs committed rows
one at a time, so the import is made atomic for readers by the
[read fence](#read-fence-during-the-drain): every read of the tree is
refused until the drain has applied its last entry, and the whole
import becomes visible at once when the fence lifts. The decision rows
and the settle above keep each saga's outcome atomic across the import
itself. In particular, a prepared row the drain imports after the
saga's terminal has already reached the receiver through the live
stream is settled as a committed write, rather than refused as a late
prepare and left pre-saga beside its siblings (issue #4526).
Modelled in `AtomicCommitCrossCluster`: the real drain, with
incremental replication interleaved and the fence, is clean on every
property, and lifting the fence early or on failure fires
`RAllOrNothing`. For a cross-tree sub-saga the fence is held past the
drain until the receiver's barrier decides (see
[Cross-tree sub-sagas](#snapshot-and-in-flight-atomic-visibility) above),
which the cross-tree slice of the model checks clean.

