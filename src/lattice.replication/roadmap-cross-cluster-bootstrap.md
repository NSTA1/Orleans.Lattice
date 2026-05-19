# Cross-cluster bootstrap transport - scoped roadmap

This file is a **scoped roadmap** that sits alongside the canonical
[`roadmap.md`](./roadmap.md). It documents a gap in the snapshot/bootstrap
story that surfaced during end-to-end testing of the
`samples/MultiSiteManufacturing` two-cluster federation, and proposes a
prioritised set of new features to close it.

The file is named `roadmap-cross-cluster-bootstrap.md` (rather than appended
to the canonical roadmap) because the work it describes is a single
concern, large enough to warrant its own design + acceptance scope, and
not yet committed to any release. When the work lands, the items here
will graduate to `roadmap.md` (or a future Phase 9 "Cross-cluster
bootstrap transport" section) and this file will retain only a pointer
to the resolved entries.

Items use a fresh `R-15X` numbering block to avoid collision with the
existing `R-050`–`R-093` snapshot/bootstrap items and the in-flight
`R-094`–`R-104` atomic-batch / WAL-GC items in the canonical roadmap.
Numeric ids are assigned in roughly the order the items were drafted; the
authoritative implementation order is the sequencing chain in §5, not the
numeric order (in particular `R-158` sequences ahead of `R-154` because
`R-158` is a silent-correctness gate on first-payload-over-the-wire).

---

## 1. Problem statement

A receiver whose per-origin high-water mark is older than the sender's
oldest WAL entry triggers the auto-bootstrap path (`R-052 ✓ shipped`).
The auto-bootstrap path drives the receiver-side state machine
(`R-051 ✓ shipped`), which calls `ISnapshotProvider.ExportAsync` against
whichever provider is registered in DI.

The default `ISnapshotProvider` registered by `AddLatticeReplication`
(`LatticeSnapshotProvider`) reads the **local** tree:

```csharp
// src/lattice.replication/LatticeSnapshotProvider.cs (EnumerateAsync)
var lattice = _grainFactory.GetGrain<ILattice>(treeName);
await foreach (var pair in lattice
    .EntriesAsync(cancellationToken: cancellationToken)
    .ConfigureAwait(false))
{
    var versioned = await lattice
        .GetWithVersionAsync(pair.Key, cancellationToken)
        .ConfigureAwait(false);
    if (versioned.Value is null) continue;             // tombstoned mid-scan
    if (hasUpperBound && versioned.Version > asOfHlc) continue;
    yield return new SnapshotEntry
    {
        Key = pair.Key,
        Value = versioned.Value,
        Timestamp = versioned.Version,
    };
}
```

Note the per-entry `GetWithVersionAsync` round-trip: the snapshot entry's
`Timestamp` is the commit-time HLC recovered from a second RPC, not from
the `EntriesAsync` enumeration. The sender-side handler in `R-151` must
preserve that two-call shape (or expose a faster bulk equivalent), or
the shipped entry's `Timestamp` will fall back to `default(HybridLogicalClock)`
and the receiver's `PinSnapshotAsync` cut will be wrong.

This is correct for the *intra-cluster* snapshot-as-a-tool path
(`R-093 ✓ shipped`: an operator snapshots a tree, restores it later in
the same cluster, and seeds the local vector clock from the surviving
`LwwEntry.VectorClock` slots) - there the local tree IS the authoritative
source.

It is **not** correct for the *cross-cluster* bootstrap path. On a fresh
receiver in cluster B that has fallen off cluster A's WAL, the local
tree in cluster B is empty (or stale). `ExportAsync` yields zero entries.
The state machine completes the snapshot phase, pins
`SnapshotAsOfHlc = HybridLogicalClock.Zero` and an empty
`causalStableFrontier`, and transitions to `LiveIncremental`. The local
HWM stays at zero. The next maintenance probe trips fall-off again.
The cluster spins forever in a fall-off → auto-bootstrap → no-op-snapshot
loop and never receives a single user write across cluster boundaries.

### Why the existing roadmap does not close the gap

The canonical roadmap's snapshot/bootstrap items deliver:

| Item | Scope |
| --- | --- |
| `R-050 ✓ shipped` | The `ISnapshotProvider` *abstraction*. Default impl reads the local tree. |
| `R-051 ✓ shipped` | Receiver-side state machine. Consumes whichever `ISnapshotProvider` is in DI. |
| `R-052 ✓ shipped` | Auto-bootstrap trigger (already firing). |
| `R-053 ✓ shipped` | Operator-driven re-seed (delegates to the same coordinator + provider). |
| `R-084 ✓ shipped` | Snapshot cut-point semantics (`asOfHlc`, `causalStableFrontier`). |
| `R-088 ✓ shipped` | Bootstrap → incremental causal handoff verification. |
| `R-093 ✓ shipped` | Intra-cluster snapshot/restore VC reconstruction. |

`R-050`'s design intent is explicit on the missing piece (lines 442–443
of the canonical roadmap):

> *Avoid hard-coding "remote peer" in the API surface - keep it*
> *`ISnapshotProvider.ExportAsync(treeName, asOfHlc, ct)` and let the*
> *consumer decide what to do with the stream.*

In other words: the package deliberately ships only the seam. The
`Orleans.Lattice.Replication.Grpc` package ships the corresponding
*live-incremental* transport (`IReplicationTransport`), but does not
ship a cross-cluster *bootstrap-snapshot* transport. There is no
existing or in-flight roadmap item that supplies one. Hosts that need
working cross-cluster bootstrap today must register their own
`ISnapshotProvider` implementation and supply their own RPC plumbing.

This roadmap file proposes closing that gap inside the package family
so hosts get working cross-cluster bootstrap out of the box, with the
gRPC transport package shipping a default implementation.

---

## 2. Secondary finding: bootstrap path bypasses `IReplicationApplier`

Independent of the transport gap, the receiver-side bootstrap pump
(`LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync`) calls
`IReplicationApplyGrain.ApplySetAsync` directly per snapshot entry,
bypassing the canonical `IReplicationApplier` (and therefore every
decorator stacked on top of it: dead-letter, causal buffer, host-supplied
fan-out hooks).

For typed CRDTs replicated under `LatticeMergeMode.OrSet` /
`LatticeMergeMode.PnCounter` / `LatticeMergeMode.LwwRegister`, this means
host code that observes apply via a decorator (e.g. to fire UI live-update
events) only sees *live-incremental* entries, not bootstrap-arrived
entries. A receiver that catches up via bootstrap therefore picks up the
new state but does not raise the host's per-key change events, so any
attached observer that runs **only** in response to those events misses
a window.

The fix is mechanically small (route through `IReplicationApplier` for
the snapshot drain too) but is wire/contract-shaped because it changes
the observable side-effects of bootstrap. Treated as a first-class item
below.

---

## 3. Proposed features (priority + dependency order)

- [x] **R-150 ✓ shipped - Cross-cluster `ISnapshotProvider` transport contract** *(no deps)*

  Define the transport-shaped sub-interface that delivers a snapshot
  stream from a sender cluster to a receiver cluster. Pure abstraction
  plus a contract-test fixture; no concrete implementation in this item.

  **Shape:**

  ```csharp
  public interface IRemoteSnapshotTransport
  {
      IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
          string treeName,
          string sourceClusterId,
          HybridLogicalClock fromAsOfHlc,
          CancellationToken ct);

      Task<RemoteSnapshotMetadata> GetMetadataAsync(
          string treeName,
          string sourceClusterId,
          CancellationToken ct);
  }
  ```

  The metadata RPC returns the sender's current `(asOfHlc,
  causalStableFrontier)` cut-point - captured atomically with the start
  of the stream so the receiver can `PinSnapshotAsync` correctly without
  requiring the sender to embed cut-point markers inside the entry stream.

  **Atomic-batch coordination is deferred.** Earlier drafts of this item
  reserved a `SagaBlacklist` slot on the metadata DTO for forward-compat
  with the staging-buffer + quiesce-timeout shape originally proposed in
  the canonical `R-102` retrospective. That shape (`IReplicationTxBufferGrain`,
  `AtomicBatchDelivery`, `SnapshotSagaQuiesceTimeout`, per-saga blacklist)
  has since been **retired**: `test/lattice/DeletionMandateHygieneTests.cs`
  lists those identifiers as `DoomedIdentifiers` and fails the build if
  they reappear anywhere under `src/` or `test/`. The replacement
  universal-visibility primitive lands per-leaf (`_pendingTx`) and is
  surfaced through the receiver wire seams `ApplyPreparedSetAsync`,
  `ApplyPreparedDeleteAsync`, and `ApplyTxTerminalAsync` on
  `IReplicationApplyGrain`. How that primitive interacts with a
  cross-cluster bootstrap drain (in particular, what producer-side
  transactions are mid-prepare when `ExportAsync` is invoked, and how
  the receiver-side prepared state is reconstructed during snapshot apply)
  is **not yet a defined contract** in either the canonical roadmap or
  this scoped one. Rather than reserve a metadata slot whose semantics
  are unknown, this item ships without atomic-batch coordination; a
  follow-up item must define the bootstrap/atomic-visibility handoff
  against the actual `ITxRegistryGrain` + `_pendingTx` shape before any
  prepared-transaction metadata is added to `RemoteSnapshotMetadata` or
  to the `R-154` wire format. Until then, a producer running an in-flight
  multi-key transaction concurrent with a cross-cluster bootstrap may
  deliver a split view to the bootstrapping peer: this is a known
  limitation, not silently masked.

  `IRemoteSnapshotTransport` is a separate seam from `IReplicationTransport`
  (which today carries live-incremental push only). Keeping them split
  lets a host plug a different binding for snapshot vs. live (e.g.
  HTTP/S3 for snapshot bulk, gRPC for live tail).

  **Acceptance:** contract-test fixture parameterised over a transport
  implementation, asserts metadata-then-stream is consistent under
  concurrent sender writes (snapshot is a point-in-time view of `asOfHlc`,
  not a moving target).

---

- [x] **R-151 ✓ shipped - Sender-side snapshot service handler** *(deps: R-150 ✓)*

  A service registered on the *sender* silo that responds to inbound
  `IRemoteSnapshotTransport.RequestSnapshotAsync` calls by invoking the
  **sender's** local `LatticeSnapshotProvider` against its own tree and
  streaming the entries back through the transport.

  The handler is independent of the transport binding - gRPC, in-process,
  or test-loopback can all reuse the same handler. Concrete bindings
  plug in via the transport's host-registration surface (`R-154` for gRPC).

  Sequenced before the receiver-side adapter (`R-152`) because the
  receiver-side integration test for `R-152` requires a working sender
  to round-trip against; landing the sender first lets `R-152`'s
  acceptance suite use the real handler instead of a hand-rolled stub.

  **Acceptance:** handler unit tests + a transport-agnostic loopback
  fixture asserting metadata-then-stream consistency under concurrent
  sender writes (correctness side of the `R-150` contract test, on the
  sender side).

---

- [x] **R-152 ✓ shipped - Receiver-side `RemoteSnapshotProvider` adapter** *(deps: R-150 ✓, R-151 ✓)*

  An `IBootstrapSnapshotSource` implementation that drains snapshots
  from a peer cluster through a registered
  `IRemoteSnapshotTransport`. `AddLatticeReplication` registers a
  factory that resolves `IBootstrapSnapshotSource` to
  `RemoteSnapshotProvider` whenever an `IRemoteSnapshotTransport` is
  present in the same service collection (the active-active default),
  and to a `LocalBootstrapSnapshotSource` wrapper over the local
  `ISnapshotProvider` otherwise (the single-cluster recovery path).
  The seam is split from sender-side `ISnapshotProvider` so a single
  silo can simultaneously serve outbound snapshot requests via
  `LatticeRemoteSnapshotService` and bootstrap its own tree from a
  peer.

  **Shape (as shipped):**

  ```csharp
  public interface IBootstrapSnapshotSource : ISnapshotProvider { }

  public sealed class RemoteSnapshotProvider : IBootstrapSnapshotSource
  {
      public RemoteSnapshotProvider(
          IRemoteSnapshotTransport transport,
          ILogger<RemoteSnapshotProvider> logger);

      // Three-arg overload is the supported entry point. The legacy
      // two-arg overload throws because the adapter cannot address a
      // sender peer without the source cluster id.
      public Task<SnapshotStream> ExportAsync(
          string treeName,
          string sourceClusterId,
          HybridLogicalClock fromAsOfHlc,
          CancellationToken ct);
  }
  ```

  **Design choice (resolved):** contract widening over resolver
  indirection. `ISnapshotProvider` was widened with an additive
  default-interface overload
  `ExportAsync(treeName, sourceClusterId, asOfHlc, ct)` and
  `LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync` was updated to
  call the new overload, passing
  `state.State.SourceClusterId` directly. This eliminated the
  tree-to-peer-mapping-out-of-sync failure mode that an
  `IRemoteSnapshotPeerResolver` would have introduced; the
  intra-cluster `LatticeSnapshotProvider` inherits the default-impl
  overload, which validates the cluster id and delegates to the
  legacy two-arg overload, so the additive surface is non-breaking.

  **Composition choice (resolved):** active-active by default. The
  first cut of this entry exposed a `siloBuilder.AddRemoteSnapshotProvider()`
  helper that swapped the silo's sole `ISnapshotProvider`
  registration, forcing the host to choose sender-only or
  receiver-only at composition time. The shipped shape replaces that
  helper with an `IBootstrapSnapshotSource` seam separate from
  sender-side `ISnapshotProvider`, plus a DI factory in
  `AddLatticeReplication` that auto-selects the cross-cluster adapter
  when an `IRemoteSnapshotTransport` is registered. Hosts that want
  to force the local-only path can pre-register a custom
  `IBootstrapSnapshotSource` before `AddLatticeReplication`; the
  default factory then no-ops.

  **Acceptance (met):** integration test `RemoteSnapshotProviderIntegrationTests`
  under `Orleans.TestingHost` with two clusters: cluster A pre-populated
  with N entries (tree `rsp-bootstrap`, origin `rsp-site-a`), cluster B
  fresh. Cluster B's silo registers an in-process `IRemoteSnapshotTransport`
  stub that round-trips against cluster A's `LatticeRemoteSnapshotService`;
  `AddLatticeReplication`'s factory flips the bootstrap seam to
  `RemoteSnapshotProvider` automatically. Triggers auto-bootstrap on
  cluster B, asserts all N entries arrive and `localHwm` advances past
  zero.

---

- [x] **R-153 - `LatticeBootstrapCoordinatorGrain` routes snapshot drain through `IReplicationApplier`** *(no deps; landed ahead of R-152 as part of v4 readiness)*

  Internal-seam change: `DrainSnapshotAsync` switches from
  `IReplicationApplyGrain.ApplySetAsync(...)` to
  `IReplicationApplier.ApplyAsync(...)` so every host decorator (dead-letter
  tracking, causal buffer, host-supplied per-key change observers) fires
  identically for bootstrap-arrived entries and live-incremental entries.

  This is gated on `R-152` because it only becomes a user-visible change
  once cross-cluster bootstrap actually delivers entries. Today the
  bootstrap path drains an empty stream so the bypass has no observable
  effect; the moment payload starts flowing, the missing observer signal
  becomes a regression risk.

  **Compatibility:** the canonical `ReplicationApplier` already preserves
  the source HLC + origin id verbatim and routes through the per-origin
  HWM dedupe, so re-routing through it is correctness-preserving. The
  overflow-into-DLQ path becomes available for bootstrap-arrived entries
  the same way it is today for live-incremental entries.

  **Acceptance:** unit test that a `MaxApplyRetries`-failing snapshot
  entry parks in the DLQ via the decorator chain rather than throwing
  out of `BootstrapAsync`. Unit test that a host-registered
  `IReplicationApplier` decorator observes every snapshot entry exactly
  once. Existing bootstrap acceptance tests (`R-088 ✓ shipped`) re-pass.

---

- [x] **R-154 ✓ shipped - gRPC binding for `IRemoteSnapshotTransport`** *(deps: R-150 ✓, R-151 ✓, R-153 ✓)*

  Concrete `IRemoteSnapshotTransport` implementation in
  `Orleans.Lattice.Replication.Grpc`, mirroring the existing
  `GrpcPushTransport`'s deployment shape:

  - New `Bootstrap.proto` defining `GetMetadata` (unary) and
    `RequestSnapshot` (server-streaming) RPCs.
  - New `GrpcRemoteSnapshotTransport` (client) and
    `GrpcRemoteSnapshotService` (server handler invoking the `R-151`
    service).
  - Host-registration extension
    `LatticeReplicationGrpcServiceCollectionExtensions.AddGrpcRemoteSnapshotTransport(...)`,
    symmetric with `AddGrpcPushTransport(...)`.
  - Reuses the same `GrpcPushTransportOptions`-style configuration shape
    (TLS, deadline, channel reuse).

  Listed as depending on `R-153` (the apply-through-decorator change),
  not just on the transport-abstraction items, because the first
  cross-cluster bootstrap that actually delivers payload over the wire
  must exhibit the intended decorator-fan-out behaviour or the
  decorator-side regression will ship to operators alongside the
  transport.

  **Acceptance:** end-to-end gRPC-backed integration test with two
  `TestCluster`s wired via gRPC; cluster B bootstraps from cluster A
  through the wire. Reuses the `R-152` integration test scaffolding.
  If `R-102` / `R-103` have shipped, the suite extends `R-103`'s
  snapshot-during-saga atomic-visibility assertion across the gRPC
  transport: a producer running an in-flight `SetManyAtomicAsync` on
  cluster A while cluster B drains its bootstrap snapshot must observe
  either (a) all keys of the batch in the snapshot stream, or (b) zero
  keys in the snapshot plus all keys in the post-bootstrap incremental
  stream - never a partial-snapshot view that splits the batch on the
  bootstrapped peer.

  **Retrospective:** Shipped as a code-first gRPC binding rather than a
  `Bootstrap.proto`-driven generator pipeline, matching the
  `GrpcPushTransport` precedent. The new public surface in
  `Orleans.Lattice.Replication.Grpc`: `GrpcRemoteSnapshotTransport`
  (client `IRemoteSnapshotTransport`), `LatticeRemoteSnapshotGrpcService`
  (server-side handler delegating to the `R-151`
  `LatticeRemoteSnapshotService`), `GrpcRemoteSnapshotTransportOptions`
  (per-source-cluster endpoint map, TLS-by-default gate, channel-config
  hook), and three new DI helpers on
  `LatticeReplicationGrpcServiceCollectionExtensions`:
  `AddGrpcRemoteSnapshotTransport(...)`,
  `AddLatticeReplicationGrpcSnapshotServer()`, and
  `MapLatticeReplicationGrpcSnapshotService()`. Two new wire DTOs in the
  core replication package (`RemoteSnapshotMetadataRequest`,
  `RemoteSnapshotStreamItem`) carry the marshalled request and
  per-message payload through Orleans serializers, with stable aliases
  `olr.sr` / `olr.si`. The auth interceptor
  (`LatticeReplicationGrpcAuthInterceptor`) was widened to recognise the
  new `orleans.lattice.replication.LatticeRemoteSnapshot` service
  prefix and to enforce on both unary and server-streaming RPCs so the
  shared-secret gate covers the new binding identically to the push
  transport. The client transport translates
  `RpcException(StatusCode.Cancelled)` into the canonical
  `OperationCanceledException` so receivers can rely on a single
  cancellation contract regardless of binding. **Test coverage:** 140
  non-Chaos tests under `Orleans.Lattice.Replication.Grpc.Tests`. The
  gRPC-backed contract driver `GrpcRemoteSnapshotTransportContractTests`
  inherits the shared `RemoteSnapshotTransportContractTests` acceptance
  suite (linked into the gRPC test project as a `Compile` item rather
  than via a project reference to avoid double-discovery of the
  replication test fixtures) and runs every metadata, streaming,
  point-in-time, validation, and cancellation case across a
  `TestServer`-hosted `LatticeRemoteSnapshotGrpcService`. New unit
  fixtures pin the marshaller round-trip, the method-holder
  service/name slots, the client transport's argument-validation and
  disposal contracts, the options defaults, and the server service's
  validation surface. Full suite green: 140/140 gRPC + 1405/1405
  replication + 34/34 Azure Table (non-Chaos).

---

- [x] **R-155 - Auto-bootstrap fall-off observability under coordinator absorption** *(no new deps; refines `R-051 ✓ shipped` / `R-052 ✓ shipped`)* ✓ shipped

  Independent of the transport work but observable only once the transport
  work lands. Today `LatticeFallOffLogDetector.CheckAndTriggerAsync`
  unconditionally bumps `LatticeReplicationMetrics.PeerFellOffLog` and
  emits a `LogWarning` (lines 62-69) **before** calling into
  `ILatticeBootstrapCoordinator`, which already absorbs a duplicate
  same-source kickoff at
  `LatticeBootstrapCoordinatorGrain.TryInitiateBootstrapAsync` (the
  in-progress branch at lines 120-132 returns without persisting any
  state when the source cluster id matches). The coordinator-level
  idempotency therefore makes the trigger semantically harmless, but
  the detector-level metric and log fire on **every probe** while a
  drain is in flight. In the current pre-transport state this manifests
  as the harmless infinite-no-op loop seen in the `MultiSiteManufacturing`
  sample. Once payload starts flowing it becomes a real problem: a
  still-draining cross-cluster bootstrap may take minutes; every probe
  during the drain re-bumps `PeerFellOffLog` and re-emits the warning,
  inflating dashboards and misfiring operator alerts.

  **Fix:** narrow the detector so it consults the coordinator's
  `InProgress`-from-same-source state **before** bumping the metric and
  emitting the warning. When the coordinator reports "already in progress
  from `sourceClusterId`", the detector returns `BootstrapTriggered = true,
  Suppressed = true` and emits a new `PeerFellOffLogSuppressed` counter
  (so operators can distinguish "didn't detect" from "detected and
  coordinator already running"); `PeerFellOffLog` is not double-counted
  and the warning is downgraded to `Debug`. No new options or validator
  needed - the coordinator's existing per-tree single activation is the
  authoritative rate-limit, and this item is purely the
  observability-side fix that makes the existing idempotency visible to
  metrics consumers.

  **Acceptance:** detector unit tests (`Suppressed = false` when no
  drain is running, `Suppressed = true` when coordinator reports same-source
  in-progress, `PeerFellOffLog` increments exactly once per drain cycle
  across N probes within the drain, `PeerFellOffLogSuppressed` increments
  on every suppressed probe).

---

- [x] **R-156 - Bootstrap progress observability** *(deps: R-152 ✓, R-153 ✓)* ✓ shipped

  Three new instruments on the existing `orleans.lattice.replication` meter
  plus a structured log at each phase transition. Mirrors the per-peer
  observability pattern from `R-064 ✓ shipped`:

  - `bootstrap.entries_received` `Counter<long>` tagged `tree`+`source`,
    incremented per snapshot entry applied (post-decorator chain).
  - `bootstrap.bytes_received` `Counter<long>` tagged `tree`+`source`,
    incremented by `entry.Value?.Length ?? 0` per applied entry.
  - `bootstrap.duration` `Histogram<double>` (ms) tagged
    `tree`+`source`+`outcome`, recorded once per terminal state transition
    (`outcome` ∈ { `live`, `failed`, `timed_out` }).

  Plus a structured `LogInformation` at every phase transition
  (`Idle → RequestingSnapshot`, `→ ApplyingSnapshot`,
  `→ IncrementalHandoff`, `→ LiveIncremental`, `→ Failed`) carrying
  `treeName`, `sourceClusterId`, and the `LastAppliedHlc` cursor so an
  operator tailing the silo log can follow a single bootstrap run end
  to end.

  **Acceptance:** per-instrument unit tests under existing
  `LatticeReplicationMetricsTests` patterns. Phase-transition log tests
  under existing `LatticeBootstrapCoordinatorGrainTests` patterns.
  Documented in `docs/lattice.replication/observability.md` as a new
  "Bootstrap instruments" section.

---

- [x] **R-157 - Operator-facing "force re-bootstrap" admin RPC widening** *(deps: R-152 ✓, refines `R-053 ✓ shipped`)* ✓ shipped

  `R-053`'s `ILatticeReplicationAdmin.RequestSnapshotAsync(treeName,
  sourceClusterId, ct)` already routes through the bootstrap coordinator,
  but its rate-limit window (`OperatorReseedMinInterval`, default 1 minute)
  was sized assuming the underlying snapshot drain is intra-cluster and
  fast. Once the cross-cluster transport lands, a real cross-cluster
  re-seed against a large tree may exceed 1 minute of wall-clock time
  and a follow-up call within the window will be denied even though
  the previous call's drain has not completed.

  **Fix:** widen the admin shape with a `ForceRequestSnapshotAsync`
  overload that bypasses the rate limit (intended for
  disaster-recovery / scheduled re-seed scenarios) and returns the same
  `OperatorReseedDecision` shape. The default rate-limited
  `RequestSnapshotAsync` remains the call operators reach for routinely;
  the bypass overload is opt-in and audit-logged at `Information` level
  on every call.

  **Acceptance:** admin unit tests (bypass honoured within window, bypass
  audit-logs, both overloads share the `OperatorReseedDecision` return
  shape). Documented in `docs/lattice.replication/snapshot-bootstrap.md`
  under the existing "Operator-driven re-seed" section.

---

- [x] **R-158 - Bootstrap respects per-tree `LatticeMergeMode`** *(deps: R-152 ✓, R-153 ✓; sequence before R-154 ships payload)* ✓ shipped

  `LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync` constructs every
  bootstrap-arrived `WalRecord` with `Mode = LatticeMergeMode.LwwRegister`
  hardcoded (line 328 in the current source), regardless of the per-tree
  merge mode configured on `LatticeReplicationOptions.ReplicatedTrees`
  (`IReadOnlyDictionary<string, LatticeMergeMode>`, supporting `OrSet`,
  `PnCounter`, and `LwwRegister`). In the current pre-transport state
  the drain yields zero entries so the hardcode is invisible; the moment
  cross-cluster bootstrap actually delivers payload (`R-154`), an
  `OrSet`-mode tree on the receiver will merge incoming bootstrap entries
  under LWW semantics - a silent CRDT-correctness regression.

  **Fix:** resolve the merge mode from
  `IOptionsMonitor<LatticeReplicationOptions>.Get(treeName).ReplicatedTrees`
  in `DrainSnapshotAsync` and stamp `WalRecord.Mode` from that lookup,
  defaulting to `LatticeMergeMode.LwwRegister` when the tree is not
  enumerated in `ReplicatedTrees` (preserves the current behaviour for
  trees that bootstrap intra-cluster only).

  **Sequencing rationale:** this is a one-line change today, but it must
  land **before** `R-154` ships payload over the wire to avoid shipping
  the silent-correctness window to operators on the same release as the
  transport.

  **Acceptance:** unit test that a `ReplicatedTrees[treeName] =
  LatticeMergeMode.OrSet` configuration produces `WalRecord.Mode = OrSet`
  in the records passed to `IReplicationApplier.ApplyAsync` during
  `DrainSnapshotAsync`. Unit test that an unconfigured tree falls back
  to `LwwRegister`. Existing bootstrap acceptance tests re-pass.

---

- [x] **R-159 - Bootstrap drain resumes on transient transport faults** *(deps: R-154 ✓, R-156 ✓)*

  `LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync` is wrapped in a
  `try { ... } catch (Exception ex) { ... }` block (lines 205-260 of the
  current source) that persists `Phase = Failed`, tears down the
  keepalive reminder, and rethrows. Any exception from the snapshot
  drain - including transient gRPC faults from a `RemoteSnapshotProvider`
  built on `R-152` / `R-154` (typically `RpcException` with
  `StatusCode.Unavailable` from a transient peer-side connection drop) -
  therefore parks the bootstrap in `Failed` and requires operator
  intervention via `ILatticeReplicationAdmin.ForceRequestSnapshotAsync`
  (`R-157`) to retry.

  For large cross-cluster trees over real networks this is the wrong
  default: the existing per-origin HWM dedupe already makes a resumed
  drain idempotent (the `CursorPersistEntryInterval = 100`-entry cursor
  persistence at lines 342-346 caps the replay cost), so a bounded
  auto-retry inside `DrainSnapshotAsync` is strictly cheaper than
  surfacing every transient transport blip as a `Failed` phase.

  **Fix:** introduce a transient-fault classification seam (initially:
  `RpcException` with `StatusCode.Unavailable` / `DeadlineExceeded` /
  `Cancelled-not-from-caller-ct` for the gRPC binding) and wrap the
  drain with a bounded exponential-backoff retry inside the
  `ApplyingSnapshot` phase, configurable via existing
  `BoundedExponentialRetryPolicyOptions` style. Non-transient faults
  (`InvalidOperationException`, schema mismatches, applier-level DLQ
  exhaustion) still pivot to `Failed` as today.

  **Acceptance:** unit tests with a stub `ISnapshotProvider` that fails
  the first N drains with a classified-transient exception and succeeds
  on the N+1th; assert `Phase` never observes `Failed`, `LastAppliedHlc`
  advances monotonically, total replayed entries are bounded by
  `CursorPersistEntryInterval * N`. Unit test that a non-transient
  exception still parks `Failed` on the first failure.

---

- [x] **R-160 - Snapshot-bootstrap-time atomic visibility for in-flight sagas** *(deps: R-154 ✓; refines `F-055` receiver-side atomic visibility across the bootstrap boundary)* ✓ shipped

  Closes the "commit-during-export" window currently documented as a
  known limitation in `docs/lattice.replication/snapshot-bootstrap.md`
  ("Snapshot and in-flight atomic visibility" section). The shipped
  F-055 invariant proves all-or-nothing per-saga visibility on the
  **steady-state WAL pipeline** (prepared+terminal hops routed through
  `ApplyPreparedSetAsync` / `ApplyPreparedDeleteAsync` /
  `ApplyTxTerminalAsync` on `IReplicationApplyGrain`, with the
  receiver-side per-tree `ITxRegistryGrain` as the linearization
  point). The chaos suite
  `CrossClusterAtomicVisibilityChaosTests.Concurrent_cross_cluster_sagas_under_partition_remain_atomically_visible_on_every_site`
  exercises that path under partition cycling. F-055 deliberately did
  **not** preserve atomic visibility across the bootstrap boundary
  itself: the prior staging-buffer mechanism
  (`IReplicationTxBufferGrain` + `SnapshotSagaQuiesceTimeout` +
  per-saga blacklist) that backed snapshot-during-saga atomicity was
  retired in the same commit, along with the
  `AtomicBatchDeliveryChaosTests.SnapshotDuringSaga` /
  `SnapshotReplaceSemantics` /
  `LatticeBootstrapCoordinatorGrainTests.SagaBlacklist` test files
  that previously enforced it. The replacement is the documented
  operator-quiesce workaround.

  The remaining window is precise: `LatticeSnapshotProvider.EnumerateAsync`
  walks the producer's committed `Entries` projection per leaf via
  `ILattice.EntriesAsync` + `GetWithVersionAsync`. Prepared writes
  live in the per-tx pending bucket and are deliberately invisible to
  this enumerator; the terminal flip on the producer is the
  linearization point that moves a saga's writes from `_pendingTx`
  into `Entries` on each affected leaf. If a saga's terminal mark
  fires between leaf reads (e.g. L1 -> terminal-flip -> L2 -> L3),
  the snapshot captures L2/L3 keys but not L1 keys for that saga. The
  receiver's incremental phase eventually re-delivers the L1 keys
  (the per-origin HWM dedupe makes the L2/L3 overlap a no-op), but a
  bootstrapping reader briefly observes a partial view across that
  specific window.

  **Fix shape (sketch; design lands as part of the item):** pin a
  per-tree linearization point at the start of `ExportAsync` via the
  receiver-side seam used by `ITxRegistryGrain`, then either

  - (option A, exporter-side resolution) co-export each leaf's
    `_pendingTx` slice alongside its committed `Entries` projection
    plus the per-tree `ITxRegistryGrain` terminal-decision snapshot
    captured at the linearization point. The receiver replays each
    saga's prepared hops through `ApplyPreparedSetAsync` /
    `ApplyPreparedDeleteAsync` and the captured terminal decisions
    through `ApplyTxTerminalAsync`, so receiver-side visibility flips
    atomically per saga at apply time exactly as in the steady-state
    pipeline. This widens `SnapshotEntry` with prepared-state slots
    (`[Id(3..5)]` on `SnapshotEntry` or a sibling DTO) and adds a
    terminal-decisions section to the snapshot stream.

  - (option B, exporter-side cut at terminal-only frontier) read the
    per-tree `ITxRegistryGrain`'s "highest terminally-decided HLC at
    snapshot start" `T` and walk the committed projection at `T`. Any
    saga whose terminal mark on the producer lands at HLC > `T` is
    treated as "not in the snapshot" for every key (including keys
    whose terminal-flip on individual leaves already happened, by
    suppressing them via a per-saga filter on the export stream). The
    receiver gets a clean all-or-nothing-per-saga snapshot at `T` and
    picks up post-`T` sagas through the existing incremental WAL.

  Option B is the lower-blast-radius choice (no `SnapshotEntry`
  widening, no receiver-side prepared-replay path on the bootstrap
  drain) and is the working assumption for sequencing; option A is
  the alternative if the per-saga filter in option B turns out to be
  too costly under high saga churn. Either way the item must define
  the bootstrap/atomic-visibility handoff against the actual
  `ITxRegistryGrain` + `_pendingTx` shape - the deferred design that
  `R-150`'s "atomic-batch coordination is deferred" note flagged.

  **Acceptance:** new integration test
  `BootstrapAtomicVisibilityTests.Concurrent_producer_saga_during_bootstrap_is_atomically_visible_or_absent_on_the_bootstrapped_peer`
  under `Orleans.TestingHost` with the in-process loopback transport:
  producer authors `SetManyAtomicAsync` sagas continuously across a
  configurable key-range while a fresh receiver auto-bootstraps; the
  bootstrapped peer's post-handoff view (sampled mid-bootstrap and at
  steady state) must show every saga as either fully visible or
  fully absent, never a partial subset, matching the steady-state
  `CrossClusterAtomicVisibilityChaosTests` invariant across the
  bootstrap boundary. Stretch goal: also a Chaos-category variant
  under partition cycling that combines the two acceptance shapes
  into one fixture. Documentation update removes the
  "Snapshot and in-flight atomic visibility" workaround paragraph
  from `docs/lattice.replication/snapshot-bootstrap.md` and replaces
  it with the cross-bootstrap-boundary atomic-visibility invariant
  statement.

  **Sequencing rationale:** ships after `R-154` because it is observable
  only when payload flows over the wire (today's empty drain trivially
  satisfies the all-or-nothing predicate). Independent of `R-155`-`R-159`;
  composes with `R-159`'s bounded-retry shape because a transient-fault
  resumed drain must re-pin the same `T` (option B) or replay the same
  prepared-state slice (option A) to keep the per-saga predicate
  stable across the resume boundary.

---

## 4. Non-goals

- **Per-entry `OriginClusterId` and `VectorClock` preservation across
  bootstrap.** `SnapshotEntry` carries only `Key`, `Value`, and
  `Timestamp` (`[Id(0..2)]`); the bootstrap coordinator stamps every
  applied record with `OriginClusterId = state.State.SourceClusterId`
  and `VectorClock = null`
  (`LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync` lines 327-329).
  For a tree whose entries were originally authored by a third cluster
  and reach the bootstrapping peer via an intermediate sender, the true
  authoring origin is collapsed to the bootstrap sender's id. This is
  acceptable for the v1 transport because the per-origin HWM is keyed
  on `(treeName, sourceClusterId)` and the receiver's view of the
  bootstrap-source as the origin is internally consistent under live
  incremental delivery after handoff. Preserving per-entry origin and
  VC across bootstrap is a separate concern that requires extending
  `SnapshotEntry` with `[Id(3)] OriginClusterId` and `[Id(4)] VectorClock`
  slots and an `LwwEntry`-level VC slot in the core library, and is
  tracked elsewhere if needed.

- **Cross-cluster compaction frontier coordination.** Today each cluster
  GCs its WAL by `min(local consumer cursor, ttl ceiling)`
  (`R-061 ✓ shipped` + `R-083 ✓ shipped`). A receiver that has fallen
  off and recovers via cross-cluster bootstrap does not pin the sender's
  WAL while it bootstraps; the sender can continue compacting under
  the existing predicate. This is fine because the bootstrap stream
  is point-in-time at `asOfHlc`, and incremental delivery resumes from
  the snapshot's `causalStableFrontier`. Not in scope here.

- **Cross-cluster transport authentication / authorization.** The gRPC
  binding (`R-154`) reuses the existing `GrpcPushTransportOptions` TLS
  + bearer-token configuration shape. Per-tree ACLs (cluster A may
  bootstrap tree X but not tree Y from cluster B) are a separate
  concern, tracked elsewhere if needed.

- **Snapshot delta encoding / compression.** First-cut transport ships
  raw `SnapshotEntry` over the wire. Compression and delta encoding
  are pure perf concerns, plug into the same transport via a wrapping
  decorator, and are independent of the correctness work here.

- **Sample-side closure.** The `samples/MultiSiteManufacturing` sample
  can close its own gap independently by registering a sample-side
  `ISnapshotProvider` that uses its existing in-process
  `MultiClusterDirectTransport` to fetch from the peer cluster. That
  is sample-internal work and not gated on this roadmap.

---

## 5. Sequencing notes

`R-153` has already shipped (ahead of the rest of this scoped roadmap as
part of v4 readiness); the remaining items are sequenced as:

`R-150` → `R-151` → `R-152` → `R-158` → `R-154` → { `R-155`, `R-156`,
`R-157`, `R-159`, `R-160` }.

`R-150` (the contract) blocks everything downstream. `R-151` (the sender
handler) lands next so `R-152`'s acceptance suite can round-trip against
a real handler rather than a stub. `R-158` (per-tree merge-mode
propagation through the bootstrap drain) **must** land before `R-154`
because `R-154` is the first item that delivers payload over the wire,
and the current hardcoded `LatticeMergeMode.LwwRegister` would silently
corrupt `OrSet` / `PnCounter` trees the moment payload arrives. `R-155`,
`R-156`, `R-157`, `R-159`, and `R-160` are quality-of-service refinements
that become observable only once the core transport is real; they can
land in any order after `R-154` (`R-159`'s bounded-retry shape composes
naturally with `R-156`'s observability surface, and `R-160`'s
cross-bootstrap-boundary atomicity work composes naturally with
`R-159`'s resume contract, but neither chain is a hard ordering
requirement).
