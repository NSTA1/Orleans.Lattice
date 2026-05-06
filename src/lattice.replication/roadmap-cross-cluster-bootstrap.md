# Cross-cluster bootstrap transport — scoped roadmap

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

Items use a fresh `R-1XX` numbering block to avoid collision with the
existing `R-050`–`R-093` snapshot/bootstrap items in the canonical roadmap.
**Item ordering is topological:** prerequisite items always have a lower
`R-1NN` number than the items that depend on them, so the implementation
order is the same as the numeric order.

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
// src/lattice.replication/LatticeSnapshotProvider.cs (~line 60)
await foreach (var entry in _grainFactory
    .GetGrain<ILattice>(treeName)
    .EntriesAsync(default, default, ct)
    .WithCancellation(ct))
{
    yield return new SnapshotEntry(entry.Key, entry.Value, ...);
}
```

This is correct for the *intra-cluster* snapshot-as-a-tool path
(`R-093 ✓ shipped`: an operator snapshots a tree, restores it later in
the same cluster, and seeds the local vector clock from the surviving
`LwwEntry.VectorClock` slots) — there the local tree IS the authoritative
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

> *Avoid hard-coding "remote peer" in the API surface — keep it*
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

For typed CRDTs replicated under `ReplicationMode.OrSet` /
`ReplicationMode.PnCounter` / `ReplicationMode.LwwRegister`, this means
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

- [ ] **R-100 — Cross-cluster `ISnapshotProvider` transport contract** *(no deps)*

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
  causalStableFrontier)` cut-point — captured atomically with the start
  of the stream so the receiver can `PinSnapshotAsync` correctly without
  requiring the sender to embed cut-point markers inside the entry stream.

  `IRemoteSnapshotTransport` is a separate seam from `IReplicationTransport`
  (which today carries live-incremental push only). Keeping them split
  lets a host plug a different binding for snapshot vs. live (e.g.
  HTTP/S3 for snapshot bulk, gRPC for live tail).

  **Acceptance:** contract-test fixture parameterised over a transport
  implementation, asserts metadata-then-stream is consistent under
  concurrent sender writes (snapshot is a point-in-time view of `asOfHlc`,
  not a moving target).

---

- [ ] **R-101 — Sender-side snapshot service handler** *(deps: R-100)*

  A service registered on the *sender* silo that responds to inbound
  `IRemoteSnapshotTransport.RequestSnapshotAsync` calls by invoking the
  **sender's** local `LatticeSnapshotProvider` against its own tree and
  streaming the entries back through the transport.

  The handler is independent of the transport binding — gRPC, in-process,
  or test-loopback can all reuse the same handler. Concrete bindings
  plug in via the transport's host-registration surface (`R-104` for gRPC).

  Sequenced before the receiver-side adapter (`R-102`) because the
  receiver-side integration test for `R-102` requires a working sender
  to round-trip against; landing the sender first lets `R-102`'s
  acceptance suite use the real handler instead of a hand-rolled stub.

  **Acceptance:** handler unit tests + a transport-agnostic loopback
  fixture asserting metadata-then-stream consistency under concurrent
  sender writes (correctness side of the `R-100` contract test, on the
  sender side).

---

- [ ] **R-102 — Receiver-side `RemoteSnapshotProvider` adapter** *(deps: R-100, R-101)*

  An `ISnapshotProvider` implementation that hosts can register *before*
  `AddLatticeReplication` to override the local-tree default. It calls
  `IRemoteSnapshotTransport.GetMetadataAsync` to obtain the cut-point,
  then `RequestSnapshotAsync` to drain the stream, and yields each entry
  through the existing `ISnapshotProvider.ExportAsync` shape so the
  receiver-side state machine (`R-051 ✓ shipped`) sees no behavioural
  change other than the entries actually arriving.

  **Shape:**

  ```csharp
  public sealed class RemoteSnapshotProvider : ISnapshotProvider
  {
      public RemoteSnapshotProvider(
          IRemoteSnapshotTransport transport,
          IRemoteSnapshotPeerResolver peerResolver, // string treeName -> string sourceClusterId
          ILogger<RemoteSnapshotProvider> logger);

      public Task<SnapshotStream> ExportAsync(
          string treeName,
          HybridLogicalClock fromAsOfHlc,
          CancellationToken ct);
  }
  ```

  The `IRemoteSnapshotPeerResolver` indirection is required because the
  receiver-side state machine knows the *tree* it is bootstrapping but
  not the *cluster id* of the canonical sender; that mapping is a host
  deployment concern (one peer per tree in a hub-spoke topology, multiple
  peers in a mesh) and must not be hard-coded into the package.

  Listed as depending on `R-101` (not just `R-100`) because the
  acceptance suite below round-trips against the real sender-side
  handler rather than a hand-rolled stub, which catches metadata /
  stream contract drifts a stub would mask.

  **Acceptance:** integration test under `Orleans.TestingHost` with two
  clusters: cluster A pre-populated with N entries, cluster B fresh.
  Configure cluster B's `ISnapshotProvider` as `RemoteSnapshotProvider`
  with a transport stub that round-trips against cluster A's `ILattice`.
  Trigger auto-bootstrap on cluster B, assert all N entries arrive,
  assert `localHwm` advances past zero, assert no further fall-off
  detection fires for the next 30 seconds.

---

- [ ] **R-103 — `LatticeBootstrapCoordinatorGrain` routes snapshot drain through `IReplicationApplier`** *(deps: R-102)*

  Internal-seam change: `DrainSnapshotAsync` switches from
  `IReplicationApplyGrain.ApplySetAsync(...)` to
  `IReplicationApplier.ApplyAsync(...)` so every host decorator (dead-letter
  tracking, causal buffer, host-supplied per-key change observers) fires
  identically for bootstrap-arrived entries and live-incremental entries.

  This is gated on `R-102` because it only becomes a user-visible change
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

- [ ] **R-104 — gRPC binding for `IRemoteSnapshotTransport`** *(deps: R-100, R-101, R-103)*

  Concrete `IRemoteSnapshotTransport` implementation in
  `Orleans.Lattice.Replication.Grpc`, mirroring the existing
  `GrpcPushTransport`'s deployment shape:

  - New `Bootstrap.proto` defining `GetMetadata` (unary) and
    `RequestSnapshot` (server-streaming) RPCs.
  - New `GrpcRemoteSnapshotTransport` (client) and
    `GrpcRemoteSnapshotService` (server handler invoking the `R-101`
    service).
  - Host-registration extension
    `LatticeReplicationGrpcServiceCollectionExtensions.AddGrpcRemoteSnapshotTransport(...)`,
    symmetric with `AddGrpcPushTransport(...)`.
  - Reuses the same `GrpcPushTransportOptions`-style configuration shape
    (TLS, deadline, channel reuse).

  Listed as depending on `R-103` (the apply-through-decorator change),
  not just on the transport-abstraction items, because the first
  cross-cluster bootstrap that actually delivers payload over the wire
  must exhibit the intended decorator-fan-out behaviour or the
  decorator-side regression will ship to operators alongside the
  transport.

  **Acceptance:** end-to-end gRPC-backed integration test with two
  `TestCluster`s wired via gRPC; cluster B bootstraps from cluster A
  through the wire. Reuses the `R-102` integration test scaffolding.

---

- [ ] **R-105 — Auto-bootstrap rate limit + concurrency floor** *(no new deps; refines `R-051 ✓ shipped` / `R-052 ✓ shipped`)*

  Independent of the transport work but observable only once the transport
  work lands. Today `LatticeFallOffLogDetector.CheckAndTriggerAsync`
  delegates idempotent kickoff to `ILatticeBootstrapCoordinator`
  (per-tree, per-source-cluster mutex via grain activation) — but a
  malformed `senderOldestAvailableHlc` source could trigger a fall-off
  detection on every probe. In the current pre-transport state this
  manifests as the harmless infinite-no-op loop seen in the
  `MultiSiteManufacturing` sample. Once payload starts flowing it becomes
  a real problem: a still-draining bootstrap is interrupted by a fresh
  trigger that the coordinator absorbs as idempotent, but the metric
  (`PeerFellOffLog`) inflates and operator alerts misfire.

  **Fix:** introduce a per-`(treeName, sourceClusterId)` minimum interval
  on the *detector* (not the coordinator), default 30s, configurable via
  `LatticeReplicationOptions.AutoBootstrapMinInterval`. Detector-side
  suppression returns `BootstrapTriggered = false` and emits a new
  `PeerFellOffLogSuppressed` counter so operators can tell "didn't
  detect" from "detected and suppressed".

  **Acceptance:** detector unit tests (within-window suppressed, after-window
  honoured, per-tree + per-source independence, `TimeSpan.Zero` disables
  suppression entirely). New options + validator (negative rejected,
  zero allowed, positive allowed). New metric.

---

- [ ] **R-106 — Bootstrap progress observability** *(deps: R-102, R-103)*

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

- [ ] **R-107 — Operator-facing "force re-bootstrap" admin RPC widening** *(deps: R-102, refines `R-053 ✓ shipped`)*

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

## 4. Non-goals

- **Cross-cluster compaction frontier coordination.** Today each cluster
  GCs its WAL by `min(local consumer cursor, ttl ceiling)`
  (`R-061 ✓ shipped` + `R-083 ✓ shipped`). A receiver that has fallen
  off and recovers via cross-cluster bootstrap does not pin the sender's
  WAL while it bootstraps; the sender can continue compacting under
  the existing predicate. This is fine because the bootstrap stream
  is point-in-time at `asOfHlc`, and incremental delivery resumes from
  the snapshot's `causalStableFrontier`. Not in scope here.

- **Cross-cluster transport authentication / authorization.** The gRPC
  binding (`R-104`) reuses the existing `GrpcPushTransportOptions` TLS
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

Item ids are assigned in topological order so the numeric order is the
implementation order:

`R-100` → `R-101` → `R-102` → `R-103` → `R-104` → `R-105` → `R-106` → `R-107`.

`R-100` (the contract) blocks everything else. `R-101` (the sender
handler) lands next so `R-102`'s acceptance suite can round-trip against
a real handler rather than a stub. `R-103` (route the bootstrap drain
through `IReplicationApplier`) lands before `R-104` (the gRPC binding)
so the first cross-cluster bootstrap that actually delivers payload
over the wire exhibits the intended decorator-fan-out behaviour.
`R-105`, `R-106`, and `R-107` are quality-of-service refinements that
become observable only once the core transport is real and can land in
any order after `R-104`; they are listed in numeric order purely for
convenience and do not depend on each other.
