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

- [ ] **R-151 - Sender-side snapshot service handler** *(deps: R-150 ✓)*

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

- [ ] **R-152 - Receiver-side `RemoteSnapshotProvider` adapter** *(deps: R-150 ✓, R-151)*

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

  The `IRemoteSnapshotPeerResolver` indirection is shown above as one
  way to recover the sender cluster id from the tree name, on the
  assumption that `ISnapshotProvider.ExportAsync` cannot itself receive
  the cluster id. That assumption is **worth re-examining before this
  item lands**: the receiver-side coordinator already has the value in
  hand on `BootstrapCoordinatorState.SourceClusterId`
  (`LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync` reads it at
  `state.State.SourceClusterId` before calling `_snapshotProvider.ExportAsync`).
  Two implementation shapes are therefore viable, and the choice must be
  made up front because it determines `R-150`'s contract:

  - **Resolver indirection (sketched above).** `ISnapshotProvider.ExportAsync`
    keeps its current `(treeName, asOfHlc, ct)` shape; `RemoteSnapshotProvider`
    injects `IRemoteSnapshotPeerResolver` to recover the sender id. Pro:
    no change to the public `ISnapshotProvider` surface. Con: hosts must
    keep the resolver and `LatticeReplicationOptions.ReplicationPeers` in
    sync; resolver indirection is invisible to the coordinator that
    already has the value.
  - **Contract widening.** Add an overload
    `ExportAsync(treeName, sourceClusterId, asOfHlc, ct)` to `ISnapshotProvider`
    (default-impl delegates to the existing overload, ignoring the new
    arg) and have `LatticeBootstrapCoordinatorGrain.DrainSnapshotAsync`
    call the new overload, passing `state.State.SourceClusterId` directly.
    Pro: removes the resolver DI requirement and the
    tree-to-peer-mapping-out-of-sync failure mode; the intra-cluster
    `LatticeSnapshotProvider` simply ignores the new arg. Con: additive
    public API surface on a v1-shipped interface.

  Recommendation pending: contract widening is the smaller-blast-radius
  shape because it eliminates a class of misconfiguration entirely and
  the additive overload is non-breaking. Settle this before `R-150`
  freezes.

  Listed as depending on `R-151` (not just `R-150`) because the
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

- [ ] **R-154 - gRPC binding for `IRemoteSnapshotTransport`** *(deps: R-150 ✓, R-151, R-153 ✓)*

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

---

- [ ] **R-155 - Auto-bootstrap fall-off observability under coordinator absorption** *(no new deps; refines `R-051 ✓ shipped` / `R-052 ✓ shipped`)*

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

- [ ] **R-156 - Bootstrap progress observability** *(deps: R-152, R-153 ✓)*

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

- [ ] **R-157 - Operator-facing "force re-bootstrap" admin RPC widening** *(deps: R-152, refines `R-053 ✓ shipped`)*

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

- [ ] **R-158 - Bootstrap respects per-tree `LatticeMergeMode`** *(deps: R-152, R-153 ✓; sequence before R-154 ships payload)*

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

- [ ] **R-159 - Bootstrap drain resumes on transient transport faults** *(deps: R-154, R-156)*

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
`R-157`, `R-159` }.

`R-150` (the contract) blocks everything downstream. `R-151` (the sender
handler) lands next so `R-152`'s acceptance suite can round-trip against
a real handler rather than a stub. `R-158` (per-tree merge-mode
propagation through the bootstrap drain) **must** land before `R-154`
because `R-154` is the first item that delivers payload over the wire,
and the current hardcoded `LatticeMergeMode.LwwRegister` would silently
corrupt `OrSet` / `PnCounter` trees the moment payload arrives. `R-155`,
`R-156`, `R-157`, and `R-159` are quality-of-service refinements that
become observable only once the core transport is real; they can land in
any order after `R-154` (`R-159`'s bounded-retry shape composes naturally
with `R-156`'s observability surface but does not strictly depend on it).
