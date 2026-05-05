# MultiSiteManufacturing — migration to `Orleans.Lattice.Replication`

Staged migration of the sample's hand-rolled cross-cluster replication
pipeline (`src/MultiSiteManufacturing.Host/Replication/`) over to the
shipped `Orleans.Lattice.Replication` and
`Orleans.Lattice.Replication.Grpc` packages.

The motivation, the gap between the sample and the package, and the
cost / value of each step are written up in
[`docs/lattice.replication/replication-design.md`](../../docs/lattice.replication/replication-design.md)
(the design notes the sample originally produced).

Each step is shippable on its own and leaves the sample in a working
state. The plan stages the cutover so that, if a step turns up an
unanticipated package gap, work pauses there without leaving the
sample broken.

---

## Steps

- [x] **Step 1 — Wire the package alongside the host-rolled pipeline.**
  Reference `Orleans.Lattice.Replication` and
  `Orleans.Lattice.Replication.Grpc`; call `AddLatticeReplication`,
  `AddLatticeReplicationGrpcPushTransport`,
  `AddLatticeReplicationGrpcServer`, `MapLatticeReplicationGrpcService`.
  Declare a single brand-new tree `mfg-facts-v2` opted in to
  `ReplicationMode.LwwRegister`. Add a tiny mirror hosted service that
  copies every fact emitted to the existing `mfg-facts` tree into
  `mfg-facts-v2` so the package's pipeline has traffic to ship. The
  existing host-rolled pipeline keeps running on `mfg-facts`,
  `mfg-site-activity-index`, `mfg-part-crdt` — both systems are
  oblivious to each other because their tree-id sets are disjoint.
  Outcome: the empty **Orleans.Lattice — Replication** Grafana panel
  starts rendering data for `mfg-facts-v2` and the
  `orleans.lattice.replication` meter is no longer dormant. README's
  empty-dashboard footnote becomes a "side-by-side comparison" note.

- [x] **Step 2 — Cut over `mfg-facts` to the package; remove the
  host-rolled outbound for that tree.** Move `mfg-facts` into the
  package's `ReplicatedTrees` map (`LwwRegister`); remove it from
  `ReplicationTopology.ReplicatedTrees` so the host-rolled outgoing
  filter stops appending to `_replog__mfg-facts`. The
  baseline-replay tap that lives in `ReplicationInboundEndpoint`
  moves to an `IChangeFeed` (or commit-time `IMutationObserver`)
  subscriber that re-emits every replicated fact into the local
  `BaselineFactBackend`. Delete the now-orphan `mfg-facts-v2` tree
  and the mirror hosted service from step 1.

- [x] **Step 3 — Cut over `mfg-site-activity-index` to the package.**
  Identical shape to step 2: add to the package's `ReplicatedTrees`
  map under `LwwRegister`; remove from the host-rolled topology.
  No baseline tap needed — this tree has no baseline analogue.

- [ ] **Step 4 — Split `mfg-part-crdt` into typed-CRDT trees.**
  Replace the single `mfg-part-crdt` tree (today: G-Set on the
  `labels/*` keys, LWW-Register on the `operator` key, with a
  per-key replication filter) with two trees:
  `mfg-part-labels` opted in as `ReplicationMode.OrSet`,
  `mfg-part-operator` either left unreplicated or opted in as
  `LwwRegister`. Convert `PartCrdtStore` to author through
  `lattice.OrSet(key)` and `lattice.LwwRegister(key)` accessors
  rather than raw `SetAsync` calls. Update `PartCrdtStoreTests`
  accordingly. This is the step that delivers the typed-CRDT-delta
  demo the package was built for.

- [ ] **Step 5 — Delete the host-rolled pipeline.**
  Remove the entire `src/MultiSiteManufacturing.Host/Replication/`
  folder (`LatticeReplicationFilter`, `ReplicationLogWriter`,
  `ReplicatorGrain` + state, `ReplogJanitorGrain`,
  `ReplicationInboundEndpoint`, `ReplicationHttpClient`,
  `ReplicationTopology`, `ReplicationWireTypes`, `ReplogKeyCodec`,
  `ReplicationActivityTracker`, `ReplicationBootstrapHostedService`,
  `ClusterReplicationStatsGrain`). Remove the corresponding
  `test/MultiSiteManufacturing.Tests/Replication/` files
  (`ReplogKeyCodecTests`, `ReplicatorCursorAdvanceTests`,
  `ReplicationInboundBaselineReplayTests`). Remove the
  `Replication__*` config keys from
  `appsettings.cluster.{us,eu}.json` and `docker-compose.yml`.
  Rewrite `architecture.md` §5 (the cross-cluster replication
  sequence diagram) around the package's WAL + gRPC push transport
  topology. Point `README.md` at `docs/lattice.replication/` for
  the wire format and bootstrap protocol rather than re-deriving
  them in `approach.md`.

- [ ] **Step 6 — Migrate the chaos surface.**
  Today's Tier 4b ("app-level replication disconnect") is an
  `IReplicationDisconnectGrain` flag the host-rolled outbound tick
  and inbound endpoint both consult. Re-implement it as an
  `IReplicationTransport` decorator that wraps `GrpcPushTransport`
  and returns "unavailable" when the singleton flag is set. The
  decorator registers via a `silo.ConfigureServices` `Decorate(...)`
  call so the existing UI flyout toggle still drives it. Tier 5
  (real `docker network disconnect`) is transport-agnostic and
  needs no work.

---

## Out of scope for this migration

- **Snapshot / bootstrap protocol** for new peers and long-offline
  peers (`ISnapshotProvider`, `ILatticeBootstrapCoordinator`). The
  sample's two clusters never start cold against one another — each
  brings its own seeder — so the migration intentionally does not
  exercise the bootstrap path. Documented in
  `docs/lattice.replication/snapshot-bootstrap.md`.
- **Dead-letter queue tooling.** The package's
  `ILatticeReplicationDeadLetters` is wired in by default; surfacing
  DLQ contents in the dashboard UI is a sample enhancement that can
  ride a separate change.
- **Vector-clock causal+ apply.** The package supports causal+
  delivery; the sample's workload (one authoritative cluster per
  part for the lifetime of the part) does not exhibit cross-cluster
  concurrent writes on the same key, so the migration uses the
  simpler per-origin-HWM mode.
