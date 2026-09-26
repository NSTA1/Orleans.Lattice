# MultiSiteManufacturing - glossary

Terms used throughout the sample. Split into the **domain** side
(manufacturing / quality / traceability) and the **technical** side
(Orleans, Lattice, CRDTs, replication).

---

## Domain

| Term | Meaning |
|---|---|
| **HPT blade** | High-Pressure Turbine blade. The single product family modelled in v1 (`HPT-BLD-S1`). |
| **Family** | Short code identifying a product line (e.g. `HPT-BLD-S1`, `HPT-DSK-S1`, `LPC-BLD`). |
| **Serial number** | Globally unique part identifier, shaped `{family}-{year}-{seq:D5}`. Example: `HPT-BLD-S1-2028-00142`. |
| **Process stage** | A point in the part lifecycle: `Forge`, `HeatTreat`, `Machining`, `NDT`, `MRB`, `FAI`. |
| **Process site** | A physical facility where a stage is performed (e.g. *Ohio Forge*, *Toulouse NDT Lab*). One-to-many with stages. |
| **Forge** | Raw forging operation - the first stage. |
| **Heat treat** | Vacuum / aging thermal cycles that develop material properties. |
| **Machining** | 5-axis milling to final geometry. |
| **CMM** | Coordinate Measuring Machine - dimensional inspection. |
| **NDT** | Non-Destructive Testing - finds flaws without damaging the part. |
| **FPI** | Fluorescent Penetrant Inspection - NDT technique for surface cracks. |
| **Eddy current** | NDT technique using electromagnetic induction for near-surface flaws. |
| **X-ray** | NDT technique for internal defects. |
| **NC / NCR** | Non-Conformance / Non-Conformance Report. Raised when a defect is discovered; carries a severity (`Minor`, `Major`, `Critical`). |
| **MRB** | Material Review Board. Dispositions non-conformances as `UseAsIs`, `Rework`, `Scrap`, or `ReturnToVendor`. |
| **Disposition** | The decision an MRB makes about a non-conforming part. |
| **UseAsIs** | MRB disposition accepting the part despite a non-conformance; demotes `FlaggedForReview` (and armed `Rework`) back to `Nominal`. |
| **Rework** | Corrective operation to bring a non-conforming part back into spec. Also the corresponding lattice state. |
| **Scrap** | Terminal disposition - the part is destroyed. Terminal compliance state. |
| **FAI** | First Article Inspection - the final certification step; produces a `FaiReportId` and an `InspectorId`. |
| **Heat lot** | A batch of material sharing the same heat-treat history; carried on `ProcessStepCompleted` facts where relevant. |
| **Digital thread** | The ordered, queryable history of every fact recorded against a part across sites. What the sample's UI renders per part. |
| **Severity lattice** | The totally ordered set of compliance states: `Nominal < UnderInspection < FlaggedForReview < Rework < Scrap`. |
| **ComplianceState** | Current lattice position of a part, computed by folding its fact log. |
| **Retest armed** | Internal flag that gates `MrbDisposition(UseAsIs)` demotion of `Rework` -> `Nominal`. Set by a passing post-rework inspection or `ReworkCompleted(retestPassed=true)`; cleared by a failed retest or failed inspection, by any non-conformance, and by any MRB disposition. |
| **Operator** | Human user driving the UI. The sample uses a static `operator:demo` identity - no operator sign-in in v1. |

---

## Technical

### Orleans and hosting

| Term | Meaning |
|---|---|
| **Orleans** | Distributed actor framework ("virtual actors" called grains) used by the sample for single-writer-per-entity coordination, timers, reminders, and persistence. |
| **Grain** | Virtual actor - a uniquely-keyed, single-threaded object managed by Orleans. Transparently activated on first use and deactivated when idle. |
| **Grain interface** | The public API of a grain (`ISomethingGrain`). All grain calls go through this interface. |
| **Grain key** | The identity of a grain instance. In this sample, enum values like `ProcessSite.OhioForge` and short strings like backend names (`"baseline"`, `"lattice"`). |
| **Silo** | An Orleans host process. This sample runs two silos per cluster. |
| **Cluster** | A set of silos sharing a membership table. The sample runs two - `us` and `eu`. |
| **Grain storage** | Orleans' persistent state facility. Here backed by Azure Table Storage; the sample's own grains use the `msmfgGrainState` provider. |
| **Grain timer** | A periodic callback registered inside a grain activation. No minimum period; auto-disposed on deactivation. Used for the replication shipper's steady-state pump (every 100 ms by default). |
| **Reminder** | Durable, cluster-wide scheduled callback surviving silo restarts. Minimum period of 1 minute. Used for the replication shipper's keepalive. |
| **RequestContext** | Orleans per-call ambient dictionary flowing across grain calls. The sample no longer threads its own loop-break flag through it - loop-breaking now rides the origin stamped on every write-ahead-log record (see Replication section below). |
| **TestingHost** | Orleans' in-process test-cluster fixture. Each of the sample's test fixtures stands up its own single-silo cluster with in-memory storage (the gRPC contract tests instead start the host itself in its in-memory `Testing` environment); the coordinated-restore test stands up two such clusters over one shared backup sink directory. |

### Lattice

| Term | Meaning |
|---|---|
| **Orleans.Lattice** | The library this sample exercises. Distributed B+ tree with string keys and `byte[]` values, sharded across Orleans grains. |
| **Tree** | A named Lattice namespace (e.g. `mfg-facts`). Independent key space, independent persistence. |
| **ILattice** | The public tree API - `GetAsync`, `SetAsync`, `DeleteAsync`, range scans. |
| **HLC** | Hybrid Logical Clock. A `(wallClockTicks, counter)` pair that provides a monotonic, roughly-wall-clock-aligned ordering across distributed producers. |
| **Range scan** | Half-open key-range iteration over a tree; the primitive behind per-part history and the partition-heal shadow sweep. |
| **Tag index** | The built-in `Orleans.Lattice` secondary index (opened through the injected `ILatticeTagIndexFactory`). Associates tags with keys and answers `WithAnyTags` / `WithAllTags` queries from a sibling `tag-{name}` membership tree. Powers the sample's per-site "parts at site X" view over the part-major `mfg-site-activity` tree. |
| **Lex order** | Lexicographic byte order of keys. Zero-padded HLC components (`D20`, `D10`) embedded in keys make lex order match HLC order. |

### CRDTs and convergence

| Term | Meaning |
|---|---|
| **CRDT** | Conflict-free Replicated Data Type. A structure whose merge operation is commutative, associative, and idempotent, so replicas converge regardless of delivery order or duplication. |
| **OR-Set** | Observed-remove set. Supports add and remove; merge unions the add and remove dots, so a concurrent add beats a remove (add-wins). Used for `mfg-part-labels` (one OR-Set per serial). |
| **LWW register** | Last-Writer-Wins register. A single-cell CRDT where the "latest" write (by some timestamp) wins. Used for `mfg-part-operator` (one register per serial). Safe only when every replica compares the **same** timestamp - which is why the sample leaves that tree out of cross-cluster replication. |
| **Fold** | Left-to-right reduction over a sequence - `(state, fact) → state'`. Both backends fold the same fact list; they differ only in the order. |
| **HLC-ordered fold** | Folding after sorting by `(WallClockTicks, Counter, FactId)`. Converges under reorder. The sample's lattice backend. |
| **Arrival-order fold** | Folding in the order facts were appended to a grain's list. Drifts under reorder. The sample's baseline backend. |
| **Convergence** | Property that replicas processing the same set of updates (in any order) reach the same state. |
| **Divergence** | State where two replicas (or two backends) disagree. In the sample, dashboard rows where baseline ≠ lattice are highlighted red. |
| **Shadow prefix** | Key-prefix discipline used by `PartCrdtStore` during a simulated partition - writes go to a shadow key, and `PartitionHealHostedService` promotes them back onto the canonical key on heal. |

### Replication

Provided by `Orleans.Lattice.Replication` (shipper + applier, shipping
from the core write-ahead log) and `Orleans.Lattice.Replication.Grpc`
(HTTP/2 gRPC push transport).

| Term | Meaning |
|---|---|
| **WAL** | The core `Orleans.Lattice` per-tree write-ahead log of every committed mutation, persisted here by `Orleans.Lattice.Storage.AzureTable`. Replication ships from it; it replaces the sample's earlier hand-rolled `_replog__{tree}` tree. |
| **Shipper grain** | One package-managed grain per `(tree, peer-cluster)` pair. Drains the WAL, calls `IReplicationTransport.SendAsync`, advances its per-peer cursor on ack. |
| **Applier** | Receiver-side package component that merges incoming batches into the local lattice using the tree's CRDT semantics (`LwwRegister`, `OrFlag`, or `OrSet`). |
| **Cursor** | The shipper's per-peer high-watermark - everything at or before this HLC has been successfully shipped to the peer. The package persists it in grain storage. |
| **Replication mode** | Per-tree CRDT semantic chosen on opt-in. `LwwRegister` for `mfg-facts` (write-once keys) and `mfg-site-activity` (the newest fact per part-at-site wins); `OrFlag` for the `tag-mfg-site` membership tree (enable-wins flag-CRDT membership); `OrSet` for set-typed values (`mfg-part-labels`); unreplicated trees stay cluster-local. |
| **Per-origin HWM** | The receiver's per-origin high-water mark: the highest HLC applied from each origin, used to dedupe re-delivered entries. Loop-breaking is separate - a replicated apply lands in the receiver's WAL under its source origin, and a shipper ships only locally-authored entries. Together they replace the sample's earlier `RequestContext["lattice.replay"]` flag. |
| **IReplicationApplier** | Package-side seam invoked once per cross-cluster apply. `BaselineReplicationApplier` (sample-side) decorates the package's singleton to mirror `mfg-facts` writes into the divergence-visualisation backend and raise `FederationRouter.FactReplicated`; `ChaosReplicationApplier` (sample-side, Tier 4b inbound half) wraps it outermost and rejects every apply while the disconnect flag is set. |
| **IReplicationTransport** | Single-method (`SendAsync`) seam between the shipper and the wire. `ChaosReplicationTransport` (sample-side, Tier 4b) decorates it; the package-side gRPC push transport is the concrete implementation. |
| **Opt-in (tree level)** | `LatticeReplicationOptions.ReplicatedTrees` - a tree -> `LatticeMergeMode` map. The shipper observes only listed trees. |
| **Traefik** | The HTTP reverse proxy fronting each cluster. Four routers per cluster: sticky-session for the UI, round-robin (no health check) for the `/orleans.lattice.replication.*` gRPC service path, and round-robin with active health check for both the `/orleans.lattice.api.state/*` read-only state API browsed by the explorer and the `/orleans.lattice.api.backup/*` backup control API. |
| **Multi-homed container** | A container attached to more than one Docker network. In this sample, each Traefik is attached to both cluster networks and is the only cross-cluster bridge. |
| **Tier-N chaos** | The sample's fault-injection taxonomy (tiers 1-5 + 4b). Each tier models a distinct failure class at a distinct seam. See [`approach.md`](./approach.md) §4. |

### Storage

| Term | Meaning |
|---|---|
| **Azurite** | The local Azure Storage emulator. The sample runs three instances under Docker Compose: one per cluster plus the shared `azurite-backup` account both clusters reach. |
| **`msmfgGrainState`** | Grain-storage provider name (not a table) for the sample's own grains - chaos config, seed flag, baseline part grains. Like every Azure Table grain-storage provider in the sample it writes Orleans' default `OrleansGrainState` table. |
| **`OrleansLatticeWal`** | Azure Table holding the core write-ahead log of every Lattice tree - `mfg-facts`, `mfg-site-activity`, `tag-mfg-site`, `mfg-part-labels`, `mfg-part-operator` (the `Orleans.Lattice.Storage.AzureTable` default table name). The trees' node state lives in `OrleansGrainState` through the `lattice` grain-storage provider, as do the replication shipper's cursors. |

---

## Acronyms at a glance

| Acronym | Meaning |
|---|---|
| CMM | Coordinate Measuring Machine |
| CRDT | Conflict-free Replicated Data Type |
| FAI | First Article Inspection |
| FPI | Fluorescent Penetrant Inspection |
| HLC | Hybrid Logical Clock |
| HPT | High-Pressure Turbine |
| LB | Load Balancer |
| LPC | Low-Pressure Compressor |
| LWW | Last-Writer-Wins |
| MES | Manufacturing Execution System |
| MRB | Material Review Board |
| NC / NCR | Non-Conformance / Non-Conformance Report |
| NDT | Non-Destructive Testing |
| QMS | Quality Management System |
| RTV | Return To Vendor |
