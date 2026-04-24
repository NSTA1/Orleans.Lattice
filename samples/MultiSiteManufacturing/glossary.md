
# MultiSiteManufacturing — glossary

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
| **Forge** | Raw forging operation — the first stage. |
| **Heat treat** | Vacuum / aging thermal cycles that develop material properties. |
| **Machining** | 5-axis milling to final geometry. |
| **CMM** | Coordinate Measuring Machine — dimensional inspection. |
| **NDT** | Non-Destructive Testing — finds flaws without damaging the part. |
| **FPI** | Fluorescent Penetrant Inspection — NDT technique for surface cracks. |
| **Eddy current** | NDT technique using electromagnetic induction for near-surface flaws. |
| **X-ray** | NDT technique for internal defects. |
| **NC / NCR** | Non-Conformance / Non-Conformance Report. Raised when a defect is discovered; carries a severity (`Minor`, `Major`, `Critical`). |
| **MRB** | Material Review Board. Dispositions non-conformances as `UseAsIs`, `Rework`, `Scrap`, or `ReturnToVendor`. |
| **Disposition** | The decision an MRB makes about a non-conforming part. |
| **UseAsIs** | MRB disposition accepting the part despite a non-conformance; demotes `FlaggedForReview` (and armed `Rework`) back to `Nominal`. |
| **Rework** | Corrective operation to bring a non-conforming part back into spec. Also the corresponding lattice state. |
| **Scrap** | Terminal disposition — the part is destroyed. Terminal compliance state. |
| **FAI** | First Article Inspection — the final certification step; produces a `FaiReportId` and an `inspectorId`. |
| **Heat lot** | A batch of material sharing the same heat-treat history; carried on `ProcessStepCompleted` facts where relevant. |
| **Digital thread** | The ordered, queryable history of every fact recorded against a part across sites. What the sample's UI renders per part. |
| **Severity lattice** | The totally ordered set of compliance states: `Nominal < UnderInspection < FlaggedForReview < Rework < Scrap`. |
| **ComplianceState** | Current lattice position of a part, computed by folding its fact log. |
| **Retest armed** | Internal flag that gates `MRBDisposition(UseAsIs)` demotion of `Rework` → `Nominal`. Set by a passing post-rework inspection or `ReworkCompleted(retestPassed=true)`; cleared by a failed retest. |
| **Operator** | Human user driving the UI. The sample uses a static `"demo"` label — no authentication in v1. |

---

## Technical

### Orleans and hosting

| Term | Meaning |
|---|---|
| **Orleans** | Distributed actor framework ("virtual actors" called grains) used by the sample for single-writer-per-entity coordination, timers, reminders, and persistence. |
| **Grain** | Virtual actor — a uniquely-keyed, single-threaded object managed by Orleans. Transparently activated on first use and deactivated when idle. |
| **Grain interface** | The public API of a grain (`ISomethingGrain`). All grain calls go through this interface. |
| **Grain key** | The identity of a grain instance. In this sample, strings like `"mfg-facts|heattreat"` or enum values like `ProcessSite.OhioForge`. |
| **Silo** | An Orleans host process. This sample runs two silos per cluster. |
| **Cluster** | A set of silos sharing a membership table. The sample runs two — `forge` and `heattreat`. |
| **Grain storage** | Orleans' persistent state facility. Here backed by Azure Table Storage (`msmfgGrainState`). |
| **Grain timer** | A periodic callback registered inside a grain activation. No minimum period; auto-disposed on deactivation. Used for the replicator's 3-second shipping loop. |
| **Reminder** | Durable, cluster-wide scheduled callback surviving silo restarts. Minimum period of 1 minute. Used for the replicator keepalive and the replog janitor. |
| **RequestContext** | Orleans per-call ambient dictionary flowing across grain calls. Used to carry the `lattice.replay = sourceCluster` flag that breaks the A → B → A replication loop. |
| **Outgoing grain call filter** | An `IOutgoingGrainCallFilter` interceptor that runs around every outgoing grain call. The sample uses one (`LatticeReplicationFilter`) to append to the replog after every replicated `SetAsync` / `DeleteAsync`. |
| **TestingHost** | Orleans' in-process test-cluster fixture. The sample's integration tests materialise a single cluster with in-memory storage. |

### Lattice

| Term | Meaning |
|---|---|
| **Orleans.Lattice** | The library this sample exercises. Distributed B+ tree with string keys and `byte[]` values, sharded across Orleans grains. |
| **Tree** | A named Lattice namespace (e.g. `mfg-facts`). Independent key space, independent persistence. |
| **ILattice** | The public tree API — `GetAsync`, `SetAsync`, `DeleteAsync`, range scans. |
| **HLC** | Hybrid Logical Clock. A `(wallClockTicks, counter)` pair that provides a monotonic, roughly-wall-clock-aligned ordering across distributed producers. |
| **Range scan** | Half-open key-range iteration over a tree; the primitive behind per-part history, per-site activity, replog shipping, and janitor pruning. |
| **Lex order** | Lexicographic byte order of keys. Zero-padded HLC components (`D20`, `D10`) embedded in keys make lex order match HLC order. |

### CRDTs and convergence

| Term | Meaning |
|---|---|
| **CRDT** | Conflict-free Replicated Data Type. A structure whose merge operation is commutative, associative, and idempotent, so replicas converge regardless of delivery order or duplication. |
| **G-Set** | Grow-only Set. A CRDT that only supports add; merge is set union. Used for `{serial}/labels/{label}`. |
| **LWW register** | Last-Writer-Wins register. A single-cell CRDT where the "latest" write (by some timestamp) wins. Used for `{serial}/operator`. Safe only when every replica compares the **same** timestamp — which is why the sample filters this key out of cross-cluster replication today. |
| **Fold** | Left-to-right reduction over a sequence — `(state, fact) → state'`. Both backends fold the same fact list; they differ only in the order. |
| **HLC-ordered fold** | Folding after sorting by `(WallClockTicks, Counter, FactId)`. Converges under reorder. The sample's lattice backend. |
| **Arrival-order fold** | Folding in the order facts were appended to a grain's list. Drifts under reorder. The sample's baseline backend. |
| **Convergence** | Property that replicas processing the same set of updates (in any order) reach the same state. |
| **Divergence** | State where two replicas (or two backends) disagree. In the sample, dashboard rows where baseline ≠ lattice are highlighted red. |
| **Shadow prefix** | Key-prefix discipline used by `PartCrdtStore` during a simulated partition — writes go to a shadow key, and `PartitionHealHostedService` promotes them back onto the canonical key on heal. |

### Replication

| Term | Meaning |
|---|---|
| **Replog** | HLC-ordered replication log — a Lattice tree (`_replog__{tree}`) acting as the discovery ledger for "what changed locally since cursor X?". |
| **Replicator grain** | `IReplicatorGrain`, one per `(tree, peer-cluster)`. Persists a cursor, scans the replog, ships batches to the peer, advances the cursor on ack. |
| **Cursor** | The replicator's high-watermark HLC — everything at or before this has been successfully shipped to the peer. |
| **Janitor** | `IReplogJanitorGrain`. Compacts the replog by deleting entries older than `min(peer cursors) − retention`. |
| **Anti-entropy** | Backstop full-tree sweep that re-ships any primary entry newer than the cursor; catches the rare race where the filter fails after the primary write succeeds. |
| **Loop-break** | The `RequestContext["lattice.replay"]` flag pattern that prevents an inbound replicated apply from re-entering the replog and shipping itself back. |
| **Opt-in (tree level)** | `ReplicationTopology.ReplicatedTrees` — whitelist of trees whose writes the filter observes. |
| **Opt-in (key level)** | `ReplicationTopology.IsKeyReplicated(tree, key)` — per-key filter applied on top of the tree-level whitelist. |
| **Traefik** | The HTTP reverse proxy fronting each cluster. Two routers per cluster: sticky-session for the UI, round-robin with active health check for `/replicate`. |
| **Multi-homed container** | A container attached to more than one Docker network. In this sample, each Traefik is attached to both cluster networks and is the only cross-cluster bridge. |
| **Tier-N chaos** | The sample's fault-injection taxonomy (tiers 1–5 + 4b). Each tier models a distinct failure class at a distinct seam. See [`approach.md`](./approach.md) §4. |

### Storage

| Term | Meaning |
|---|---|
| **Azurite** | The local Azure Storage emulator. The sample ships two instances (one per cluster) under Docker Compose. |
| **`msmfgGrainState`** | Azure Table holding Orleans grain state — chaos config, replicator cursors, seed flag, baseline part grains, inventory. |
| **`msmfgLatticeFacts`** | Azure Table holding every Lattice tree — facts, site-activity index, part CRDT, replogs. |

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
