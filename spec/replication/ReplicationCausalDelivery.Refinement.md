# Refinement note: the causal-delivery module to code

This note maps [`ReplicationCausalDelivery.tla`](ReplicationCausalDelivery.tla),
the focused companion of [`Replication.tla`](Replication.tla), to the code. It
follows the conventions of [`Refinement.md`](Refinement.md), which maps the
main module and explains the Detector column. This module answers one
question the main module's two-write instance bounds out: whether a receiver
can deadlock waiting for causal dependencies when every shipper blocks at the
head of its line. It models the intended design of #4464 (acknowledge a parked
entry, hold it durably, drain it once its dependency is met), and the option of
withholding the acknowledgement stands as the mutation
`EventualConvergenceDeferredParkStalls`.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `authored` | History of every write | Not a production variable: the set the property quantifies over. |
| `known[x]` | The writes a writer holds | A writer's replica, and through it the frontier it can stamp as a dependency (`LatticeVectorClockContext`). |
| `log[s]` | A writer's writes in shipping order | The order `ReplicationShipperGrain.MergeOneBatchAsync` drains the WAL partitions in, which is per-leaf HLC order across partitions and so can differ from authoring order (#1060). |
| `cursor[s]` | Acknowledged position of the shipper to the receiver | `ReplicationShipperState.PartitionCursors`, which advances only on a positive acknowledgement. |
| `applied` | The writes merged at the receiver | The receiver's replicas. |
| `hwm[o]` | The receiver's per-origin high-water mark | `ReplicationHighWaterMarkState.Vector`. |
| `buf` | The receiver's causal buffer | `CausalApplyBuffer`; durable in the intended design (#4464). |
| `wake` | A drain is pending | A pending `ReplicationApplier.DrainBufferAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Author(o, h, d, p)` | A writer commits a write and the shipper's merge places it | A leaf commit, its HLC from `BPlusLeafGrain.AdvanceClockOrOverride`, an optional application frontier, and the position the shipper's HLC-ordered merge gives it. **Over-approximation:** the write may take any position the shipper has not passed, which covers every interleaving of partitions with unordered leaf clocks. | Yes: `ReplicationShipperGrainTests.DrainBatchAsync_cold_partition_new_below_cursor_entry_is_shipped_not_dropped`. |
| `Learn(x, w)` | A writer applies the other writer's write | `ReplicationApplier.ApplyAsync` at a writer, collapsed to one step because the main module checks that link. | Yes: `ReplicationApplierTests.ApplyAsync_advances_hwm_after_successful_apply`. |
| `Deliver(s)` | The shipper delivers its head entry and the receiver runs its pipeline | `ReplicationApplier.ApplyAsync`: a held entry is a duplicate, a met dependency (`CausalApplyBuffer.DependenciesSatisfied`) is merged and advances the high-water mark, an unmet one is parked (`ReplicationApplier.ParkAsync`). Every outcome is acknowledged, as production's transport does (`Accepted` on a park). One entry per delivery: a larger batch can carry an entry past a deferred one, and the instance excludes that rescue so the deadlock it would hide is reachable. | Partial (#4464): `ReplicationApplierTests.ApplyAsync_parks_entry_when_vector_clock_dependency_missing`; the durable buffer is the gap. |
| `Drain` | Release of satisfied buffered entries | `ReplicationApplier.DrainBufferAsync`, re-armed by the park that buffered the entry, which is the intended design's same-turn re-check. | Yes: `ReplicationApplierTests.ApplyAsync_drains_buffered_entries_when_dependency_arrives`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `EventualConvergence` | Every write reaches the receiver even when shipping order inverts a cross-origin dependency cycle: parked entries are acknowledged, so no shipper stalls behind one, and they are drained once their dependencies arrive. Withholding the acknowledgement deadlocks (`EventualConvergenceDeferredParkStalls`). | Partial (#4464): `ReplicationApplierTests.ApplyAsync_drains_chain_of_dependent_entries_in_one_call`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

| Property | Why it holds on the base | Class | Firing mutations |
|----------|--------------------------|-------|------------------|
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOkCausalHlcRunaway` |
| EventualConvergence | A parked entry is acknowledged, so the head of every line moves; a pending drain runs. Withholding the acknowledgement is option B of #4464. | Faithful for the intended design; blind for production until #4464 lands | `EventualConvergenceDeferredParkStalls`, `EventualConvergenceDrainNeverReleases`, `EventualConvergenceDependencyOnUnseenWrite` |

No mutation adds an action, and every `EventualConvergence` mutation leaves the
fairness intact.

## Deliberate abstraction gaps

- Everything the main module covers - the cycle-break, the identity cache,
  dead-lettering, restarts, bootstrap - is out of scope here; see
  [`Refinement.md`](Refinement.md#deliberate-abstraction-gaps).
- One receiver, two writers, two writes each, HLCs up to 2, one-entry
  batches. A batch large enough to carry an entry past a deferred one is
  bounded out, deliberately: it is a rescue that production's batch size does
  not guarantee.
- The link between the two writers is a single learn step, not a modelled
  replication edge.
