# Refinement note: BackupProvenance to code

This note maps [`BackupProvenance.tla`](BackupProvenance.tla) - what a backup
chain records about the writes it captured - to the production symbols that play
each role, and to the detector tests that would notice production deviating
from it. It is a documented mapping, not a machine-checked refinement proof.

The rules the module checks run in one dependency-free core,
`BackupChainFrontier`, which both capture collectors and the capture service's
two consistency cuts route through: `BackupChainFrontier.NormalizeOrigin` (the
#2621 empty-origin rule), `BackupChainFrontier.Observe` (the per-origin
high-water), `BackupChainFrontier.FullCut` (#3758) and
`BackupChainFrontier.IncrementalCut` (the chain's frontier never regresses).
Its unit suite is `BackupChainFrontierTests`; the rules are not
schedule-sensitive, so they have no Coyote model, by the same precedent as
`TerminalArrivalTally`.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `log` | The tree's writes, each with its author, its stored origin stamp, and its HLC | Rows' `LwwEntry.OriginClusterId` and `LwwEntry.Timestamp`. A local (own) write's row carries no origin (`null`, #5524); its WAL record carries the tree's resolved origin (`string.Empty` from `DefaultLatticeOriginClusterIdResolver` on a host with no cluster identity, the local cluster id on a replicated tree), which `LatticeRowOrigin.FromRecord` turns back into no origin wherever a row or an increment's entry is built from the record (#5529). The model's unstamped `""` stamp is that no-origin case. |
| `clock` | The local HLC | The silo's hybrid logical clock, which merges a replicated write's stamp on receipt. |
| `chain` | The backup chain: a full link then increments, each with its covered range, cut HLC and provenance | `BackupManifest.ConsistencyCut` (`BackupConsistencyCut.HlcTimestamp`, `BackupConsistencyCut.PerOriginFrontier`), `BackupManifest.Provenance` (`BackupOriginProvenance`), and `BackupManifest.BaseBackupId` for the chain. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `LocalWrite` | A locally authored write, unstamped | The own write's row, stored with no origin, and its WAL record's resolved-origin stamp (`DefaultLatticeOriginClusterIdResolver`, or the local cluster id on a replicated tree), mapped back to no origin by `LatticeRowOrigin.FromRecord`. **Environment argument:** a local write is always possible until the log bound. | Yes: `RawEntryCollectorTests.StreamAsync_empty_origin_cluster_id_does_not_add_high_water` and `RawEntryCollectorTests.StreamAsync_counts_unstamped_origin_entries`. |
| `ReplicatedApply` | A peer origin's write applied with its stamp and the peer's HLC, possibly below writes already present | `ReplicationApplier` applying a shipped entry with its `OriginClusterId` and timestamp. **Environment argument:** any HLC in range, so the peer's clock is unconstrained against the local one. | Yes: `RawEntryCollectorTests.StreamAsync_positive_ticks_are_stored_as_origin_high_water` and `RawEntryCollectorTests.StreamAsync_lower_ticks_do_not_replace_higher_high_water`. |
| `CaptureFull` | A full capture covers every write and starts a chain | `RawEntryCollector` (through `BackupChainFrontier.NormalizeOrigin` and `BackupChainFrontier.Observe`) and `LatticeBackupCaptureService.BuildConsistencyCut` (through `BackupChainFrontier.FullCut`); `LatticeBackupCaptureService.BuildProvenance` builds the list. | Yes: `RawEntryCollectorTests.StreamAsync_empty_origin_cluster_id_does_not_add_high_water` (red when an unstamped row keeps the empty origin), `BackupChainFrontierTests.FullCut_is_the_captured_high_water_when_the_registry_anchor_is_zero` (red when the cut ignores the captured high-water), and `RawEntryCollectorTests.BuildProvenance_rejects_an_empty_origin_key_with_an_attributable_message`. |
| `CaptureIncremental` | An increment covers the writes since its base, never regressing the chain's frontier | `IncrementalDeltaCollector` (through the same core) and `LatticeBackupCaptureService.BuildIncrementalCut` (through `BackupChainFrontier.IncrementalCut`). | Yes: `BackupChainFrontierTests.IncrementalCut_carries_the_base_forward_when_the_delta_is_older`, `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_on_a_full_base_pins_the_wal_at_the_base_frontier_and_carries_it_forward`, `IncrementalDeltaCollectorTests.OnEntry_positive_origin_ticks_are_stored_as_high_water`, `IncrementalDeltaCollectorTests.OnEntry_an_own_write_stamped_with_the_local_cluster_id_records_no_origin` (red when an increment keeps an own write's local-cluster stamp, #5529), and `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_with_a_local_cluster_id_stores_own_writes_with_no_origin` (red when the capture service does not pass the tree's resolved local origin to the collector, #5529). |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `ProvenanceNoEmptyOrigin` | No manifest names the empty origin. In production the `BackupOriginProvenance` constructor rejects one, so the defect this forbids presents as every capture of a locally authored tree throwing - the #2621 outage - not as a bad manifest. | Yes: `RawEntryCollectorTests.StreamAsync_empty_origin_cluster_id_does_not_add_high_water` and `BackupChainFrontierTests.NormalizeOrigin_maps_an_unstamped_row_to_no_origin`. |
| `ProvenanceCoversCaptured` | Every captured write from a real origin is attributed to it at a high-water at or above its HLC; no real origin is silently dropped. | Yes: `RawEntryCollectorTests.StreamAsync_lower_ticks_do_not_replace_higher_high_water` and `BackupChainFrontierTests.Observe_keeps_the_highest_tick_per_origin`. |
| `FrontierCoversCaptured` | Every link's cut HLC is at or above every write it captured: the frontier an incremental pins the WAL at (#3758). | Yes: `BackupChainFrontierTests.FullCut_is_the_captured_high_water_when_the_registry_anchor_is_zero` and `LatticeBackupIncrementalCaptureTests.CaptureIncrementalAsync_on_a_full_base_pins_the_wal_at_the_base_frontier_and_carries_it_forward`. |
| `ChainFrontierMonotonic` | A chain's frontier never regresses from one link to the next. | Yes: `BackupChainFrontierTests.IncrementalCut_carries_the_base_forward_when_the_delta_is_older` and `BackupChainFrontierTests.IncrementalCut_with_an_empty_delta_keeps_the_base`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

Per #2321's taxonomy every property is reached and falsifiable: each has a
protocol-level mutation (an edit to an action, or to the collector's
normalisation that `CaptureFull` and `CaptureIncremental` read). No mutation adds
an action. The module has no liveness property: nothing in it waits on anything.

## Deliberate abstraction gaps

- **Per-origin frontier of an increment.** `BackupConsistencyCut.PerOriginFrontier`
  and an increment's provenance describe that link's delta only, not the chain;
  the module checks that each link covers its own writes, and that the
  tree-wide HLC frontier never regresses. Nothing in production consumes the
  per-origin frontier (the restore path re-syncs through replication, not from
  it), so a chain-level per-origin monotonicity property would constrain only
  descriptive metadata. That is stated rather than checked.
  Production symbols: `BackupConsistencyCut.PerOriginFrontier`.
- **Keys.** Each write is its own key, so a later write never supersedes an
  earlier one in a capture. Last-writer-wins folding is the capture module's and
  the core's, not the bookkeeping's.
  No production symbol: the behaviour is the capture module's and the core's,
  mapped in that module's own refinement note.
- **The resume points.** The per-partition WAL offsets an increment resumes from
  (`BackupConsistencyCut.WalPartitionOffsets`) and the WAL fall-off fallback are
  not modelled; they are the WAL module's territory.
  Production symbols: `BackupConsistencyCut.WalPartitionOffsets`.
