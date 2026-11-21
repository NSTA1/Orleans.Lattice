# Refinement note: the receiver-initiated re-seed module to code

[`ReplicationReceiverReseed.tla`](ReplicationReceiverReseed.tla) checks the
forced gap a shipper marks when its peer's lineage of the tree moves because
the receiver bootstrapped on its own, and that the bootstrap settles the gap
without a second, redundant re-seed when an export covering the log's retained
floor exists (#5794).

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `head` | The sender's log head | The per-tree WAL a shipper drains; one partition stands for production's several. |
| `trim` | The log's purged prefix | The WAL's trimmed floor, which retention advances only past the shippers' purge holds (`WalPurgeHoldGrain`). |
| `epoch` | The current export epoch | `ReplicationExportEpochState.Epoch`, read by `ReplicationShipperGrain` through `IReplicationExportEpochGrain.GetAsync`. |
| `cut[e]` | The head export `e` recorded it opened at | `ReplicationExportCut`, recorded by `LatticeSnapshotProvider.RecordExportCutAsync` into `ReplicationExportEpochState.Cuts` through `IReplicationExportEpochGrain.RecordCutAsync`. |
| `snap[e]` | What export `e` holds | Not a production variable: the contents of the export the bootstrap copied. |
| `trusted[e]` | Export `e` opened on this log under the purge hold | Not a production variable: the ghost `NoRedundantReseed` is judged against. Production can only see a recorded cut. |
| `recv` | The receiver's writes | The receiver's replica of the tree. |
| `cursor` | The shipper's acknowledged position | `ReplicationShipperState.PartitionCursors`. |
| `moved` | The receiver's lineage moved unseen | A `ReplicationAck.ReceiverLineage` on the next acknowledgement that differs from the lineage the shipper last recorded for the peer. |
| `moves` | Receiver-initiated bootstraps so far | Not a production variable: it bounds the instance to one such bootstrap. |
| `echo` | The receiver's completed bootstrap epoch | The completed export epoch the receiver's `ReplicationReseedResponder` reports, which every acknowledgement echoes. |
| `marker` | The shipper's re-seed marker | `ReplicationShipperState.ReseedRequiredEpoch`. |
| `retainFrom` | The rewind point once the gap settles | The retained-floor restart the shipper records when it takes the peer off the log (`ReplicationShipperGrain.TakePeerOffLogStateAsync`). |
| `redundant` | A re-seed ran though the echo covered the gap | Not a production variable: the ghost `NoRedundantReseed` checks. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Write` | The sender commits a write | A leaf commit appended to the WAL. | Yes: `BPlusLeafGrainTests.MergeMany_advances_local_clock_past_incoming_max`. |
| `Purge` | The log trims its oldest write | WAL retention, bounded by each shipper's acknowledged position and by the purge hold a shipper keeps while it is off the log. | Yes: `WalGcShipperOffsetFloorTests.Retention_trim_past_a_shipper_records_its_purge_hold_before_the_trim`, `WalGcShipperOffsetFloorTests.Shipper_releases_a_purge_hold_its_durable_position_covers`. |
| `Export` | The sender takes an export | A bootstrap export: the epoch advance, then `LatticeSnapshotProvider.RecordExportCutAsync` recording the head it opened at. An export with no cut on this log (taken before the hold, on another physical log, or whose cut write failed) is the untrusted branch. | Yes: `ReplicationExportEpochGrainTests.RecordCutAsync_keeps_cuts_sorted_and_bounded`, `ReplicationExportEpochGrainTests.RecordCutAsync_ignores_non_positive_future_and_duplicate_epochs`, `ReplicationExportEpochGrainTests.RecordCutAsync_failed_write_rolls_back`. |
| `Ship` | The shipper delivers the next write and it is acknowledged | `ReplicationShipperGrain.MergeOneBatchAsync` and the ship paths; the cursor advances only on a positive acknowledgement. | Yes: `ReplicationShipperGrainTests.PumpOnceAsync_does_not_advance_cursor_on_negative_ack`. |
| `Bootstrap(e)` | The receiver bootstraps on its own initiative | A receiver-started `LatticeBootstrapCoordinatorGrain` bootstrap from one of the sender's exports, which moves the receiver's lineage. | Yes: `CrossClusterAtomicVisibilityTests.A_receiver_bootstrap_from_a_covering_export_settles_the_lineage_move_without_a_second_reseed`. |
| `Notice` | The shipper sees the lineage move and takes the peer off the log | `ReplicationShipperGrain.RemarkReseedRequiredAsync`, which marks at `ReplicationShipperGrain.CoveringMarkerAsync` (one below `IReplicationExportEpochGrain.GetEarliestCoveringEpochAsync`, else the current epoch) and rewinds from the retained floor when the marker is below the current epoch. | Yes: `ReplicationExportEpochGrainTests.GetEarliestCoveringEpochAsync_returns_lowest_contiguous_covering_epoch`, `ReplicationExportEpochGrainTests.GetEarliestCoveringEpochAsync_requires_the_current_epoch_to_be_recorded`, `CrossClusterAtomicVisibilityTests.A_receiver_bootstrap_without_a_covering_export_keeps_the_conservative_reseed`. |
| `Settle` | The echo exceeds the marker, so the shipper rewinds and resumes | The shipper's re-seed clear, which compares the echoed completed epoch with `ReplicationShipperState.ReseedRequiredEpoch`. | Yes: `CrossClusterAtomicVisibilityTests.Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has`. |
| `Reseed` | The echo does not settle the gap, so the receiver is re-seeded | `ReplicationReseedResponder`, which starts a bootstrap from a fresh export unless one past the marker has completed. | Yes: `ReplicationReseedResponderTests.Starts_a_bootstrap_when_none_has_completed_past_the_requested_epoch`, `ReplicationSourceFrontierAggregateGrainTests.Lineage_re_seeds_are_paced_one_at_a_time_per_peer`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `NoLostWrite` | While the link is on the log, the receiver holds every write the shipper treats as acknowledged: a covering marker never lets the shipper resume past a write the bootstrap's export did not hold. | Yes: `CrossClusterAtomicVisibilityTests.A_receiver_bootstrap_without_a_covering_export_keeps_the_conservative_reseed`, `ReplicationExportEpochGrainTests.GetEarliestCoveringEpochAsync_returns_lowest_contiguous_covering_epoch`. |
| `NoRedundantReseed` | A receiver-initiated bootstrap from an export that covers the retained floor settles the lineage move, so no second re-seed is forced (#5794). | Yes: `CrossClusterAtomicVisibilityTests.A_receiver_bootstrap_from_a_covering_export_settles_the_lineage_move_without_a_second_reseed`. |
| `Converges` | The link always returns to the log and catches up, whether the echo settles the gap or a re-seed runs. | Yes: `CrossClusterAtomicVisibilityTests.Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Property classification

| Property | Why it holds on the base | Class | Firing mutations |
| --- | --- | --- | --- |
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOKWriteIgnoresBound` |
| NoLostWrite | The marker trusts only a contiguous run of exports whose recorded cuts reach the retained floor, and the rewind restarts from that floor. | Faithfully inexpressible | `NoLostWriteTrustedExportHoldsLess`, `NoLostWriteShipDropsWrite`, `NoLostWriteBootstrapHidesLineageMove`, `NoLostWriteCoveringRunNotContiguous`, `NoLostWriteSettleSkipsRewind`, `NoLostWriteReseedKeepsOldContents` |
| NoRedundantReseed | The marker is one below the earliest covering export, so the echo of a covering bootstrap exceeds it. | Faithfully inexpressible | `NoRedundantReseedMarkAtCurrentEpoch` |
| Converges | Ship, Notice, Settle and Reseed are weakly fair, a re-seed export number is kept back, and the purge never passes the rewind point. | Faithfully inexpressible | `ConvergesPurgeIgnoresFloor` |

## Deliberate abstraction gaps

- **What the main module covers.** Delivery, the receive pipeline, restarts
  and the other forced-gap causes are out of scope here; see
  [`Refinement.md`](Refinement.md#deliberate-abstraction-gaps) and
  [`ReplicationReBootstrap.Refinement.md`](ReplicationReBootstrap.Refinement.md).
  No production symbol: the behaviour is the other modules', mapped in their
  own refinement notes.
- **One partition.** The sender's log is one sequence and the cut one head,
  where production records a cut per WAL partition and the covering check
  compares each partition's cut with its retained floor.
  No production symbol: the gap collapses production's partitions; it is not
  a production mechanism.
- **The purge-hold version gate.** Production trusts a covering export only
  while every silo honours purge holds; the model assumes they all do, and
  the fallback is the conservative marker the untrusted branch already checks.
  No production symbol: the gap is an assumption the model makes, guarded in
  production by the existing silo-version check.
- **Instance bounds.** Two writes, three exports with one kept back for a
  re-seed, and one receiver-initiated bootstrap.
  No production symbol: the gap bounds the checked instance; it is not a
  production mechanism.
