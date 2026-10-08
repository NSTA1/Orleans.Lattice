# Backup and cutover chaos tests

The backup suite exercises restore cutovers against live readers, tags and atomic writers. It checks specific bounded interleavings; it is not a general power-loss simulation or a disaster-recovery certificate for the default in-cluster sink.

## Covered scenarios

| Fixture | Concurrent change/fault | Assertion boundary |
|---|---|---|
| [BackupRestoreReconcileChaosTests](../../test/lattice.backup/Chaos/BackupRestoreReconcileChaosTests.cs) | Restore under concurrent tag reads, including one injected registry-enumeration failure; repeated restores to earlier snapshots. | Tag results contain no invented keys, converge to the retained snapshot keys, and keys absent from the snapshot are absent from the restored tree. The injected failure count is checked. |
| [CrossTreeAtomicWriteAcrossCutoverChaosTests](../../test/lattice.backup/Chaos/CrossTreeAtomicWriteAcrossCutoverChaosTests.cs) | A participant changes physical copy while cross-tree atomic writes continue. | No surfaced stale-routing failures; some rounds commit, and a final cross-tree batch commits with both final values visible. |
| [ShadowCutoverAtomicVisibilityChaosTests](../../test/lattice.backup/Chaos/ShadowCutoverAtomicVisibilityChaosTests.cs) | Restore/revert of a resharded tree and cutover during grow/shrink. | No torn/incomplete atomic batches; alias replacement/revert preconditions are checked, grow/shrink completes and the new shard map reaches its target. |
| [ShadowCutoverRoutingSelfHealChaosTests](../../test/lattice.backup/Chaos/ShadowCutoverRoutingSelfHealChaosTests.cs) | Sustained concurrent reads through a cutover and successive restores. | Readers converge to each snapshot without surfaced routing failures. |

The tests use an Orleans test cluster and explicit overlap/fault hooks. They do not claim every possible interleaving or storage-provider failure was explored. Linked source includes assertions that work really overlapped the relevant cutover rather than merely running before/after it.

## Running and interpreting coverage

These fixtures carry `Category("Chaos")`; use the repository [testing policy](../../.github/instructions/testing.instructions.md) for the applicable execution tier and prerequisites. Documentation compilation and text-hygiene gates do **not** run these chaos cases.

See [architecture](architecture.md), [verified backups](verified-backup.md) and [disaster recovery](disaster-recovery.md) for the supported backup/restore contracts. In particular, passing an in-cluster restore fixture does not make its sink durable or shared across regions.
