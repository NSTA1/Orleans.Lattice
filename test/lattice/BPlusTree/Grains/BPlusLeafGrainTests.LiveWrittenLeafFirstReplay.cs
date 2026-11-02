using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5570: a live-written leaf's durable pin leaves the block value after
/// its first WAL replay.
/// <para>
/// On the journey estate every tree reported a pin that carries no usable
/// offset within a minute of starting. Live writes carry no WAL offset, so they
/// never advance a leaf's projection checkpoint; a leaf whose rows all arrived
/// live therefore publishes the <c>(Zero, -1)</c> block from birth, and its
/// first checkpoint comes from a replay. For a resident leaf that replay is the
/// coverage-lag timer's starvation drive (issue #3300). This fixture pins the
/// whole path in one place - block before, one tick, a usable offset on the
/// wire after - so the time-to-usable that the estate measured has a detector
/// rather than a chain of per-link tests.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task A_live_written_leafs_pin_carries_a_usable_offset_after_its_first_coverage_lag_tick()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        try
        {
            var treeId = UniqueCoverageRepairTreeId("live-written-first-replay");
            var wal = new GrowingWal();

            var published = new List<MaterialiserPinReport>();
            var reporter = Substitute.For<ILeafCursorReporter>();
            reporter.FlushDurableMaterialiserFrontierAsync(
                    Arg.Any<string>(),
                    Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                    Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    published.AddRange(call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1));
                    return Task.FromResult(true);
                });

            var (grain, _, _, saved) = CreateLeafForCoverageRepair(
                persistedCheckpoint: -1L,
                reClassifyEveryN: 1000,
                treeId: treeId,
                maxConcurrentReplays: TimerAdmissionCeiling,
                replayCoordinator: wal.Coordinator,
                cursorReporter: reporter);

            await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

            // The live write: a row in the cache, and its entry appended to the
            // WAL after the leaf came online. Nothing on the write path moves the
            // checkpoint, which is the shape every tree on the estate was in.
            SeedRow(grain);
            wal.GrowTo(3);

            Assert.Multiple(() =>
            {
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(-1L),
                    "precondition: a live write does not advance the checkpoint.");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
                    "precondition: and no snapshot covers the partition.");
                Assert.That(
                    LeafDurablePinCore.Resolve(
                        currentCheckpoint: -1L, persistedCheckpoint: -1L, coveredOffset: -1L,
                        hasLiveData: true, walProvenEmpty: false, releaseNeverWrittenScannedThrough: false).Kind,
                    Is.EqualTo(LeafDurablePinKind.Block),
                    "precondition: so the pin this leaf publishes is the block the WAL GC reported.");
            });

            published.Clear();
            var reasons = new List<string>();
            using (ListenForDriverDeclinesOnTree(treeId, reasons))
            {
                await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
            }

            Assert.That(reasons, Does.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
                "input count: the tick must route the live-written leaf to the starvation drive.");

            Assert.Multiple(() =>
            {
                Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                    "the drive's replay supplies the checkpoint the live writes never did.");
                Assert.That(saved, Is.Not.Empty,
                    "a snapshot must be captured over the new checkpoint, or the pin stays min(3, -1).");
                Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                    "and its coverage must land at the checkpoint.");
                Assert.That(published, Is.Not.Empty,
                    "the pin must be republished after the drive.");
                Assert.That(published.Select(r => r.CheckpointOffset).DefaultIfEmpty(-1L).Max(), Is.EqualTo(3L),
                    "THE assertion: the offset on the wire - the one the WAL GC's offset floor reads - is usable "
                        + "after the leaf's first replay, so the tree's block ends rather than lasting without bound.");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
