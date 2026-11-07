using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5969 (W11-1 on #5605): a leaf reports its APPLIED checkpoint to the in-memory cursor
/// registry, which only the drain-lag sampler reads, while its durable pin keeps the PERSISTED
/// checkpoint the WAL GC floors on. A born sibling whose hint is held back from persistence
/// until a capture covers it (#5877, H2) has applied through the hint, so it must not read as
/// lagging from the partition's first append - and its durable pin must not run ahead of what
/// is persisted.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_held_hint_is_reported_to_the_registry_as_applied_while_the_durable_pin_keeps_the_persisted_minus_one()
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        var reported = new List<(int Partition, long Offset)>();
        reporter.ReportCheckpointAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(),
                Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                reported.Add((ci.ArgAt<int>(3), ci.ArgAt<long>(4)));
                return Task.CompletedTask;
            });
        var pinned = new List<long>();
        reporter.FlushDurableMaterialiserFrontierAsync(Arg.Any<string>(), Arg.Any<IReadOnlyList<MaterialiserPinReport>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                pinned.AddRange(ci.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1).Select(r => r.CheckpointOffset));
                return Task.FromResult(true);
            });
        reporter.When(r => r.NoteDurableMaterialiserFrontier(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>()))
            .Do(ci => pinned.Add(ci.ArgAt<long>(3)));

        // The capture always fails, so the hint stays held: never persisted above -1.
        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            treeId: UniqueCoverageRepairTreeId("held-hint-report"),
            cursorReporter: reporter,
            materialiserCheckpointInterval: TimeSpan.FromHours(1));
        state.State.Clock = new HybridLogicalClock { WallClockTicks = 100, Counter = 1 };
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        // An activation report before the hint legitimately carries -1: nothing was applied yet.
        reported.Clear();
        await grain.SetCheckpointOffsetHintsAsync([7]);

        // A replay re-asserting the applied position is a force-flush: the leaf persists with
        // the hint still held (its capture fails) and runs the post-persist tail, which reports
        // the cursor and publishes the durable pin.
        using (LatticeApplyOffsetContext.BeginScope(0, 7))
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(PersistedPartitionZero(state.State), Is.EqualTo(-1L), "precondition: the hint is held, persisted at -1");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(7L), "precondition: the leaf applied through the hint");
            Assert.That(reported.Where(r => r.Partition == 0).Select(r => r.Offset), Is.Not.Empty,
                "precondition: the leaf reported its cursor");
            Assert.That(reported.Where(r => r.Partition == 0).Select(r => r.Offset), Has.All.EqualTo(7L),
                "the registry was given the persisted -1, which the drain-lag sampler read as lag from the partition's first append (#5969)");
            Assert.That(pinned, Is.Not.Empty, "precondition: the flush tail published the durable pin");
            Assert.That(pinned, Has.None.GreaterThanOrEqualTo(0L),
                "the durable pin ran ahead of the persisted checkpoint: the WAL GC floors on it, so it must keep -1 while the hint is held");
        });
    }
}
