using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #6166: a drive's replay resumes from the leaf's current read position, pending
/// advance included, and a leaf born in this activation is not driven as starved before
/// the birth hint its creator applies after the birth.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task A_drive_resumes_from_the_pending_checkpoint_rather_than_the_persisted_one()
    {
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateCoalescingLeafWithPinCapture(wal.Coordinator);
        wal.GrowTo(3);
        await ActivateAsync(grain);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0L), "precondition: the advance is held by the coalescing window");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L), "precondition: the pending advance is 3");
        });

        wal.GrowTo(5);
        wal.Coordinator.ClearReceivedCalls();
        await grain.DriveStarvedCheckpointAsync();

        var froms = ReadSliceStarts(wal.Coordinator);
        Assert.Multiple(() =>
        {
            Assert.That(froms, Is.Not.Empty, "control: the drive read the records appended after the activation");
            Assert.That(froms, Has.All.GreaterThanOrEqualTo(3L), "the drive re-read the span its pending advance had already applied");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(5L));
        });
    }

    [Test]
    public async Task A_leaf_born_in_this_activation_is_not_driven_as_starved_before_its_birth_hint()
    {
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateCoalescingLeafWithPinCapture(wal.Coordinator);
        state.State.TreeId = null;
        wal.GrowTo(3);
        await grain.InitializeSiblingAsync(new SiblingInitialization
        {
            TreeId = "tree-6166-birth-hint",
            ShardIndex = 0,
            LowKeyInclusive = "k",
        });
        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create([1], HybridLogicalClock.Tick(HybridLogicalClock.Zero)),
        });

        wal.Coordinator.ClearReceivedCalls();
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        Assert.That(ReadSliceStarts(wal.Coordinator), Is.Empty,
            "a born leaf awaiting its birth hint replayed every partition from its start");

        // The creator's hint ends the wait, even one that moves nothing.
        await grain.SetCheckpointOffsetHintsAsync([-1L]);
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        Assert.That(ReadSliceStarts(wal.Coordinator), Is.Not.Empty,
            "control: once its hint is delivered, a leaf still without a checkpoint is driven as starved");
    }

    private static List<long> ReadSliceStarts(ILeafReplayCoordinatorGrain coordinator) =>
        coordinator.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(ILeafReplayCoordinatorGrain.ReadSliceAsync))
            .Select(c => (long)c.GetArguments()[0]!)
            .ToList();
}
