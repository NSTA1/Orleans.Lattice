using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

public partial class InMemoryWalCursorRegistryTests
{
    [Test]
    public async Task GetRegisteredTreeNames_includes_cursor_only_trees_and_retires_the_last_consumer()
    {
        var registry = new InMemoryWalCursorRegistry();
        Assert.That(await registry.GetRegisteredTreeNamesAsync(), Is.Empty);
        await registry.ReportCheckpointAsync("restarted", "consumer",
            new HybridLogicalClock { WallClockTicks = 100 }, 0, 10);
        Assert.That(await registry.GetRegisteredTreeNamesAsync(), Is.EqualTo(new[] { "restarted" }));
        await registry.UnregisterAsync("restarted", "consumer");
        Assert.That(await registry.GetRegisteredTreeNamesAsync(), Is.Empty);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Assert.That(() => registry.GetRegisteredTreeNamesAsync(cancellation.Token),
            Throws.TypeOf<OperationCanceledException>());
    }

    [Test]
    public async Task ReportCheckpoint_offsets_advance_independently_and_preserve_causal_and_blocked_floors()
    {
        var registry = new InMemoryWalCursorRegistry();
        var cursor = new HybridLogicalClock { WallClockTicks = 100 };
        var blocked = new HybridLogicalClock { WallClockTicks = 50 };
        var vector = new VersionVector();
        vector.Tick("origin");
        await registry.ReportCursorAsync("checkpoints", "consumer", cursor, vector, blocked);
        await registry.ReportCheckpointAsync("checkpoints", "consumer", cursor, 0, 10);
        var before = (await registry.SnapshotAsync("checkpoints")).Single();

        await registry.ReportCheckpointAsync("checkpoints", "consumer", cursor, 0, 20);
        await registry.ReportCheckpointAsync("checkpoints", "consumer", cursor, 0, 15);
        await registry.ReportCheckpointAsync("checkpoints", "consumer", cursor, 1, 30);
        var after = (await registry.SnapshotAsync("checkpoints")).Single();

        Assert.Multiple(() =>
        {
            Assert.That(before.CheckpointOffsets![0], Is.EqualTo(10), "published snapshots must not mutate");
            Assert.That(after.CheckpointOffsets![0], Is.EqualTo(20));
            Assert.That(after.CheckpointOffsets[1], Is.EqualTo(30));
            Assert.That(after.Cursor, Is.EqualTo(cursor));
            Assert.That(after.BlockedAtHlc, Is.EqualTo(blocked));
            Assert.That(after.CursorAdvancedAtTicks, Is.EqualTo(before.CursorAdvancedAtTicks));
        });
        Assert.That(await registry.GetMinCursorAsync("checkpoints"), Is.EqualTo(cursor));
        Assert.That(await registry.GetBlockedFloorAsync("checkpoints"), Is.EqualTo(blocked));
        Assert.That((await registry.GetCausalStableAsync("checkpoints"))!.DominatesOrEquals(vector), Is.True);
    }

    [TestCase(-1, 0)]
    [TestCase(0, -2)]
    public void ReportCheckpoint_rejects_invalid_partition_or_offset(int partition, long offset)
    {
        var registry = new InMemoryWalCursorRegistry();
        Assert.That(() => registry.ReportCheckpointAsync("tree", "consumer",
            new HybridLogicalClock { WallClockTicks = 1 }, partition, offset),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReportCheckpoint_cancellation_does_not_register_a_consumer()
    {
        var registry = new InMemoryWalCursorRegistry();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(() => registry.ReportCheckpointAsync("tree", "consumer",
            new HybridLogicalClock { WallClockTicks = 1 }, 0, 0, cts.Token),
            Throws.TypeOf<OperationCanceledException>());
        Assert.That(await registry.SnapshotAsync("tree"), Is.Empty);
    }
}
