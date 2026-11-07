using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

// Issue #5981 (W12 on #5605): ClearCheckpointAsync withdraws one partition's advisory offset
// claim, leaving the cursor and every other partition as they are, and leaves the consumer
// offset-aware (an empty map, never null) so drain age reads it as no claim, not unknown.
public partial class InMemoryWalCursorRegistryTests
{
    private static readonly HybridLogicalClock ClearCursor = new() { WallClockTicks = 100 };

    [Test]
    public async Task ClearCheckpoint_withdraws_one_partitions_offset_and_keeps_the_cursor_and_the_others()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCheckpointAsync("tree", "consumer", ClearCursor, 0, 10);
        await registry.ReportCheckpointAsync("tree", "consumer", ClearCursor, 1, 20);

        await registry.ClearCheckpointAsync("tree", "consumer", 0);

        var snapshot = (await registry.SnapshotAsync("tree")).Single();
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Cursor, Is.EqualTo(ClearCursor), "the cursor the WAL GC reads must not move");
            Assert.That(snapshot.CheckpointOffsets!.ContainsKey(0), Is.False, "the cleared partition still claims an offset");
            Assert.That(snapshot.CheckpointOffsets[1], Is.EqualTo(20L), "another partition's claim was disturbed");
        });
    }

    [Test]
    public async Task ClearCheckpoint_leaves_a_consumer_offset_aware_with_no_claim()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCheckpointAsync("tree", "claimed", ClearCursor, 0, 10);
        await registry.ReportCursorAsync("tree", "cursor-only", ClearCursor);

        await registry.ClearCheckpointAsync("tree", "claimed", 0);
        await registry.ClearCheckpointAsync("tree", "cursor-only", 0);
        await registry.ClearCheckpointAsync("tree", "unknown", 0);

        var snapshot = await registry.SnapshotAsync("tree");
        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Single(s => s.ConsumerId == "claimed").CheckpointOffsets, Is.Not.Null.And.Empty);
            Assert.That(snapshot.Single(s => s.ConsumerId == "cursor-only").CheckpointOffsets, Is.Not.Null.And.Empty,
                "a cursor-only consumer that withdraws must read as no claim, not as unknown history");
            Assert.That(snapshot.Select(s => s.ConsumerId), Has.No.Member("unknown"), "clearing must not register a consumer");
        });
    }

    [Test]
    public void ClearCheckpoint_defaults_to_a_no_op_on_a_custom_registry()
    {
        // Every member the custom registry implements throws, so the default clear must
        // complete without calling any of them.
        IWalCursorRegistry custom = new ThrowingRegistry();

        Assert.That(custom.ClearCheckpointAsync("tree", "consumer", 0).IsCompletedSuccessfully, Is.True);
    }

    private sealed class ThrowingRegistry : IWalCursorRegistry
    {
        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, HybridLogicalClock? blockedAtHlc, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, VersionVector vector, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ReportCursorAsync(string treeName, string consumerId, HybridLogicalClock cursor, VersionVector vector, HybridLogicalClock? blockedAtHlc, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task UnregisterAsync(string treeName, string consumerId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<HybridLogicalClock?> GetMinCursorAsync(string treeName, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<HybridLogicalClock?> GetMinCursorForDrainLagAsync(string treeName, long reportedAtOrAfterTicks, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<VersionVector?> GetCausalStableAsync(string treeName, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<HybridLogicalClock?> GetBlockedFloorAsync(string treeName, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<WalCursorSnapshot>> SnapshotAsync(string treeName, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
