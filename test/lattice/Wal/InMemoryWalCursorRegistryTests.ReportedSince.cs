using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

// The drain-lag sampler's copy of only the consumers that reported since its
// freshness cut-off (issue #6166). A large tree keeps a consumer per leaf per
// partition on every silo its leaves have lived on; copying them all every tick to
// find the fresh few was the dominant cost of a large tree's silo.
public sealed partial class InMemoryWalCursorRegistryTests
{
    [Test]
    public async Task CopyReportedSince_copies_only_consumers_reported_since_the_cut_off_newest_first()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCheckpointAsync(Tree, "stale-a", Hlc(10), 0, 1, CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "stale-b", Hlc(10), 1, 1, CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "moved", Hlc(10), 2, 1, CancellationToken.None);
        var cutOff = WaitUntilAfter((await SnapshotOfAsync(registry, "moved")).LastReportedAtTicks);

        await registry.ReportCheckpointAsync(Tree, "fresh", Hlc(20), 3, 2, CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "moved", Hlc(20), 2, 2, CancellationToken.None);

        var buffer = Array.Empty<WalCursorSnapshot>();
        var copied = registry.CopyReportedSince(Tree, cutOff, ref buffer);

        Assert.Multiple(() =>
        {
            Assert.That(copied, Is.EqualTo(2));
            Assert.That(buffer.Take(copied).Select(s => s.ConsumerId), Is.EqualTo(new[] { "moved", "fresh" }),
                "a re-report moves an older consumer ahead, and the copy stops at the first stale one");
            Assert.That(buffer[0].CheckpointOffsets![2], Is.EqualTo(2));
        });
    }

    [Test]
    public async Task CopyReportedSince_skips_unregistered_consumers_wherever_they_sat_in_report_order()
    {
        var registry = new InMemoryWalCursorRegistry();
        foreach (var id in new[] { "oldest", "middle-a", "middle-b", "newest" })
        {
            await registry.ReportCheckpointAsync(Tree, id, Hlc(10), 0, 1, CancellationToken.None);
        }

        await registry.UnregisterAsync(Tree, "oldest", CancellationToken.None);
        await registry.UnregisterAsync(Tree, "middle-a", CancellationToken.None);
        await registry.UnregisterAsync(Tree, "newest", CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "late", Hlc(10), 0, 1, CancellationToken.None);

        var buffer = Array.Empty<WalCursorSnapshot>();
        var copied = registry.CopyReportedSince(Tree, long.MinValue, ref buffer);

        Assert.That(buffer.Take(copied).Select(s => s.ConsumerId), Is.EqualTo(new[] { "late", "middle-b" }));
    }

    [Test]
    public async Task CopyReportedSince_shares_no_version_vector_and_allocates_nothing_into_a_buffer_that_fits()
    {
        var registry = new InMemoryWalCursorRegistry();
        var vector = new VersionVector();
        vector.Tick("origin");
        await registry.ReportCursorAsync(Tree, "vectored", Hlc(10), vector, CancellationToken.None);
        for (var i = 0; i < 1_000; i++)
        {
            await registry.ReportCheckpointAsync(Tree, $"consumer-{i}", Hlc(10), i % 16, i, CancellationToken.None);
        }

        var buffer = new WalCursorSnapshot[1_001];
        var before = GC.GetAllocatedBytesForCurrentThread();
        var copied = registry.CopyReportedSince(Tree, long.MinValue, ref buffer);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(copied, Is.EqualTo(1_001));
            Assert.That(allocated, Is.Zero, "the copy reuses the caller's buffer, where a snapshot allocates every consumer");
            Assert.That(buffer.Take(copied).Select(s => s.Vector), Has.All.Null, "the registry merges vectors in place, so none leaves its lock");
        });
    }
}
