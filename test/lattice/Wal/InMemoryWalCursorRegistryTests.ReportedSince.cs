using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

// The drain-lag sampler's visit of only the consumers that reported since its
// freshness cut-off, in place (issue #6166). A large tree keeps a consumer per leaf
// per partition on every silo its leaves have lived on; copying them all every tick
// to find the fresh few was the dominant cost of a large tree's silo.
public sealed partial class InMemoryWalCursorRegistryTests
{
    [Test]
    public async Task VisitReportedSince_visits_only_consumers_reported_since_the_cut_off_newest_first()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCheckpointAsync(Tree, "stale-a", Hlc(10), 0, 1, CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "stale-b", Hlc(10), 1, 1, CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "moved", Hlc(10), 2, 1, CancellationToken.None);
        var cutOff = WaitUntilAfter((await SnapshotOfAsync(registry, "moved")).LastReportedAtTicks);

        await registry.ReportCheckpointAsync(Tree, "fresh", Hlc(20), 3, 2, CancellationToken.None);
        await registry.ReportCheckpointAsync(Tree, "moved", Hlc(20), 2, 2, CancellationToken.None);

        var visitor = new RecordingVisitor(new string[4]);
        var visited = registry.VisitReportedSince(Tree, cutOff, ref visitor);

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(2));
            Assert.That(visitor.Seen.Take(visitor.Count), Is.EqualTo(new[] { "moved", "fresh" }),
                "a re-report moves an older consumer ahead, and the walk stops at the first stale one");
        });
    }

    [Test]
    public async Task VisitReportedSince_skips_unregistered_consumers_wherever_they_sat_in_report_order()
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

        var visitor = new RecordingVisitor(new string[4]);
        registry.VisitReportedSince(Tree, long.MinValue, ref visitor);

        Assert.That(visitor.Seen.Take(visitor.Count), Is.EqualTo(new[] { "late", "middle-b" }));
    }

    [Test]
    public async Task VisitReportedSince_allocates_nothing()
    {
        var registry = new InMemoryWalCursorRegistry();
        for (var i = 0; i < 1_000; i++)
        {
            await registry.ReportCheckpointAsync(Tree, $"consumer-{i}", Hlc(10), i % 16, i, CancellationToken.None);
        }

        var visitor = new RecordingVisitor(new string[1_000]);
        registry.VisitReportedSince(Tree, long.MinValue, ref visitor);
        visitor = new RecordingVisitor(visitor.Seen);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var visited = registry.VisitReportedSince(Tree, long.MinValue, ref visitor);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(1_000));
            Assert.That(allocated, Is.Zero, "the visit hands each consumer over in place, where a snapshot copies them all");
        });
    }

    private struct RecordingVisitor(string[] seen) : IWalCursorVisitor
    {
        public readonly string[] Seen = seen;
        public int Count;

        public void Visit(in WalCursorSnapshot snapshot) => Seen[Count++] = snapshot.ConsumerId;
    }
}
