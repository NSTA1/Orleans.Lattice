using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class InMemoryReplicationCursorRegistryTests
{
    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ReportCursorAsync_records_cursor_for_first_consumer()
    {
        var sut = new InMemoryReplicationCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));

        var min = await sut.GetMinCursorAsync("tree");
        Assert.That(min, Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task GetMinCursorAsync_returns_null_when_no_consumers_registered()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetMinCursorAsync_returns_minimum_across_consumers()
    {
        var sut = new InMemoryReplicationCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(50));
        await sut.ReportCursorAsync("tree", "peer-C", Hlc(300));

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task ReportCursorAsync_advances_existing_cursor_for_same_consumer()
    {
        var sut = new InMemoryReplicationCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task ReportCursorAsync_coalesces_stale_report_without_rolling_back()
    {
        var sut = new InMemoryReplicationCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(50));

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task UnregisterAsync_removes_consumer_from_min_computation()
    {
        var sut = new InMemoryReplicationCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(50));

        await sut.UnregisterAsync("tree", "peer-B");

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task UnregisterAsync_returns_null_when_last_consumer_removed()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));

        await sut.UnregisterAsync("tree", "peer-A");

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task UnregisterAsync_is_idempotent()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.UnregisterAsync("tree", "peer-A");
        await sut.UnregisterAsync("tree", "peer-A");
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task SnapshotAsync_returns_empty_when_no_consumers()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(await sut.SnapshotAsync("tree"), Is.Empty);
    }

    [Test]
    public async Task SnapshotAsync_returns_one_entry_per_consumer()
    {
        var sut = new InMemoryReplicationCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200));

        var snapshot = await sut.SnapshotAsync("tree");
        Assert.That(snapshot, Has.Count.EqualTo(2));
        Assert.That(snapshot.Select(s => s.ConsumerId), Is.EquivalentTo(new[] { "peer-A", "peer-B" }));
    }

    [Test]
    public async Task SnapshotAsync_records_last_reported_at_ticks()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        var before = DateTime.UtcNow.Ticks;

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));

        var after = DateTime.UtcNow.Ticks;
        var snapshot = await sut.SnapshotAsync("tree");
        var only = snapshot.Single();
        Assert.That(only.LastReportedAtTicks, Is.GreaterThanOrEqualTo(before).And.LessThanOrEqualTo(after));
    }

    [Test]
    public async Task GetMinCursorAsync_isolates_per_tree_state()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree-A", "peer-X", Hlc(100));
        await sut.ReportCursorAsync("tree-B", "peer-X", Hlc(500));

        Assert.That(await sut.GetMinCursorAsync("tree-A"), Is.EqualTo(Hlc(100)));
        Assert.That(await sut.GetMinCursorAsync("tree-B"), Is.EqualTo(Hlc(500)));
    }

    [Test]
    public void ReportCursorAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync(null!, "peer", Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_throws_on_whitespace_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("  ", "peer", Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_throws_on_null_consumer_id()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", null!, Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_throws_on_zero_cursor()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReportCursorAsync_observes_cancellation()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", Hlc(1), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void UnregisterAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.UnregisterAsync(null!, "peer"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void UnregisterAsync_throws_on_null_consumer_id()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.UnregisterAsync("tree", null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMinCursorAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.GetMinCursorAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SnapshotAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.SnapshotAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }
}
