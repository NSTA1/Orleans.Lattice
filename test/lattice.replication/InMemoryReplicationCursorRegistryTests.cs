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

    // ---- Causal+ overload ----------------------------------------------

    private static VersionVector Vc(params (string origin, long ticks)[] entries)
    {
        var vc = new VersionVector();
        foreach (var (origin, ticks) in entries)
        {
            vc.Entries[origin] = Hlc(ticks);
        }
        return vc;
    }

    [Test]
    public async Task ReportCursorAsync_vc_overload_records_vector_in_snapshot()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100), ("site-b", 50)));

        var snapshot = await sut.SnapshotAsync("tree");
        var only = snapshot.Single();
        Assert.That(only.Vector, Is.Not.Null);
        Assert.That(only.Vector!.Entries, Has.Count.EqualTo(2));
        Assert.That(only.Vector.Entries["site-a"], Is.EqualTo(Hlc(100)));
        Assert.That(only.Vector.Entries["site-b"], Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task GetCausalStableAsync_returns_null_when_no_consumers_registered()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(await sut.GetCausalStableAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetCausalStableAsync_returns_null_when_only_hlc_only_consumers_registered()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200));

        Assert.That(await sut.GetCausalStableAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetCausalStableAsync_returns_only_consumers_vector_when_only_one_reports_vc()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 50), ("site-b", 80)));

        var meet = await sut.GetCausalStableAsync("tree");
        Assert.That(meet, Is.Not.Null);
        Assert.That(meet!.Entries, Has.Count.EqualTo(2));
        Assert.That(meet.Entries["site-a"], Is.EqualTo(Hlc(50)));
        Assert.That(meet.Entries["site-b"], Is.EqualTo(Hlc(80)));
    }

    [Test]
    public async Task GetCausalStableAsync_returns_pointwise_min_across_consumers()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100), ("site-b", 200)));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200), Vc(("site-a", 50), ("site-b", 300)));

        var meet = await sut.GetCausalStableAsync("tree");
        Assert.That(meet, Is.Not.Null);
        Assert.That(meet!.Entries["site-a"], Is.EqualTo(Hlc(50)));
        Assert.That(meet.Entries["site-b"], Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task GetCausalStableAsync_drops_origins_missing_from_any_consumer()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        // peer-A knows about site-a and site-b; peer-B only knows
        // about site-a. site-b cannot be in the meet because peer-B
        // has not proven it has observed site-b at all.
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100), ("site-b", 200)));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(150), Vc(("site-a", 80)));

        var meet = await sut.GetCausalStableAsync("tree");
        Assert.That(meet, Is.Not.Null);
        Assert.That(meet!.Entries, Has.Count.EqualTo(1));
        Assert.That(meet.Entries.ContainsKey("site-b"), Is.False);
        Assert.That(meet.Entries["site-a"], Is.EqualTo(Hlc(80)));
    }

    [Test]
    public async Task GetCausalStableAsync_skips_hlc_only_consumers_in_mixed_registry()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100)));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(50)); // HLC-only

        var meet = await sut.GetCausalStableAsync("tree");
        Assert.That(meet, Is.Not.Null);
        Assert.That(meet!.Entries["site-a"], Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task ReportCursorAsync_vc_overload_coalesces_pointwise_max_for_existing_consumer()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100), ("site-b", 50)));
        // Second report carries a smaller site-a but a larger site-b
        // and adds a new origin site-c. The merge should keep the
        // pointwise max.
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(150), Vc(("site-a", 80), ("site-b", 200), ("site-c", 10)));

        var snapshot = await sut.SnapshotAsync("tree");
        var only = snapshot.Single();
        Assert.That(only.Cursor, Is.EqualTo(Hlc(150)));
        Assert.That(only.Vector!.Entries["site-a"], Is.EqualTo(Hlc(100)));
        Assert.That(only.Vector.Entries["site-b"], Is.EqualTo(Hlc(200)));
        Assert.That(only.Vector.Entries["site-c"], Is.EqualTo(Hlc(10)));
    }

    [Test]
    public async Task ReportCursorAsync_vc_overload_does_not_drop_existing_vector_when_followed_by_hlc_only_report()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100)));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));

        var snapshot = await sut.SnapshotAsync("tree");
        var only = snapshot.Single();
        Assert.That(only.Cursor, Is.EqualTo(Hlc(200)));
        Assert.That(only.Vector, Is.Not.Null);
        Assert.That(only.Vector!.Entries["site-a"], Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task ReportCursorAsync_vc_overload_attaches_vector_to_existing_hlc_only_consumer()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200), Vc(("site-a", 50)));

        var snapshot = await sut.SnapshotAsync("tree");
        var only = snapshot.Single();
        Assert.That(only.Vector, Is.Not.Null);
        Assert.That(only.Vector!.Entries["site-a"], Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task UnregisterAsync_recomputes_causal_stable_to_remaining_consumers()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 50)));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200), Vc(("site-a", 30)));

        // Initial meet pins site-a to 30 (the slower of the two).
        var initialMeet = await sut.GetCausalStableAsync("tree");
        Assert.That(initialMeet!.Entries["site-a"], Is.EqualTo(Hlc(30)));

        // Drop the slow consumer and re-read; the cache must invalidate
        // and the meet should now reflect peer-A only.
        await sut.UnregisterAsync("tree", "peer-B");
        var afterMeet = await sut.GetCausalStableAsync("tree");
        Assert.That(afterMeet!.Entries["site-a"], Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task GetCausalStableAsync_returned_clone_is_isolated_from_registry_state()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 50)));

        var meet = await sut.GetCausalStableAsync("tree");
        Assert.That(meet, Is.Not.Null);
        meet!.Entries["site-a"] = Hlc(99999);
        meet.Entries["site-z"] = Hlc(1);

        var second = await sut.GetCausalStableAsync("tree");
        Assert.That(second!.Entries["site-a"], Is.EqualTo(Hlc(50)));
        Assert.That(second.Entries.ContainsKey("site-z"), Is.False);
    }

    [Test]
    public async Task ReportCursorAsync_vc_overload_defensively_clones_input_vector()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        var caller = Vc(("site-a", 50));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), caller);

        // Caller continues to mutate after the report returns.
        caller.Entries["site-a"] = Hlc(99999);
        caller.Entries["site-z"] = Hlc(1);

        var snapshot = await sut.SnapshotAsync("tree");
        var only = snapshot.Single();
        Assert.That(only.Vector!.Entries["site-a"], Is.EqualTo(Hlc(50)));
        Assert.That(only.Vector.Entries.ContainsKey("site-z"), Is.False);
    }

    [Test]
    public void ReportCursorAsync_vc_overload_throws_on_null_vector()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", Hlc(1), null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ReportCursorAsync_vc_overload_throws_on_null_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync(null!, "peer", Hlc(1), Vc(("o", 1))),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_vc_overload_throws_on_zero_cursor()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", HybridLogicalClock.Zero, Vc(("o", 1))),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetCausalStableAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        Assert.That(
            async () => await sut.GetCausalStableAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetCausalStableAsync_observes_cancellation()
    {
        var sut = new InMemoryReplicationCursorRegistry();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await sut.GetCausalStableAsync("tree", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
