using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class InMemoryWalCursorRegistryTests
{
    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ReportCursorAsync_records_cursor_for_first_consumer()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));

        var min = await sut.GetMinCursorAsync("tree");
        Assert.That(min, Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task GetMinCursorAsync_returns_null_when_no_consumers_registered()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetMinCursorAsync_returns_minimum_across_consumers()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(50));
        await sut.ReportCursorAsync("tree", "peer-C", Hlc(300));

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task ReportCursorAsync_advances_existing_cursor_for_same_consumer()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task ReportCursorAsync_coalesces_stale_report_without_rolling_back()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(50));

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task UnregisterAsync_removes_consumer_from_min_computation()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(50));

        await sut.UnregisterAsync("tree", "peer-B");

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task UnregisterAsync_returns_null_when_last_consumer_removed()
    {
        var sut = new InMemoryWalCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));

        await sut.UnregisterAsync("tree", "peer-A");

        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task UnregisterAsync_is_idempotent()
    {
        var sut = new InMemoryWalCursorRegistry();
        await sut.UnregisterAsync("tree", "peer-A");
        await sut.UnregisterAsync("tree", "peer-A");
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task SnapshotAsync_returns_empty_when_no_consumers()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(await sut.SnapshotAsync("tree"), Is.Empty);
    }

    [Test]
    public async Task SnapshotAsync_returns_one_entry_per_consumer()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200));

        var snapshot = await sut.SnapshotAsync("tree");
        Assert.That(snapshot, Has.Count.EqualTo(2));
        Assert.That(snapshot.Select(s => s.ConsumerId), Is.EquivalentTo(new[] { "peer-A", "peer-B" }));
    }

    [Test]
    public async Task SnapshotAsync_records_last_reported_at_ticks()
    {
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
        await sut.ReportCursorAsync("tree-A", "peer-X", Hlc(100));
        await sut.ReportCursorAsync("tree-B", "peer-X", Hlc(500));

        Assert.That(await sut.GetMinCursorAsync("tree-A"), Is.EqualTo(Hlc(100)));
        Assert.That(await sut.GetMinCursorAsync("tree-B"), Is.EqualTo(Hlc(500)));
    }

    [Test]
    public void ReportCursorAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync(null!, "peer", Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_throws_on_whitespace_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("  ", "peer", Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_throws_on_null_consumer_id()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", null!, Hlc(1)),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_throws_on_zero_cursor()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReportCursorAsync_observes_cancellation()
    {
        var sut = new InMemoryWalCursorRegistry();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", Hlc(1), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void UnregisterAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.UnregisterAsync(null!, "peer"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void UnregisterAsync_throws_on_null_consumer_id()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.UnregisterAsync("tree", null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetMinCursorAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.GetMinCursorAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SnapshotAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(await sut.GetCausalStableAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetCausalStableAsync_returns_null_when_only_hlc_only_consumers_registered()
    {
        var sut = new InMemoryWalCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200));

        Assert.That(await sut.GetCausalStableAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetCausalStableAsync_returns_only_consumers_vector_when_only_one_reports_vc()
    {
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100), Vc(("site-a", 100)));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(50)); // HLC-only

        var meet = await sut.GetCausalStableAsync("tree");
        Assert.That(meet, Is.Not.Null);
        Assert.That(meet!.Entries["site-a"], Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task ReportCursorAsync_vc_overload_coalesces_pointwise_max_for_existing_consumer()
    {
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
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
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", Hlc(1), vector: (VersionVector)null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ReportCursorAsync_vc_overload_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync(null!, "peer", Hlc(1), Vc(("o", 1))),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ReportCursorAsync_vc_overload_throws_on_zero_cursor()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer", HybridLogicalClock.Zero, Vc(("o", 1))),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetCausalStableAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.GetCausalStableAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetCausalStableAsync_observes_cancellation()
    {
        var sut = new InMemoryWalCursorRegistry();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await sut.GetCausalStableAsync("tree", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // ---- Blocked-floor overload ----------------------------------------

    [Test]
    public async Task ReportCursorAsync_blocked_floor_overload_accepts_zero_cursor()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(500));

        var floor = await sut.GetBlockedFloorAsync("tree");
        Assert.That(floor, Is.EqualTo(Hlc(500)));
        // The Zero-cursor consumer must not pollute the GC's HLC min(cursor)
        // branch - GetMinCursorAsync skips it.
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.Null);
    }

    [Test]
    public void ReportCursorAsync_blocked_floor_overload_rejects_negative_cursor()
    {
        var sut = new InMemoryWalCursorRegistry();
        var negative = new HybridLogicalClock { WallClockTicks = -1, Counter = 0 };
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "applier", negative, blockedAtHlc: Hlc(500)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReportCursorAsync_blocked_floor_overload_rejects_negative_blocked_at_hlc()
    {
        var sut = new InMemoryWalCursorRegistry();
        var negative = new HybridLogicalClock { WallClockTicks = -1, Counter = 0 };
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "applier", Hlc(100), blockedAtHlc: negative),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReportCursorAsync_legacy_overload_still_rejects_zero_cursor()
    {
        // Regression: the blocked-floor overloads relax cursor=Zero; the
        // legacy single-arg overload must keep rejecting it because a
        // legacy consumer has no buffer pin and therefore would silently
        // pin the GC's HLC branch at Zero.
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.ReportCursorAsync("tree", "peer-A", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetBlockedFloorAsync_returns_null_when_no_consumer_registered()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetBlockedFloorAsync_returns_null_when_all_consumers_report_null_pin()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "peer-A", Hlc(100));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(200), blockedAtHlc: null);

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task GetBlockedFloorAsync_returns_pointwise_min_across_consumers()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier-A", HybridLogicalClock.Zero, blockedAtHlc: Hlc(500));
        await sut.ReportCursorAsync("tree", "applier-B", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));
        await sut.ReportCursorAsync("tree", "applier-C", HybridLogicalClock.Zero, blockedAtHlc: Hlc(800));

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(300)));
    }

    [Test]
    public async Task GetBlockedFloorAsync_skips_consumers_with_null_pin()
    {
        var sut = new InMemoryWalCursorRegistry();

        // Applier-A reports a pin; peer-B reports a cursor only (no pin)
        // - the meet is the applier's pin alone, not influenced by the
        // peer's HLC cursor.
        await sut.ReportCursorAsync("tree", "applier-A", HybridLogicalClock.Zero, blockedAtHlc: Hlc(500));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(100));

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(500)));
    }

    [Test]
    public async Task ReportCursorAsync_blocked_floor_replace_semantics_advances_forward()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));
        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(700));

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(700)));
    }

    [Test]
    public async Task ReportCursorAsync_blocked_floor_replace_semantics_can_lower_pin()
    {
        // Replace, not monotonic-merge: as the buffer admits new
        // transactions the lowest staged HLC can drop, and the registry
        // must reflect the new pin.
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(700));
        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(300)));
    }

    [Test]
    public async Task ReportCursorAsync_blocked_floor_replace_semantics_clears_to_null()
    {
        // The buffer drains: applier reports null to release the pin.
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));
        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(300)));

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: null);

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.Null);
    }

    [Test]
    public async Task ReportCursorAsync_legacy_overload_does_not_disturb_existing_blocked_floor()
    {
        // A legacy HLC-only re-report from the same consumer must leave
        // its buffer pin untouched (the parameter was not specified).
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", Hlc(100), blockedAtHlc: Hlc(500));
        await sut.ReportCursorAsync("tree", "applier", Hlc(200));

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(500)));
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task GetMinCursorAsync_skips_zero_cursor_blocked_floor_only_consumers()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(50));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));
        await sut.ReportCursorAsync("tree", "peer-B", Hlc(300));

        // The applier's Zero cursor must not be the GC's min - that
        // would freeze the cursor branch of the predicate forever.
        Assert.That(await sut.GetMinCursorAsync("tree"), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task SnapshotAsync_includes_blocked_at_hlc_per_consumer()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(500));
        await sut.ReportCursorAsync("tree", "peer-A", Hlc(200));

        var snapshot = await sut.SnapshotAsync("tree");
        Assert.That(snapshot, Has.Count.EqualTo(2));
        var applier = snapshot.Single(s => s.ConsumerId == "applier");
        var peer = snapshot.Single(s => s.ConsumerId == "peer-A");
        Assert.That(applier.BlockedAtHlc, Is.EqualTo(Hlc(500)));
        Assert.That(peer.BlockedAtHlc, Is.Null);
    }

    [Test]
    public async Task UnregisterAsync_invalidates_blocked_floor_cache()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier-A", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));
        await sut.ReportCursorAsync("tree", "applier-B", HybridLogicalClock.Zero, blockedAtHlc: Hlc(500));

        // Prime the cache.
        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(300)));

        // Unregister the consumer pinning the lower bound; the cache must
        // be invalidated so the next read recomputes against the
        // surviving consumer.
        await sut.UnregisterAsync("tree", "applier-A");

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(500)));
    }

    [Test]
    public async Task ReportCursorAsync_invalidates_blocked_floor_cache()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));
        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(300)));

        // The applier reports a new pin; the cached value must be
        // invalidated so the next read returns the updated floor.
        await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(700));

        Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(700)));
    }

    [Test]
    public async Task GetBlockedFloorAsync_isolates_per_tree()
    {
        var sut = new InMemoryWalCursorRegistry();

        await sut.ReportCursorAsync("tree-A", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(300));
        await sut.ReportCursorAsync("tree-B", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(700));

        Assert.That(await sut.GetBlockedFloorAsync("tree-A"), Is.EqualTo(Hlc(300)));
        Assert.That(await sut.GetBlockedFloorAsync("tree-B"), Is.EqualTo(Hlc(700)));
    }

    [Test]
    public void GetBlockedFloorAsync_throws_on_null_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.GetBlockedFloorAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetBlockedFloorAsync_throws_on_whitespace_tree_name()
    {
        var sut = new InMemoryWalCursorRegistry();
        Assert.That(
            async () => await sut.GetBlockedFloorAsync("   "),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetBlockedFloorAsync_observes_cancellation()
    {
        var sut = new InMemoryWalCursorRegistry();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await sut.GetBlockedFloorAsync("tree", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // ---- Concurrent-race coverage --------------------------------------

    /// <summary>
    /// Concurrent <c>UnregisterAsync</c> and
    /// <c>ReportCursorAsync</c> calls against the same consumer must
    /// leave the registry in one of two well-defined terminal states
    /// (either the report wins and the consumer is present with the
    /// new pin, or the unregister wins and the consumer is absent),
    /// with no torn state, no exception, no missing snapshot row, and
    /// no stale cached blocked-floor.
    /// </summary>
    [Test]
    public async Task ReportCursorAsync_and_UnregisterAsync_concurrent_race_leaves_consistent_state()
    {
        // 64 iterations to give the race window a fair chance to flip
        // each direction at least once on a multi-core host.
        for (var iter = 0; iter < 64; iter++)
        {
            var sut = new InMemoryWalCursorRegistry();

            // Seed an initial pin so a stale cache entry would be
            // observable if invalidation broke under contention.
            await sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(100));
            Assert.That(await sut.GetBlockedFloorAsync("tree"), Is.EqualTo(Hlc(100)));

            using var start = new ManualResetEventSlim(false);
            var reportTask = Task.Run(() =>
            {
                start.Wait();
                return sut.ReportCursorAsync("tree", "applier", HybridLogicalClock.Zero, blockedAtHlc: Hlc(200));
            });
            var unregisterTask = Task.Run(() =>
            {
                start.Wait();
                return sut.UnregisterAsync("tree", "applier");
            });
            start.Set();
            await Task.WhenAll(reportTask, unregisterTask);

            var snapshot = await sut.SnapshotAsync("tree");
            var floor = await sut.GetBlockedFloorAsync("tree");

            if (snapshot.Count == 0)
            {
                // Unregister won the race: the registry is empty and the
                // floor must report null. A stale cached 100 or 200 here
                // would mean Unregister failed to invalidate the cache.
                Assert.That(floor, Is.Null,
                    $"iteration {iter}: unregister winner must clear the cached blocked-floor");
            }
            else
            {
                // Report won the race: exactly one snapshot entry, with
                // the new pin (200) -- never a stale 100, never a torn
                // intermediate value.
                Assert.That(snapshot, Has.Count.EqualTo(1),
                    $"iteration {iter}: report winner must leave exactly one consumer row");
                Assert.That(snapshot[0].BlockedAtHlc, Is.EqualTo(Hlc(200)),
                    $"iteration {iter}: report winner must reflect the latest pin, never a stale value");
                Assert.That(floor, Is.EqualTo(Hlc(200)),
                    $"iteration {iter}: report winner must invalidate the cached floor");
            }
        }
    }
}
