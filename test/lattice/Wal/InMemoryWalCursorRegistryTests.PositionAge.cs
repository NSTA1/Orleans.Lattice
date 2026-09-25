using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

// Position-age discrimination on the drain-lag lag plane (issue #3131). A leaf
// materialiser re-reports its persisted, possibly ancient, clock with a fresh
// report time on every activation, so report age alone cannot tell a leaf whose
// key range has seen no write from one that is genuinely behind.
public sealed partial class InMemoryWalCursorRegistryTests
{
    private const string LeafConsumer = ILeafCursorReporter.MaterialiserConsumerIdPrefix + Tree + "_leaf-a";

    private static async Task<WalCursorSnapshot> SnapshotOfAsync(InMemoryWalCursorRegistry registry, string consumerId)
        => (await registry.SnapshotAsync(Tree, CancellationToken.None)).Single(s => s.ConsumerId == consumerId);

    [Test]
    public async Task GetMinCursorForDrainLagAsync_excludes_leaf_re_reporting_an_unmoved_ancient_position()
    {
        var registry = new InMemoryWalCursorRegistry();

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);
        var freshnessFloor = WaitUntilAfter((await SnapshotOfAsync(registry, LeafConsumer)).LastReportedAtTicks);

        // The activation / idempotent-checkpoint re-report: same position, fresh report time.
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);

        var reReported = await SnapshotOfAsync(registry, LeafConsumer);
        var lagPlaneMin = await registry.GetMinCursorForDrainLagAsync(Tree, freshnessFloor, CancellationToken.None);
        var trimFloorMin = await registry.GetMinCursorAsync(Tree, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reReported.LastReportedAtTicks, Is.GreaterThanOrEqualTo(freshnessFloor),
                "precondition: the re-report is fresh, so report age alone would count this leaf");
            Assert.That(lagPlaneMin, Is.Null,
                "a leaf whose position has not moved since the floor and whose cursor predates it is caught up, not behind");
            Assert.That(trimFloorMin, Is.EqualTo(Hlc(100)),
                "the trim floor must still pin the leaf's position so GC cannot trim entries a later activation may replay");
        });
    }

    [Test]
    public async Task GetMinCursorForDrainLagAsync_keeps_leaf_whose_position_advanced_after_the_floor()
    {
        var registry = new InMemoryWalCursorRegistry();

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);
        var freshnessFloor = WaitUntilAfter((await SnapshotOfAsync(registry, LeafConsumer)).LastReportedAtTicks);

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(200), CancellationToken.None);

        var lagPlaneMin = await registry.GetMinCursorForDrainLagAsync(Tree, freshnessFloor, CancellationToken.None);

        Assert.That(lagPlaneMin, Is.EqualTo(Hlc(200)),
            "a leaf that is draining advances its cursor, and must stay on the lag plane however far behind the head it is");
    }

    [Test]
    public async Task GetMinCursorForDrainLagAsync_keeps_leaf_whose_first_report_is_a_recent_position()
    {
        var registry = new InMemoryWalCursorRegistry();
        var freshnessFloor = DateTime.UtcNow.Ticks;
        var recent = Hlc(WaitUntilAfter(freshnessFloor));

        await registry.ReportCursorAsync(Tree, LeafConsumer, recent, CancellationToken.None);

        var lagPlaneMin = await registry.GetMinCursorForDrainLagAsync(Tree, freshnessFloor, CancellationToken.None);

        Assert.That(lagPlaneMin, Is.EqualTo(recent),
            "a leaf registering a position inside the freshness window has no position-age history yet but is plainly current");
    }

    [Test]
    public async Task GetMinCursorForDrainLagAsync_keeps_tree_wide_consumer_re_reporting_an_unmoved_position()
    {
        var registry = new InMemoryWalCursorRegistry();
        const string treeWideTailer = "view-maintainer";

        await registry.ReportCursorAsync(Tree, treeWideTailer, Hlc(100), CancellationToken.None);
        var freshnessFloor = WaitUntilAfter((await SnapshotOfAsync(registry, treeWideTailer)).LastReportedAtTicks);
        await registry.ReportCursorAsync(Tree, treeWideTailer, Hlc(100), CancellationToken.None);

        var lagPlaneMin = await registry.GetMinCursorForDrainLagAsync(Tree, freshnessFloor, CancellationToken.None);

        Assert.That(lagPlaneMin, Is.EqualTo(Hlc(100)),
            "a tree-wide tailer shares the head's scope, so a stalled one IS backlog and must not be hidden by the position-age exclusion");
    }

    [Test]
    public async Task GetMinCursorForDrainLagAsync_min_value_floor_keeps_position_stale_leaf()
    {
        var registry = new InMemoryWalCursorRegistry();

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);

        var lagPlaneMin = await registry.GetMinCursorForDrainLagAsync(Tree, long.MinValue, CancellationToken.None);

        Assert.That(lagPlaneMin, Is.EqualTo(Hlc(100)),
            "zero freshness (long.MinValue) must disable the position-age exclusion along with the report-age one");
    }

    [Test]
    public async Task SnapshotAsync_stamps_cursor_advanced_at_only_on_a_strict_advance()
    {
        var registry = new InMemoryWalCursorRegistry();

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);
        var registered = await SnapshotOfAsync(registry, LeafConsumer);

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);
        var reReported = await SnapshotOfAsync(registry, LeafConsumer);

        var beforeAdvance = WaitUntilAfter(reReported.LastReportedAtTicks);
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(200), CancellationToken.None);
        var advanced = await SnapshotOfAsync(registry, LeafConsumer);

        WaitUntilAfter(advanced.LastReportedAtTicks);
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(150), CancellationToken.None);
        var regressed = await SnapshotOfAsync(registry, LeafConsumer);

        Assert.Multiple(() =>
        {
            Assert.That(registered.CursorAdvancedAtTicks, Is.EqualTo(0L),
                "the registering report asserts a position; it does not show the consumer draining");
            Assert.That(reReported.CursorAdvancedAtTicks, Is.EqualTo(0L),
                "a re-report of the same position must not stamp an advance");
            Assert.That(advanced.CursorAdvancedAtTicks, Is.GreaterThanOrEqualTo(beforeAdvance),
                "a strictly higher cursor must stamp the advance time");
            Assert.That(regressed.Cursor, Is.EqualTo(Hlc(200)), "precondition: the registry keeps the max cursor");
            Assert.That(regressed.CursorAdvancedAtTicks, Is.EqualTo(advanced.CursorAdvancedAtTicks),
                "a lower cursor is not an advance and must keep the prior stamp");
            Assert.That(regressed.LastReportedAtTicks, Is.GreaterThan(advanced.LastReportedAtTicks),
                "precondition: the report time still moves on every report");
        });
    }

    [Test]
    public async Task SnapshotAsync_first_real_cursor_after_blocked_floor_only_registration_is_not_an_advance()
    {
        var registry = new InMemoryWalCursorRegistry();

        await registry.ReportCursorAsync(
            Tree,
            LeafConsumer,
            HybridLogicalClock.Zero,
            blockedAtHlc: Hlc(50),
            cancellationToken: CancellationToken.None);
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(100), CancellationToken.None);

        var snapshot = await SnapshotOfAsync(registry, LeafConsumer);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Cursor, Is.EqualTo(Hlc(100)), "precondition: the real cursor was recorded");
            Assert.That(snapshot.CursorAdvancedAtTicks, Is.EqualTo(0L),
                "moving off Zero asserts a first position, exactly like a registering report, so it must not stamp an advance");
        });
    }
}
