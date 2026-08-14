using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Unit tests for <see cref="InMemoryLeafCursorReporter"/>, the lightweight
/// default <see cref="ILeafCursorReporter"/> wired by <c>AddLattice</c> so
/// drain-lag back-pressure is live for every write workload. Confirms it
/// forwards reporting/unregistration to the in-memory registry, scopes the
/// bulk tree unregister to the materialiser prefix, and treats every
/// durable-pin method as a no-op (the durable backstop stays opt-in).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class InMemoryLeafCursorReporterTests
{
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ReportAsync_forwards_cursor_to_the_registry()
    {
        var registry = new InMemoryWalCursorRegistry();
        var reporter = new InMemoryLeafCursorReporter(registry);
        var consumerId = ILeafCursorReporter.MaterialiserConsumerIdPrefix + Tree + "_leaf-1";

        await reporter.ReportAsync(Tree, consumerId, Hlc(42), CancellationToken.None);

        var min = await registry.GetMinCursorAsync(Tree, CancellationToken.None);
        Assert.That(min, Is.EqualTo(Hlc(42)),
            "ReportAsync must publish the leaf cursor into the registry so the saturation sampler can read it.");
    }

    [Test]
    public async Task UnregisterAsync_removes_the_consumer_from_the_registry()
    {
        var registry = new InMemoryWalCursorRegistry();
        var reporter = new InMemoryLeafCursorReporter(registry);
        var consumerId = ILeafCursorReporter.MaterialiserConsumerIdPrefix + Tree + "_leaf-1";

        await reporter.ReportAsync(Tree, consumerId, Hlc(42), CancellationToken.None);
        await reporter.UnregisterAsync(Tree, consumerId, CancellationToken.None);

        var snapshot = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapshot.Any(s => s.ConsumerId == consumerId), Is.False);
    }

    [Test]
    public async Task UnregisterTreeAsync_clears_materialiser_prefix_but_leaves_custom_consumers()
    {
        var registry = new InMemoryWalCursorRegistry();
        var reporter = new InMemoryLeafCursorReporter(registry);
        var materialiserId = ILeafCursorReporter.MaterialiserConsumerIdPrefix + Tree + "_leaf-1";
        const string customId = "custom-bridge";

        await registry.ReportCursorAsync(Tree, materialiserId, Hlc(10), CancellationToken.None);
        await registry.ReportCursorAsync(Tree, customId, Hlc(20), CancellationToken.None);

        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);

        var snapshot = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapshot.Any(s => s.ConsumerId == materialiserId), Is.False,
            "Materialiser-prefix consumer must be unregistered.");
        Assert.That(snapshot.Any(s => s.ConsumerId == customId), Is.True,
            "Custom-prefix consumer must survive UnregisterTreeAsync.");
    }

    [Test]
    public void NoteDurableMaterialiserFrontier_is_a_no_op_without_durable_backing()
    {
        var registry = new InMemoryWalCursorRegistry();
        var reporter = new InMemoryLeafCursorReporter(registry);

        // No grain factory / durable pin store is wired in; the lightweight
        // reporter must simply do nothing rather than throw.
        Assert.DoesNotThrow(() =>
            reporter.NoteDurableMaterialiserFrontier(Tree, "consumer", Hlc(99), 99));
    }

    [Test]
    public async Task Durable_seed_methods_complete_as_no_ops()
    {
        var registry = new InMemoryWalCursorRegistry();
        var reporter = new InMemoryLeafCursorReporter(registry);

        await reporter.SeedDurableMaterialiserBlockAsync(Tree, "consumer", HybridLogicalClock.Zero, CancellationToken.None);
        await reporter.SeedDurableMaterialiserBlockManyAsync(
            Tree,
            new[] { new MaterialiserPinReport("consumer", HybridLogicalClock.Zero, -1) },
            CancellationToken.None);

        // No durable backing means no pin is published anywhere; nothing leaks
        // into the in-memory cursor registry from the seed path.
        var snapshot = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapshot, Is.Empty);
    }
}
