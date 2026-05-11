using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Adapters;

namespace Orleans.Lattice.Replication.Tests.Adapters;

/// <summary>
/// Unit tests for <see cref="LeafCursorReporter"/>: confirms the
/// adapter registered by the leaf cursor-registry integration forwards every call verbatim to the
/// underlying <see cref="IWalCursorRegistry"/>.
/// </summary>
[TestFixture]
public class LeafCursorReporterTests
{
    [Test]
    public async Task ReportAsync_forwards_to_registry()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var reporter = new LeafCursorReporter(registry);
        var cursor = new HybridLogicalClock { WallClockTicks = 12345, Counter = 0 };
        using var cts = new CancellationTokenSource();

        await reporter.ReportAsync("tree-a", "consumer-x", cursor, cts.Token);

        await registry.Received(1).ReportCursorAsync("tree-a", "consumer-x", cursor, cts.Token);
    }

    [Test]
    public async Task UnregisterAsync_forwards_to_registry()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        var reporter = new LeafCursorReporter(registry);
        using var cts = new CancellationTokenSource();

        await reporter.UnregisterAsync("tree-a", "consumer-x", cts.Token);

        await registry.Received(1).UnregisterAsync("tree-a", "consumer-x", cts.Token);
    }

    [Test]
    public void ReportAsync_propagates_registry_exceptions()
    {
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.ReportCursorAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("registry failure")));
        var reporter = new LeafCursorReporter(registry);

        Assert.That(
            async () => await reporter.ReportAsync("tree-a", "consumer-x", new HybridLogicalClock { WallClockTicks = 1 }, default),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("registry failure"));
    }

    [Test]
    public async Task UnregisterTreeAsync_unregisters_only_materialiser_consumers_for_the_tree()
    {
        // Use the real in-memory registry so we exercise the
        // adapter's snapshot+filter+unregister path against a working
        // implementation rather than a mock with hand-crafted return
        // values.
        var registry = new InMemoryWalCursorRegistry();
        var hlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        await registry.ReportCursorAsync("tree-a", "_lattice_materialiser_tree-a_leaf-1", hlc, default);
        await registry.ReportCursorAsync("tree-a", "_lattice_materialiser_tree-a_leaf-2", hlc, default);
        await registry.ReportCursorAsync("tree-a", "peer-cluster-x", hlc, default);
        await registry.ReportCursorAsync("tree-b", "_lattice_materialiser_tree-b_leaf-1", hlc, default);

        var reporter = new LeafCursorReporter(registry);
        await reporter.UnregisterTreeAsync("tree-a", default);

        var treeASnapshot = await registry.SnapshotAsync("tree-a", default);
        var treeBSnapshot = await registry.SnapshotAsync("tree-b", default);

        // Both materialiser cursors for tree-a are gone, but the
        // peer-cluster cursor on tree-a is left alone (the prefix
        // filter is exact). tree-b is untouched.
        Assert.That(treeASnapshot.Count, Is.EqualTo(1));
        Assert.That(treeASnapshot[0].ConsumerId, Is.EqualTo("peer-cluster-x"));
        Assert.That(treeBSnapshot.Count, Is.EqualTo(1));
        Assert.That(treeBSnapshot[0].ConsumerId, Is.EqualTo("_lattice_materialiser_tree-b_leaf-1"));
    }

    [Test]
    public async Task UnregisterTreeAsync_is_no_op_for_unknown_tree()
    {
        var registry = new InMemoryWalCursorRegistry();
        var reporter = new LeafCursorReporter(registry);

        Assert.DoesNotThrowAsync(async () => await reporter.UnregisterTreeAsync("never-seen", default));
    }

    [Test]
    public async Task UnregisterTreeAsync_does_not_match_other_trees_with_shared_prefix()
    {
        // Defensive: "tree-a" and "tree-a-suffix" are distinct trees,
        // but "_lattice_materialiser_tree-a-suffix_*" starts with
        // "_lattice_materialiser_tree-a" - the adapter must filter on
        // the full "{prefix}{treeName}_" boundary so a sibling tree
        // is not accidentally swept.
        var registry = new InMemoryWalCursorRegistry();
        var hlc = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        await registry.ReportCursorAsync("tree-a", "_lattice_materialiser_tree-a_leaf-1", hlc, default);
        await registry.ReportCursorAsync("tree-a-suffix", "_lattice_materialiser_tree-a-suffix_leaf-1", hlc, default);

        var reporter = new LeafCursorReporter(registry);
        await reporter.UnregisterTreeAsync("tree-a", default);

        var siblingSnapshot = await registry.SnapshotAsync("tree-a-suffix", default);
        Assert.That(siblingSnapshot.Count, Is.EqualTo(1));
        Assert.That(siblingSnapshot[0].ConsumerId, Is.EqualTo("_lattice_materialiser_tree-a-suffix_leaf-1"));
    }
}