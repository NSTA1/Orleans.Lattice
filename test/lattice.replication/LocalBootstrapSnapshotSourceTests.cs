using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for <see cref="LocalBootstrapSnapshotSource"/>, the
/// default <see cref="IBootstrapSnapshotSource"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// when no <see cref="IRemoteSnapshotTransport"/> is registered.
/// </summary>
[TestFixture]
public class LocalBootstrapSnapshotSourceTests
{
    [Test]
    public void Constructor_throws_when_local_provider_is_null()
    {
        Assert.That(
            () => new LocalBootstrapSnapshotSource(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ExportAsync_two_arg_forwards_to_local_provider()
    {
        var local = Substitute.For<ISnapshotProvider>();
        var expected = new SnapshotStream(
            "tree-a",
            HybridLogicalClock.Zero,
            new VersionVector(),
            EmptyAsync());
        local.ExportAsync("tree-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(expected));

        var source = new LocalBootstrapSnapshotSource(local);
        var actual = await source.ExportAsync("tree-a", HybridLogicalClock.Zero, CancellationToken.None);

        Assert.That(actual, Is.SameAs(expected));
        await local.Received(1).ExportAsync("tree-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExportAsync_three_arg_forwards_to_local_provider_with_source_cluster_id()
    {
        var local = Substitute.For<ISnapshotProvider>();
        var expected = new SnapshotStream(
            "tree-b",
            HybridLogicalClock.Zero,
            new VersionVector(),
            EmptyAsync());
        local.ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(expected));

        var source = new LocalBootstrapSnapshotSource(local);
        var actual = await source.ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, CancellationToken.None);

        Assert.That(actual, Is.SameAs(expected));
        await local.Received(1).ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }

    private static async IAsyncEnumerable<SnapshotEntry> EmptyAsync()
    {
        await Task.CompletedTask;
        yield break;
    }
}
