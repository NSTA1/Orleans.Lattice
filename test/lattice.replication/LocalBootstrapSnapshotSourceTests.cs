using Microsoft.Extensions.Options;
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
            () => new LocalBootstrapSnapshotSource(null!, Options("site-a")),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_options_are_null()
    {
        Assert.That(
            () => new LocalBootstrapSnapshotSource(Substitute.For<ISnapshotProvider>(), null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ExportAsync_two_arg_forwards_to_local_provider()
    {
        var local = Substitute.For<ISnapshotProvider>();
        var expected = NewStream("tree-a");
        local.ExportAsync("tree-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(expected));

        var source = new LocalBootstrapSnapshotSource(local, Options("site-a"));
        var actual = await source.ExportAsync("tree-a", HybridLogicalClock.Zero, CancellationToken.None);

        Assert.That(actual, Is.SameAs(expected));
        await local.Received(1).ExportAsync("tree-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExportAsync_three_arg_forwards_to_local_provider_when_source_is_the_local_cluster()
    {
        var local = Substitute.For<ISnapshotProvider>();
        var expected = NewStream("tree-b");
        local.ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(expected));

        var source = new LocalBootstrapSnapshotSource(local, Options("site-a"));
        var actual = await source.ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, CancellationToken.None);

        Assert.That(actual, Is.SameAs(expected));
        await local.Received(1).ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ExportAsync_three_arg_refuses_a_foreign_source_cluster_without_calling_the_local_provider()
    {
        // Issue #5109: the local source can only read the local tree, so a
        // request naming a peer cluster must not be served as a self-export.
        var local = Substitute.For<ISnapshotProvider>();
        local.ExportAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(NewStream("tree-b")));

        var source = new LocalBootstrapSnapshotSource(local, Options("site-b"));

        var ex = Assert.ThrowsAsync<LatticeBootstrapSourceMismatchException>(
            () => source.ExportAsync("tree-b", "site-a", HybridLogicalClock.Zero, CancellationToken.None));

        Assert.That(ex!.TreeName, Is.EqualTo("tree-b"));
        Assert.That(ex.RequestedSourceClusterId, Is.EqualTo("site-a"));
        Assert.That(ex.LocalClusterId, Is.EqualTo("site-b"));
        await local.DidNotReceiveWithAnyArgs().ExportAsync(default!, default(string)!, default, default);
        await local.DidNotReceiveWithAnyArgs().ExportAsync(default!, default(HybridLogicalClock), default);
    }

    [Test]
    public async Task ExportAsync_three_arg_refuses_when_the_local_cluster_id_is_not_configured()
    {
        var local = Substitute.For<ISnapshotProvider>();
        local.ExportAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(NewStream("tree-c")));

        var source = new LocalBootstrapSnapshotSource(local, Options(string.Empty));

        var ex = Assert.ThrowsAsync<LatticeBootstrapSourceMismatchException>(
            () => source.ExportAsync("tree-c", "site-a", HybridLogicalClock.Zero, CancellationToken.None));

        Assert.That(ex!.LocalClusterId, Is.Empty);
        await local.DidNotReceiveWithAnyArgs().ExportAsync(default!, default(string)!, default, default);
    }

    [Test]
    public void Mismatch_exception_context_free_constructors_carry_empty_context()
    {
        var inner = new InvalidOperationException("inner");
        foreach (var ex in new[]
        {
            new LatticeBootstrapSourceMismatchException(),
            new LatticeBootstrapSourceMismatchException("m"),
            new LatticeBootstrapSourceMismatchException("m", inner),
        })
        {
            Assert.That(ex.TreeName, Is.Empty);
            Assert.That(ex.RequestedSourceClusterId, Is.Empty);
            Assert.That(ex.LocalClusterId, Is.Empty);
            Assert.That(ex, Is.InstanceOf<ILatticeDomainFault>());
        }

        Assert.That(new LatticeBootstrapSourceMismatchException("m", inner).InnerException, Is.SameAs(inner));
    }

    internal static IOptionsMonitor<LatticeReplicationOptions> Options(string clusterId)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = clusterId };
        monitor.Get(Arg.Any<string>()).Returns(options);
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static SnapshotStream NewStream(string tree) =>
        new(tree, HybridLogicalClock.Zero, new VersionVector(), EmptyAsync());

    private static async IAsyncEnumerable<SnapshotEntry> EmptyAsync()
    {
        await Task.CompletedTask;
        yield break;
    }
}