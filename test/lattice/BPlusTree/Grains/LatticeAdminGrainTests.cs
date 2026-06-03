using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeAdminGrain"/>. Pins the split
/// between the cheap WAL-only polling path
/// (<see cref="ILatticeAdmin.PollWalUsageAsync"/>) and the deep
/// operator-driven refresh
/// (<see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>).
/// </summary>
[TestFixture]
public sealed class LatticeAdminGrainTests
{
    private static LatticeAdminGrain CreateGrain(
        IGrainFactory factory,
        ILatticeRegistry? registry = null)
    {
        if (registry is null)
        {
            registry = Substitute.For<ILatticeRegistry>();
            registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>()));
        }
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("ol.gad", LatticeConstants.AdminGrainKey));
        return new LatticeAdminGrain(context, factory, Substitute.For<ILogger<LatticeAdminGrain>>());
    }

    [Test]
    public async Task PollWalUsageAsync_calls_only_the_wal_usage_aggregator_for_each_tree()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha", "beta" }));

        var factory = Substitute.For<IGrainFactory>();
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new TreeWalUsageReport { TreeId = "alpha", WalRetainedBytes = 1 });
        var walBeta = Substitute.For<ILatticeWalUsage>();
        walBeta.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new TreeWalUsageReport { TreeId = "beta", WalRetainedBytes = 2 });
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        factory.GetGrain<ILatticeWalUsage>("beta").Returns(walBeta);

        var grain = CreateGrain(factory, registry);

        await grain.PollWalUsageAsync(CancellationToken.None);

        await walAlpha.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
        await walBeta.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
        // The deep path must never be invoked by the polling fan-out.
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeStorageUsage>(default!);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILattice>(default!);
    }

    [Test]
    public async Task PollWalUsageAsync_swallows_per_tree_failures_and_continues()
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(new[] { "alpha", "beta" }));

        var factory = Substitute.For<IGrainFactory>();
        var walAlpha = Substitute.For<ILatticeWalUsage>();
        walAlpha.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns<TreeWalUsageReport>(_ => throw new InvalidOperationException("alpha down"));
        var walBeta = Substitute.For<ILatticeWalUsage>();
        walBeta.GetWalUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new TreeWalUsageReport { TreeId = "beta", WalRetainedBytes = 2 });
        factory.GetGrain<ILatticeWalUsage>("alpha").Returns(walAlpha);
        factory.GetGrain<ILatticeWalUsage>("beta").Returns(walBeta);

        var grain = CreateGrain(factory, registry);

        Assert.That(async () => await grain.PollWalUsageAsync(CancellationToken.None), Throws.Nothing);
        await walBeta.Received(1).GetWalUsageAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PollWalUsageAsync_with_no_trees_is_a_noop()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = CreateGrain(factory);

        await grain.PollWalUsageAsync(CancellationToken.None);
        // No throws; no grain calls beyond the registry lookup.
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeWalUsage>(default!);
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_routes_through_the_cached_deep_aggregator()
    {
        var (factory, _, storage) = SetUpDeepFactoryWithOneTree("alpha", wal: 10, snap: 20, leaf: 30);
        var grain = CreateGrain(factory, BuildRegistry("alpha"));

        var report = await grain.GetTotalStorageUsageAsync(CancellationToken.None);

        await storage.Received(1).GetReportAsync(forceRefresh: false, Arg.Any<CancellationToken>());
        Assert.That(report.TotalBytes, Is.EqualTo(60));
    }

    [Test]
    public async Task RefreshStorageUsageAsync_forces_a_cache_bypass_on_each_tree()
    {
        var (factory, _, storage) = SetUpDeepFactoryWithOneTree("alpha", wal: 10, snap: 20, leaf: 30);
        var grain = CreateGrain(factory, BuildRegistry("alpha"));

        var report = await grain.RefreshStorageUsageAsync(CancellationToken.None);

        await storage.Received(1).GetReportAsync(forceRefresh: true, Arg.Any<CancellationToken>());
        Assert.That(report.TreeCount, Is.EqualTo(1));
        Assert.That(report.TotalBytes, Is.EqualTo(60));
    }

    [Test]
    public async Task RefreshStorageUsageAsync_per_tree_failure_yields_partial_report()
    {
        var registry = BuildRegistry("alpha");
        var factory = Substitute.For<IGrainFactory>();
        var storage = Substitute.For<ILatticeStorageUsage>();
        storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns<TreeStorageUsageReport>(_ => throw new InvalidOperationException("down"));
        factory.GetGrain<ILatticeStorageUsage>("alpha").Returns(storage);

        var grain = CreateGrain(factory, registry);

        var report = await grain.RefreshStorageUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeCount, Is.EqualTo(1));
            Assert.That(report.Partial, Is.True);
            Assert.That(report.Trees[0].TreeId, Is.EqualTo("alpha"));
        });
    }

    private static ILatticeRegistry BuildRegistry(params string[] treeIds)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult<IReadOnlyList<string>>(treeIds));
        return registry;
    }

    private static (IGrainFactory Factory, ILattice Lattice, ILatticeStorageUsage Storage) SetUpDeepFactoryWithOneTree(
        string treeId, long wal, long snap, long leaf)
    {
        var factory = Substitute.For<IGrainFactory>();
        var lattice = Substitute.For<ILattice>();
        var storage = Substitute.For<ILatticeStorageUsage>();
        storage.GetReportAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(new TreeStorageUsageReport
            {
                TreeId = treeId,
                WalRetainedBytes = wal,
                SnapshotBytes = snap,
                LeafStateBytes = leaf,
                TotalBytes = wal + snap + leaf,
                SampledAt = DateTimeOffset.UtcNow,
            });
        factory.GetGrain<ILatticeStorageUsage>(treeId).Returns(storage);
        factory.GetGrain<ILattice>(treeId).Returns(lattice);
        return (factory, lattice, storage);
    }
}
