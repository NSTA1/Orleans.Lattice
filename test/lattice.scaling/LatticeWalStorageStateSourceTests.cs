using System.Collections.Immutable;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="LatticeWalStorageStateSource"/>: the mapping of the
/// core administrative surface (<see cref="ILatticeAdmin"/>,
/// <see cref="IWalStorageProviderCatalog"/>, <see cref="IWalSaturationSignal"/>)
/// into a <see cref="WalStorageSample"/>, the continuous-saturation tracking that
/// feeds the debounce window, and the degraded-to-empty fallback when no grain
/// factory is available. Uses substituted core seams so no cluster is required.
/// </summary>
[TestFixture]
public sealed class LatticeWalStorageStateSourceTests
{
    private const string AdminGrainKey = "_lattice_admin";
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static IWalStorageProviderCatalog Catalog(params string[] keys)
    {
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.Keys.Returns(keys);
        return catalog;
    }

    private static IGrainFactory FactoryFor(ILatticeAdmin admin)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(AdminGrainKey).Returns(admin);
        return factory;
    }

    private static ClusterStorageUsageReport ClusterReport(params (string TreeId, long WalBytes)[] trees)
    {
        var builder = ImmutableArray.CreateBuilder<TreeStorageUsageReport>();
        foreach (var (treeId, walBytes) in trees)
        {
            builder.Add(new TreeStorageUsageReport { TreeId = treeId, WalRetainedBytes = walBytes });
        }

        return new ClusterStorageUsageReport { TreeCount = trees.Length, Trees = builder.ToImmutable() };
    }

    private static WalPlacement Placement(string treeId, params (int Partition, string Key)[] partitions)
    {
        var builder = ImmutableArray.CreateBuilder<WalPartitionPlacement>();
        foreach (var (partition, key) in partitions)
        {
            builder.Add(new WalPartitionPlacement { Partition = partition, ProviderKey = key, ResolvableOnThisSilo = true });
        }

        return new WalPlacement { TreeId = treeId, Partitions = builder.ToImmutable() };
    }

    [Test]
    public async Task No_grain_factory_yields_empty_sample()
    {
        var source = new LatticeWalStorageStateSource(TimeProvider.System, Catalog("default"), grainFactory: null);

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sample.Trees, Is.Empty);
            Assert.That(sample.CatalogKeys, Is.Empty);
        });
    }

    [Test]
    public async Task Admin_failure_yields_empty_sample()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns<ClusterStorageUsageReport>(_ => throw new InvalidOperationException("boom"));
        var source = new LatticeWalStorageStateSource(TimeProvider.System, Catalog("default"), grainFactory: FactoryFor(admin));

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.That(sample.Trees, Is.Empty);
    }

    [Test]
    public async Task Maps_trees_placement_saturation_and_catalog_keys()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(ClusterReport(("t1", 500)));
        admin.GetWalPlacementAsync("t1", Arg.Any<CancellationToken>())
            .Returns(Placement("t1", (0, "acct-a"), (1, "acct-b")));

        var saturation = Substitute.For<IWalSaturationSignal>();
        saturation.GetCurrentState("t1").Returns(WalSaturationState.Throttled);

        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0), Catalog("acct-a", "acct-b"), saturation, FactoryFor(admin));

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sample.Trees, Has.Count.EqualTo(1));
            var tree = sample.Trees[0];
            Assert.That(tree.TreeId, Is.EqualTo("t1"));
            Assert.That(tree.WalRetainedBytes, Is.EqualTo(500));
            Assert.That(tree.Saturation, Is.EqualTo(WalSaturationState.Throttled));
            Assert.That(tree.Partitions.Select(p => p.ProviderKey), Is.EqualTo(new[] { "acct-a", "acct-b" }));
            Assert.That(sample.CatalogKeys, Is.EquivalentTo(new[] { "acct-a", "acct-b" }));
        });
    }

    [Test]
    public async Task Continuous_saturation_accrues_across_ticks()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(ClusterReport(("t1", 10)));
        admin.GetWalPlacementAsync("t1", Arg.Any<CancellationToken>())
            .Returns(Placement("t1", (0, "acct-a")));

        var saturation = Substitute.For<IWalSaturationSignal>();
        saturation.GetCurrentState("t1").Returns(WalSaturationState.Saturated);

        var time = new MutableTimeProvider(T0);
        var source = new LatticeWalStorageStateSource(time, Catalog("acct-a"), saturation, FactoryFor(admin));

        var first = await source.SampleAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(45));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Trees[0].SaturatedFor, Is.EqualTo(TimeSpan.Zero));
            Assert.That(second.Trees[0].SaturatedFor, Is.EqualTo(TimeSpan.FromSeconds(45)));
        });
    }

    [Test]
    public async Task Recovery_to_healthy_resets_saturation_window()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(ClusterReport(("t1", 10)));
        admin.GetWalPlacementAsync("t1", Arg.Any<CancellationToken>())
            .Returns(Placement("t1", (0, "acct-a")));

        var saturation = Substitute.For<IWalSaturationSignal>();
        var time = new MutableTimeProvider(T0);
        var source = new LatticeWalStorageStateSource(time, Catalog("acct-a"), saturation, FactoryFor(admin));

        saturation.GetCurrentState("t1").Returns(WalSaturationState.Saturated);
        await source.SampleAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(60));

        saturation.GetCurrentState("t1").Returns(WalSaturationState.Healthy);
        var recovered = await source.SampleAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(5));

        saturation.GetCurrentState("t1").Returns(WalSaturationState.Saturated);
        var reSaturated = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(recovered.Trees[0].SaturatedFor, Is.EqualTo(TimeSpan.Zero));
            // Continuity was reset by the healthy tick, so the new spell starts at zero.
            Assert.That(reSaturated.Trees[0].SaturatedFor, Is.EqualTo(TimeSpan.Zero));
        });
    }
}
