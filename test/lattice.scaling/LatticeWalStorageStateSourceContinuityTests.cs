using System.Collections.Immutable;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the bookkeeping seams of <see cref="LatticeWalStorageStateSource"/>
/// that the mapping tests do not reach: the continuity map that records how long
/// each tree has been saturated (including pruning entries for trees that have
/// gone away, so the map cannot grow unbounded), the empty-shape early returns
/// for a cluster with no trees and a tree with no WAL placement, and the
/// catalogue-key snapshot when no catalogue is registered.
/// </summary>
[TestFixture]
public sealed class LatticeWalStorageStateSourceContinuityTests
{
    private const string AdminGrainKey = "_lattice_admin";
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static IGrainFactory FactoryFor(ILatticeAdmin admin)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(AdminGrainKey).Returns(admin);
        return factory;
    }

    private static ClusterStorageUsageReport ClusterReport(params string[] treeIds)
    {
        var builder = ImmutableArray.CreateBuilder<TreeStorageUsageReport>();
        foreach (var treeId in treeIds)
        {
            builder.Add(new TreeStorageUsageReport { TreeId = treeId, WalRetainedBytes = 100L });
        }

        return new ClusterStorageUsageReport { TreeCount = treeIds.Length, Trees = builder.ToImmutable() };
    }

    private static WalPlacement Placement(string treeId, params (int Partition, string Key)[] partitions)
    {
        var builder = ImmutableArray.CreateBuilder<WalPartitionPlacement>();
        foreach (var (partition, key) in partitions)
        {
            builder.Add(new WalPartitionPlacement
            {
                Partition = partition,
                ProviderKey = key,
                ResolvableOnThisSilo = true,
            });
        }

        return new WalPlacement { TreeId = treeId, Partitions = builder.ToImmutable() };
    }

    /// <summary>
    /// An admin substitute whose per-tree saturation is driven by a mutable map,
    /// so a test can change a tree's state between samples.
    /// </summary>
    private static ILatticeAdmin AdminReturning(
        Func<ClusterStorageUsageReport> report,
        Func<string, WalPlacement>? placement = null)
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetTotalStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(_ => report());
        admin.GetWalPlacementAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => (placement ?? (t => Placement(t, (0, "default"))))(call.ArgAt<string>(0)));
        return admin;
    }

    private sealed class MapSaturationSignal : IWalSaturationSignal
    {
        public Dictionary<string, WalSaturationState> States { get; } = new(StringComparer.Ordinal);

        public WalSaturationState GetCurrentState(string treeId) =>
            States.TryGetValue(treeId, out var state) ? state : WalSaturationState.Healthy;

        public WalSaturationState GetAggregateState()
        {
            var worst = WalSaturationState.Healthy;
            foreach (var state in States.Values)
            {
                if (state > worst)
                {
                    worst = state;
                }
            }

            return worst;
        }

        public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    [Test]
    public async Task A_cluster_with_no_trees_yields_an_empty_sample_with_catalogue_keys()
    {
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.Keys.Returns(new[] { "acct-a", "acct-b" });
        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0),
            catalog,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport())));

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sample.Trees, Is.Empty);
            Assert.That(
                sample.CatalogKeys,
                Is.EquivalentTo(new[] { "acct-a", "acct-b" }),
                "Registered keys are reported even when no tree exists yet.");
        });
    }

    [Test]
    public async Task A_tree_with_no_wal_placement_maps_to_an_empty_partition_list()
    {
        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0),
            catalog: null,
            grainFactory: FactoryFor(AdminReturning(
                () => ClusterReport("t-a"),
                placement: t => Placement(t))));

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.That(sample.Trees.Single().Partitions, Is.Empty);
    }

    [Test]
    public async Task No_catalogue_yields_no_catalogue_keys()
    {
        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0),
            catalog: null,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a"))));

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.That(sample.CatalogKeys, Is.Empty);
    }

    [Test]
    public async Task An_empty_catalogue_yields_no_catalogue_keys()
    {
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.Keys.Returns(Array.Empty<string>());
        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0),
            catalog,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a"))));

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.That(sample.CatalogKeys, Is.Empty);
    }

    [Test]
    public async Task Saturation_continuity_accumulates_across_samples()
    {
        var clock = new MutableTimeProvider(T0);
        var saturation = new MapSaturationSignal();
        saturation.States["t-a"] = WalSaturationState.Saturated;
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            saturationSignal: saturation,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a"))));

        var first = await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(3));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                first.Trees.Single().SaturatedFor,
                Is.EqualTo(TimeSpan.Zero),
                "The first saturated observation only records the start instant.");
            Assert.That(second.Trees.Single().SaturatedFor, Is.EqualTo(TimeSpan.FromMinutes(3)));
        });
    }

    [Test]
    public async Task A_clock_that_does_not_advance_reports_zero_continuity()
    {
        var clock = new MutableTimeProvider(T0);
        var saturation = new MapSaturationSignal();
        saturation.States["t-a"] = WalSaturationState.Throttled;
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            saturationSignal: saturation,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a"))));

        await source.SampleAsync(CancellationToken.None);
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.That(
            second.Trees.Single().SaturatedFor,
            Is.EqualTo(TimeSpan.Zero),
            "A non-advancing clock must never report negative continuity.");
    }

    [Test]
    public async Task Recovering_to_healthy_resets_the_continuity_window()
    {
        var clock = new MutableTimeProvider(T0);
        var saturation = new MapSaturationSignal();
        saturation.States["t-a"] = WalSaturationState.Saturated;
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            saturationSignal: saturation,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a"))));

        await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(5));

        saturation.States["t-a"] = WalSaturationState.Healthy;
        await source.SampleAsync(CancellationToken.None);

        saturation.States["t-a"] = WalSaturationState.Saturated;
        clock.Advance(TimeSpan.FromMinutes(5));
        var afterRecovery = await source.SampleAsync(CancellationToken.None);

        Assert.That(
            afterRecovery.Trees.Single().SaturatedFor,
            Is.EqualTo(TimeSpan.Zero),
            "Recovery clears the tracked start, so the next saturation starts a fresh window.");
    }

    [Test]
    public async Task Continuity_for_a_tree_that_disappears_is_pruned()
    {
        var clock = new MutableTimeProvider(T0);
        var saturation = new MapSaturationSignal();
        saturation.States["t-a"] = WalSaturationState.Saturated;
        saturation.States["t-b"] = WalSaturationState.Saturated;

        var trees = new[] { "t-a", "t-b" };
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            saturationSignal: saturation,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport(trees))));

        await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(4));

        // t-a is dropped from the cluster; its continuity entry must be pruned
        // rather than retained forever.
        trees = new[] { "t-b" };
        await source.SampleAsync(CancellationToken.None);

        // t-a comes back. If its stale entry had survived the prune it would
        // report the full elapsed window instead of starting fresh.
        clock.Advance(TimeSpan.FromMinutes(4));
        trees = new[] { "t-a", "t-b" };
        var third = await source.SampleAsync(CancellationToken.None);

        var reborn = third.Trees.Single(t => t.TreeId == "t-a");
        var survivor = third.Trees.Single(t => t.TreeId == "t-b");

        Assert.Multiple(() =>
        {
            Assert.That(reborn.SaturatedFor, Is.EqualTo(TimeSpan.Zero), "t-a's stale entry must have been pruned.");
            Assert.That(survivor.SaturatedFor, Is.EqualTo(TimeSpan.FromMinutes(8)), "t-b was never absent.");
        });
    }

    [Test]
    public async Task Continuity_is_pruned_when_every_tree_disappears()
    {
        var clock = new MutableTimeProvider(T0);
        var saturation = new MapSaturationSignal();
        saturation.States["t-a"] = WalSaturationState.Saturated;

        var trees = new[] { "t-a" };
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            saturationSignal: saturation,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport(trees))));

        await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(6));

        trees = Array.Empty<string>();
        var empty = await source.SampleAsync(CancellationToken.None);

        clock.Advance(TimeSpan.FromMinutes(6));
        trees = new[] { "t-a" };
        var reborn = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(empty.Trees, Is.Empty);
            Assert.That(reborn.Trees.Single().SaturatedFor, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public async Task Continuity_pruning_is_a_no_op_when_nothing_is_tracked()
    {
        var clock = new MutableTimeProvider(T0);
        var trees = new[] { "t-a" };
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport(trees))));

        await source.SampleAsync(CancellationToken.None);
        trees = Array.Empty<string>();

        Assert.DoesNotThrowAsync(async () => await source.SampleAsync(CancellationToken.None));
    }

    [Test]
    public void A_cancelled_token_propagates_rather_than_degrading_to_an_empty_sample()
    {
        var clock = new MutableTimeProvider(T0);
        var source = new LatticeWalStorageStateSource(
            clock,
            catalog: null,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a", "t-b"))));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await source.SampleAsync(cts.Token));
    }

    [Test]
    public async Task An_injected_logger_is_used_instead_of_the_null_logger()
    {
        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0),
            catalog: null,
            saturationSignal: null,
            grainFactory: FactoryFor(AdminReturning(() => ClusterReport("t-a"))),
            logger: NullLogger<LatticeWalStorageStateSource>.Instance);

        var sample = await source.SampleAsync(CancellationToken.None);

        Assert.That(sample.Trees.Single().TreeId, Is.EqualTo("t-a"));
    }

    [Test]
    public async Task Partition_placement_is_mapped_verbatim()
    {
        var source = new LatticeWalStorageStateSource(
            new MutableTimeProvider(T0),
            catalog: null,
            grainFactory: FactoryFor(AdminReturning(
                () => ClusterReport("t-a"),
                placement: t => Placement(t, (0, "acct-a"), (1, "acct-b")))));

        var sample = await source.SampleAsync(CancellationToken.None);
        var partitions = sample.Trees.Single().Partitions;

        Assert.Multiple(() =>
        {
            Assert.That(partitions, Has.Count.EqualTo(2));
            Assert.That(partitions[0].Partition, Is.Zero);
            Assert.That(partitions[0].ProviderKey, Is.EqualTo("acct-a"));
            Assert.That(partitions[1].Partition, Is.EqualTo(1));
            Assert.That(partitions[1].ProviderKey, Is.EqualTo("acct-b"));
        });
    }
}
