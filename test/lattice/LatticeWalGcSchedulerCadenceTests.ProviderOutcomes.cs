using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [TestCase("idle")]
    [TestCase("stranded")]
    [TestCase("blocked")]
    [TestCase("no_consumer")]
    [TestCase("no_partitions")]
    public async Task Gc_distinguishes_each_of_the_five_non_reclaiming_states(string expected)
    {
        var tree = $"gc-five-states-{expected}";
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryWalCursorRegistry();
        if (expected != "no_consumer")
        {
            await registry.ReportCursorAsync(tree, "shipper", OutcomeHlc(10));
        }

        if (expected != "idle")
        {
            await provider.AppendBatchAsync(tree, 0,
                [OutcomeEntry(tree, 0, OutcomeHlc(20))], CancellationToken.None);
        }

        IReadOnlyDictionary<string, HybridLogicalClock>? pins = expected == "blocked"
            ? new Dictionary<string, HybridLogicalClock>
            {
                [$"_lattice_materialiser_{tree}_leaf-1"] = HybridLogicalClock.Zero,
            }
            : null;
        using var services = MissingProviderServices(provider);
        var gc = expected == "no_partitions"
            ? new LatticeWalGc(services, registry, SinglePartitionMonitor())
            : RealGc(provider, registry, pins);

        var report = await gc.RunOnceAsync(tree);
        var (arm, passes) = await OutcomeOfOnePassAsync(tree, gc);
        using var recorder = passes;
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(tree, 0, -1, 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }

        Assert.Multiple(() =>
        {
            Assert.That(arm, Is.EqualTo(expected));
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(report.ShardsScanned, Is.EqualTo(expected == "no_partitions" ? 0 : 1),
                "an unresolvable provider is not a visited partition");
            Assert.That(survivors.Count, Is.EqualTo(expected == "idle" ? 0 : 1));
            Assert.That(passes.Counted.Single().Tag(LatticeTenantLabel.TagTenant), Is.Not.Null);
        });
    }

    private static ServiceProvider MissingProviderServices(IWalStorageProvider provider)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetWalPlacementAsync(Arg.Any<string>())
            .Returns(Task.FromResult(WalPlacementPin.Create().WithPartition(0, "missing", 1)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out Arg.Any<IWalStorageProvider>())
            .Returns(call => { call[1] = provider; return true; });
        var services = new ServiceCollection();
        services.AddSingleton(provider);
        services.AddSingleton(new LatticeOptionsResolver(
            factory, SinglePartitionMonitor(), logger: null, walProviderCatalog: catalog));
        return services.BuildServiceProvider();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Gc_no_resolvable_partition_is_not_confused_with_no_predicate(bool ttl)
    {
        const string Tree = "gc-missing-provider-no-cursor";
        using var services = MissingProviderServices(new InMemoryWalStorageProvider());
        var monitor = SinglePartitionMonitor();
        monitor.CurrentValue.WalRetention = ttl ? TimeSpan.FromHours(1) : null;
        var gc = new LatticeWalGc(services, new InMemoryWalCursorRegistry(), monitor);

        var report = await gc.RunOnceAsync(Tree);
        var (arm, passes) = await OutcomeOfOnePassAsync(Tree, gc);
        using var recorder = passes;
        Assert.Multiple(() =>
        {
            Assert.That(report.ShardsScanned, Is.Zero);
            Assert.That(arm, Is.EqualTo("no_partitions"));
        });
    }

    [TestCase(false, false, "no_consumer")]
    [TestCase(true, false, "idle")]
    [TestCase(true, true, "reclaimed")]
    public async Task Gc_one_resolvable_partition_preserves_existing_outcomes(
        bool cursor, bool entries, string expected)
    {
        const string Tree = "gc-partially-resolvable";
        var provider = new InMemoryWalStorageProvider();
        if (entries)
        {
            await provider.AppendBatchAsync(Tree, 1,
                [OutcomeEntry(Tree, 0, OutcomeHlc(10))], CancellationToken.None);
        }

        var registry = new InMemoryWalCursorRegistry();
        if (cursor)
        {
            await registry.ReportCursorAsync(Tree, "shipper", OutcomeHlc(30));
        }

        using var services = MissingProviderServices(provider);
        var monitor = SinglePartitionMonitor();
        monitor.CurrentValue.WalPartitions = 2;
        var gc = new LatticeWalGc(services, registry, monitor);
        var (arm, passes) = await OutcomeOfOnePassAsync(Tree, gc);
        using var recorder = passes;
        var report = await gc.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.ShardsScanned, Is.EqualTo(1));
            Assert.That(arm, Is.EqualTo(expected));
        });
    }
}
