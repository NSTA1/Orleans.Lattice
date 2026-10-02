using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Gate for the garbage collector's half of issue #4238: a pass addresses WAL
/// partitions by the count the tree registry pinned, not by the configured
/// <see cref="LatticeOptions.WalPartitions"/>.
/// <para>
/// The pin is set when a tree is registered and is immutable after it, so a
/// silo configured for fewer partitions than a tree was registered with must
/// still collect every one of that tree's partitions. Reading the configured
/// value leaves the partitions above it unscanned forever - their WAL is never
/// trimmed - and reads every pin's partition suffix against the wrong count.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcPinnedPartitionsTests
{
    private const string Tree = "pinned-partitions-gc-4238";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = [1],
            Timestamp = ts,
        },
    };

    private static IOptionsMonitor<LatticeOptions> Monitor(int configuredPartitions)
    {
        // The durability hold engages for a tree that has never published a
        // durable offset floor, which would mask the cursor axis under test.
        var options = new LatticeOptions { WalPartitions = configuredPartitions, WalDurabilityHoldCeilingBytes = 0 };
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider ServicesPinnedTo(
        int pinnedPartitions,
        IWalStorageProvider provider,
        IOptionsMonitor<LatticeOptions> monitor)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
            MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
            ShardCount = LatticeConstants.DefaultShardCount,
            WalPartitions = pinnedPartitions,
        }));
        registry.GetWalPlacementAsync(Arg.Any<string>()).Returns(_ => Task.FromResult(WalPlacementPin.Create()));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = provider; return true; });

        var services = new ServiceCollection();
        services.AddSingleton(provider);
        services.AddSingleton(new LatticeOptionsResolver(factory, monitor, logger: null, walProviderCatalog: catalog));
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task RunOnceAsync_collects_a_partition_the_registry_pinned_above_the_configured_count()
    {
        // Pinned to 4, configured for 1: the entries live on partition 3, and a
        // forward cursor has passed both of them, so both are eligible.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(Tree, 3, [Entry(0, Hlc(10)), Entry(1, Hlc(20))], CancellationToken.None);

        var cursors = new InMemoryWalCursorRegistry();
        await cursors.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var monitor = Monitor(configuredPartitions: 1);
        LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();
        var sut = new LatticeWalGc(ServicesPinnedTo(pinnedPartitions: 4, provider, monitor), cursors, monitor);

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
            "partition 3 exists on a tree pinned to 4 partitions, whatever this silo is configured with, "
                + "so its eligible entries must be collected.");
    }
}
