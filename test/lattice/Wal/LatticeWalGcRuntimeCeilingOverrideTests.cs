using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Wiring tests for the per-tree runtime WAL retained-byte ceiling override
/// (issue #3333) as <see cref="LatticeWalGc"/> actually consumes it.
/// <para>
/// These are deliberately separate from the resolver unit tests. Those prove
/// <c>LatticeOptionsResolver.GetWalMaxRetainedBytesAsync</c> returns the right
/// number when it is called; they say nothing about whether the garbage
/// collector calls it. Replacing the GC's per-pass resolve with a plain read of
/// the static option would leave every one of those unit tests green while the
/// feature was dead - the override would be persisted, reported, and never
/// honoured. The assertions below fail in exactly that case, so they are what
/// makes the override load-bearing rather than merely stored.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcRuntimeCeilingOverrideTests
{
    private const string Tree = "ceiling-override-tree";

    [SetUp]
    public void Setup() => LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        // Mirrors the sibling byte-pressure fixture: the durability hold engages
        // by default for a tree that has never published a durable offset floor,
        // which is true of every tree here, and would mask the ceiling axis.
        options.WalDurabilityHoldCeilingBytes = 0;
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static TreeRegistryEntry Entry(long? walMaxRetainedBytes) => new()
    {
        MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
        MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
        ShardCount = LatticeConstants.DefaultShardCount,
        WalPartitions = 1,
        WalMaxRetainedBytes = walMaxRetainedBytes,
    };

    /// <summary>
    /// Builds a service provider carrying a real <see cref="LatticeOptionsResolver"/>
    /// over a substituted registry, so the GC takes its resolver-present branch
    /// exactly as a silo does. The returned registry substitute can be
    /// re-stubbed mid-test to simulate a runtime override change.
    /// </summary>
    private static (IServiceProvider Services, ILatticeRegistry Registry) ServicesWithResolver(
        IWalStorageProvider provider,
        IOptionsMonitor<LatticeOptions> monitor,
        long? initialOverride)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>())
            .Returns(_ => Task.FromResult<TreeRegistryEntry?>(Entry(initialOverride)));
        // The GC resolves a per-partition provider from the durable placement
        // pin whenever a resolver is present. An unstubbed substitute yields a
        // null pin and the pass faults before it ever reaches the ceiling, so
        // this stub is what lets the fixture assert on the ceiling at all.
        registry.GetWalPlacementAsync(Arg.Any<string>())
            .Returns(_ => Task.FromResult(WalPlacementPin.Create()));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // The resolver-present branch resolves the partition's provider through
        // the catalog; without one it fails closed before reaching the ceiling.
        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out Arg.Any<IWalStorageProvider>())
            .Returns(ci => { ci[1] = provider; return true; });

        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        sc.AddSingleton(new LatticeOptionsResolver(factory, monitor, logger: null, walProviderCatalog: catalog));
        return (sc.BuildServiceProvider(), registry);
    }

    private static WalEntry WalEntry(long offset, string key, byte[] value, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = ts,
        },
    };

    private static async Task SeedAsync(IWalStorageProvider provider) =>
        await provider.AppendBatchAsync(
            Tree,
            0,
            [
                WalEntry(0, "a", new byte[100], Hlc(10)),
                WalEntry(1, "b", new byte[100], Hlc(20)),
            ],
            CancellationToken.None);

    [Test]
    public async Task RunOnceAsync_honours_the_per_tree_runtime_ceiling_override()
    {
        // The static option says 50; the registry pins 5000. The pass must
        // report the pinned value. If the GC ever stops resolving per pass this
        // assertion sees 50 and fails - which is the whole point of the fixture.
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider);
        var monitor = Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 50 });
        var (services, _) = ServicesWithResolver(provider, monitor, 5000);

        var sut = new LatticeWalGc(services, new InMemoryWalCursorRegistry(), monitor);

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.ByteCeiling, Is.EqualTo(5000),
            "The GC must resolve the per-tree override, not read the static option.");
    }

    [Test]
    public async Task RunOnceAsync_falls_back_to_the_static_option_when_no_override_is_pinned()
    {
        // The no-override path must stay byte-identical to the pre-feature
        // behaviour, so the feature cannot change a tree nobody configured.
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider);
        var monitor = Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 50 });
        var (services, _) = ServicesWithResolver(provider, monitor, null);

        var sut = new LatticeWalGc(services, new InMemoryWalCursorRegistry(), monitor);

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.ByteCeiling, Is.EqualTo(50),
            "With no override pinned the resolved ceiling must equal the static option exactly.");
    }

    [Test]
    public async Task RunOnceAsync_observes_an_override_changed_between_passes_without_a_restart()
    {
        // The load-bearing claim of issue #3333: a ceiling corrected at runtime
        // lands on the NEXT garbage-collection pass, with no silo restart. The
        // same LatticeWalGc instance runs both passes, so a memoised ceiling
        // anywhere between the registry and the GC fails here and only here.
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider);
        var monitor = Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 50 });
        var (services, registry) = ServicesWithResolver(provider, monitor, 5000);

        var sut = new LatticeWalGc(services, new InMemoryWalCursorRegistry(), monitor);

        var before = await sut.RunOnceAsync(Tree);
        Assert.That(before.ByteCeiling, Is.EqualTo(5000));

        registry.GetEntryAsync(Arg.Any<string>())
            .Returns(_ => Task.FromResult<TreeRegistryEntry?>(Entry(21474836480)));

        var after = await sut.RunOnceAsync(Tree);

        Assert.That(after.ByteCeiling, Is.EqualTo(21474836480),
            "A ceiling raised at runtime must be honoured by the next pass without a silo restart.");
    }
}
