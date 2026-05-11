using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Unit tests for <see cref="InMemoryWalCursorRegistry"/> covering the
/// invariants the registry promised on its way into core: defensive
/// clone-on-return for the causal-stable frontier, blocked-floor-only
/// consumers excluded from <see cref="IWalCursorRegistry.GetMinCursorAsync"/>,
/// <see cref="ILeafCursorReporter.UnregisterTreeAsync"/> leaving
/// custom-prefix consumers intact, and the DI extensions registering
/// in idempotent <c>TryAddSingleton</c> form with a factory override
/// path.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class InMemoryWalCursorRegistryTests
{
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task GetCausalStableAsync_returns_independent_clone_per_call()
    {
        var registry = new InMemoryWalCursorRegistry();
        var vector = new VersionVector();
        vector.Entries["origin-a"] = Hlc(100);
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(10), vector, CancellationToken.None);

        var first = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(first, Is.Not.Null);

        // Mutate the returned vector — a defensive registry must not
        // observe the mutation on the next call.
        first!.Entries["origin-evil"] = Hlc(9_999);

        var second = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(second, Is.Not.Null);
        Assert.That(second!.Entries.ContainsKey("origin-evil"), Is.False,
            "GetCausalStableAsync must clone before returning so caller mutation cannot poison the cache.");
    }

    [Test]
    public async Task GetMinCursorAsync_skips_blocked_floor_only_consumers()
    {
        var registry = new InMemoryWalCursorRegistry();
        // Buffer-pin-only consumer: cursor=Zero, blocked-floor set.
        await registry.ReportCursorAsync(
            Tree,
            "buffer-consumer",
            HybridLogicalClock.Zero,
            blockedAtHlc: Hlc(50),
            cancellationToken: CancellationToken.None);

        // Regular cursor consumer.
        await registry.ReportCursorAsync(Tree, "cursor-consumer", Hlc(200), CancellationToken.None);

        var min = await registry.GetMinCursorAsync(Tree, CancellationToken.None);
        Assert.That(min, Is.EqualTo(Hlc(200)),
            "Blocked-floor-only consumers must be excluded from the cursor meet so a buffer pin does not disable cursor-based trimming.");

        var blockedFloor = await registry.GetBlockedFloorAsync(Tree, CancellationToken.None);
        Assert.That(blockedFloor, Is.EqualTo(Hlc(50)),
            "Blocked-floor consumer must still contribute to the blocked-floor meet.");
    }

    [Test]
    public async Task ReReporting_with_same_cursor_and_no_new_metadata_preserves_cached_causal_stable()
    {
        var registry = new InMemoryWalCursorRegistry();
        var vector = new VersionVector();
        vector.Entries["origin-a"] = Hlc(100);
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(10), vector, CancellationToken.None);

        // Materialise the cache.
        var first = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(first, Is.Not.Null);
        Assert.That(first!.Entries["origin-a"], Is.EqualTo(Hlc(100)));

        // No-op re-report: same cursor, no new vector, no blocked-floor.
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(10), CancellationToken.None);

        var second = await registry.GetCausalStableAsync(Tree, CancellationToken.None);
        Assert.That(second, Is.Not.Null);
        Assert.That(second!.Entries.ContainsKey("origin-a"), Is.True,
            "Cached causal-stable frontier must survive a no-observable-change re-report.");
        Assert.That(second.Entries["origin-a"], Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task LeafCursorReporter_UnregisterTreeAsync_leaves_custom_prefix_consumers_intact()
    {
        var registry = new InMemoryWalCursorRegistry();
        var materialiserId = ILeafCursorReporter.MaterialiserConsumerIdPrefix + Tree + "_leaf-1";
        const string customId = "custom-bridge";

        await registry.ReportCursorAsync(Tree, materialiserId, Hlc(10), CancellationToken.None);
        await registry.ReportCursorAsync(Tree, customId, Hlc(20), CancellationToken.None);

        var reporter = new LeafCursorReporter(registry);
        await reporter.UnregisterTreeAsync(Tree, CancellationToken.None);

        var snapshot = await registry.SnapshotAsync(Tree, CancellationToken.None);
        Assert.That(snapshot.Any(s => s.ConsumerId == materialiserId), Is.False,
            "Materialiser-prefix consumer must have been unregistered.");
        Assert.That(snapshot.Any(s => s.ConsumerId == customId), Is.True,
            "Custom-prefix consumer must survive UnregisterTreeAsync.");
    }

    [Test]
    public void AddWalCursorRegistry_factory_overload_wins_over_default()
    {
        var custom = Substitute.For<IWalCursorRegistry>();
        var sc = new ServiceCollection();
        var siloBuilder = SiloBuilderHelper.Wrap(sc);
        siloBuilder.AddWalCursorRegistry(_ => custom);
        // Second call with the default factory must not override the
        // host-supplied one (TryAddSingleton).
        siloBuilder.AddWalCursorRegistry();

        var sp = sc.BuildServiceProvider();
        Assert.That(sp.GetRequiredService<IWalCursorRegistry>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeWalGc_factory_overload_wins_over_default()
    {
        var custom = Substitute.For<ILatticeWalGc>();
        var sc = new ServiceCollection();
        var siloBuilder = SiloBuilderHelper.Wrap(sc);
        siloBuilder.AddLatticeWalGc(_ => custom);
        siloBuilder.AddLatticeWalGc();

        var sp = sc.BuildServiceProvider();
        Assert.That(sp.GetRequiredService<ILatticeWalGc>(), Is.SameAs(custom));
    }

    [Test]
    public void AddWalCursorRegistry_TryAddSingleton_is_idempotent_on_second_default_call()
    {
        var sc = new ServiceCollection();
        var siloBuilder = SiloBuilderHelper.Wrap(sc);
        siloBuilder.AddWalCursorRegistry();
        siloBuilder.AddWalCursorRegistry();

        var sp = sc.BuildServiceProvider();
        var registries = sp.GetServices<IWalCursorRegistry>().ToArray();
        Assert.That(registries, Has.Length.EqualTo(1),
            "TryAddSingleton must guarantee a single registration even after repeated AddWalCursorRegistry calls.");
        Assert.That(registries[0], Is.InstanceOf<InMemoryWalCursorRegistry>());
    }

    /// <summary>
    /// Minimal <see cref="ISiloBuilder"/> shim that exposes the
    /// underlying <see cref="IServiceCollection"/> the
    /// <c>AddWal*</c> extension methods route through. Lets the unit
    /// tests exercise the extensions without spinning up an Orleans
    /// host.
    /// </summary>
    private static class SiloBuilderHelper
    {
        public static ISiloBuilder Wrap(IServiceCollection services) => new SiloBuilderStub(services);

        private sealed class SiloBuilderStub(IServiceCollection services) : ISiloBuilder
        {
            public Microsoft.Extensions.Configuration.IConfiguration Configuration { get; } =
                new Microsoft.Extensions.Configuration.ConfigurationBuilder().Build();

            public IServiceCollection Services { get; } = services;
        }
    }
}
