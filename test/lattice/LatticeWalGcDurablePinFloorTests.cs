using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the durable leaf-materialiser pin floor in
/// <see cref="LatticeWalGc"/> (issue #919). The floor lowers the GC trim point
/// to account for durable leaf checkpoints whose owning leaf is MISSING from
/// the in-memory cursor registry - the post-restart window where a forward
/// consumer (e.g. the replication shipper) has re-reported its durably-advanced
/// cursor but a dormant leaf has not yet re-activated and re-reported its lower
/// pin. The safety lemma under test: a too-low/stale pin only retains more WAL
/// (safe); only a too-high cursor is dangerous, so a missing pin may lower but
/// never raise the floor, a present consumer is governed by its fresher
/// in-memory cursor, and a Zero pin blocks the cursor trim entirely.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcDurablePinFloorTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(30)) },
            CancellationToken.None);
        return provider;
    }

    // Seeds a single-partition WAL whose head entry sits at headTicks wall
    // clock (the prior entry at half that), so a drain-lag test can place the
    // materialiser frontier a controlled distance behind the head.
    private static async Task<InMemoryWalStorageProvider> SeededProviderWithHeadAsync(long headTicks)
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(headTicks / 2)), Entry(1, Hlc(headTicks)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock>? durablePins)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        if (durablePins is not null)
        {
            var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
            pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));

            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
            sc.AddSingleton(factory);
        }

        return sc.BuildServiceProvider();
    }

    private static async Task<List<long>> SurvivingOffsetsAsync(IWalStorageProvider provider)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }
        return survivors;
    }

    [Test]
    public async Task RunOnceAsync_missing_durable_pin_floors_trim_below_forward_consumer()
    {
        // The reproduction of issue #919: the registry holds only the forward
        // consumer (shipper) at the WAL head; the leaf's durable pin (offset 0,
        // HLC 10) is absent from the registry after a restart. Without the
        // floor the GC would trim the whole prefix to HLC 30 and lose the
        // committed-but-not-yet-checkpointed tail.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(10),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)),
                "The trim floor must drop to the durable leaf pin, not the forward consumer.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1),
                "Only the entry at or below the durable leaf checkpoint may be trimmed.");
        });

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 1L, 2L }),
            "The committed tail above the leaf checkpoint must survive the GC.");
    }

    [Test]
    public async Task RunOnceAsync_present_consumer_stale_durable_pin_does_not_lower_floor()
    {
        // The leaf is present in the registry at its fresh HLC 30. A staler
        // durable pin (HLC 10) under the SAME consumer id must be ignored so
        // steady-state trimming is unchanged.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(10),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(30)));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "A present consumer's fresh in-memory cursor governs; the stale durable pin is skipped.");
        });
    }

    [Test]
    public async Task RunOnceAsync_zero_durable_pin_for_missing_consumer_blocks_cursor_trim()
    {
        // A leaf that activated but never checkpointed seeds a Zero "block"
        // pin. When that leaf is missing from the registry, the GC must not
        // trim by cursor at all (the WAL head is retained until the leaf
        // checkpoints), even though the forward consumer is at the head.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = HybridLogicalClock.Zero,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.Null,
                "A missing never-checkpointed leaf must disable the cursor trim branch.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        });

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task RunOnceAsync_missing_durable_pin_above_registry_min_never_raises_floor()
    {
        // Safety lemma: a missing durable pin may only LOWER the floor. A pin
        // (HLC 30) above the registry min (HLC 10) must not raise the trim
        // point - the registry min still governs.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(10));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(30),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task RunOnceAsync_empty_durable_pins_trims_by_registry_min()
    {
        // No durable pins -> behaviour identical to the pre-fix GC.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(20));

        var sut = new LatticeWalGc(
            Services(provider, new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(20)));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task RunOnceAsync_without_grain_factory_trims_by_registry_min()
    {
        // No grain factory in DI (a bare-IServiceProvider construction, as in
        // the existing GC unit tests) -> the durable floor is never consulted
        // and the registry min governs unchanged.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(20));

        var sut = new LatticeWalGc(Services(provider, durablePins: null), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(20)));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(2));
        });
    }

    private static IOptionsMonitor<LatticeOptions> MonitorWithLagThreshold(TimeSpan? threshold)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalSaturationMaterialiserLagThreshold = threshold,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    [Test]
    public async Task RunOnceAsync_drain_lag_beyond_threshold_records_over_threshold_level()
    {
        // The WAL head sits ~9s of wall clock ahead of the materialiser
        // frontier (registry min), so the head-relative drain lag exceeds the
        // 5s threshold and the GC records an over-threshold standing level for
        // the sampler to read (issue #1030 back-pressure surface).
        WalCommitLogWriter._materialiserDrainLagLevels.Clear();
        var headTicks = TimeSpan.FromSeconds(100).Ticks;
        var frontierTicks = TimeSpan.FromSeconds(91).Ticks;
        var provider = await SeededProviderWithHeadAsync(headTicks);
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(frontierTicks));

        var sut = new LatticeWalGc(
            Services(provider, durablePins: null),
            registry,
            MonitorWithLagThreshold(TimeSpan.FromSeconds(5)));

        await sut.RunOnceAsync(Tree);

        Assert.That(
            WalCommitLogWriter._materialiserDrainLagLevels.TryGetValue(Tree, out var level),
            Is.True,
            "An enabled drain-lag input must record a standing level every pass.");
        Assert.That(
            level.LagTicks,
            Is.GreaterThan(TimeSpan.FromSeconds(5).Ticks),
            "A pass whose head-relative drain lag exceeds the threshold must record an over-threshold level.");
    }

    [Test]
    public async Task RunOnceAsync_caught_up_frontier_records_zero_lag_level()
    {
        // The frontier has reached the WAL head: head-relative lag is zero, so
        // the GC records a zero standing level - no false positive on a
        // caught-up / idle tree (the historical now-minus-cursor measure would
        // have reported the whole epoch as lag here).
        WalCommitLogWriter._materialiserDrainLagLevels.Clear();
        var headTicks = TimeSpan.FromSeconds(100).Ticks;
        var provider = await SeededProviderWithHeadAsync(headTicks);
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(headTicks));

        var sut = new LatticeWalGc(
            Services(provider, durablePins: null),
            registry,
            MonitorWithLagThreshold(TimeSpan.FromSeconds(5)));

        await sut.RunOnceAsync(Tree);

        Assert.That(
            WalCommitLogWriter._materialiserDrainLagLevels.TryGetValue(Tree, out var level),
            Is.True);
        Assert.That(
            level.LagTicks,
            Is.EqualTo(0L),
            "A frontier caught up to the WAL head must record zero lag - no idle-tree false positive.");
    }

    [Test]
    public async Task RunOnceAsync_drain_lag_disabled_records_no_level()
    {
        WalCommitLogWriter._materialiserDrainLagLevels.Clear();
        var provider = await SeededProviderWithHeadAsync(TimeSpan.FromSeconds(100).Ticks);
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(TimeSpan.FromSeconds(10).Ticks));

        var sut = new LatticeWalGc(
            Services(provider, durablePins: null),
            registry,
            MonitorWithLagThreshold(null));

        await sut.RunOnceAsync(Tree);

        Assert.That(
            WalCommitLogWriter._materialiserDrainLagLevels.ContainsKey(Tree),
            Is.False,
            "With the drain-lag input disabled (threshold null), no pass may record a level.");
    }
}
