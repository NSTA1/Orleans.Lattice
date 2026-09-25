using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for issue #3576 (re-review): a pass on which the durable
/// materialiser pin census or offset census cannot be read must trim nothing.
/// <para>
/// The durable pins are the only evidence of a dormant leaf that has not
/// re-registered with the in-memory cursor registry since a restart. The GC
/// used to catch the read failure and carry on with the registry minimum
/// (pins) or with no offset floor (offsets), which reclaims exactly the
/// un-checkpointed WAL tail the dormant leaf still needs. The pin store reads
/// every shard and bucket in one joined fan-in, so a single unreadable slot
/// loses the whole census, not one shard's share of it.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcCensusUnavailableTests
{
    private const string Tree = "tree";
    private const string DormantLeaf = "_lattice_materialiser_tree_leaf-dormant";
    private const int PinShards = 4;

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

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

    private static IOptionsMonitor<LatticeOptions> Monitor(TimeSpan? retention = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalMaterialiserPinShards = PinShards,
            WalDurabilityHoldCeilingBytes = 0,
            WalRetention = retention,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// A shard that is NOT the dormant leaf's own, so a failure there proves the
    /// joined fan-in loses every shard's pins rather than only its own.
    /// </summary>
    private static int FaultedShard()
        => (WalMaterialiserPinRouting.AuthoritativeKeyIndex(DormantLeaf, PinShards) + 1) % PinShards;

    /// <summary>
    /// Builds a GC over <see cref="PinShards"/> pin shards. The dormant leaf's
    /// pin and offset live on its authoritative shard; <paramref name="pinsThrowOn"/>
    /// and <paramref name="offsetsThrowOn"/> fault one shard's read.
    /// </summary>
    private static LatticeWalGc Gc(
        IWalStorageProvider provider,
        IWalCursorRegistry registry,
        HybridLogicalClock? dormantPin,
        long? dormantOffset,
        int? pinsThrowOn = null,
        int? offsetsThrowOn = null,
        RecordingLogger? logger = null,
        TimeSpan? retention = null)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        if (logger is not null)
        {
            sc.AddSingleton<ILogger<LatticeWalGc>>(logger);
        }

        var ownShard = WalMaterialiserPinRouting.AuthoritativeKeyIndex(DormantLeaf, PinShards);
        var keys = WalMaterialiserPinRouting.EnumerateReadKeys(Tree, PinShards);
        var factory = Substitute.For<IGrainFactory>();
        for (var i = 0; i < keys.Count; i++)
        {
            var pins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
            var offsets = new Dictionary<string, long>(StringComparer.Ordinal);
            if (i == ownShard && dormantPin is { } pin)
            {
                pins[DormantLeaf] = pin;
                if (dormantOffset is { } offset)
                {
                    offsets[DormantLeaf] = offset;
                }
            }

            var grain = Substitute.For<IWalMaterialiserPinGrain>();
            if (i == pinsThrowOn)
            {
                grain.GetPinsAsync().Returns<Task<IReadOnlyDictionary<string, HybridLogicalClock>>>(
                    _ => Task.FromException<IReadOnlyDictionary<string, HybridLogicalClock>>(
                        new TimeoutException("pin bucket read timed out")));
            }
            else
            {
                grain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(pins));
            }

            if (i == offsetsThrowOn)
            {
                grain.GetPinOffsetsAsync().Returns<Task<IReadOnlyDictionary<string, long>>>(
                    _ => Task.FromException<IReadOnlyDictionary<string, long>>(
                        new TimeoutException("pin bucket read timed out")));
            }
            else
            {
                grain.GetPinOffsetsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(offsets));
            }

            factory.GetGrain<IWalMaterialiserPinGrain>(keys[i]).Returns(grain);
        }

        sc.AddSingleton(factory);
        return new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor(retention));
    }

    private static async Task<IWalCursorRegistry> ActiveShipperAsync(long ticks)
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(ticks));
        return registry;
    }

    /// <summary>
    /// The reviewer's scenario: every entry is below the active consumers'
    /// cursor at 5000, and only the dormant leaf's durable pin at 100 retains
    /// the three above it.
    /// </summary>
    private static async Task<InMemoryWalStorageProvider> DormantTailProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(50)), Entry(1, Hlc(200)), Entry(2, Hlc(3_000)), Entry(3, Hlc(4_000)) },
            CancellationToken.None);
        return provider;
    }

    [Test]
    public async Task RunOnceAsync_one_pin_shard_read_failing_trims_nothing()
    {
        var provider = await DormantTailProviderAsync();
        var logger = new RecordingLogger();
        // No offset report for the dormant leaf (it predates the offsets plane,
        // or its seed was lost), so the offset floor cannot stand in for the
        // pin: only the pin census retains the tail.
        var gc = Gc(
            provider, await ActiveShipperAsync(5_000), Hlc(100), dormantOffset: null,
            pinsThrowOn: FaultedShard(), logger: logger);

        var report = await gc.RunOnceAsync(Tree);
        var lowest = await provider.GetLowestOffsetAsync(Tree, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "An unreadable pin census must fail the pass closed, not trim on the registry minimum.");
            Assert.That(lowest, Is.EqualTo(0),
                "The dormant leaf's un-checkpointed tail must still be readable.");
            Assert.That(report.MinCursor, Is.Null);
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "The skipped pass must read as blocked, so the scheduler does not treat it as a healthy quiet tree.");
            Assert.That(logger.Warnings, Has.Some.Contains("fails closed").And.Contains("pin"),
                "The skipped pass must name its reason.");
        });
    }

    [Test]
    public async Task RunOnceAsync_one_offset_shard_read_failing_trims_nothing()
    {
        // The pins read succeeds, so the HLC floor is the dormant leaf's 100.
        // The leaf has applied through offset 0 only; offsets 2 and 3 are
        // low-HLC reap envelopes above its checkpoint, HLC-eligible under that
        // floor. Only the offset floor retains them, and it is unreadable.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(50)), Entry(1, Hlc(90)), Entry(2, Hlc(5)), Entry(3, Hlc(6)) },
            CancellationToken.None);
        var logger = new RecordingLogger();
        var gc = Gc(
            provider, await ActiveShipperAsync(5_000), Hlc(100), dormantOffset: 0,
            offsetsThrowOn: FaultedShard(), logger: logger);

        var report = await gc.RunOnceAsync(Tree);
        var lowest = await provider.GetLowestOffsetAsync(Tree, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "An unreadable offset census must fail the pass closed, not trim on the HLC floor alone.");
            Assert.That(lowest, Is.EqualTo(0));
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(logger.Warnings, Has.Some.Contains("fails closed").And.Contains("offset"));
        });
    }

    [Test]
    public async Task RunOnceAsync_census_failure_also_suppresses_ttl_trimming()
    {
        // The offset veto that bounds a TTL trim is part of what is missing, so
        // a configured WalRetention must not trim through an unknown floor
        // either. Every entry is decades older than a one-tick retention.
        var provider = await DormantTailProviderAsync();
        var gc = Gc(
            provider, await ActiveShipperAsync(5_000), Hlc(100), dormantOffset: null,
            pinsThrowOn: FaultedShard(), retention: TimeSpan.FromTicks(1));

        var report = await gc.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.Zero);
    }

    [Test]
    public async Task RunOnceAsync_successful_empty_census_still_trims()
    {
        // Positive control: a census that was READ and is empty is evidence
        // that nobody else holds the WAL, not an unknown. It must trim exactly
        // as before the fail-closed change.
        var provider = await DormantTailProviderAsync();
        var logger = new RecordingLogger();
        var gc = Gc(provider, await ActiveShipperAsync(5_000), dormantPin: null, dormantOffset: null, logger: logger);

        var report = await gc.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(4));
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.Available));
            Assert.That(logger.Warnings, Has.None.Contains("fails closed"));
        });
    }

    [Test]
    public async Task RunOnceAsync_readable_dormant_pin_trims_only_below_it()
    {
        // Positive control for the pin itself: once readable, the dormant pin
        // retains its tail and releases only what it has passed.
        var provider = await DormantTailProviderAsync();
        var gc = Gc(provider, await ActiveShipperAsync(5_000), Hlc(100), dormantOffset: 0);

        var report = await gc.RunOnceAsync(Tree);
        var lowest = await provider.GetLowestOffsetAsync(Tree, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
            Assert.That(lowest, Is.EqualTo(1));
        });
    }

    /// <summary>Captures formatted warning messages.</summary>
    private sealed class RecordingLogger : ILogger<LatticeWalGc>
    {
        private readonly List<string> _warnings = new();

        public IReadOnlyList<string> Warnings
        {
            get
            {
                lock (_warnings)
                {
                    return _warnings.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                lock (_warnings)
                {
                    _warnings.Add(formatter(state, exception));
                }
            }
        }
    }
}
