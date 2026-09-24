using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

[TestFixture]
public sealed class LatticeWalGcBlockedConsumerCensusTests
{
    private const string InstrumentName = "orleans.lattice.wal.gc.blocked_consumers";

    [TestCase(1)]
    [TestCase(4)]
    public async Task RunOnceAsync_counts_beyond_the_report_cap_without_changing_trim(int partitions)
    {
        using var harness = new Harness(partitions);
        harness.SeedBlockers(40);
        await harness.Registry.ReportCursorAsync(harness.Tree, "shipper", new HybridLogicalClock { WallClockTicks = 10 });
        for (var partition = 0; partition < partitions; partition++)
            await harness.AppendAsync(partition);
        var report = await harness.Gc.RunOnceAsync(harness.Tree);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Observe(), Is.EqualTo(40));
            Assert.That(report.BlockingConsumerIds, Has.Count.EqualTo(8));
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(report.EntriesTrimmed, Is.Zero);
        });
        for (var partition = 0; partition < partitions; partition++)
        {
            var remaining = 0;
            await foreach (var entry in harness.Provider.ReadAsync(harness.Tree, partition, -1, 10, CancellationToken.None))
                remaining++;
            Assert.That(remaining, Is.EqualTo(1), "the census must leave real, cursor-eligible WAL entries retained");
        }
    }

    [Test]
    public async Task RunOnceAsync_excludes_present_covered_and_proven_empty_pins()
    {
        using var harness = new Harness(2);
        harness.SeedBlockers(40);
        await harness.Registry.ReportCursorAsync(harness.Tree, harness.Consumer(0), new HybridLogicalClock { WallClockTicks = 10 });
        harness.Offsets[harness.Consumer(1)] = 0;
        // Populate the offset plane for every remaining consumer, so there is
        // no population gap. Partition 1 has never received an entry.
        foreach (var id in harness.Pins.Keys)
        {
            harness.Offsets.TryAdd(id, -1);
        }
        await harness.AppendAsync(0);

        await harness.Gc.RunOnceAsync(harness.Tree);
        Assert.That(harness.Observe(), Is.EqualTo(19));
    }

    [Test]
    public async Task RunOnceAsync_counts_population_gaps_and_other_blockers_once()
    {
        using var harness = new Harness(1);
        harness.SeedBlockers(40);
        harness.Offsets[harness.Consumer(0)] = 0;
        harness.Offsets[harness.Consumer(1)] = -1;
        await harness.AppendAsync(0);
        var report = await harness.Gc.RunOnceAsync(harness.Tree);
        Assert.Multiple(() =>
        {
            Assert.That(harness.Observe(), Is.EqualTo(39), "38 gaps plus one unusable abstained pin");
            Assert.That(report.BlockingConsumerId, Is.EqualTo(harness.Consumer(2)),
                "the original gap report must survive the diagnostic continuation");
            Assert.That(report.EntriesTrimmed, Is.Zero);
        });
    }

    [Test]
    public async Task RunOnceAsync_snapshot_failure_preserves_the_population_gap_refusal()
    {
        using var harness = new Harness(1);
        harness.SeedBlockers(40);
        harness.Offsets[harness.Consumer(0)] = 0;
        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(harness.Tree, Arg.Any<CancellationToken>())
            .Returns<Task<IReadOnlyList<WalCursorSnapshot>>>(_ => throw new InvalidOperationException("registry unavailable"));

        var report = await harness.CreateGc(registry).RunOnceAsync(harness.Tree);
        Assert.Multiple(() =>
        {
            Assert.That(harness.Observe(), Is.EqualTo(-1));
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(report.BlockingConsumerId, Is.EqualTo(harness.Consumer(1)));
        });
    }

    [Test]
    public async Task RunOnceAsync_replaces_the_population_with_a_measured_zero()
    {
        using var harness = new Harness(1);
        harness.SeedBlockers(40);
        await harness.Gc.RunOnceAsync(harness.Tree);
        Assert.That(harness.Observe(), Is.EqualTo(40));
        harness.Pins.Clear();
        await harness.Gc.RunOnceAsync(harness.Tree);
        Assert.That(harness.Observe(), Is.Zero);
    }

    [Test]
    public async Task RunOnceAsync_primes_zero_before_the_first_await()
    {
        using var harness = new Harness(1);
        var registry = Substitute.For<IWalCursorRegistry>();
        var pending = new TaskCompletionSource<HybridLogicalClock?>();
        registry.GetMinCursorAsync(harness.Tree, Arg.Any<CancellationToken>()).Returns(pending.Task);
        var gc = harness.CreateGc(registry);
        var pass = gc.RunOnceAsync(harness.Tree);
        try
        {
            Assert.That(harness.Observe(), Is.Zero);
        }
        finally
        {
            pending.SetResult(null);
            await pass;
        }
    }

    [Test]
    public async Task RunOnceAsync_unreadable_pins_report_unknown_not_a_healthy_zero()
    {
        using var harness = new Harness(1);
        harness.PinGrain.GetPinsAsync().Returns<Task<IReadOnlyDictionary<string, HybridLogicalClock>>>(
            _ => throw new InvalidOperationException("pin storage unavailable"));
        await harness.Gc.RunOnceAsync(harness.Tree);
        Assert.That(harness.Observe(), Is.EqualTo(-1));
    }

    private sealed class Harness : IDisposable
    {
        private readonly ServiceProvider _services;
        private readonly IOptionsMonitor<LatticeOptions> _options;
        private readonly MeterListener _listener;
        private long? _observed;

        public string Tree { get; } = "blocked-census-" + Guid.NewGuid().ToString("N");
        public Dictionary<string, HybridLogicalClock> Pins { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, long> Offsets { get; } = new(StringComparer.Ordinal);
        public IWalMaterialiserPinGrain PinGrain { get; } = Substitute.For<IWalMaterialiserPinGrain>();
        public InMemoryWalStorageProvider Provider { get; } = new();
        public InMemoryWalCursorRegistry Registry { get; } = new();
        public LatticeWalGc Gc { get; }
        private readonly int _partitions;

        public Harness(int partitions)
        {
            _partitions = partitions;
            var options = new LatticeOptions { WalPartitions = partitions, WalDurabilityHoldCeilingBytes = 0 };
            _options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
            _options.CurrentValue.Returns(options);
            _options.Get(Arg.Any<string>()).Returns(options);
            PinGrain.GetPinsAsync().Returns(_ => Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(Pins));
            PinGrain.GetPinOffsetsAsync().Returns(_ => Task.FromResult<IReadOnlyDictionary<string, long>>(Offsets));
            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(PinGrain);
            _services = new ServiceCollection().AddSingleton<IWalStorageProvider>(Provider)
                .AddSingleton(factory).BuildServiceProvider();
            Gc = CreateGc(Registry);
            _listener = MeterListening.StartForMeter(LatticeMetrics.Meter, listener =>
                listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
                {
                    if (instrument.Name != InstrumentName) return;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, Tree))
                            _observed = value;
                    }
                }));
        }

        public string Consumer(int index) => $"_lattice_materialiser_{Tree}_leaf-{index}_{index % _partitions}";

        public void SeedBlockers(int count)
        {
            for (var i = 0; i < count; i++) Pins[Consumer(i)] = HybridLogicalClock.Zero;
        }

        public LatticeWalGc CreateGc(IWalCursorRegistry registry) => new(_services, registry, _options);

        public Task AppendAsync(int partition) => Provider.AppendBatchAsync(Tree, partition, [new WalEntry
        {
            Offset = 0,
            Mutation = new LatticeMutation
            {
                TreeId = Tree, Key = "key", Value = [1], Kind = MutationKind.Set,
                Timestamp = new HybridLogicalClock { WallClockTicks = 1 }, OriginClusterId = "test",
            },
        }], CancellationToken.None);

        public long? Observe()
        {
            _observed = null;
            _listener.RecordObservableInstruments();
            return _observed;
        }

        public void Dispose()
        {
            _listener.Dispose();
            _services.Dispose();
        }
    }
}
