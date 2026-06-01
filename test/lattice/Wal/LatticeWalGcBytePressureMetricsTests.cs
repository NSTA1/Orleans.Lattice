using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Verifies the advisory byte-pressure policy emits the storage-policy
/// counters (<c>storage.policy.trim_triggered</c> with <c>reason=byte_pressure</c>
/// and <c>storage.policy.bytes_reclaimed</c>) with the byte counts the spec's
/// acceptance criteria require.
/// </summary>
[TestFixture]
public sealed class LatticeWalGcBytePressureMetricsTests
{
    private const string Tree = "bp-metrics-tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeOptions> Monitor(LatticeOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(IWalStorageProvider provider)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        return sc.BuildServiceProvider();
    }

    private static WalEntry Entry(long offset, string key, byte[] value, HybridLogicalClock ts) => new()
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

    [Test]
    public async Task RunOnceAsync_caught_up_consumer_over_ceiling_emits_trim_triggered_and_bytes_reclaimed()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(Tree, 0,
        [
            Entry(0, "a", new byte[10], Hlc(10)),
            Entry(1, "b", new byte[10], Hlc(20)),
            Entry(2, "c", new byte[10], Hlc(30)),
        ], CancellationToken.None);

        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer", Hlc(30));

        using var collector = new CounterCollector();

        var sut = new LatticeWalGc(
            Services(provider),
            registry,
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.BytePressureTriggered, Is.True);
            Assert.That(report.BytePressureOverThreshold, Is.False);

            var triggered = collector.Sum(LatticeMetrics.StoragePolicyTrimTriggeredName, Tree);
            Assert.That(triggered, Is.EqualTo(1), "exactly one byte-pressure trim should be scheduled");
            Assert.That(collector.HasReason(LatticeMetrics.StoragePolicyTrimTriggeredName, "byte_pressure"), Is.True);

            var reclaimed = collector.Sum(LatticeMetrics.StoragePolicyBytesReclaimedName, Tree);
            Assert.That(reclaimed, Is.GreaterThan(0), "reclaimed bytes must reflect the freed footprint");
        });
    }

    [Test]
    public async Task RunOnceAsync_lagging_consumer_over_ceiling_triggers_but_reclaims_nothing()
    {
        var provider = new InMemoryWalStorageProvider();
        // No consumer cursor -> nothing is trim-eligible.
        await provider.AppendBatchAsync(Tree, 0,
        [
            Entry(0, "a", new byte[100], Hlc(10)),
            Entry(1, "b", new byte[100], Hlc(20)),
        ], CancellationToken.None);

        using var collector = new CounterCollector();

        var sut = new LatticeWalGc(
            Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = 50 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.BytePressureTriggered, Is.True);
            Assert.That(report.BytePressureOverThreshold, Is.True);

            Assert.That(collector.Sum(LatticeMetrics.StoragePolicyTrimTriggeredName, Tree), Is.EqualTo(1));
            // Nothing was safely reclaimable; the reclaim counter never emits.
            Assert.That(collector.Sum(LatticeMetrics.StoragePolicyBytesReclaimedName, Tree), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task RunOnceAsync_policy_disabled_emits_no_storage_policy_counters()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(Tree, 0, [Entry(0, "a", new byte[100], Hlc(10))], CancellationToken.None);

        using var collector = new CounterCollector();

        var sut = new LatticeWalGc(
            Services(provider),
            new InMemoryWalCursorRegistry(),
            Monitor(new LatticeOptions { WalPartitions = 1, WalMaxRetainedBytes = null }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.BytePressureTriggered, Is.False);
            Assert.That(collector.Sum(LatticeMetrics.StoragePolicyTrimTriggeredName, Tree), Is.EqualTo(0));
            Assert.That(collector.Sum(LatticeMetrics.StoragePolicyBytesReclaimedName, Tree), Is.EqualTo(0));
        });
    }

    /// <summary>
    /// Captures long-valued counter measurements on the core meter, filtering
    /// at read time by instrument name and <c>tree</c> tag so parallel test
    /// interference on the shared meter does not pollute the assertion.
    /// </summary>
    private sealed class CounterCollector : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _lock = new();

        public CounterCollector()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                        l.EnableMeasurementEvents(inst);
                },
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            lock (_lock)
            {
                _records.Add((instrument.Name, value, tags.ToArray()));
            }
        }

        public long Sum(string name, string tree)
        {
            lock (_lock)
            {
                return _records
                    .Where(r => r.Name == name
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree))
                    .Sum(r => r.Value);
            }
        }

        public bool HasReason(string name, string reason)
        {
            lock (_lock)
            {
                return _records.Any(r => r.Name == name
                    && r.Tags.Any(t => t.Key == LatticeMetrics.TagReason && (string?)t.Value == reason));
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
