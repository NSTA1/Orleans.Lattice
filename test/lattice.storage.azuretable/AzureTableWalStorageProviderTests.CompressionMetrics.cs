using System.Diagnostics.Metrics;
using Azure.Data.Tables;

namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// White-box tests for the per-batch WAL compression-savings accounting on
/// <see cref="AzureTableWalStorageProvider"/>: the <see cref="AzureTableWalStorageProvider.WalCompressionStats"/>
/// totals the encode helper accumulates, and the
/// <c>orleans.lattice.storage.wal.*</c> counters
/// <see cref="AzureTableWalStorageProvider.RecordWalCompressionMetrics"/>
/// emits from them. Emission through the full append path is covered
/// end-to-end (emulator-gated) by
/// <see cref="CompressedAzureTableWalIntegrationTests"/>.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    [Test]
    public void Encode_stats_report_savings_for_a_compressible_batch()
    {
        var provider = CreateCompressingProvider(minPayloadBytes: 0);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(
            partitionKey,
            new[] { CompressibleEntry(0, 4096), CompressibleEntry(1, 4096) },
            actions,
            out var stats);

        Assert.Multiple(() =>
        {
            Assert.That(stats.UncompressedBytes, Is.GreaterThan(0));
            Assert.That(stats.StoredBytes, Is.LessThan(stats.UncompressedBytes),
                "a compressible batch must store fewer bytes than it encoded");
            Assert.That(stats.SkippedDisabled, Is.Zero);
            Assert.That(stats.SkippedBelowThreshold, Is.Zero);
            Assert.That(stats.SkippedInflationGuard, Is.Zero);
        });
    }

    [Test]
    public void Encode_stats_attribute_an_incompressible_row_to_the_inflation_guard()
    {
        var provider = CreateCompressingProvider(minPayloadBytes: 0);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(
            partitionKey, new[] { IncompressibleEntry(0, 2048) }, actions, out var stats);

        Assert.Multiple(() =>
        {
            Assert.That(stats.SkippedInflationGuard, Is.EqualTo(1));
            Assert.That(stats.SkippedBelowThreshold, Is.Zero);
            Assert.That(stats.SkippedDisabled, Is.Zero);
            Assert.That(stats.StoredBytes, Is.EqualTo(stats.UncompressedBytes),
                "a verbatim row stores exactly its encoded length");
        });
    }

    [Test]
    public void Encode_stats_attribute_a_below_threshold_row_to_the_threshold()
    {
        var provider = CreateCompressingProvider(minPayloadBytes: 100_000);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(
            partitionKey, new[] { CompressibleEntry(0, 256) }, actions, out var stats);

        Assert.Multiple(() =>
        {
            Assert.That(stats.SkippedBelowThreshold, Is.EqualTo(1));
            Assert.That(stats.SkippedInflationGuard, Is.Zero);
            Assert.That(stats.SkippedDisabled, Is.Zero);
            Assert.That(stats.StoredBytes, Is.EqualTo(stats.UncompressedBytes));
        });
    }

    [Test]
    public void Encode_stats_attribute_rows_to_disabled_when_compression_is_off()
    {
        var provider = CreateCompressingProvider(compression: LatticeCompression.None);
        var partitionKey = AzureTableWalStorageProvider.BuildPartitionKey("compress-pin", 0);
        var actions = new List<TableTransactionAction>();

        provider.EncodeEntriesForBatch(
            partitionKey, new[] { CompressibleEntry(0, 4096) }, actions, out var stats);

        Assert.Multiple(() =>
        {
            Assert.That(stats.SkippedDisabled, Is.EqualTo(1));
            Assert.That(stats.SkippedBelowThreshold, Is.Zero);
            Assert.That(stats.SkippedInflationGuard, Is.Zero);
            Assert.That(stats.StoredBytes, Is.EqualTo(stats.UncompressedBytes));
        });
    }

    [Test]
    public void RecordWalCompressionMetrics_emits_both_byte_totals_tagged_by_tree()
    {
        const string tree = "wal-metrics-bytes";
        using var collector = new WalCompressionCounterCollector();

        AzureTableWalStorageProvider.RecordWalCompressionMetrics(
            tree,
            new AzureTableWalStorageProvider.WalCompressionStats
            {
                UncompressedBytes = 1_000,
                StoredBytes = 400,
            });

        Assert.Multiple(() =>
        {
            Assert.That(collector.Sum(LatticeMetrics.StorageWalUncompressedBytesName, tree), Is.EqualTo(1_000));
            Assert.That(collector.Sum(LatticeMetrics.StorageWalStoredBytesName, tree), Is.EqualTo(400));
            Assert.That(collector.Sum(LatticeMetrics.StorageWalCompressionSkippedName, tree), Is.Zero,
                "no rows skipped, so the skip counter must not emit");
        });
    }

    [Test]
    public void RecordWalCompressionMetrics_emits_skip_buckets_only_when_nonzero()
    {
        const string tree = "wal-metrics-skips";
        using var collector = new WalCompressionCounterCollector();

        AzureTableWalStorageProvider.RecordWalCompressionMetrics(
            tree,
            new AzureTableWalStorageProvider.WalCompressionStats
            {
                UncompressedBytes = 500,
                StoredBytes = 500,
                SkippedBelowThreshold = 2,
                SkippedInflationGuard = 1,
            });

        Assert.Multiple(() =>
        {
            Assert.That(collector.SumByReason(tree, "below_threshold"), Is.EqualTo(2));
            Assert.That(collector.SumByReason(tree, "inflation_guard"), Is.EqualTo(1));
            Assert.That(collector.SumByReason(tree, "disabled"), Is.Zero,
                "the disabled bucket was zero, so it must not emit");
        });
    }

    [Test]
    public void RecordWalCompressionMetrics_attributes_the_disabled_skip_bucket()
    {
        const string tree = "wal-metrics-disabled";
        using var collector = new WalCompressionCounterCollector();

        AzureTableWalStorageProvider.RecordWalCompressionMetrics(
            tree,
            new AzureTableWalStorageProvider.WalCompressionStats
            {
                UncompressedBytes = 300,
                StoredBytes = 300,
                SkippedDisabled = 3,
            });

        Assert.Multiple(() =>
        {
            Assert.That(collector.SumByReason(tree, "disabled"), Is.EqualTo(3));
            Assert.That(collector.SumByReason(tree, "below_threshold"), Is.Zero);
            Assert.That(collector.SumByReason(tree, "inflation_guard"), Is.Zero);
        });
    }

    /// <summary>
    /// Captures long-valued measurements on the core
    /// <see cref="LatticeMetrics.Meter"/>, filtering at read time by
    /// instrument name plus <c>tree</c> (and optionally <c>reason</c>) tag so
    /// parallel tests writing the same shared counters do not cross-pollute.
    /// </summary>
    private sealed class WalCompressionCounterCollector : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly List<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> _records = new();
        private readonly object _lock = new();

        public WalCompressionCounterCollector()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                    {
                        l.EnableMeasurementEvents(inst);
                    }
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

        public long SumByReason(string tree, string reason)
        {
            lock (_lock)
            {
                return _records
                    .Where(r => r.Name == LatticeMetrics.StorageWalCompressionSkippedName
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree)
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagReason && (string?)t.Value == reason))
                    .Sum(r => r.Value);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
