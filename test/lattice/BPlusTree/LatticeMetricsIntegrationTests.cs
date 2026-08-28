using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for the <see cref="LatticeMetrics"/> instruments. Each
/// test attaches a <see cref="MeterListener"/>, exercises the relevant grain
/// surface, and asserts that the expected counter / histogram received data
/// points with the expected tags.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class LatticeMetricsIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// Captures every measurement reported on the <see cref="LatticeMetrics.Meter"/>
    /// instrument instance (by object identity) for the lifetime of the test.
    /// </summary>
    private sealed class MetricRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

        public MetricRecorder()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                        l.EnableMeasurementEvents(inst);
                }
            };
            _listener.SetMeasurementEventCallback<long>(OnLong);
            _listener.SetMeasurementEventCallback<double>(OnDouble);
            _listener.Start();
        }

        private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
            => Records.Add((instrument.Name, value, tags.ToArray()));

        private void OnDouble(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
            => Records.Add((instrument.Name, value, tags.ToArray()));

        public long Sum(string instrumentName, string treeId)
        {
            long total = 0;
            foreach (var r in Records)
            {
                if (r.Name != instrumentName) continue;
                if (!r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)) continue;
                total += (long)r.Value;
            }
            return total;
        }

        public int CountFor(string instrumentName, string treeId)
        {
            var count = 0;
            foreach (var r in Records)
            {
                if (r.Name != instrumentName) continue;
                if (!r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)) continue;
                count++;
            }
            return count;
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task MeterName_is_orleans_lattice()
    {
        Assert.That(LatticeMetrics.Meter.Name, Is.EqualTo("orleans.lattice"));
        Assert.That(LatticeMetrics.MeterName, Is.EqualTo("orleans.lattice"));
    }

    [Test]
    public async Task SetAsync_emits_shard_write_counter_with_tree_and_shard_tags()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-set-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var writes = recorder.Sum("orleans.lattice.shard.writes", treeId);
        Assert.That(writes, Is.GreaterThanOrEqualTo(1));

        // Confirm at least one write carried a numeric shard tag.
        var shardTagged = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.shard.writes" &&
            r.Tags.Any(t => t.Key == LatticeMetrics.TagShard && t.Value is int));
        Assert.That(shardTagged, Is.True);
    }

    [Test]
    public async Task SetManyAsync_advances_the_write_counter_once_per_shard_but_records_every_entry()
    {
        // Regression for issue #1648: orleans.lattice.shard.writes is a
        // per-OPERATION counter, so a batched write advances it once per shard
        // touched regardless of entry count. The companion per-RECORD counter
        // orleans.lattice.shard.records_written must account for every entry, so
        // bulk and batch ingestion is observable rather than silently
        // under-represented on the throughput panels.
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-setmany-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        const int entryCount = 50;
        var entries = new List<KeyValuePair<string, byte[]>>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            entries.Add(new KeyValuePair<string, byte[]>(
                $"batch-key-{i:D4}", Encoding.UTF8.GetBytes($"v{i}")));
        }

        await tree.SetManyAsync(entries);

        var writes = recorder.Sum("orleans.lattice.shard.writes", treeId);
        var records = recorder.Sum("orleans.lattice.shard.records_written", treeId);

        // Every entry is accounted for, exactly once, across all shards.
        Assert.That(records, Is.EqualTo(entryCount),
            "records_written must count every entry the batch carried.");

        // The operation counter advances at most once per shard (the fixture has
        // four), which is precisely the divergence the companion counter exists
        // to expose.
        Assert.That(writes, Is.LessThan(entryCount),
            "shard.writes is per-operation and must not scale with batch size.");
        Assert.That(records, Is.GreaterThan(writes));
    }

    [Test]
    public async Task SetAsync_records_exactly_one_entry_on_the_records_written_counter()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-records-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        // On the single-key path the two counters coincide: one operation,
        // one record.
        Assert.That(recorder.Sum("orleans.lattice.shard.records_written", treeId),
            Is.EqualTo(1));

        var shardTagged = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.shard.records_written" &&
            r.Tags.Any(t => t.Key == LatticeMetrics.TagShard && t.Value is int));
        Assert.That(shardTagged, Is.True);
    }

    [Test]
    public async Task GetAsync_emits_shard_read_counter()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-get-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await tree.GetAsync("k1");

        Assert.That(recorder.Sum("orleans.lattice.shard.reads", treeId),
            Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task DeleteAsync_emits_tombstones_created_counter()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-del-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        await tree.DeleteAsync("k1");

        Assert.That(recorder.Sum("orleans.lattice.leaf.tombstones.created", treeId),
            Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteRangeAsync_emits_bulk_tombstones_created()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-delrange-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 5; i++)
            await tree.SetAsync($"a{i}", Encoding.UTF8.GetBytes("v"));

        await tree.DeleteRangeAsync("a0", "a~");

        Assert.That(recorder.Sum("orleans.lattice.leaf.tombstones.created", treeId),
            Is.EqualTo(5));
    }

    [Test]
    public async Task SetAsync_emits_leaf_write_duration_histogram()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-writedur-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(recorder.CountFor("orleans.lattice.leaf.write.duration", treeId),
            Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task KeysAsync_emits_scan_duration_with_keys_operation_tag()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-scan-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 3; i++)
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));

        await foreach (var _ in tree.KeysAsync()) { }

        var keysScan = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.leaf.scan.duration" &&
            r.Tags.Any(t => t.Key == LatticeMetrics.TagOperation && (t.Value as string) == "keys") &&
            r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId));
        Assert.That(keysScan, Is.True, "Expected at least one scan duration with operation=keys");
    }

    [Test]
    public async Task EntriesAsync_emits_scan_duration_with_entries_operation_tag()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-entries-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        for (var i = 0; i < 3; i++)
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes("v"));

        await foreach (var _ in tree.EntriesAsync()) { }

        var entriesScan = recorder.Records.Any(r =>
            r.Name == "orleans.lattice.leaf.scan.duration" &&
            r.Tags.Any(t => t.Key == LatticeMetrics.TagOperation && (t.Value as string) == "entries") &&
            r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId));
        Assert.That(entriesScan, Is.True);
    }

    [Test]
    public async Task LeafSplits_counter_fires_when_leaf_overflows_MaxLeafKeys()
    {
        using var recorder = new MetricRecorder();
        var treeId = $"metrics-leafsplit-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        // FourShardClusterFixture pins MaxLeafKeys = 4; inserting 40 keys across
        // 4 shards guarantees at least one leaf overflows and splits.
        for (var i = 0; i < 40; i++)
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes("v"));

        Assert.That(recorder.Sum("orleans.lattice.leaf.splits", treeId),
            Is.GreaterThanOrEqualTo(1));
    }

    /// <summary>
    /// Operational invariant of the chained-fold digest design: a whole-tree
    /// poll of <see cref="ILattice.GetLeafProjectionDigestAsync"/> issues
    /// exactly one grain call per physical shard. A regression that fell back
    /// to walking every leaf would manifest as more than
    /// <see cref="FourShardClusterFixture.TestShardCount"/> increments on the
    /// per-shard digest-read counter, even though the public surface still
    /// returns the same digest.
    /// </summary>
    [Test]
    public async Task WholeTreePoll_emits_one_digest_read_per_shard()
    {
        var treeId = $"metrics-digest-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed enough keys to push at least one shard past a single leaf so the
        // "one call per shard" property is being asserted against the
        // internal-node aggregate path, not just the flat-tree fast path.
        for (var i = 0; i < 30; i++)
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        // Attach the recorder *after* the seed traffic so writes don't pollute
        // the digest-read counter; only the poll loop below should produce data
        // points on orleans.lattice.shard.digest_reads.
        using var recorder = new MetricRecorder();

        for (var s = 0; s < FourShardClusterFixture.TestShardCount; s++)
        {
            _ = await tree.GetLeafProjectionDigestAsync(s);
        }

        // Exactly one increment per shard - no more, no less. Going above N
        // would mean the read path fanned out across leaves; going below N
        // would mean the counter is missing one of the dispatch arms (empty
        // shard, flat tree, or internal subtree).
        Assert.That(recorder.Sum("orleans.lattice.shard.digest_reads", treeId),
            Is.EqualTo(FourShardClusterFixture.TestShardCount),
            "A whole-tree digest poll must issue exactly one shard-root grain call per physical shard.");

        // Each data point must carry both the tree tag and a distinct shard
        // tag so dashboards can attribute the call to a shard.
        var perShard = recorder.Records
            .Where(r => r.Name == "orleans.lattice.shard.digest_reads"
                        && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId))
            .Select(r => r.Tags.First(t => t.Key == LatticeMetrics.TagShard).Value)
            .OfType<int>()
            .OrderBy(i => i)
            .ToArray();
        Assert.That(perShard, Is.EqualTo(Enumerable.Range(0, FourShardClusterFixture.TestShardCount).ToArray()),
            "Each shard index in [0, TestShardCount) must be tagged exactly once.");
    }
}