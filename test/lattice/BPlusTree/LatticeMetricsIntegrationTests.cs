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
public class LatticeMetricsIntegrationTests
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
}