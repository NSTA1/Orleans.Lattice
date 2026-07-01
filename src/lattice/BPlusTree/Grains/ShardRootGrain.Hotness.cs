namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Volatile in-memory hotness counters for the shard root grain.
/// Counters are incremented on each read/write operation and reset
/// on grain deactivation (no persistence cost). Each increment also
/// publishes a data point to the corresponding
/// <see cref="LatticeMetrics"/> instrument so an OpenTelemetry pipeline
/// can observe per-shard throughput without polling
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetHotnessAsync"/>.
/// </summary>
internal sealed partial class ShardRootGrain
{
    private long _readOps;
    private long _writeOps;
    private readonly DateTime _countersSince = DateTime.UtcNow;

    private int? _cachedShardIndex;
    private int ShardIndex
    {
        get
        {
            if (_cachedShardIndex is { } cached) return cached;
            var key = context.GrainId.Key.ToString()!;
            _cachedShardIndex = ParseShardGrainKey(key).shardIndex;
            return _cachedShardIndex.Value;
        }
    }

    // Per-activation cache of the two OTel tag pairs that RecordRead /
    // RecordWrite stamp on every call. Both tag values are immutable for the
    // lifetime of the activation: TreeId is parsed once from the grain key,
    // and ShardIndex is parsed once from the same key. Without caching, every
    // shard read/write allocates one int->object box (for ShardIndex) per
    // RecordRead / RecordWrite call - O(writes) on the dominant per-call
    // hot path. Holding the boxed value in a field reduces that to O(1) per
    // activation.
    private KeyValuePair<string, object?>[]? _cachedMetricTags;
    private KeyValuePair<string, object?>[] GetMetricTags()
    {
        return _cachedMetricTags ??=
        [
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex),
        ];
    }

    /// <summary>Increments the read operation counter and publishes a meter data point.</summary>
    private void RecordRead()
    {
        _readOps++;
        LatticeMetrics.ShardReads.Add(1, GetMetricTags());
    }

    /// <summary>Increments the write operation counter and publishes a meter data point.</summary>
    private void RecordWrite()
    {
        _writeOps++;
        LatticeMetrics.ShardWrites.Add(1, GetMetricTags());
    }

    /// <inheritdoc />
    public Task<ShardHotness> GetHotnessAsync()
    {
        return Task.FromResult(new ShardHotness
        {
            Reads = _readOps,
            Writes = _writeOps,
            Window = DateTime.UtcNow - _countersSince,
        });
    }
}
