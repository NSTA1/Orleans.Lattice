namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Volatile in-memory hotness counters for the shard root grain.
/// Counters are incremented on each read/write operation and reset
/// on grain deactivation (no persistence cost). Each increment also
/// publishes a data point to the corresponding
/// <see cref="LatticeMetrics"/> instrument so an OpenTelemetry pipeline
/// can observe per-shard throughput without polling
/// <see cref="IShardRootGrain.GetHotnessAsync"/>.
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
            var sep = key.LastIndexOf('/');
            var parsed = sep >= 0 && int.TryParse(key.AsSpan(sep + 1), out var idx) ? idx : 0;
            _cachedShardIndex = parsed;
            return parsed;
        }
    }

    /// <summary>Increments the read operation counter and publishes a meter data point.</summary>
    private void RecordRead()
    {
        _readOps++;
        LatticeMetrics.ShardReads.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex));
    }

    /// <summary>Increments the write operation counter and publishes a meter data point.</summary>
    private void RecordWrite()
    {
        _writeOps++;
        LatticeMetrics.ShardWrites.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, ShardIndex));
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
