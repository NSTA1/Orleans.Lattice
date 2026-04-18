namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Volatile in-memory hotness counters for the shard root grain.
/// Counters are incremented on each read/write operation and reset
/// on grain deactivation (no persistence cost).
/// </summary>
internal sealed partial class ShardRootGrain
{
    private long _readOps;
    private long _writeOps;
    private readonly DateTime _countersSince = DateTime.UtcNow;

    /// <summary>Increments the read operation counter.</summary>
    private void RecordRead() => _readOps++;

    /// <summary>Increments the write operation counter.</summary>
    private void RecordWrite() => _writeOps++;

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
