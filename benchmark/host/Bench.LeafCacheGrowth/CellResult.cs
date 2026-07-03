namespace Orleans.Lattice.Benchmark.LeafCacheGrowth;

/// <summary>
/// A single 1-second-cadence memory + cache-footprint sample taken during a
/// cell's read workload.
/// </summary>
/// <param name="ElapsedMs">Milliseconds since the workload started.</param>
/// <param name="WorkingSetBytes"><c>Process.WorkingSet64</c> at the sample.</param>
/// <param name="GcTotalBytes"><c>GC.GetTotalMemory(false)</c> at the sample.</param>
/// <param name="CacheEntryCount">Live rows mirrored in the cache.</param>
/// <param name="CacheValueBytes">Summed non-null value-payload bytes in the cache.</param>
internal readonly record struct MemorySample(
    long ElapsedMs,
    long WorkingSetBytes,
    long GcTotalBytes,
    int CacheEntryCount,
    long CacheValueBytes);

/// <summary>
/// Aggregated result for one (entry_count x value_bytes) probe cell: the
/// baseline unbounded cache footprint and steady-state read-latency envelope.
/// </summary>
internal sealed class CellResult
{
    public int EntryCount { get; init; }
    public int ValueBytes { get; init; }
    public int CacheEntryCount { get; init; }
    public long CacheValueBytes { get; init; }
    public long WorkingSetBaselineBytes { get; init; }
    public long WorkingSetPeakBytes { get; init; }
    public long WorkingSetDeltaBytes { get; init; }
    public long GcTotalBaselineBytes { get; init; }
    public long GcTotalPeakBytes { get; init; }
    public long ReadCount { get; init; }
    public double ReadP50Micros { get; init; }
    public double ReadP99Micros { get; init; }
    public List<MemorySample> Samples { get; init; } = new();
}
