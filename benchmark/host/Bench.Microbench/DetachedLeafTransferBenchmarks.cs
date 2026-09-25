using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Measures detached-leaf split transfer planning, dictionary construction and
/// donor removal, excluding Orleans dispatch and source population. The baseline
/// reproduces the old empty-boundary result; the candidate calls the shipped planner.
/// Run with --suite detachedtransfer. The 64-row control fits one 16 KiB batch.
/// </summary>
[MemoryDiagnoser]
public class DetachedLeafTransferBenchmarks
{
    private const long Budget = 16L * 1024;
    private const int TransfersPerInvoke = 32;
    private LeafEntryCache[] _caches = null!;
    private KeyValuePair<string, LwwValue<byte[]>>[] _rows = null!;
    private string _pivot = null!;

    /// <summary>Donor size, including the left half that does not move.</summary>
    [Params(64, 2048)]
    public int RowCount { get; set; }

    /// <summary>Creates the immutable source rows outside the timed operation.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _rows = Enumerable.Range(0, RowCount)
            .Select(i => new KeyValuePair<string, LwwValue<byte[]>>(
                $"k{i:D6}", LwwValue<byte[]>.Create(new byte[256],
                    new HybridLogicalClock { WallClockTicks = 100L + i })))
            .ToArray();
        _pivot = _rows[RowCount / 2].Key;
    }

    /// <summary>Restores the resident donors outside each measured iteration.</summary>
    [IterationSetup]
    public void ResetDonor()
    {
        _caches = new LeafEntryCache[TransfersPerInvoke];
        for (var i = 0; i < _caches.Length; i++)
        {
            var cache = new LeafEntryCache(new(StringComparer.Ordinal));
            foreach (var (key, row) in _rows)
            {
                cache.StoreRow(key, row);
            }
            _caches[i] = cache;
        }
    }

    /// <summary>The previous no-frame branch transferred the entire right half.</summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = TransfersPerInvoke)]
    public int Unbounded()
    {
        var transferred = 0;
        foreach (var cache in _caches)
        {
            transferred += Transfer(cache, []);
        }
        return transferred;
    }

    /// <summary>The production planner bounds transfers even without a frame.</summary>
    [Benchmark(OperationsPerInvoke = TransfersPerInvoke)]
    public int Bounded()
    {
        var transferred = 0;
        foreach (var cache in _caches)
        {
            transferred += Transfer(cache, cache.GetTransferBatchBoundariesWithoutHydrating(_pivot, Budget));
        }
        return transferred;
    }

    private int Transfer(LeafEntryCache cache, IReadOnlyList<string> boundaries)
    {
        var start = _pivot;
        var transferred = 0;
        for (var i = 0; i <= boundaries.Count; i++)
        {
            var end = i < boundaries.Count ? boundaries[i] : null;
            var batch = new Dictionary<string, LwwValue<byte[]>>();
            foreach (var (key, value) in cache.EnumerateRange(start, end))
            {
                batch[key] = value;
            }
            transferred += batch.Count;
            foreach (var key in batch.Keys)
            {
                cache.Remove(key);
            }
            if (end is null)
            {
                break;
            }
            start = end;
        }
        if (transferred != RowCount / 2)
        {
            throw new InvalidOperationException("The benchmark must transfer exactly the right half.");
        }
        return transferred;
    }
}
