using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-shape comparisons for bootstrap complete-pin resolution, snapshot
/// front-door entry reuse, and independent WAL-head capture. Registry lanes model
/// the two-to-one calls proved by production regression tests; head capture uses
/// the production bounded fan-out. Neither is a silo-throughput measurement.
/// <para>
/// Run with <c>--suite snapshotcontrol</c>. Yield isolates dispatch overhead;
/// Delay1Ms models latency whose absolute duration depends on the OS timer.
/// Unchanged destination checks, options construction and leaf freezes are
/// excluded from both arms. Setup verifies positional result equivalence.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SnapshotControlPlaneBenchmarks
{
    /// <summary>The three changed dispatch sites.</summary>
    public enum Site
    {
        /// <summary>Complete, unaliased bootstrap owner pins: two reads to one.</summary>
        BootstrapOwner,
        /// <summary>Snapshot front-door sizing and routing: two reads to one.</summary>
        SnapshotStart,
        /// <summary>Sixteen independent WAL heads: serial to bounded overlap.</summary>
        PartitionHeads,
    }

    /// <summary>The simulated grain round-trip behaviour.</summary>
    public enum HopModel
    {
        /// <summary>One asynchronous dispatch without timer latency.</summary>
        Yield,
        /// <summary>A nominal millisecond, rounded by the platform timer.</summary>
        Delay1Ms,
    }

    /// <summary>The changed site to measure.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The simulated grain hop.</summary>
    [ParamsAllValues]
    public HopModel Hop { get; set; }

    /// <summary>Reject non-equivalent arms before measuring.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var baseline = BeforeAsync().GetAwaiter().GetResult();
        var candidate = AfterAsync().GetAwaiter().GetResult();
        if (!baseline.SequenceEqual(candidate))
            throw new InvalidOperationException("Dispatch arms returned different positional results.");
    }

    /// <summary>The previous serial dispatch shape.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long[]> BeforeAsync()
    {
        if (Path != Site.PartitionHeads)
        {
            _ = await ReadAsync(0);
            return [await ReadAsync(0)];
        }

        var heads = new long[16];
        for (var partition = 0; partition < heads.Length; partition++)
            heads[partition] = await ReadAsync(partition);
        return heads;
    }

    /// <summary>The entry-reuse or bounded dispatch shape.</summary>
    [Benchmark]
    public async Task<long[]> AfterAsync()
    {
        if (Path != Site.PartitionHeads)
            return [await ReadAsync(0)];

        return await BoundedFanOut.RunAsync(16, BoundedFanOut.DefaultWidth, ReadAsync);
    }

    private async Task<long> ReadAsync(int partition)
    {
        if (Hop == HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
        return partition + 1;
    }
}
