using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the WAL partition reads <c>LatticeStateObserver</c> pays to seed a
/// change-observation cursor. A fresh subscription reads every partition's next
/// sequence; a resumed one reads every partition's next sequence and live entry
/// count to prove its offset is still retained. Those reads used to run one
/// partition after another, so opening or resuming a subscription cost one WAL
/// round trip per partition; the shipped seed issues every partition's reads at
/// once and awaits them together.
/// <list type="bullet">
/// <item><see cref="Seed.Fresh"/> - one next-sequence read per partition.</item>
/// <item><see cref="Seed.Resume"/> - a next-sequence then a live-count read per
/// partition (kept in order within a partition).</item>
/// </list>
/// <para>
/// <b>Read the hop model.</b> <see cref="HopModel.Yield"/> prices only the
/// dispatch overhead of each awaited call. <see cref="HopModel.Delay1Ms"/>
/// models a remote round trip with <c>Task.Delay(1)</c>, which the OS timer
/// rounds up (about 15.6 ms on Windows, about 1 ms on Linux), so its absolute
/// times are platform-dependent and its <b>ratio</b> is the evidence: it tracks
/// the serial WAL round trips removed from the seed.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=cursorseedfanout</c> (or
/// <c>--suite cursorseedfanout</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class StateCursorSeedFanOutBenchmarks
{
    private WalPartition[] _partitions = null!;

    /// <summary>Which cursor seed is measured; see the class summary.</summary>
    [Params(Seed.Fresh, Seed.Resume)]
    public Seed Mode { get; set; }

    /// <summary>The tree's WAL partition count.</summary>
    [Params(4, 16)]
    public int Partitions { get; set; }

    /// <summary>How each simulated WAL grain call completes; see the class summary.</summary>
    [Params(HopModel.Yield, HopModel.Delay1Ms)]
    public HopModel Hop { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _partitions = new WalPartition[Partitions];
        for (var p = 0; p < Partitions; p++)
        {
            _partitions[p] = new WalPartition(Hop, NextSequence: 100 + p, LiveCount: 50);
        }

        var before = Serial_Partition_Reads_Async().GetAwaiter().GetResult();
        var after = Concurrent_Partition_Reads_Async().GetAwaiter().GetResult();
        if (before != after)
        {
            throw new InvalidOperationException($"Cursor seed lanes disagree: before={before}, after={after}.");
        }
    }

    [Benchmark(Baseline = true, Description = "partitions read one after another")]
    public async Task<long> Serial_Partition_Reads_Async()
    {
        var total = 0L;
        for (var p = 0; p < _partitions.Length; p++)
        {
            total += Mode == Seed.Fresh
                ? await _partitions[p].GetNextSequenceAsync()
                : await ResumeOneAsync(_partitions[p]);
        }

        return total;
    }

    [Benchmark(Description = "every partition read at once")]
    public async Task<long> Concurrent_Partition_Reads_Async()
    {
        var reads = new Task<long>[_partitions.Length];
        for (var p = 0; p < _partitions.Length; p++)
        {
            reads[p] = Mode == Seed.Fresh
                ? _partitions[p].GetNextSequenceAsync()
                : ResumeOneAsync(_partitions[p]);
        }

        var results = await Task.WhenAll(reads);
        var total = 0L;
        foreach (var result in results)
        {
            total += result;
        }

        return total;
    }

    private static async Task<long> ResumeOneAsync(WalPartition partition)
    {
        var next = await partition.GetNextSequenceAsync();
        var live = await partition.GetLiveEntryCountAsync();
        return next - live;
    }

    /// <summary>Which cursor seed is measured.</summary>
    public enum Seed
    {
        /// <summary>A fresh subscription reading every partition's tail.</summary>
        Fresh,

        /// <summary>A resumed subscription checking every partition's retention.</summary>
        Resume,
    }

    /// <summary>How a simulated grain call completes.</summary>
    public enum HopModel
    {
        /// <summary>One yield: prices dispatch overhead only.</summary>
        Yield,

        /// <summary>One <c>Task.Delay(1)</c>: prices a remote round trip at the OS timer floor.</summary>
        Delay1Ms,
    }

    /// <summary>Stands in for one WAL shard grain; every call completes asynchronously per the hop model.</summary>
    private sealed record WalPartition(HopModel Hop, long NextSequence, long LiveCount)
    {
        public async Task<long> GetNextSequenceAsync()
        {
            await HopAsync();
            return NextSequence;
        }

        public async Task<long> GetLiveEntryCountAsync()
        {
            await HopAsync();
            return LiveCount;
        }

        private async Task HopAsync()
        {
            if (Hop == HopModel.Delay1Ms)
            {
                await Task.Delay(1);
            }
            else
            {
                await Task.Yield();
            }
        }
    }
}
