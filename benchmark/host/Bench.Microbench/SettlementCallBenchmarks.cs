using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-only model of the conditional retention fallback at three production
/// callers. Both lanes retain the status read and local recorded-verdict lookup;
/// only the second transport dispatch disappears. Not real Orleans throughput
/// or message allocations. Run with --suite settlementcalls.
/// </summary>
[MemoryDiagnoser]
public class SettlementCallBenchmarks
{
    /// <summary>The independent production paths sharing a conditional read fusion.</summary>
    public enum Site
    {
        /// <summary>Settlement of a re-shipped replicated prepare.</summary>
        ReplicatedPrepare,
        /// <summary>Pre-check before migrating a prepared bucket.</summary>
        MigrationPrecheck,
        /// <summary>Late forwarded prepare deduplication on a cold leaf.</summary>
        ForwardedPrepare,
    }

    /// <summary>The production caller model.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>False is the unchanged one-call control.</summary>
    [Params(false, true)]
    public bool Expired { get; set; }

    /// <summary>The existing simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;
    private int _recordedReads;

    /// <summary>Prove equal answers, conditional local work and exact dispatch counts.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = _recordedReads = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        var beforeReads = _recordedReads;
        _calls = _recordedReads = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != (Expired ? 2 : 1) || _calls != 1
            || beforeReads != (Expired ? 1 : 0) || _recordedReads != beforeReads)
            throw new InvalidOperationException("Settlement answer, retained local work or dispatch count differs.");
    }

    /// <summary>The previous conditional second RPC.</summary>
    [Benchmark(Baseline = true)]
    public async Task<bool> BeforeAsync()
    {
        await DispatchAsync();
        var status = ReadStatus();
        if (status == TxStatus.Indeterminate)
        {
            await DispatchAsync();
            status = ReadRecorded();
        }
        return Consume(status);
    }

    /// <summary>The same conditional reads within one RPC.</summary>
    [Benchmark]
    public async Task<bool> AfterAsync()
    {
        await DispatchAsync();
        var status = ReadStatus();
        if (status == TxStatus.Indeterminate)
            status = ReadRecorded();
        return Consume(status);
    }

    private TxStatus ReadStatus() => Expired ? TxStatus.Indeterminate : TxStatus.Committed;

    private TxStatus ReadRecorded()
    {
        _recordedReads++;
        return TxStatus.Committed;
    }

    private bool Consume(TxStatus status) => Path == Site.ForwardedPrepare
        ? status is TxStatus.Committed or TxStatus.Aborted
        : status == TxStatus.Committed;

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
