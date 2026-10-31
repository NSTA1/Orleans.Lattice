using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Dispatch-only model of retention and participant-read fusion at production
/// callers. Both lanes retain the status read and conditional supplementary read;
/// only transport dispatches disappear. Not real Orleans throughput or message
/// allocations. Run with --suite settlementcalls or --suite settlementfusion.
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
        /// <summary>Activation self-terminalisation, retaining per-txid fault containment.</summary>
        SelfTerminalise,
        /// <summary>One owning registry's post-migration batch of eight transactions.</summary>
        MigrationCleanup,
        /// <summary>Local forwarded prepare: status, then participants only if undecided.</summary>
        ForwardedForgetting,
    }

    /// <summary>The production caller model.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>Retention expiry, or a forgotten local saga. False keeps a live participant row.</summary>
    [Params(false, true)]
    public bool Expired { get; set; }

    /// <summary>The existing simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;
    private int _supplementaryReads;

    /// <summary>Prove equal answers, conditional local work and exact dispatch counts.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = _supplementaryReads = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        var beforeReads = _supplementaryReads;
        _calls = _supplementaryReads = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var expectedReads = Path == Site.ForwardedForgetting ? 1 : Expired ? TransactionCount : 0;
        if (before != after || beforeCalls != 1 + expectedReads || _calls != 1
            || beforeReads != expectedReads || _supplementaryReads != beforeReads)
            throw new InvalidOperationException("Settlement answer, retained local work or dispatch count differs.");
    }

    /// <summary>The previous conditional second RPC.</summary>
    [Benchmark(Baseline = true)]
    public async Task<bool> BeforeAsync()
    {
        await DispatchAsync();
        var result = true;
        for (var i = 0; i < TransactionCount; i++)
        {
            var status = ReadStatus();
            if (NeedsSupplement(status))
            {
                await DispatchAsync();
                status = ReadSupplement();
            }
            result &= Consume(status);
        }
        return result;
    }

    /// <summary>The same conditional reads within one RPC.</summary>
    [Benchmark]
    public async Task<bool> AfterAsync()
    {
        await DispatchAsync();
        var result = true;
        for (var i = 0; i < TransactionCount; i++)
        {
            var status = ReadStatus();
            if (NeedsSupplement(status))
                status = ReadSupplement();
            result &= Consume(status);
        }
        return result;
    }

    private int TransactionCount => Path == Site.MigrationCleanup ? 8 : 1;

    private bool NeedsSupplement(TxStatus status) => Path == Site.ForwardedForgetting
        ? status is not (TxStatus.Committed or TxStatus.Aborted)
        : status == TxStatus.Indeterminate;

    private TxStatus ReadStatus() => Path == Site.ForwardedForgetting ? TxStatus.InFlight
        : Expired ? TxStatus.Indeterminate : TxStatus.Committed;

    private TxStatus ReadSupplement()
    {
        _supplementaryReads++;
        // The local participant row answers absent only after forgetting.
        return Path == Site.ForwardedForgetting && !Expired ? TxStatus.InFlight : TxStatus.Committed;
    }

    private bool Consume(TxStatus status) => Path is Site.ForwardedPrepare or Site.ForwardedForgetting
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
