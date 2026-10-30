using BenchmarkDotNet.Attributes;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Extends the frontier dispatch benchmarks with three held-source RPC elisions.
/// The candidate runs the production CheckAsync; the baseline is its former
/// per-identity probe loop. All identities are held, so neither arm mutates state.
/// Yield and Delay1Ms price dispatch only, not live Orleans throughput or storage.
/// Windows timer rounding affects absolute delays; compare ratios and RPC counts.
/// Run with --suite frontierprobes.
/// </summary>
[MemoryDiagnoser]
public class CausalFrontierProbeBenchmarks
{
    /// <summary>The three independently batched source probes.</summary>
    public enum Site
    {
        /// <summary>Persisted causal-buffer holds.</summary>
        CausalBuffer,
        /// <summary>Durably parked dead letters.</summary>
        DeadLetters,
        /// <summary>Remembered applied identities missing from an export.</summary>
        ExportApplied,
    }

    /// <summary>The authoritative source.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>Small and larger dependency vectors.</summary>
    [Params(4, 40)]
    public int Dependencies { get; set; }

    /// <summary>Scheduling-only and simulated remote-hop models.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private const string Origin = "bench-origin";
    private const string Tree = "bench-tree";
    private ICausalApplyBufferGrain _buffer = null!;
    private IReplicationDeadLetterGrain _deadLetters = null!;
    private IReplicationHighWaterMarkGrain _hwm = null!;
    private ReplicationOriginFrontierGrain _frontier = null!;
    private HybridLogicalClock[] _required = null!;
    private int _calls;

    /// <summary>Assert equal verdicts and exact production call budgets before timing.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _required = Enumerable.Range(1, Dependencies).Select(i => new HybridLogicalClock { WallClockTicks = i }).ToArray();
        var factory = Substitute.For<IGrainFactory>();
        _buffer = Substitute.For<ICausalApplyBufferGrain>();
        _deadLetters = Substitute.For<IReplicationDeadLetterGrain>();
        _hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        factory.GetGrain<ICausalApplyBufferGrain>(Tree, Arg.Any<string?>()).Returns(_buffer);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree, Arg.Any<string?>()).Returns(_deadLetters);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree, Arg.Any<string?>()).Returns(_hwm);
        _buffer.IsHoldingManyAsync(Origin, Arg.Any<IReadOnlyList<HybridLogicalClock>>())
            .Returns(call => ManyAsync(((IReadOnlyList<HybridLogicalClock>)call[1]).Count, true));
        _deadLetters.IsHoldingManyAsync(Origin, Arg.Any<IReadOnlyList<HybridLogicalClock>>(), Arg.Any<CancellationToken>())
            .Returns(call => ManyAsync(((IReadOnlyList<HybridLogicalClock>)call[1]).Count, true));
        _hwm.HasAppliedManyAsync(Origin, Arg.Any<IReadOnlyList<HybridLogicalClock>>())
            .Returns(call => ManyAsync(((IReadOnlyList<HybridLogicalClock>)call[1]).Count, false));
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("replication-origin-frontier", Origin));
        var state = new FakePersistentState<ReplicationOriginFrontierState>();
        state.State.AggregateLowWatermark = new HybridLogicalClock { WallClockTicks = 1000 };
        state.State.HeldBySource[Path switch
        {
            Site.CausalBuffer => ReplicationOriginFrontierGrain.BufferSource(Tree),
            Site.DeadLetters => ReplicationOriginFrontierGrain.DeadLetterSource(Tree),
            Site.ExportApplied => ReplicationOriginFrontierGrain.ExportSource(Tree),
            _ => throw new InvalidOperationException("Unknown source."),
        }] = _required.ToHashSet();
        _frontier = new ReplicationOriginFrontierGrain(context, factory, state);
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != Dependencies || after != before || beforeCalls != Dependencies
            || _calls != (Dependencies + ReplicationOriginFrontierGrain.HeldProbeBatchSize - 1)
                / ReplicationOriginFrontierGrain.HeldProbeBatchSize)
        {
            throw new InvalidOperationException("Causal-frontier verdicts or RPC budgets differ.");
        }
    }

    /// <summary>
    /// The former per-identity authoritative confirmation: one source call per
    /// identity. The single-identity probe methods were removed once production
    /// stopped calling them, so each call is a batch of one.
    /// </summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        _calls = 0;
        var heldCount = 0;
        foreach (var timestamp in _required)
        {
            var held = Path switch
            {
                Site.CausalBuffer => (await _buffer.IsHoldingManyAsync(Origin, [timestamp]))[0],
                Site.DeadLetters => (await _deadLetters.IsHoldingManyAsync(Origin, [timestamp], CancellationToken.None))[0],
                Site.ExportApplied => !(await _hwm.HasAppliedManyAsync(Origin, [timestamp]))[0],
                _ => throw new InvalidOperationException("Unknown source."),
            };
            if (held) heldCount++;
        }

        return heldCount;
    }

    /// <summary>The production bounded source-major confirmation path.</summary>
    [Benchmark]
    public async Task<int> AfterAsync()
    {
        _calls = 0;
        var verdicts = await _frontier.CheckAsync(_required, CancellationToken.None);
        return verdicts.Count(verdict => verdict == CausalDependencyVerdict.Unmet);
    }

    private async Task<bool[]> ManyAsync(int count, bool held)
    {
        await DispatchAsync();
        var result = new bool[count];
        if (held) Array.Fill(result, true);
        return result;
    }

    private async Task DispatchAsync()
    {
        _calls++;
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
