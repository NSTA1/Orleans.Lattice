using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Extends the WalPollingCallBenchmarks dispatch model for reseed probes and
/// tenant sampling, shadow cleanup, bootstrap registration and confirmation heads.
/// Yield prices scheduling; Delay1Ms simulates a remote hop
/// (subject to the Windows timer floor), not real Orleans throughput.
/// Run with --suite reseedsampling, or --suite coordinatorcleanup for the latter three.
/// </summary>
[MemoryDiagnoser]
public class ReseedSamplingBenchmarks
{
    /// <summary>The optimized production call shapes.</summary>
    public enum Site
    {
        /// <summary>One undecided transaction: recorded status plus participants becomes one probe.</summary>
        PrepareClamp,
        /// <summary>Eight partition heads, captured before any marker publication.</summary>
        RetainedHeads,
        /// <summary>Eight warm usage reports, applied in input order after settling.</summary>
        TenantUsage,
        /// <summary>Restore-shadow existence and provenance from one registry entry.</summary>
        ShadowCleanup,
        /// <summary>Existing unaliased bootstrap tree registration from one entry.</summary>
        BootstrapRegistration,
        /// <summary>Post-source-observation head vector, in partition order.</summary>
        ConfirmationHeads,
    }

    /// <summary>The call site measured.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The existing harness transport model.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private int _calls;

    /// <summary>Check equivalent results and the exact call budgets before measurement.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        if (before != after || beforeCalls != (IsElision ? 2 : 8)
            || _calls != (IsElision ? 1 : 8))
            throw new InvalidOperationException("Reseed/sampling result or dispatch budget differs.");
    }

    /// <summary>The previous serial dispatch shape.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> BeforeAsync()
    {
        if (IsElision)
        {
            await DispatchAsync(0);
            return await DispatchAsync(1);
        }

        var sum = 0;
        for (var i = 0; i < 8; i++) sum += await DispatchAsync(i);
        return sum;
    }

    /// <summary>The combined probe or bounded complete-vector read used by production.</summary>
    [Benchmark]
    public async Task<int> AfterAsync()
    {
        if (IsElision) return await DispatchAsync(1);
        var results = await BoundedFanOut.RunAsync(8, BoundedFanOut.DefaultWidth, DispatchAsync);
        var sum = 0;
        foreach (var result in results) sum += result;
        return sum;
    }

    private async Task<int> DispatchAsync(int slot)
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
        return slot;
    }

    private bool IsElision => Path is Site.PrepareClamp or Site.ShadowCleanup or Site.BootstrapRegistration;
}
