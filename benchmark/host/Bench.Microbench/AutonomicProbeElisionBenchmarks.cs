using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates grain-call elisions on control-plane and autonomic paths.
/// Each simulated grain call completes per the hop model, and every lane counts
/// the calls it issues so the shapes can be checked against each other.
/// <list type="bullet">
/// <item><see cref="Site.CreateTree"/> - <c>LatticeTreeAdmin.CreateTreeAsync</c>
/// used to make three serial registry calls (exists, register, read back the
/// entry); it now makes one <c>RegisterAndGetEntryAsync</c> call.</item>
/// <item><see cref="Site.MaintenanceProbe"/> - the hot-shard monitor and the
/// healing orchestrator used to ask the four <c>ILattice</c> status verbs,
/// each of which forwarded to its coordinator (eight serial calls when the tree
/// is idle); they now read the four coordinators directly (four serial calls)
/// through <c>TreeMaintenanceProbe</c>.</item>
/// <item><see cref="Site.ShardSampling"/> - a sampling pass used to issue three
/// status calls per shard (hotness, pending bulk, splitting) as three
/// concurrent fan-outs; it now issues one <c>GetSamplingStatusAsync</c> per
/// shard. Latency was already concurrent, so the evidence here is the message
/// count and the <see cref="HopModel.Yield"/> dispatch overhead.</item>
/// <item><see cref="Site.ReshardStandOff"/> and <see cref="Site.ConsolidationFinalise"/>
/// - shrink planning and donor retirement each probe snapshot then merge
/// directly, instead of forwarding through two ILattice status calls.</item>
/// <item><see cref="Site.OptionsSeeding"/> - resolving an incomplete existing
/// row uses the registration result instead of reading the entry back.</item>
/// </list>
/// <para>
/// <b>Read the hop model.</b> <see cref="HopModel.Yield"/> prices only the
/// dispatch overhead of each awaited call. <see cref="HopModel.Delay1Ms"/>
/// models a remote round trip with <c>Task.Delay(1)</c>, which the OS timer
/// rounds up (about 15.6 ms on Windows, about 1 ms on Linux), so its absolute
/// times are platform-dependent and its <b>ratio</b> is the evidence.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=autonomicprobes</c> (or
/// <c>--suite autonomicprobes</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency. <c>--suite maintenanceelisions</c> selects only the
/// reshard, consolidation and options-seeding lanes.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AutonomicProbeElisionBenchmarks
{
    private const int ShardCount = 8;

    private int _calls;

    /// <summary>Which call site is measured; see the class summary.</summary>
    [Params(Site.CreateTree, Site.MaintenanceProbe, Site.ShardSampling,
        Site.ReshardStandOff, Site.ConsolidationFinalise, Site.OptionsSeeding)]
    public Site Path { get; set; }

    /// <summary>How each simulated grain call completes; see the class summary.</summary>
    [Params(HopModel.Yield, HopModel.Delay1Ms)]
    public HopModel Hop { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _calls = 0;
        var before = Before_Async().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = After_Async().GetAwaiter().GetResult();
        var afterCalls = _calls;
        if (before != after)
        {
            throw new InvalidOperationException($"{Path} lanes disagree: before={before}, after={after}.");
        }

        var (expectedBefore, expectedAfter) = Path switch
        {
            Site.CreateTree => (3, 1),
            Site.MaintenanceProbe => (8, 4),
            Site.ShardSampling => (3 * ShardCount, ShardCount),
            Site.OptionsSeeding => (3, 2),
            _ => (4, 2),
        };
        if (beforeCalls != expectedBefore || afterCalls != expectedAfter)
        {
            throw new InvalidOperationException(
                $"{Path} call counts drifted: before={beforeCalls} (expected {expectedBefore}), after={afterCalls} (expected {expectedAfter}).");
        }

        Console.WriteLine($"[microbench] {Path}: grain calls before={beforeCalls}, after={afterCalls}");
    }

    [Benchmark(Baseline = true, Description = "before")]
    public Task<int> Before_Async() => Path switch
    {
        Site.CreateTree => CreateTreeBeforeAsync(),
        Site.MaintenanceProbe => MaintenanceBeforeAsync(),
        Site.ShardSampling => SamplingBeforeAsync(),
        Site.OptionsSeeding => OptionsSeedingBeforeAsync(),
        _ => SnapshotMergeBeforeAsync(),
    };

    [Benchmark(Description = "after")]
    public Task<int> After_Async() => Path switch
    {
        Site.CreateTree => CreateTreeAfterAsync(),
        Site.MaintenanceProbe => MaintenanceAfterAsync(),
        Site.ShardSampling => SamplingAfterAsync(),
        Site.OptionsSeeding => OptionsSeedingAfterAsync(),
        _ => SnapshotMergeAfterAsync(),
    };

    private async Task<int> SnapshotMergeBeforeAsync()
        => await ForwardedAsync() + await ForwardedAsync();

    private async Task<int> SnapshotMergeAfterAsync()
        => await CallAsync(0) + await CallAsync(0);

    // An existing incomplete row needs no purge probe; registration is a
    // no-op that returns the effective row without repairing its missing pins.
    private async Task<int> OptionsSeedingBeforeAsync()
    {
        await CallAsync(0);
        await CallAsync(0);
        return await CallAsync(7);
    }

    private async Task<int> OptionsSeedingAfterAsync()
    {
        await CallAsync(0);
        return await CallAsync(7);
    }

    private async Task<int> CreateTreeBeforeAsync()
    {
        var exists = await CallAsync(0);
        var created = await CallAsync(1);
        var entry = await CallAsync(7);
        return exists + created + entry;
    }

    private async Task<int> CreateTreeAfterAsync() => 1 + await CallAsync(7);

    private async Task<int> MaintenanceBeforeAsync()
    {
        // Each ILattice status verb is one hop to the lattice grain plus one
        // forward to the coordinator.
        var busy = 0;
        for (var verb = 0; verb < 4; verb++)
        {
            busy += await ForwardedAsync();
        }

        return busy;
    }

    private async Task<int> MaintenanceAfterAsync()
    {
        var busy = 0;
        for (var coordinator = 0; coordinator < 4; coordinator++)
        {
            busy += await CallAsync(0);
        }

        return busy;
    }

    private async Task<int> SamplingBeforeAsync()
    {
        var hotness = new Task<int>[ShardCount];
        var pending = new Task<int>[ShardCount];
        var splitting = new Task<int>[ShardCount];
        for (var i = 0; i < ShardCount; i++)
        {
            hotness[i] = CallAsync(i);
            pending[i] = CallAsync(0);
            splitting[i] = CallAsync(1);
        }

        await Task.WhenAll(hotness);
        await Task.WhenAll(pending);
        await Task.WhenAll(splitting);
        var total = 0;
        for (var i = 0; i < ShardCount; i++)
        {
            total += hotness[i].Result + pending[i].Result + splitting[i].Result;
        }

        return total;
    }

    private async Task<int> SamplingAfterAsync()
    {
        var status = new Task<int>[ShardCount];
        for (var i = 0; i < ShardCount; i++)
        {
            status[i] = CallAsync(i + 1);
        }

        await Task.WhenAll(status);
        var total = 0;
        for (var i = 0; i < ShardCount; i++)
        {
            total += status[i].Result;
        }

        return total;
    }

    private async Task<int> ForwardedAsync()
    {
        await HopAsync();
        return await CallAsync(0);
    }

    private async Task<int> CallAsync(int value)
    {
        await HopAsync();
        return value;
    }

    private async Task HopAsync()
    {
        System.Threading.Interlocked.Increment(ref _calls);
        if (Hop == HopModel.Delay1Ms)
        {
            await Task.Delay(1);
        }
        else
        {
            await Task.Yield();
        }
    }

    /// <summary>Which call site is measured.</summary>
    public enum Site
    {
        /// <summary>Tree creation's registry round trips.</summary>
        CreateTree,

        /// <summary>The autonomic maintenance-in-flight probe.</summary>
        MaintenanceProbe,

        /// <summary>The per-shard status fan-out of a sampling pass.</summary>
        ShardSampling,

        /// <summary>Reshard shrink's snapshot/merge stand-off (four calls to two).</summary>
        ReshardStandOff,

        /// <summary>Consolidation donor retirement's stand-off (four calls to two).</summary>
        ConsolidationFinalise,

        /// <summary>Lazy options seeding of an incomplete row (three registry calls to two).</summary>
        OptionsSeeding,
    }

    /// <summary>How a simulated grain call completes.</summary>
    public enum HopModel
    {
        /// <summary>One yield: prices dispatch overhead only.</summary>
        Yield,

        /// <summary>One <c>Task.Delay(1)</c>: prices a remote round trip at the OS timer floor.</summary>
        Delay1Ms,
    }
}
