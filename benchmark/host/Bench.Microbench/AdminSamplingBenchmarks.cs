using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Models view sample dispatch fusion, runtime-view lookup, read overlap and
/// participant existence checks. Dispatches are simulated, not real Orleans
/// throughput or message allocation evidence. Snapshots exclude serialization,
/// factory bookkeeping and production durability waits. Run --suite adminsampling
/// for scalar paths or --suite catalogsampling for page and participant paths.
/// </summary>
[MemoryDiagnoser]
public class AdminSamplingBenchmarks
{
    /// <summary>The sampling path being modelled.</summary>
    public enum Site
    {
        /// <summary>Lag, active generation and count: three dispatches become two.</summary>
        ViewSample,
        /// <summary>One dictionary lookup replaces a 512-registration array and scan.</summary>
        RuntimeLookup,
        /// <summary>Two independent reads overlap; dispatch count remains two.</summary>
        TreeStats,
        /// <summary>Eight fresh runtime-view scalar lookups become one bounded lookup.</summary>
        PageLookup,
        /// <summary>Eight default-handle samples overlap; sixteen dispatches remain.</summary>
        SampleWave,
        /// <summary>A scalar participant existence read avoids a 512-participant sorted snapshot.</summary>
        ParticipantPresence,
    }

    /// <summary>The caller path.</summary>
    [ParamsAllValues]
    public Site Path { get; set; }

    /// <summary>The simulated transport.</summary>
    [Params(LeafChainWalkElisionBenchmarks.HopModel.Yield, LeafChainWalkElisionBenchmarks.HopModel.Delay1Ms)]
    public LeafChainWalkElisionBenchmarks.HopModel Hop { get; set; }

    private sealed record Registration(string ViewName, bool IsAggregation);

    private readonly Dictionary<string, Registration> _registrations = new(StringComparer.Ordinal);
    private int _calls;
    private readonly HashSet<int> _participants = [];

    /// <summary>Validate equal answers and the exact dispatch budget.</summary>
    [GlobalSetup]
    public void Setup()
    {
        for (var i = 0; i < 512; i++)
        {
            _registrations.Add($"view-{i}", new Registration($"view-{i}", i % 2 == 0));
            _participants.Add(i);
        }
        _calls = 0;
        var before = BeforeAsync().GetAwaiter().GetResult();
        var beforeCalls = _calls;
        _calls = 0;
        var after = AfterAsync().GetAwaiter().GetResult();
        var expectedBefore = Path switch
        {
            Site.PageLookup => 8,
            Site.SampleWave => 16,
            Site.ParticipantPresence or Site.RuntimeLookup => 1,
            Site.ViewSample => 3,
            _ => 2,
        };
        var expectedAfter = Path switch
        {
            Site.PageLookup or Site.ParticipantPresence or Site.RuntimeLookup => 1,
            Site.SampleWave => 16,
            _ => 2,
        };
        if (before != after || beforeCalls != expectedBefore || _calls != expectedAfter)
            throw new InvalidOperationException("Sampling answer or dispatch budget differs.");
    }

    /// <summary>The previous caller.</summary>
    [Benchmark(Baseline = true)]
    public async Task<long> BeforeAsync()
    {
        if (Path == Site.PageLookup)
        {
            long count = 0;
            for (var i = 0; i < 8; i++)
            {
                await DispatchAsync();
                if (_registrations.TryGetValue($"view-{i}", out var registration) && registration.IsAggregation)
                    count++;
            }
            return count;
        }
        if (Path == Site.SampleWave)
        {
            long total = 0;
            for (var i = 0; i < 8; i++)
                total += await SampleAsync();
            return total;
        }
        await DispatchAsync();
        if (Path == Site.ParticipantPresence)
        {
            var snapshot = new int[_participants.Count];
            _participants.CopyTo(snapshot);
            Array.Sort(snapshot);
            return snapshot.Length == 0 ? 0 : 1;
        }
        if (Path == Site.RuntimeLookup)
        {
            var snapshot = _registrations.Values.ToArray();
            foreach (var registration in snapshot)
                if (registration.ViewName == "view-510")
                    return registration.IsAggregation ? 1 : 0;
            return -1;
        }
        await DispatchAsync();
        if (Path == Site.ViewSample)
            await DispatchAsync();
        return 42;
    }

    /// <summary>The fused, scalar or overlapped caller.</summary>
    [Benchmark]
    public async Task<long> AfterAsync()
    {
        if (Path == Site.PageLookup)
        {
            var names = new string[8];
            for (var i = 0; i < names.Length; i++) names[i] = $"view-{i}";
            await DispatchAsync();
            var kinds = new bool?[names.Length];
            for (var i = 0; i < names.Length; i++)
                if (_registrations.TryGetValue(names[i], out var registration))
                    kinds[i] = registration.IsAggregation;
            long count = 0;
            foreach (var kind in kinds)
                if (kind is true) count++;
            return count;
        }
        if (Path == Site.SampleWave)
        {
            var samples = await Orleans.Lattice.BPlusTree.BoundedFanOut.RunAsync(8, 32, async _ => await SampleAsync());
            long total = 0;
            foreach (var sample in samples) total += sample;
            return total;
        }
        if (Path == Site.ParticipantPresence)
        {
            await DispatchAsync();
            return _participants.Count == 0 ? 0 : 1;
        }
        if (Path == Site.TreeStats)
            await Task.WhenAll(DispatchAsync(), DispatchAsync());
        else
        {
            await DispatchAsync();
            if (Path == Site.RuntimeLookup)
                return _registrations.TryGetValue("view-510", out var registration)
                    ? registration.IsAggregation ? 1 : 0
                    : -1;
            await DispatchAsync();
        }
        return 42;
    }

    private async Task<long> SampleAsync()
    {
        await DispatchAsync();
        await DispatchAsync();
        return 42;
    }

    private async Task DispatchAsync()
    {
        Interlocked.Increment(ref _calls);
        if (Hop == LeafChainWalkElisionBenchmarks.HopModel.Yield)
            await Task.Yield();
        else
            await Task.Delay(1);
    }
}
