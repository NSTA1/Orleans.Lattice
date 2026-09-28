using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The launch cost of one WAL GC reactivation pass's touches (issue #3761). The
/// baseline is the fan-out the scheduler used before - every touch started at
/// once and joined with <c>Task.WhenAll</c> - and the candidate is
/// <see cref="ShareBoundedTouchRunner"/> at an unnarrowed width and at a share
/// of three. Each touch yields once so it completes asynchronously, as a grain
/// call does, and is never refused. Run with --suite sharetouch.
/// </summary>
[MemoryDiagnoser]
public class ShareBoundedTouchFanOutBenchmarks
{
    /// <summary>Touches in the pass; 32 is <c>MaxReactivationTouchesPerPass</c>.</summary>
    [Params(4, 32)]
    public int Touches { get; set; }

    /// <summary>The pre-#3761 shape: launch everything, then <c>Task.WhenAll</c>.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> WhenAllFanOut()
    {
        var touches = new Task<bool>[Touches];
        await ((Task)(touches[0] = TouchAsync(0))).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        for (var i = 1; i < touches.Length; i++)
        {
            touches[i] = TouchAsync(i);
        }

        var results = await Task.WhenAll(touches).ConfigureAwait(false);
        return results.Length;
    }

    /// <summary>The runner with no share narrowing (share unknown, width = count).</summary>
    [Benchmark]
    public async Task<int> RunnerUnnarrowed()
    {
        var results = new bool[Touches];
        await ShareBoundedTouchRunner.RunAsync(
            Touches, Touches, leadAlone: true, results, static (r, i) => TouchInto(r, i), static _ => true)
            .ConfigureAwait(false);
        return results.Length;
    }

    /// <summary>The runner held to a starvation share of three.</summary>
    [Benchmark]
    public async Task<int> RunnerShareOfThree()
    {
        var results = new bool[Touches];
        await ShareBoundedTouchRunner.RunAsync(
            Touches, 3, leadAlone: true, results, static (r, i) => TouchInto(r, i), static _ => true)
            .ConfigureAwait(false);
        return results.Length;
    }

    private static async Task<bool> TouchAsync(int index)
    {
        await Task.Yield();
        return index < 0;
    }

    private static async Task<bool> TouchInto(bool[] results, int index)
    {
        await Task.Yield();
        results[index] = true;
        return false;
    }
}
