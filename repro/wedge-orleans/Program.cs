// Repro for the residual WAL wedge investigation (fix/wedge, 2026-06-03).
//
// The repro is incrementally extended: each new scenario adds one condition
// the real Lattice wedge has that the previous scenarios did not, gated
// behind a console-app argument so prior scenarios remain runnable. The
// minimal `baseline` scenario does NOT reproduce the wedge - any future
// scenario that DOES reproduce names the condition that was missing.
//
// See repro/wedge-orleans/wedge-plan.md for the full bisect plan and
// candidate ranking. See repro/wedge-orleans/README.md for usage.
//
// Build : dotnet build repro/wedge-orleans/Orleans.Lattice.Repro.Wedge.csproj -c Release
// Run   : dotnet run --project repro/wedge-orleans -c Release -- [--scenario <list>] [--wall-clock-cap <seconds>] [--load-count <N>]
//
//   --scenario   Comma-separated list. Default: baseline. Known scenarios:
//                  baseline  - 4 baseline arms (caller in Main vs caller in
//                              wrapper grain) x (WaitAsync(TimeSpan) vs
//                              linked CTS + WaitAsync(token)). Minimal
//                              wedge-shape; does NOT reproduce.
//                  load      - silo-wide load: N concurrent wrapper-grain
//                              dispatches against N distinct blocker keys,
//                              each using the WaitAsync(TimeSpan)-inside-a-
//                              grain shape (closest match to the real wedge
//                              caller pattern). Tests whether having many
//                              simultaneously-parked turns is sufficient.
//                  singleton - adds the DI-singleton helper hop: wrapper
//                              grain calls a singleton dispatcher (modelled
//                              on Lattice's WalCommitLogWriter shape: shared
//                              instance across all grains, .ConfigureAwait(false)
//                              on its internal await, WaitAsync(TimeSpan)
//                              deadline applied INSIDE the singleton against
//                              the blocker grain's parked Task). Tests
//                              whether the singleton-helper hop is what
//                              prevents the deadline from firing.
//   --wall-clock-cap   Per-arm wall-clock cap in seconds. Default 30.
//   --load-count       Concurrent dispatches in the `load` / `singleton`
//                      scenarios. Default 32 (rough match to the real
//                      cohort's parked-frame counts; cheap to scale up).

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using System.Diagnostics;

namespace Orleans.Lattice.Repro.Wedge;

public interface IBlockingGrain : IGrainWithIntegerKey
{
    // Deliberately ignores the cancellation token. Matches the wedge shape
    // where the grain body cannot be cancelled by anything the caller does.
    Task<int> BlockForeverAsync(CancellationToken cancellationToken);
}

public sealed class BlockingGrain : Grain, IBlockingGrain
{
    public async Task<int> BlockForeverAsync(CancellationToken _)
    {
        // CancellationToken.None - ignore the caller's token completely.
        await Task.Delay(Timeout.Infinite, CancellationToken.None).ConfigureAwait(false);
        return 42; // never reached
    }
}

public interface IWrapperGrain : IGrainWithIntegerKey
{
    // Runs the WaitAsync(TimeSpan) test from INSIDE a grain context against
    // a specific blocker key, so the `load` scenario can fan many wrapper
    // grains against many distinct blocker activations.
    Task<long> TryDispatchWithWaitAsyncTimeoutAsync(long blockerKey, TimeSpan timeout, TimeSpan wallClockCap);

    // Runs the linked-CTS Option-B pattern from INSIDE a grain context.
    Task<long> TryDispatchWithLinkedCtsAsync(long blockerKey, TimeSpan timeout, TimeSpan wallClockCap);

    // Routes through a DI-singleton helper that itself awaits the blocker
    // grain's parked Task with a WaitAsync(TimeSpan) bound. Mirrors the
    // Lattice grain -> WalCommitLogWriter -> WalShardGrain shape, including
    // the singleton's internal .ConfigureAwait(false). Tests whether the
    // singleton-hop is the missing condition that suppresses cancellation
    // on the real wedge path.
    Task<long> TryDispatchViaSingletonAsync(long blockerKey, TimeSpan timeout, TimeSpan wallClockCap);
}

public sealed class WrapperGrain(IGrainFactory factory, IBlockingDispatcher dispatcher) : Grain, IWrapperGrain
{
    public async Task<long> TryDispatchWithWaitAsyncTimeoutAsync(long blockerKey, TimeSpan timeout, TimeSpan wallClockCap)
    {
        var blocker = factory.GetGrain<IBlockingGrain>(blockerKey);
        var sw = Stopwatch.StartNew();
        var capTask = Task.Delay(wallClockCap);
        var grainCall = blocker.BlockForeverAsync(CancellationToken.None);
        try
        {
            // ConfigureAwait(true) deliberately: simulate the BPlusLeafGrain
            // caller shape on the real wedge path (default ConfigureAwait).
            var winner = await Task.WhenAny(grainCall.WaitAsync(timeout), capTask).ConfigureAwait(true);
            if (winner == capTask)
            {
                return -1L; // wall-clock cap hit; WaitAsync never fired.
            }
            await winner.ConfigureAwait(true); // surface exception if any
            return -2L; // unexpected success
        }
        catch (TimeoutException)
        {
            return sw.ElapsedMilliseconds; // GOOD - WaitAsync fired
        }
    }

    public async Task<long> TryDispatchWithLinkedCtsAsync(long blockerKey, TimeSpan timeout, TimeSpan wallClockCap)
    {
        var blocker = factory.GetGrain<IBlockingGrain>(blockerKey);
        var sw = Stopwatch.StartNew();
        var capTask = Task.Delay(wallClockCap);
        using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        deadlineCts.CancelAfter(timeout);
        var grainCall = blocker.BlockForeverAsync(deadlineCts.Token);
        try
        {
            var winner = await Task.WhenAny(grainCall.WaitAsync(deadlineCts.Token), capTask).ConfigureAwait(true);
            if (winner == capTask)
            {
                return -1L;
            }
            await winner.ConfigureAwait(true);
            return -2L;
        }
        catch (OperationCanceledException) when (deadlineCts.IsCancellationRequested)
        {
            return sw.ElapsedMilliseconds; // GOOD - linked CTS fired
        }
    }

    public async Task<long> TryDispatchViaSingletonAsync(long blockerKey, TimeSpan timeout, TimeSpan wallClockCap)
    {
        var sw = Stopwatch.StartNew();
        var capTask = Task.Delay(wallClockCap);
        // ConfigureAwait(true): default, captures the wrapper grain's
        // context as the resume target for the singleton's returned Task.
        // Matches BPlusLeafGrain.cs:1027 which awaits walWriter.AppendManyAsync(...)
        // with default ConfigureAwait.
        var singletonCall = dispatcher.DispatchAsync(blockerKey, timeout);
        var winner = await Task.WhenAny(singletonCall, capTask).ConfigureAwait(true);
        if (winner == capTask)
        {
            return -1L;
        }
        try
        {
            await winner.ConfigureAwait(true); // surface exception
            return -2L;
        }
        catch (TimeoutException)
        {
            return sw.ElapsedMilliseconds; // GOOD - singleton's WaitAsync fired and propagated through the wrapper grain context
        }
    }
}

/// <summary>
/// DI-singleton helper. Models the Lattice grain -&gt;
/// <c>WalCommitLogWriter</c> -&gt; <c>WalShardGrain</c> shape: shared
/// across all grains, takes <see cref="IGrainFactory"/> by DI, uses
/// <c>.ConfigureAwait(false)</c> on its internal await, and applies a
/// <c>WaitAsync(TimeSpan)</c> bound on the inner grain RPC. If the
/// singleton's bound fails to propagate a <see cref="TimeoutException"/>
/// to the caller, this scenario reproduces the real-wedge failure mode.
/// </summary>
public interface IBlockingDispatcher
{
    Task DispatchAsync(long blockerKey, TimeSpan timeout);
}

public sealed class BlockingDispatcher(IGrainFactory factory) : IBlockingDispatcher
{
    public async Task DispatchAsync(long blockerKey, TimeSpan timeout)
    {
        var blocker = factory.GetGrain<IBlockingGrain>(blockerKey);
        // .ConfigureAwait(false) mirrors WalCommitLogWriter's pattern -
        // the singleton's catch / rethrow runs on the threadpool free of
        // any caller-captured grain context.
        try
        {
            await blocker.BlockForeverAsync(CancellationToken.None).WaitAsync(timeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"singleton dispatch to blocker {blockerKey} exceeded {timeout}");
        }
    }
}

internal sealed record ReproConfig(
    IReadOnlyList<string> Scenarios,
    TimeSpan TimeoutBudget,
    TimeSpan WallClockCap,
    int LoadCount);

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var config = ParseArgs(args);

        Console.WriteLine($"[repro] starting in-process Orleans silo (single host, LocalhostClustering, in-memory storage)");
        Console.WriteLine($"[repro] scenarios               : {string.Join(",", config.Scenarios)}");
        Console.WriteLine($"[repro] WaitAsync timeout budget: {config.TimeoutBudget.TotalSeconds:0.##}s");
        Console.WriteLine($"[repro] wall-clock cap per arm  : {config.WallClockCap.TotalSeconds:0.##}s");
        Console.WriteLine($"[repro] load-count              : {config.LoadCount}");
        Console.WriteLine();

        using var host = Host.CreateDefaultBuilder()
            .ConfigureLogging(l => l.ClearProviders()) // keep output focused on [repro] lines
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.AddMemoryGrainStorageAsDefault();
                silo.Services.AddSingleton<IBlockingDispatcher, BlockingDispatcher>();
            })
            .Build();

        await host.StartAsync();
        Console.WriteLine("[repro] silo started");
        Console.WriteLine();

        var factory = host.Services.GetRequiredService<IGrainFactory>();
        var pass = true;

        foreach (var scenario in config.Scenarios)
        {
            pass &= scenario switch
            {
                "baseline"  => await RunBaselineAsync(factory, config),
                "load"      => await RunLoadAsync(factory, config),
                "singleton" => await RunSingletonAsync(factory, config),
                _ => HandleUnknown(scenario),
            };
            Console.WriteLine();
        }

        Console.WriteLine($"[repro] ===== OVERALL: {(pass ? "ALL ARMS FIRED THEIR DEADLINES" : "AT LEAST ONE ARM DID NOT FIRE")} =====");
        Console.WriteLine();

        // Stop the silo before exit (some blocked grain turns are intentionally
        // leaked; the host shutdown is best-effort and bounded).
        try
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await host.StopAsync(stopCts.Token);
        }
        catch
        {
            // Best-effort - the blocked grain calls are expected to refuse drain.
        }

        return pass ? 0 : 1;
    }

    // -------- Scenarios --------

    private static async Task<bool> RunBaselineAsync(IGrainFactory factory, ReproConfig config)
    {
        var pass = true;
        const long blockerKey = 0L;

        Console.WriteLine("===== SCENARIO baseline =====");
        Console.WriteLine("===== ARM 1: caller in console Main (no grain context) =====");

        pass &= await RunArmAsync(
            label: "ARM 1.A - WaitAsync(TimeSpan)",
            wallClockCap: config.WallClockCap,
            timeoutBudget: config.TimeoutBudget,
            run: async () =>
            {
                var blocker = factory.GetGrain<IBlockingGrain>(blockerKey);
                var sw = Stopwatch.StartNew();
                try
                {
                    await blocker.BlockForeverAsync(CancellationToken.None).WaitAsync(config.TimeoutBudget);
                    return -2L;
                }
                catch (TimeoutException)
                {
                    return sw.ElapsedMilliseconds;
                }
            });

        pass &= await RunArmAsync(
            label: "ARM 1.B - linked-CTS + WaitAsync(token)",
            wallClockCap: config.WallClockCap,
            timeoutBudget: config.TimeoutBudget,
            run: async () =>
            {
                var blocker = factory.GetGrain<IBlockingGrain>(blockerKey);
                var sw = Stopwatch.StartNew();
                using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
                deadlineCts.CancelAfter(config.TimeoutBudget);
                try
                {
                    await blocker.BlockForeverAsync(deadlineCts.Token).WaitAsync(deadlineCts.Token);
                    return -2L;
                }
                catch (OperationCanceledException) when (deadlineCts.IsCancellationRequested)
                {
                    return sw.ElapsedMilliseconds;
                }
            });

        Console.WriteLine();
        Console.WriteLine("===== ARM 2: caller in a wrapper grain (grain context captured on await) =====");

        var wrapper = factory.GetGrain<IWrapperGrain>(1);
        pass &= await RunWrappedArmAsync(
            label: "ARM 2.A - WaitAsync(TimeSpan) inside a grain",
            wallClockCap: config.WallClockCap,
            timeoutBudget: config.TimeoutBudget,
            run: () => wrapper.TryDispatchWithWaitAsyncTimeoutAsync(blockerKey, config.TimeoutBudget, config.WallClockCap));

        pass &= await RunWrappedArmAsync(
            label: "ARM 2.B - linked-CTS + WaitAsync(token) inside a grain",
            wallClockCap: config.WallClockCap,
            timeoutBudget: config.TimeoutBudget,
            run: () => wrapper.TryDispatchWithLinkedCtsAsync(blockerKey, config.TimeoutBudget, config.WallClockCap));

        return pass;
    }

    /// <summary>
    /// Silo-wide load scenario. Fans <see cref="ReproConfig.LoadCount"/>
    /// concurrent wrapper-grain dispatches against <c>LoadCount</c> distinct
    /// blocker keys (so every dispatch parks its own blocker activation,
    /// matching the real cohort's "many simultaneously-parked turns across
    /// many activations" load shape). Each dispatch uses the
    /// WaitAsync(TimeSpan)-inside-a-grain shape (real-wedge caller match).
    /// Passes only if EVERY concurrent dispatch fires its deadline.
    /// </summary>
    private static async Task<bool> RunLoadAsync(IGrainFactory factory, ReproConfig config)
    {
        Console.WriteLine($"===== SCENARIO load (count={config.LoadCount}) =====");
        Console.WriteLine($"[repro] dispatching {config.LoadCount} concurrent wrapper-grain calls against {config.LoadCount} distinct blocker keys ...");

        // One wrapper grain per dispatch so each grain's own scheduler is
        // saturated independently (mirrors the real "many WalShardGrain
        // activations parked simultaneously" shape rather than one grain
        // making many calls serially).
        var startedAt = Stopwatch.StartNew();
        var tasks = new Task<long>[config.LoadCount];
        for (var i = 0; i < config.LoadCount; i++)
        {
            // Wrapper-grain keys 1000+i, blocker keys 1000+i.
            // Offset away from key 0/1 used by the baseline scenario so a
            // mixed `--scenario baseline,load` run does not reuse parked
            // activations across scenarios.
            var wrapper = factory.GetGrain<IWrapperGrain>(1000 + i);
            var blockerKey = (long)(1000 + i);
            tasks[i] = wrapper.TryDispatchWithWaitAsyncTimeoutAsync(blockerKey, config.TimeoutBudget, config.WallClockCap);
        }
        var results = await Task.WhenAll(tasks);
        startedAt.Stop();

        var fired = 0;
        var didNotFire = 0;
        var sentinel = 0;
        long minMs = long.MaxValue;
        long maxMs = long.MinValue;
        long sumMs = 0;
        foreach (var ms in results)
        {
            if (ms == -1L) { didNotFire++; }
            else if (ms < 0) { sentinel++; }
            else
            {
                fired++;
                if (ms < minMs) { minMs = ms; }
                if (ms > maxMs) { maxMs = ms; }
                sumMs += ms;
            }
        }
        var meanMs = fired == 0 ? 0 : sumMs / fired;
        Console.WriteLine($"[repro] load result: fired={fired}/{config.LoadCount} did-not-fire={didNotFire} sentinel={sentinel} fire-time min/mean/max={minMs}/{meanMs}/{maxMs}ms (wall {startedAt.ElapsedMilliseconds}ms)");

        if (didNotFire > 0)
        {
            Console.WriteLine($"[repro] SCENARIO load - {didNotFire} dispatch(es) DID NOT FIRE within wall-clock cap ({config.WallClockCap.TotalSeconds:0.##}s). REPRO of the wedge.");
            return false;
        }
        Console.WriteLine($"[repro] SCENARIO load - all {fired} dispatches fired their deadlines. OK.");
        return sentinel == 0;
    }

    /// <summary>
    /// Singleton-hop scenario. Fans <see cref="ReproConfig.LoadCount"/>
    /// concurrent wrapper-grain dispatches that each route through a shared
    /// DI singleton helper (<see cref="IBlockingDispatcher"/>) before
    /// reaching the blocker grain. Mirrors the real Lattice path of grain
    /// -&gt; <c>WalCommitLogWriter</c> -&gt; <c>WalShardGrain</c>: shared
    /// singleton with <c>.ConfigureAwait(false)</c> internally and a
    /// <c>WaitAsync(TimeSpan)</c> deadline applied inside the singleton.
    /// Passes only if EVERY dispatch's singleton-applied deadline
    /// propagates a <see cref="TimeoutException"/> back to the wrapper
    /// grain (which awaits with default ConfigureAwait, capturing its own
    /// grain context as the resume target - the exact shape that fails in
    /// the real WAL wedge).
    /// </summary>
    private static async Task<bool> RunSingletonAsync(IGrainFactory factory, ReproConfig config)
    {
        Console.WriteLine($"===== SCENARIO singleton (count={config.LoadCount}) =====");
        Console.WriteLine($"[repro] dispatching {config.LoadCount} concurrent wrapper-grain calls through a shared singleton helper against {config.LoadCount} distinct blocker keys ...");

        var startedAt = Stopwatch.StartNew();
        var tasks = new Task<long>[config.LoadCount];
        for (var i = 0; i < config.LoadCount; i++)
        {
            // Wrapper-grain keys 2000+i, blocker keys 2000+i (offset away
            // from baseline=0/1 and load=1000+i so a combined
            // `--scenario baseline,load,singleton` run does not reuse
            // parked activations across scenarios).
            var wrapper = factory.GetGrain<IWrapperGrain>(2000 + i);
            var blockerKey = (long)(2000 + i);
            tasks[i] = wrapper.TryDispatchViaSingletonAsync(blockerKey, config.TimeoutBudget, config.WallClockCap);
        }
        var results = await Task.WhenAll(tasks);
        startedAt.Stop();

        var fired = 0;
        var didNotFire = 0;
        var sentinel = 0;
        long minMs = long.MaxValue;
        long maxMs = long.MinValue;
        long sumMs = 0;
        foreach (var ms in results)
        {
            if (ms == -1L) { didNotFire++; }
            else if (ms < 0) { sentinel++; }
            else
            {
                fired++;
                if (ms < minMs) { minMs = ms; }
                if (ms > maxMs) { maxMs = ms; }
                sumMs += ms;
            }
        }
        var meanMs = fired == 0 ? 0 : sumMs / fired;
        Console.WriteLine($"[repro] singleton result: fired={fired}/{config.LoadCount} did-not-fire={didNotFire} sentinel={sentinel} fire-time min/mean/max={minMs}/{meanMs}/{maxMs}ms (wall {startedAt.ElapsedMilliseconds}ms)");

        if (didNotFire > 0)
        {
            Console.WriteLine($"[repro] SCENARIO singleton - {didNotFire} dispatch(es) DID NOT FIRE within wall-clock cap ({config.WallClockCap.TotalSeconds:0.##}s). REPRO of the wedge.");
            return false;
        }
        Console.WriteLine($"[repro] SCENARIO singleton - all {fired} dispatches fired their deadlines. OK.");
        return sentinel == 0;
    }

    private static bool HandleUnknown(string scenario)
    {
        Console.Error.WriteLine($"[repro] unknown scenario: '{scenario}'. Valid: baseline, load, singleton.");
        return false;
    }

    // -------- Reusable arm runners --------

    private static async Task<bool> RunArmAsync(string label, TimeSpan wallClockCap, TimeSpan timeoutBudget, Func<Task<long>> run)
    {
        Console.WriteLine($"[repro] {label} - dispatching ...");
        var capTask = Task.Delay(wallClockCap);
        var runTask = run();
        var winner = await Task.WhenAny(runTask, capTask);
        if (winner == capTask)
        {
            Console.WriteLine($"[repro] {label} - DID NOT FIRE within wall-clock cap ({wallClockCap.TotalSeconds:0.##}s). REPRO of the wedge.");
            return false;
        }
        var elapsedMs = await runTask;
        if (elapsedMs <= 0)
        {
            Console.WriteLine($"[repro] {label} - sentinel return ({elapsedMs}); unexpected.");
            return false;
        }
        Console.WriteLine($"[repro] {label} - fired in {elapsedMs}ms (target {timeoutBudget.TotalMilliseconds}ms). OK.");
        return true;
    }

    private static async Task<bool> RunWrappedArmAsync(string label, TimeSpan wallClockCap, TimeSpan timeoutBudget, Func<Task<long>> run)
    {
        Console.WriteLine($"[repro] {label} - dispatching through wrapper grain ...");
        var elapsedMs = await run();
        if (elapsedMs == -1L)
        {
            Console.WriteLine($"[repro] {label} - DID NOT FIRE within wall-clock cap ({wallClockCap.TotalSeconds:0.##}s). REPRO of the wedge.");
            return false;
        }
        if (elapsedMs < 0)
        {
            Console.WriteLine($"[repro] {label} - sentinel return ({elapsedMs}); unexpected.");
            return false;
        }
        Console.WriteLine($"[repro] {label} - fired in {elapsedMs}ms (target {timeoutBudget.TotalMilliseconds}ms). OK.");
        return true;
    }

    // -------- Args --------

    private static ReproConfig ParseArgs(string[] args)
    {
        var scenarios = new List<string> { "baseline" };
        var timeoutBudget = TimeSpan.FromSeconds(2);
        var wallClockCap = TimeSpan.FromSeconds(30);
        var loadCount = 32;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--scenario":
                    if (i + 1 >= args.Length) { throw new ArgumentException("--scenario requires a comma-separated value."); }
                    scenarios = args[++i].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
                    break;
                case "--timeout-budget":
                    if (i + 1 >= args.Length) { throw new ArgumentException("--timeout-budget requires a value in seconds."); }
                    timeoutBudget = TimeSpan.FromSeconds(double.Parse(args[++i], System.Globalization.CultureInfo.InvariantCulture));
                    break;
                case "--wall-clock-cap":
                    if (i + 1 >= args.Length) { throw new ArgumentException("--wall-clock-cap requires a value in seconds."); }
                    wallClockCap = TimeSpan.FromSeconds(double.Parse(args[++i], System.Globalization.CultureInfo.InvariantCulture));
                    break;
                case "--load-count":
                    if (i + 1 >= args.Length) { throw new ArgumentException("--load-count requires an integer."); }
                    loadCount = int.Parse(args[++i], System.Globalization.CultureInfo.InvariantCulture);
                    break;
                default:
                    throw new ArgumentException($"unknown argument: '{args[i]}'");
            }
        }

        return new ReproConfig(scenarios, timeoutBudget, wallClockCap, loadCount);
    }
}
