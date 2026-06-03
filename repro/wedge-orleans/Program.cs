// Minimal repro for the residual WAL wedge investigation (fix/wedge, 2026-06-03).
//
// Hypothesis the wedge cohort observed: Task.WaitAsync(TimeSpan) on a Task
// returned by an Orleans-generated grain RPC proxy (vanilla Task<T> via
// .AsTask() on InvokeAsync<T>) does NOT fire its timeout when the callee
// grain method blocks forever, even though the threadpool TimerThread is
// alive and threadpool workers are idle.
//
// Two arms:
//   ARM 1 - caller in console Main, no grain context: if WaitAsync fires
//           here, the wedge is something specifically about the caller
//           grain context (i.e. our real wedge requires the caller to be
//           on a wedged grain scheduler).
//   ARM 2 - caller in a wrapper grain (caller IS on a grain context):
//           more closely matches the real wedge shape. If WaitAsync does
//           NOT fire here, we have an unambiguous report for dotnet/orleans.
//
// Each arm tries BOTH WaitAsync(TimeSpan) AND the linked-CTS pattern that
// the wedge investigation's Option B used.
//
// Build: dotnet build .scratch/probe/Probe.csproj
// Run:   dotnet run --project .scratch/probe/Probe.csproj

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
    // Runs the WaitAsync(TimeSpan) test from INSIDE a grain context.
    Task<long> TryDispatchWithWaitAsyncTimeoutAsync(TimeSpan timeout, TimeSpan wallClockCap);

    // Runs the linked-CTS Option-B pattern from INSIDE a grain context.
    Task<long> TryDispatchWithLinkedCtsAsync(TimeSpan timeout, TimeSpan wallClockCap);
}

public sealed class WrapperGrain(IGrainFactory factory) : Grain, IWrapperGrain
{
    public async Task<long> TryDispatchWithWaitAsyncTimeoutAsync(TimeSpan timeout, TimeSpan wallClockCap)
    {
        var blocker = factory.GetGrain<IBlockingGrain>(0);
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

    public async Task<long> TryDispatchWithLinkedCtsAsync(TimeSpan timeout, TimeSpan wallClockCap)
    {
        var blocker = factory.GetGrain<IBlockingGrain>(0);
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
}

internal static class Program
{
    private static readonly TimeSpan TimeoutBudget = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan WallClockCap = TimeSpan.FromSeconds(30);

    public static async Task<int> Main(string[] args)
    {
        Console.WriteLine($"[repro] starting in-process Orleans silo (single host, LocalhostClustering, in-memory storage)");
        Console.WriteLine($"[repro] WaitAsync timeout budget : {TimeoutBudget.TotalSeconds:0.##}s");
        Console.WriteLine($"[repro] wall-clock cap per arm   : {WallClockCap.TotalSeconds:0.##}s");
        Console.WriteLine();

        using var host = Host.CreateDefaultBuilder(args)
            .ConfigureLogging(l => l.ClearProviders()) // keep output focused on [repro] lines
            .UseOrleans(silo =>
            {
                silo.UseLocalhostClustering();
                silo.AddMemoryGrainStorageAsDefault();
            })
            .Build();

        await host.StartAsync();
        Console.WriteLine("[repro] silo started");
        Console.WriteLine();

        var factory = host.Services.GetRequiredService<IGrainFactory>();
        var pass = true;

        // --- ARM 1: caller is console Main (no grain context) ---
        Console.WriteLine("===== ARM 1: caller in console Main (no grain context) =====");

        pass &= await RunArmAsync(
            label: "ARM 1.A - WaitAsync(TimeSpan)",
            run: async () =>
            {
                var blocker = factory.GetGrain<IBlockingGrain>(0);
                var sw = Stopwatch.StartNew();
                try
                {
                    await blocker.BlockForeverAsync(CancellationToken.None).WaitAsync(TimeoutBudget);
                    return -2L;
                }
                catch (TimeoutException)
                {
                    return sw.ElapsedMilliseconds;
                }
            });

        pass &= await RunArmAsync(
            label: "ARM 1.B - linked-CTS + WaitAsync(token)",
            run: async () =>
            {
                var blocker = factory.GetGrain<IBlockingGrain>(0);
                var sw = Stopwatch.StartNew();
                using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
                deadlineCts.CancelAfter(TimeoutBudget);
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

        // --- ARM 2: caller is itself a grain (grain context captured on await) ---
        Console.WriteLine();
        Console.WriteLine("===== ARM 2: caller in a wrapper grain (grain context captured on await) =====");

        var wrapper = factory.GetGrain<IWrapperGrain>(1);
        // Use a fresh blocker key per ARM-2 sub-test so the previous test's
        // parked turn does not block the next test's first dispatch on the
        // SAME activation (the blocker's grain context is now stuck).
        // Single blocker key 0 is shared by all arms - acceptable here because
        // each arm uses a wall-clock cap; a parked dispatch from a prior arm
        // does not block this arm's WaitAsync timer.
        pass &= await RunWrappedArmAsync(
            label: "ARM 2.A - WaitAsync(TimeSpan) inside a grain",
            run: () => wrapper.TryDispatchWithWaitAsyncTimeoutAsync(TimeoutBudget, WallClockCap));

        pass &= await RunWrappedArmAsync(
            label: "ARM 2.B - linked-CTS + WaitAsync(token) inside a grain",
            run: () => wrapper.TryDispatchWithLinkedCtsAsync(TimeoutBudget, WallClockCap));

        Console.WriteLine();
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

    private static async Task<bool> RunArmAsync(string label, Func<Task<long>> run)
    {
        Console.WriteLine($"[repro] {label} - dispatching ...");
        var capSw = Stopwatch.StartNew();
        var capTask = Task.Delay(WallClockCap);
        var runTask = run();
        var winner = await Task.WhenAny(runTask, capTask);
        if (winner == capTask)
        {
            Console.WriteLine($"[repro] {label} - DID NOT FIRE within wall-clock cap ({WallClockCap.TotalSeconds:0.##}s). REPRO of the wedge.");
            return false;
        }
        var elapsedMs = await runTask;
        if (elapsedMs <= 0)
        {
            Console.WriteLine($"[repro] {label} - sentinel return ({elapsedMs}); unexpected.");
            return false;
        }
        Console.WriteLine($"[repro] {label} - fired in {elapsedMs}ms (target {TimeoutBudget.TotalMilliseconds}ms). OK.");
        return true;
    }

    private static async Task<bool> RunWrappedArmAsync(string label, Func<Task<long>> run)
    {
        Console.WriteLine($"[repro] {label} - dispatching through wrapper grain ...");
        var elapsedMs = await run();
        if (elapsedMs == -1L)
        {
            Console.WriteLine($"[repro] {label} - DID NOT FIRE within wall-clock cap ({WallClockCap.TotalSeconds:0.##}s). REPRO of the wedge.");
            return false;
        }
        if (elapsedMs < 0)
        {
            Console.WriteLine($"[repro] {label} - sentinel return ({elapsedMs}); unexpected.");
            return false;
        }
        Console.WriteLine($"[repro] {label} - fired in {elapsedMs}ms (target {TimeoutBudget.TotalMilliseconds}ms). OK.");
        return true;
    }
}
