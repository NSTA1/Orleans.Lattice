using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Background service that issues <c>ILattice.GetAsync</c> calls at a configured rate to
/// generate read-side load for the <c>read-heavy-*</c> and <c>read-write-mix-*</c> benchmark
/// scenarios. Discovers the active keyspace by periodically cursor-paging the lattice, then
/// picks a key per tick according to <see cref="LatticeReadDriverOptions.Pattern"/>. Emits
/// reads/misses/errors/duration via <see cref="LatticeReadDriverMetrics"/> so the bench
/// harness''s auto-discovery picks the metrics up under the
/// <c>vehicle_fleet_simulator_read_driver_*</c> prefix.
/// </summary>
public sealed class LatticeReadDriver(
    IGrainFactory grainFactory,
    IOptions<LatticeReadDriverOptions> options,
    LatticeReadDriverMetrics metrics,
    ILogger<LatticeReadDriver> logger) : BackgroundService
{
    private string[] _keyspace = Array.Empty<string>();
    private long _sequentialCursor;

    /// <summary>The driver loop. Exits immediately when <c>ReadDriver:Enabled=false</c>.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        if (!opts.Enabled || opts.RatePerSecond <= 0)
        {
            logger.LogInformation("LatticeReadDriver disabled (Enabled={Enabled}, RatePerSecond={Rate}).", opts.Enabled, opts.RatePerSecond);
            return;
        }

        logger.LogInformation(
            "LatticeReadDriver starting: tree={TreeId} rate={Rate}/s pattern={Pattern} concurrency={Concurrency} warmup={Warmup}s",
            opts.TreeId, opts.RatePerSecond, opts.Pattern, opts.Concurrency, opts.WarmupDelay.TotalSeconds);

        try { await Task.Delay(opts.WarmupDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        var lattice = grainFactory.GetGrain<ILattice>(opts.TreeId);

        // Initial keyspace scan. If empty, the per-worker loop below short-sleeps and retries
        // until the simulator has populated something.
        await RefreshKeyspaceAsync(lattice, opts, stoppingToken);

        // ─── N-worker deadline-paced rate limiter ───────────────────────────────────────
        //
        // The previous implementation used a single PeriodicTimer at sub-ms period plus a
        // per-tick batch of `perTick` issues, each gated by `await SemaphoreSlim.WaitAsync`.
        // On Linux containers (incl. Docker-on-Windows via WSL2) `PeriodicTimer` periods of
        // ~1ms absorb hrtimer slack and per-tick async overhead collapses the effective tick
        // rate from a nominal 1000 Hz to ~250 Hz. At BENCH_READ_RATE_PER_SECOND=38000 with
        // perTick=38 that capped observed throughput at ~9.4k reads/s regardless of the
        // configured rate or concurrency. Bumping concurrency 32→128 produced ~0% gain —
        // textbook signature of a pacer upstream of the gate, not a saturated downstream.
        //
        // The replacement model spawns `Concurrency` persistent worker tasks, each pacing
        // itself to `RatePerSecond / Concurrency` reads/s using `Stopwatch`-based deadline
        // arithmetic. A worker can have only one outstanding read at a time, so worker count
        // IS the inflight cap — the explicit semaphore gate dissolves. Phase-staggering by
        // `workerId × perWorkerInterval / workerCount` distributes issuance evenly across
        // each per-worker interval rather than producing N-sized bursts in lockstep. Worker 0
        // owns keyspace refresh so concurrent refreshers don't stomp on `_keyspace`.
        //
        // For the calibrated bench config (38000/s × 128 workers), each worker targets
        // 297 reads/s = one read every 3.37 ms. Observed p50 GetAsync latency is 0.34 ms,
        // so a worker spends ~90% of its wall-clock budget in `Task.Delay` — well-clear of
        // the hrtimer-slack regime that broke the original PeriodicTimer pacer.
        var workerCount = Math.Max(1, opts.Concurrency);
        var perWorkerInterval = TimeSpan.FromSeconds(workerCount / (double)opts.RatePerSecond);

        logger.LogInformation(
            "LatticeReadDriver pacing: {Workers} workers × {PerWorkerHz:F1}/s = {GlobalHz:N0}/s target (interval={IntervalMs:F2} ms/worker)",
            workerCount,
            1.0 / perWorkerInterval.TotalSeconds,
            opts.RatePerSecond,
            perWorkerInterval.TotalMilliseconds);

        // Wrapped in a single-element array so the worker-0 closure can update it without
        // capturing a `ref` local (forbidden inside async lambdas).
        var lastRefreshTimestamp = new long[] { Stopwatch.GetTimestamp() };

        var workers = Enumerable.Range(0, workerCount)
            .Select(workerId => RunWorkerAsync(workerId, workerCount, lattice, opts, perWorkerInterval, lastRefreshTimestamp, stoppingToken));

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown — swallow.
        }
    }

    private async Task RunWorkerAsync(
        int workerId,
        int workerCount,
        ILattice lattice,
        LatticeReadDriverOptions opts,
        TimeSpan perWorkerInterval,
        long[] lastRefreshTimestamp,
        CancellationToken stoppingToken)
    {
        // Phase-stagger: worker 0 starts at t=0, worker N-1 at t=(N-1)/N × interval. Without
        // this every worker would issue in lockstep every perWorkerInterval, producing
        // N-sized bursts followed by silence rather than a smooth global rate.
        var phaseOffset = TimeSpan.FromTicks(perWorkerInterval.Ticks * workerId / workerCount);
        try { await Task.Delay(phaseOffset, stoppingToken); }
        catch (OperationCanceledException) { return; }

        // Per-worker RNG seeded distinctly so workers don't all walk the same key sequence.
        var rng = new Random(unchecked(workerId * 73 ^ Environment.TickCount));
        var startTicks = Stopwatch.GetTimestamp();
        long issued = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            // Worker 0 owns keyspace refresh. Other workers just observe `_keyspace` via the
            // race-free volatile read implicit in the field load below.
            if (workerId == 0)
            {
                var sinceRefresh = Stopwatch.GetElapsedTime(Volatile.Read(ref lastRefreshTimestamp[0]));
                if (sinceRefresh >= opts.KeyspaceRefreshInterval)
                {
                    await RefreshKeyspaceAsync(lattice, opts, stoppingToken);
                    Volatile.Write(ref lastRefreshTimestamp[0], Stopwatch.GetTimestamp());
                }
            }

            var keyspace = _keyspace;
            if (keyspace.Length == 0)
            {
                // Simulator hasn't published anything yet (or the refresh failed). Short-sleep
                // and retry. Don't advance `issued` — we don't want the deadline arithmetic
                // to think we're catching up on absent reads.
                try { await Task.Delay(TimeSpan.FromMilliseconds(100), stoppingToken); }
                catch (OperationCanceledException) { return; }
                startTicks = Stopwatch.GetTimestamp();
                continue;
            }

            var key = opts.Pattern switch
            {
                ReadDriverPattern.Sequential =>
                    keyspace[(int)((uint)Interlocked.Increment(ref _sequentialCursor) % (uint)keyspace.Length)],
                _ => keyspace[rng.Next(keyspace.Length)],
            };

            // Inline await — no fire-and-forget. workerCount workers each issuing one read at
            // a time = exactly workerCount inflight at peak, which is the desired cap.
            await IssueReadInlineAsync(lattice, key, stoppingToken);
            issued++;

            // perWorkerInterval × issued is monotonic, so a slow read catches up on the next
            // iteration without bursting beyond perWorkerInterval-ahead. If the worker falls
            // more than one whole interval behind (e.g. multi-second stall during keyspace
            // refresh, or downstream latency spike), reset the baseline so we don't run flat
            // out trying to catch up retroactively.
            var targetElapsed = TimeSpan.FromTicks(perWorkerInterval.Ticks * issued);
            var actualElapsed = Stopwatch.GetElapsedTime(startTicks);
            var remaining = targetElapsed - actualElapsed;

            if (remaining > TimeSpan.Zero)
            {
                try { await Task.Delay(remaining, stoppingToken); }
                catch (OperationCanceledException) { return; }
            }
            else if (remaining < -perWorkerInterval)
            {
                startTicks = Stopwatch.GetTimestamp();
                issued = 0;
            }
        }
    }

    private async Task IssueReadInlineAsync(ILattice lattice, string key, CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var value = await lattice.GetAsync(key, cancellationToken);
            metrics.Reads.Add(1);
            if (value is null) metrics.Misses.Add(1);
        }
        catch (OperationCanceledException)
        {
            // Shutdown - swallow.
        }
        catch (Exception ex)
        {
            metrics.Errors.Add(1);
            logger.LogDebug(ex, "GetAsync failed for key={Key}", key);
        }
        finally
        {
            metrics.DurationMs.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    }

    private async Task RefreshKeyspaceAsync(ILattice lattice, LatticeReadDriverOptions opts, CancellationToken cancellationToken)
    {
        try
        {
            var cursorId = await lattice.OpenKeyCursorAsync(cancellationToken: cancellationToken);
            var collected = new List<string>(capacity: Math.Min(opts.KeyspaceSampleSize, 4096));
            const int pageSize = 512;
            while (collected.Count < opts.KeyspaceSampleSize)
            {
                var page = await lattice.NextKeysAsync(cursorId, pageSize, cancellationToken);
                if (page.Keys is { Count: > 0 })
                {
                    collected.AddRange(page.Keys);
                }
                if (!page.HasMore || page.Keys is null || page.Keys.Count == 0) break;
            }
            if (collected.Count > 0)
            {
                _keyspace = collected.ToArray();
                logger.LogDebug("LatticeReadDriver keyspace refreshed: {Count} keys", _keyspace.Length);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Keyspace refresh failed; reusing previous sample of {Count} keys.", _keyspace.Length);
        }
    }
}