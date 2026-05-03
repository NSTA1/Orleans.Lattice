using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Background service that issues <c>ILattice.SetAsync</c> calls at a configured rate to
/// generate write-side load on the replica silo for bidirectional-replication scenarios.
/// Mirrors <see cref="LatticeReadDriver"/>'s N-worker deadline-paced rate limiter so the
/// pacing model is identical between read and write generators.
/// </summary>
/// <remarks>
/// Without this driver the bidirectional-replication scenario is unidirectional in
/// practice: the simulator API only writes to the origin cluster, so the replica's
/// outbound WAL stays empty and the reverse-direction ship/apply histograms never fire.
/// Running this on the replica side produces a stream of <c>SetAsync</c> calls against the
/// same tree the origin replicates into, which the replication observer captures into the
/// replica's WAL, which the per-(tree, peer) shipper drains and pushes to the origin.
/// </remarks>
public sealed class LatticeWriteDriver(
    IGrainFactory grainFactory,
    IOptions<LatticeWriteDriverOptions> options,
    LatticeWriteDriverMetrics metrics,
    ILogger<LatticeWriteDriver> logger) : BackgroundService
{
    /// <summary>The driver loop. Exits immediately when <c>WriteDriver:Enabled=false</c>.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        if (!opts.Enabled || opts.RatePerSecond <= 0)
        {
            logger.LogInformation(
                "LatticeWriteDriver disabled (Enabled={Enabled}, RatePerSecond={Rate}).",
                opts.Enabled, opts.RatePerSecond);
            return;
        }

        logger.LogInformation(
            "LatticeWriteDriver starting: tree={TreeId} rate={Rate}/s concurrency={Concurrency} keyspace={Keyspace} valueBytes={ValueBytes} warmup={Warmup}s",
            opts.TreeId, opts.RatePerSecond, opts.Concurrency,
            opts.KeyspaceSize, opts.ValueSizeBytes, opts.WarmupDelay.TotalSeconds);

        try { await Task.Delay(opts.WarmupDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        var lattice = grainFactory.GetGrain<ILattice>(opts.TreeId);

        var workerCount = Math.Max(1, opts.Concurrency);
        var perWorkerInterval = TimeSpan.FromSeconds(workerCount / (double)opts.RatePerSecond);

        logger.LogInformation(
            "LatticeWriteDriver pacing: {Workers} workers x {PerWorkerHz:F1}/s = {GlobalHz:N0}/s target (interval={IntervalMs:F2} ms/worker)",
            workerCount,
            1.0 / perWorkerInterval.TotalSeconds,
            opts.RatePerSecond,
            perWorkerInterval.TotalMilliseconds);

        var workers = Enumerable.Range(0, workerCount)
            .Select(workerId => RunWorkerAsync(workerId, workerCount, lattice, opts, perWorkerInterval, stoppingToken));

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown - swallow.
        }
    }

    private async Task RunWorkerAsync(
        int workerId,
        int workerCount,
        ILattice lattice,
        LatticeWriteDriverOptions opts,
        TimeSpan perWorkerInterval,
        CancellationToken stoppingToken)
    {
        // Phase-stagger so workers don''''t issue in lockstep.
        var phaseOffset = TimeSpan.FromTicks(perWorkerInterval.Ticks * workerId / workerCount);
        try { await Task.Delay(phaseOffset, stoppingToken); }
        catch (OperationCanceledException) { return; }

        // Per-worker RNG seeded distinctly so workers walk distinct key sequences.
        var rng = new Random(unchecked(workerId * 73 ^ Environment.TickCount));

        // Per-worker scratch buffer for the value payload. Reused across iterations - not
        // shared across workers so concurrent SetAsync calls don''''t alias the same array.
        var valueBuffer = new byte[Math.Max(1, opts.ValueSizeBytes)];
        rng.NextBytes(valueBuffer);

        var keyspaceSize = Math.Max(1, opts.KeyspaceSize);
        var startTicks = Stopwatch.GetTimestamp();
        long issued = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            var keyIndex = rng.Next(keyspaceSize);
            var key = $"{opts.KeyPrefix}{keyIndex}";

            await IssueWriteInlineAsync(lattice, key, valueBuffer, stoppingToken);
            issued++;

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
                // Worker fell more than one whole interval behind; reset baseline.
                startTicks = Stopwatch.GetTimestamp();
                issued = 0;
            }
        }
    }

    private async Task IssueWriteInlineAsync(
        ILattice lattice,
        string key,
        byte[] valueBuffer,
        CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            await lattice.SetAsync(key, valueBuffer, cancellationToken);
            metrics.Writes.Add(1);
        }
        catch (OperationCanceledException)
        {
            // Shutdown - swallow.
        }
        catch (Exception ex)
        {
            metrics.Errors.Add(1);
            logger.LogDebug(ex, "SetAsync failed for key={Key}", key);
        }
        finally
        {
            metrics.DurationMs.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    }
}