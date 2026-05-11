using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Lattice;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// Background service that issues <c>ILattice.SetManyAtomicAsync</c> sagas at a configured
/// rate. Drives the atomic-write throughput / latency benchmarks and (when paired with a
/// replication overlay) the cross-cluster atomic visibility benchmarks.
/// </summary>
/// <remarks>
/// Pacing model mirrors <see cref="LatticeWriteDriver"/>: N persistent worker tasks share
/// the rate budget, each on its own <c>Stopwatch</c>-based deadline of
/// <c>RatePerSecond / Concurrency</c> sagas/s, phase-staggered for even temporal
/// distribution. Per-worker key prefixes (<c>{KeyPrefix}w{workerId:D2}-{j}</c>) keep
/// concurrent sagas disjoint so the leaf-level prepare path does not abort sagas against
/// each other under load.
/// </remarks>
public sealed class LatticeAtomicSagaDriver(
    IGrainFactory grainFactory,
    IOptions<LatticeAtomicSagaDriverOptions> options,
    LatticeAtomicSagaDriverMetrics metrics,
    ILogger<LatticeAtomicSagaDriver> logger) : BackgroundService
{
    /// <summary>The driver loop. Exits immediately when
    /// <c>AtomicSagaDriver:Enabled=false</c>.</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var opts = options.Value;
        if (!opts.Enabled || opts.RatePerSecond <= 0)
        {
            logger.LogInformation(
                "LatticeAtomicSagaDriver disabled (Enabled={Enabled}, RatePerSecond={Rate}).",
                opts.Enabled, opts.RatePerSecond);
            return;
        }

        logger.LogInformation(
            "LatticeAtomicSagaDriver starting: tree={TreeId} rate={Rate}/s concurrency={Concurrency} batchSize={BatchSize} keyspace={Keyspace} valueBytes={ValueBytes} warmup={Warmup}s",
            opts.TreeId, opts.RatePerSecond, opts.Concurrency, opts.BatchSize,
            opts.KeyspaceSize, opts.ValueSizeBytes, opts.WarmupDelay.TotalSeconds);

        try { await Task.Delay(opts.WarmupDelay, stoppingToken); }
        catch (OperationCanceledException) { return; }

        var lattice = grainFactory.GetGrain<ILattice>(opts.TreeId);

        var workerCount = Math.Max(1, opts.Concurrency);
        var perWorkerInterval = TimeSpan.FromSeconds(workerCount / (double)opts.RatePerSecond);

        logger.LogInformation(
            "LatticeAtomicSagaDriver pacing: {Workers} workers x {PerWorkerHz:F2}/s = {GlobalHz:N0}/s target sagas (interval={IntervalMs:F2} ms/worker, batchSize={BatchSize})",
            workerCount,
            1.0 / perWorkerInterval.TotalSeconds,
            opts.RatePerSecond,
            perWorkerInterval.TotalMilliseconds,
            opts.BatchSize);

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
        LatticeAtomicSagaDriverOptions opts,
        TimeSpan perWorkerInterval,
        CancellationToken stoppingToken)
    {
        // Phase-stagger so workers don't issue in lockstep.
        var phaseOffset = TimeSpan.FromTicks(perWorkerInterval.Ticks * workerId / workerCount);
        try { await Task.Delay(phaseOffset, stoppingToken); }
        catch (OperationCanceledException) { return; }

        // Per-worker RNG seeded distinctly so workers walk distinct key sequences.
        var rng = new Random(unchecked(workerId * 73 ^ Environment.TickCount));

        // Per-worker scratch buffer for the value payload. Reused across iterations - not
        // shared across workers so concurrent SetManyAtomicAsync calls don't alias the
        // same array. Every batch entry references the same buffer reference; the lattice
        // grain copies before persisting.
        var valueBuffer = new byte[Math.Max(1, opts.ValueSizeBytes)];
        rng.NextBytes(valueBuffer);

        var batchSize = Math.Max(1, opts.BatchSize);
        var keyspaceSize = Math.Max(batchSize, opts.KeyspaceSize);
        var workerPrefix = $"{opts.KeyPrefix}w{workerId:D2}-";

        // Pre-allocated batch list. Cleared and rebuilt each iteration; capacity matches
        // BatchSize so no resizes occur on the hot path.
        var batch = new List<KeyValuePair<string, byte[]>>(batchSize);

        var startTicks = Stopwatch.GetTimestamp();
        long issued = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            batch.Clear();
            for (var j = 0; j < batchSize; j++)
            {
                var keyIndex = rng.Next(keyspaceSize);
                batch.Add(new KeyValuePair<string, byte[]>($"{workerPrefix}{keyIndex}-{j}", valueBuffer));
            }

            await IssueSagaAsync(lattice, batch, stoppingToken);
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

    private async Task IssueSagaAsync(
        ILattice lattice,
        List<KeyValuePair<string, byte[]>> batch,
        CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            await lattice.SetManyAtomicAsync(batch, cancellationToken);
            metrics.Sagas.Add(1);
        }
        catch (OperationCanceledException)
        {
            // Shutdown - swallow.
        }
        catch (Exception ex)
        {
            metrics.Errors.Add(1);
            logger.LogDebug(ex, "SetManyAtomicAsync failed for batch of size {Size}", batch.Count);
        }
        finally
        {
            metrics.DurationMs.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }
    }
}
