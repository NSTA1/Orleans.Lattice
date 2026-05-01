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

        // Initial keyspace scan. If empty, retry with backoff - the simulator may not have
        // produced anything yet.
        await RefreshKeyspaceAsync(lattice, opts, stoppingToken);
        var lastRefresh = Stopwatch.GetTimestamp();

        // Rate-limit via fixed-period ticker. PeriodicTimer rounds to ms; for rates > 1000/s
        // we batch issues per tick instead of trying to subdivide a sub-ms period.
        var ticksPerSecond = Math.Max(1, Math.Min(opts.RatePerSecond, 1000));
        var perTick = Math.Max(1, opts.RatePerSecond / ticksPerSecond);
        var tickPeriod = TimeSpan.FromMilliseconds(1000.0 / ticksPerSecond);
        using var ticker = new PeriodicTimer(tickPeriod);

        var rng = new Random(unchecked((int)(DateTime.UtcNow.Ticks ^ Environment.CurrentManagedThreadId)));
        using var concurrencyGate = new SemaphoreSlim(opts.Concurrency, opts.Concurrency);

        while (await ticker.WaitForNextTickAsync(stoppingToken))
        {
            // Periodic keyspace refresh so newly-written vehicle ids get exercised.
            if (Stopwatch.GetElapsedTime(lastRefresh) >= opts.KeyspaceRefreshInterval)
            {
                await RefreshKeyspaceAsync(lattice, opts, stoppingToken);
                lastRefresh = Stopwatch.GetTimestamp();
            }

            var keyspace = _keyspace;
            if (keyspace.Length == 0) continue;

            for (var i = 0; i < perTick; i++)
            {
                if (stoppingToken.IsCancellationRequested) break;

                var key = opts.Pattern switch
                {
                    ReadDriverPattern.Sequential => keyspace[(int)((uint)Interlocked.Increment(ref _sequentialCursor) % (uint)keyspace.Length)],
                    _ => keyspace[rng.Next(keyspace.Length)],
                };

                await concurrencyGate.WaitAsync(stoppingToken);
                _ = IssueReadAsync(lattice, key, concurrencyGate, stoppingToken);
            }
        }
    }

    private async Task IssueReadAsync(ILattice lattice, string key, SemaphoreSlim gate, CancellationToken cancellationToken)
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
            gate.Release();
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