using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

namespace Orleans.Lattice;

/// <summary>
/// Benchmark-only diagnostic for awaits that run long or never complete. Set
/// <c>LATTICE_BENCH_AWAIT_DIAG_MS</c> to a positive threshold in milliseconds in the
/// silo environment and every instrumented await that takes at least that long
/// emits one <c>[await-diag] state=done</c> line on stdout, while awaits still
/// outstanding past the threshold are summarised per region on a periodic
/// <c>[await-diag] state=pending</c> line - which is what makes an await that never
/// resumes visible at all.
/// </summary>
/// <remarks>
/// <para>
/// Read once at type initialisation, exactly as <c>LATTICE_BENCH_TRACE_PERSIST</c>
/// is: unset, zero, or unparseable disables it, and a disabled
/// <see cref="Begin"/> returns a default scope without touching the clock, a
/// collection or the allocator, so production and the unit-test harness pay one
/// static read per instrumented site.
/// </para>
/// <para>
/// Output is bounded: completed-await lines are capped per region per second and
/// the excess is folded into the next pending summary as a suppressed count, so a
/// convoy of thousands of slow point writes cannot flood the log.
/// </para>
/// </remarks>
internal static class AwaitDiagnostics
{
    /// <summary>The environment variable holding the threshold in milliseconds.</summary>
    internal const string ThresholdVariable = "LATTICE_BENCH_AWAIT_DIAG_MS";

    /// <summary>Completed-await lines emitted per region per second before suppression.</summary>
    internal const int DoneLinesPerRegionPerSecond = 20;

    private static readonly ConcurrentDictionary<long, Pending> InFlight = new();
    private static readonly ConcurrentDictionary<string, RegionStats> Regions = new(StringComparer.Ordinal);
    private static long _nextId;
    private static Timer? _watchdog;

    /// <summary>The configured threshold, or 0 when the diagnostic is disabled.</summary>
    internal static readonly long ThresholdMs = ParseThresholdMs(Environment.GetEnvironmentVariable(ThresholdVariable));

    /// <summary>Whether the diagnostic is enabled for this process.</summary>
    internal static bool Enabled => ThresholdMs > 0;

    /// <summary>Parses a threshold value; anything but a positive integer disables the diagnostic.</summary>
    internal static long ParseThresholdMs(string? value) =>
        long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var ms) && ms > 0 ? ms : 0;

    /// <summary>
    /// Starts timing an await. Dispose the returned scope when the await completes.
    /// </summary>
    internal static Scope Begin(string region, GrainId grain, string? detail = null)
    {
        if (!Enabled)
            return default;

        EnsureWatchdog();
        var id = Interlocked.Increment(ref _nextId);
        InFlight[id] = new Pending(region, grain.ToString(), detail, Stopwatch.GetTimestamp());
        return new Scope(id);
    }

    /// <summary>Times one task under <paramref name="region"/>; returns the task unchanged when disabled.</summary>
    internal static Task<T> Time<T>(Task<T> task, string region, GrainId grain, string? detail = null)
    {
        if (!Enabled || task.IsCompleted)
            return task;

        return TimeSlowAsync(task, region, grain, detail);
    }

    private static async Task<T> TimeSlowAsync<T>(Task<T> task, string region, GrainId grain, string? detail)
    {
        using var scope = Begin(region, grain, detail);
        return await task;
    }

    /// <summary>Formats one diagnostic line. Exposed for tests.</summary>
    internal static string FormatLine(string state, string region, double ms, string? grain, string? detail) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"[await-diag] state={state} region={region} ms={ms:F0} grain={grain ?? "-"}{(string.IsNullOrEmpty(detail) ? string.Empty : " " + detail)}");

    private static void Complete(long id)
    {
        if (!InFlight.TryRemove(id, out var pending))
            return;

        var ms = Stopwatch.GetElapsedTime(pending.Started).TotalMilliseconds;
        if (ms < ThresholdMs)
            return;

        var stats = Regions.GetOrAdd(pending.Region, static _ => new RegionStats());
        if (stats.TryAdmitDoneLine(ms))
            Write(FormatLine("done", pending.Region, ms, pending.Grain, pending.Detail));
    }

    private static void EnsureWatchdog()
    {
        if (Volatile.Read(ref _watchdog) is not null)
            return;

        var period = TimeSpan.FromMilliseconds(Math.Max(ThresholdMs, 5_000));
        var timer = new Timer(static _ => ReportPending(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        if (Interlocked.CompareExchange(ref _watchdog, timer, null) is null)
            timer.Change(period, period);
        else
            timer.Dispose();
    }

    private static void ReportPending()
    {
        try
        {
            var now = Stopwatch.GetTimestamp();
            var byRegion = new Dictionary<string, (int Count, double OldestMs, Pending Oldest)>(StringComparer.Ordinal);
            foreach (var pending in InFlight.Values)
            {
                var ms = Stopwatch.GetElapsedTime(pending.Started, now).TotalMilliseconds;
                if (ms < ThresholdMs)
                    continue;

                if (byRegion.TryGetValue(pending.Region, out var agg))
                {
                    byRegion[pending.Region] = ms > agg.OldestMs
                        ? (agg.Count + 1, ms, pending)
                        : (agg.Count + 1, agg.OldestMs, agg.Oldest);
                }
                else
                {
                    byRegion[pending.Region] = (1, ms, pending);
                }
            }

            foreach (var (region, agg) in byRegion)
            {
                Write(FormatLine(
                    "pending",
                    region,
                    agg.OldestMs,
                    agg.Oldest.Grain,
                    string.Create(CultureInfo.InvariantCulture, $"count={agg.Count} oldestDetail=[{agg.Oldest.Detail}]")));
            }

            foreach (var (region, stats) in Regions)
            {
                var (suppressed, maxMs) = stats.DrainSuppressed();
                if (suppressed > 0)
                {
                    Write(FormatLine(
                        "suppressed",
                        region,
                        maxMs,
                        null,
                        string.Create(CultureInfo.InvariantCulture, $"count={suppressed}")));
                }
            }
        }
        catch
        {
            // A diagnostic must never fault the timer thread.
        }
    }

    private static void Write(string line)
    {
        try
        {
            Console.Out.WriteLine(line);
        }
        catch
        {
            // Losing a diagnostic line is a bounded loss.
        }
    }

    /// <summary>A timing scope returned by <see cref="Begin"/>.</summary>
    internal readonly struct Scope : IDisposable
    {
        private readonly long _id;

        internal Scope(long id) => _id = id;

        /// <inheritdoc />
        public void Dispose()
        {
            if (_id != 0)
                Complete(_id);
        }
    }

    private sealed record Pending(string Region, string Grain, string? Detail, long Started);

    private sealed class RegionStats
    {
        private readonly object _gate = new();
        private long _windowSecond = -1;
        private int _emittedInWindow;
        private long _suppressed;
        private double _suppressedMaxMs;

        public bool TryAdmitDoneLine(double ms)
        {
            var second = Environment.TickCount64 / 1000;
            lock (_gate)
            {
                if (second != _windowSecond)
                {
                    _windowSecond = second;
                    _emittedInWindow = 0;
                }

                if (_emittedInWindow < DoneLinesPerRegionPerSecond)
                {
                    _emittedInWindow++;
                    return true;
                }

                _suppressed++;
                if (ms > _suppressedMaxMs)
                    _suppressedMaxMs = ms;
                return false;
            }
        }

        public (long Count, double MaxMs) DrainSuppressed()
        {
            lock (_gate)
            {
                var result = (_suppressed, _suppressedMaxMs);
                _suppressed = 0;
                _suppressedMaxMs = 0;
                return result;
            }
        }
    }
}
