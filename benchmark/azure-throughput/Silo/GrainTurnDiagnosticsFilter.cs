// Benchmark-only grain-turn occupancy diagnostic.
//
// Enabled by BENCH_GRAIN_TURN_DIAG_MS (a positive threshold in milliseconds).
// Every incoming call to a lattice grain is timed from the moment the grain
// begins executing it until it returns. A call at or beyond the threshold
// prints one completed line; calls still executing past the threshold are
// summarised every period, which is what exposes a turn that never finishes:
//
//   [turn-diag] state=done ms=93128 grain=bplusleaf/<id> method=AppendTxTerminalAsync
//   [turn-diag] state=pending ms=61457 grain=bplusleaf/<id> method=... count=3
//
// Because the filter runs inside the target activation's turn, the measured
// interval excludes mailbox queueing: a long `done` on a non-reentrant grain
// is the turn that held it, and the calls queued behind it show up as long
// caller-side awaits with no matching long turn of their own.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// Times incoming lattice grain calls and logs those that run past a threshold.
/// Registered only when <c>BENCH_GRAIN_TURN_DIAG_MS</c> is a positive integer.
/// </summary>
internal sealed class GrainTurnDiagnosticsFilter : IIncomingGrainCallFilter
{
    private const int DoneLinesPerSecond = 40;

    private readonly long _thresholdMs;
    private readonly ConcurrentDictionary<long, (string Grain, string Method, long Started)> _inFlight = new();
    private readonly Timer _watchdog;
    private long _nextId;
    private long _window = -1;
    private int _emitted;
    private long _suppressed;

    /// <summary>Creates the filter with the given threshold.</summary>
    public GrainTurnDiagnosticsFilter(long thresholdMs)
    {
        _thresholdMs = thresholdMs;
        var period = TimeSpan.FromMilliseconds(Math.Max(thresholdMs, 5_000));
        _watchdog = new Timer(_ => ReportPending(), null, period, period);
    }

    /// <summary>Parses the threshold; anything but a positive integer disables the filter.</summary>
    public static long ParseThresholdMs(string? value) =>
        long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var ms) && ms > 0 ? ms : 0;

    /// <inheritdoc />
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        var grainType = context.TargetId.Type.ToString() ?? string.Empty;
        if (!grainType.StartsWith("bplus", StringComparison.Ordinal)
            && !grainType.StartsWith("_lattice", StringComparison.Ordinal)
            && !grainType.Contains("shard", StringComparison.OrdinalIgnoreCase))
        {
            await context.Invoke();
            return;
        }

        var id = Interlocked.Increment(ref _nextId);
        var started = Stopwatch.GetTimestamp();
        _inFlight[id] = (context.TargetId.ToString(), context.ImplementationMethod?.Name ?? context.Request.GetMethodName(), started);
        try
        {
            await context.Invoke();
        }
        finally
        {
            if (_inFlight.TryRemove(id, out var entry))
            {
                var ms = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
                if (ms >= _thresholdMs && TryAdmit())
                    Write(string.Create(CultureInfo.InvariantCulture, $"[turn-diag] state=done ms={ms:F0} grain={entry.Grain} method={entry.Method}"));
            }
        }
    }

    private bool TryAdmit()
    {
        var second = Environment.TickCount64 / 1000;
        lock (_inFlight)
        {
            if (second != _window)
            {
                _window = second;
                _emitted = 0;
            }

            if (_emitted < DoneLinesPerSecond)
            {
                _emitted++;
                return true;
            }

            _suppressed++;
            return false;
        }
    }

    private void ReportPending()
    {
        try
        {
            var now = Stopwatch.GetTimestamp();
            var groups = new Dictionary<(string Grain, string Method), (int Count, double OldestMs)>();
            foreach (var entry in _inFlight.Values)
            {
                var ms = Stopwatch.GetElapsedTime(entry.Started, now).TotalMilliseconds;
                if (ms < _thresholdMs)
                    continue;

                var key = (entry.Grain, entry.Method);
                groups[key] = groups.TryGetValue(key, out var agg)
                    ? (agg.Count + 1, Math.Max(agg.OldestMs, ms))
                    : (1, ms);
            }

            foreach (var ((grain, method), agg) in groups.OrderByDescending(g => g.Value.OldestMs).Take(25))
                Write(string.Create(CultureInfo.InvariantCulture, $"[turn-diag] state=pending ms={agg.OldestMs:F0} grain={grain} method={method} count={agg.Count}"));

            long suppressed;
            lock (_inFlight)
            {
                suppressed = _suppressed;
                _suppressed = 0;
            }

            if (suppressed > 0)
                Write(string.Create(CultureInfo.InvariantCulture, $"[turn-diag] state=suppressed count={suppressed}"));
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
}
