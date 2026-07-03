using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Backs the compiled-snapshot <c>epoch</c> and <c>age</c> observable gauges on
/// the <see cref="LatticeAuthMetrics.Meter"/>. Each
/// <see cref="CompiledPolicySnapshotMaintainer"/> registers itself here on
/// construction; the two gauges are created once (process-wide, idempotent) and
/// their measurement callbacks - which run only when an OpenTelemetry listener
/// scrapes the meter - report one measurement per live maintainer.
/// </summary>
/// <remarks>
/// Maintainers are held by <see cref="WeakReference{T}"/> so a maintainer whose
/// silo has shut down drops out of the observation set on the next scrape once
/// it is garbage-collected, without the maintainer needing an explicit
/// unregister step. This keeps the registry leak-free across a multi-silo,
/// single-process test cluster.
/// </remarks>
internal static class AuthSnapshotGaugeRegistry
{
    private static readonly object Lock = new();
    private static readonly List<WeakReference<CompiledPolicySnapshotMaintainer>> Sources = new();
    private static readonly TimeProvider Time = TimeProvider.System;
    private static bool _registered;

    /// <summary>
    /// Registers <paramref name="maintainer"/> as a snapshot-gauge source and
    /// ensures the epoch and age gauges are created on the meter (idempotent).
    /// </summary>
    /// <param name="maintainer">The maintainer whose epoch / age is observed.</param>
    public static void Register(CompiledPolicySnapshotMaintainer maintainer)
    {
        ArgumentNullException.ThrowIfNull(maintainer);
        lock (Lock)
        {
            Sources.Add(new WeakReference<CompiledPolicySnapshotMaintainer>(maintainer));
            if (!_registered)
            {
                LatticeAuthMetrics.Meter.CreateObservableGauge(
                    LatticeAuthMetrics.SnapshotEpochName,
                    ObserveEpoch,
                    unit: "{epoch}",
                    description: "Monotonic epoch of the compiled authorization policy snapshot.");

                LatticeAuthMetrics.Meter.CreateObservableGauge(
                    LatticeAuthMetrics.SnapshotAgeName,
                    ObserveAge,
                    unit: "s",
                    description: "Seconds since the compiled authorization policy snapshot was last rebuilt.");

                _registered = true;
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveEpoch()
    {
        foreach (var maintainer in LiveSources())
        {
            yield return new Measurement<long>(maintainer.CurrentEpoch);
        }
    }

    private static IEnumerable<Measurement<double>> ObserveAge()
    {
        var now = Time.GetUtcNow();
        foreach (var maintainer in LiveSources())
        {
            if (maintainer.LastRebuildUtc is { } last)
            {
                var seconds = Math.Max(0d, (now - last).TotalSeconds);
                yield return new Measurement<double>(seconds);
            }
        }
    }

    private static List<CompiledPolicySnapshotMaintainer> LiveSources()
    {
        var live = new List<CompiledPolicySnapshotMaintainer>();
        lock (Lock)
        {
            for (var i = Sources.Count - 1; i >= 0; i--)
            {
                if (Sources[i].TryGetTarget(out var maintainer))
                {
                    live.Add(maintainer);
                }
                else
                {
                    Sources.RemoveAt(i);
                }
            }
        }

        return live;
    }
}
