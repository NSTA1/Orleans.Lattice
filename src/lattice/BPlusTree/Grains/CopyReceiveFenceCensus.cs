using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo census of the restored copies whose receive fence is closed (issue
/// #4593), and the source of the
/// <see cref="LatticeMetrics.CopyReceiveClosedAgeGaugeName"/> observable gauge.
/// <para>
/// A coordinated restore closes its restored copy before the alias swap and
/// opens it when the saga's fence lifts. A copy that stays closed defers every
/// replicated write to its tree indefinitely, so how long it has been closed is
/// the reading an operator alarms on. Each closed
/// <see cref="CopyReceiveFenceGrain"/> activation enrols its copy; opening the
/// copy or deactivating the grain withdraws it. The saga's fence grain touches
/// its closed copies on every poll while the fence is held, so a copy stuck
/// closed stays activated, and therefore stays reported.
/// </para>
/// <para>
/// No closed copy means no measurement. That is a correct reading here rather
/// than an ambiguous one: the gauge reports a property of closed copies, and an
/// open copy has no age.
/// </para>
/// </summary>
internal static class CopyReceiveFenceCensus
{
    /// <summary>Closed copies on this silo, keyed by physical tree id, valued by their close time in UTC ticks.</summary>
    private static readonly ConcurrentDictionary<string, long> ClosedCopies = new(StringComparer.Ordinal);

    /// <summary>
    /// The observable gauge. Declared below the state <see cref="Observe"/> reads,
    /// because a listener may observe the gauge as soon as it is published.
    /// </summary>
    internal static readonly ObservableGauge<double> Gauge =
        LatticeMetrics.Meter.CreateObservableGauge(
            LatticeMetrics.CopyReceiveClosedAgeGaugeName,
            Observe, unit: "s",
            description: "Seconds each restored copy on this silo has had its receive fence closed by a coordinated restore, tagged by physical tree. Absent when no copy is closed.");

    /// <summary>Enrols a closed copy, or refreshes its close time.</summary>
    /// <param name="physicalTreeId">The closed copy.</param>
    /// <param name="closedAtTicks">When it was closed, in UTC ticks.</param>
    internal static void Enrol(string physicalTreeId, long closedAtTicks) =>
        ClosedCopies[physicalTreeId] = closedAtTicks;

    /// <summary>Withdraws a copy's enrolment, if it is still the one enrolled at <paramref name="closedAtTicks"/>.</summary>
    /// <param name="physicalTreeId">The copy.</param>
    /// <param name="closedAtTicks">The close time the caller enrolled.</param>
    internal static void Withdraw(string physicalTreeId, long closedAtTicks) =>
        ClosedCopies.TryRemove(new KeyValuePair<string, long>(physicalTreeId, closedAtTicks));

    /// <summary>Returns whether a copy is enrolled as closed on this silo.</summary>
    /// <param name="physicalTreeId">The copy.</param>
    internal static bool IsEnrolled(string physicalTreeId) => ClosedCopies.ContainsKey(physicalTreeId);

    /// <summary>Emits one measurement per closed copy: its age in seconds.</summary>
    /// <returns>One measurement per closed copy.</returns>
    internal static IEnumerable<Measurement<double>> Observe()
    {
        var now = DateTime.UtcNow.Ticks;
        var measurements = new List<Measurement<double>>(ClosedCopies.Count);
        foreach (var (physicalTreeId, closedAtTicks) in ClosedCopies)
        {
            var age = Math.Max(0, now - closedAtTicks) / (double)TimeSpan.TicksPerSecond;
            measurements.Add(new Measurement<double>(
                age,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, physicalTreeId),
                LatticeTenantLabel.ForTree(physicalTreeId)));
        }

        return measurements;
    }
}
