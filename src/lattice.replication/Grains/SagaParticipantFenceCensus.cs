using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-silo census of cross-cluster saga participants that still hold their
/// cutover fence after its window expired (issue #4637), published as the
/// <see cref="FenceHeldAgeName"/> gauge.
/// <para>
/// A prepared participant that voted commit no longer compensates on its fence
/// timer alone: when the timer fires it asks the coordinator for the saga's
/// decision. While the coordinator answers that the decision is pending, or
/// cannot be reached, the participant keeps its prepared state and its fence,
/// because compensating then could leave its cluster on the pre-restore tree
/// while another cluster commits. The gauge reports, per reason, the age in
/// seconds of the oldest such fence past its window, so a coordinator that
/// stays unreachable raises a growing, alarmable series instead of a silent
/// mixed outcome.
/// </para>
/// <para>
/// The census is per silo and in memory. A participant re-reports itself on
/// every fence tick, and an entry not refreshed within
/// <see cref="StaleAfter"/> - its activation moved to another silo, say - is
/// dropped, so it never pins a false alarm.
/// </para>
/// </summary>
internal static class SagaParticipantFenceCensus
{
    /// <summary>Instrument name of the fence-held age gauge.</summary>
    public const string FenceHeldAgeName = "orleans.lattice.replication.saga.participant.fence_held_age";

    /// <summary>
    /// <see cref="LatticeReplicationMetrics.TagReason"/> value: the coordinator
    /// answered that the saga's decision is still pending.
    /// </summary>
    public const string ReasonDecisionPending = "decision_pending";

    /// <summary>
    /// <see cref="LatticeReplicationMetrics.TagReason"/> value: the coordinator
    /// could not be reached, or refused the query.
    /// </summary>
    public const string ReasonCoordinatorUnreachable = "coordinator_unreachable";

    /// <summary>
    /// How long an entry stays in the census without a refresh. Two fence ticks,
    /// so a single delayed reminder does not drop a held fence from the gauge.
    /// </summary>
    internal static readonly TimeSpan StaleAfter = TimeSpan.FromMinutes(11);

    private static readonly ConcurrentDictionary<string, Entry> Held = new(StringComparer.Ordinal);

    /// <summary>
    /// The age, in seconds, of the oldest cutover fence still held past its
    /// window on this silo, tagged by reason. Declared last, below every field
    /// its callback reads (see the metrics declaration-order rule).
    /// </summary>
    public static readonly ObservableGauge<double> FenceHeldAge =
        LatticeReplicationMetrics.Meter.CreateObservableGauge(
            FenceHeldAgeName,
            Observe,
            unit: "s",
            description: "Age in seconds of the oldest cross-cluster saga cutover fence a prepared participant still holds past its window, waiting for the coordinator's decision, tagged by reason (decision_pending, coordinator_unreachable).");

    /// <summary>
    /// Records that the participant for <paramref name="sagaId"/> still holds
    /// its fence past <paramref name="fenceDeadlineTicks"/> for
    /// <paramref name="reason"/>.
    /// </summary>
    public static void Hold(string sagaId, long fenceDeadlineTicks, string reason) =>
        Held[sagaId] = new Entry(fenceDeadlineTicks, DateTime.UtcNow.Ticks, reason);

    /// <summary>Removes <paramref name="sagaId"/>: its participant reached a terminal phase.</summary>
    public static void Release(string sagaId) => Held.TryRemove(sagaId, out _);

    /// <summary>Clears the census. Test hook.</summary>
    internal static void ResetForTest() => Held.Clear();

    /// <summary>
    /// The oldest age, in seconds, currently held for <paramref name="reason"/>,
    /// or <see langword="null"/> when none is. Test hook.
    /// </summary>
    internal static double? OldestAgeSeconds(string reason)
    {
        double? oldest = null;
        foreach (var (_, age, entryReason) in Fresh(DateTime.UtcNow.Ticks))
        {
            if (entryReason == reason && (oldest is null || age > oldest)) oldest = age;
        }

        return oldest;
    }

    private static IEnumerable<Measurement<double>> Observe()
    {
        double? pending = null, unreachable = null;
        foreach (var (_, age, reason) in Fresh(DateTime.UtcNow.Ticks))
        {
            if (reason == ReasonCoordinatorUnreachable)
            {
                if (unreachable is null || age > unreachable) unreachable = age;
            }
            else if (pending is null || age > pending)
            {
                pending = age;
            }
        }

        if (pending is { } p)
            yield return Measure(p, ReasonDecisionPending);
        if (unreachable is { } u)
            yield return Measure(u, ReasonCoordinatorUnreachable);
    }

    private static Measurement<double> Measure(double ageSeconds, string reason) =>
        new(ageSeconds,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.Platform);

    private static IEnumerable<(string SagaId, double AgeSeconds, string Reason)> Fresh(long nowTicks)
    {
        foreach (var (sagaId, entry) in Held)
        {
            if (nowTicks - entry.LastSeenTicks > StaleAfter.Ticks)
            {
                Held.TryRemove(sagaId, out _);
                continue;
            }

            var age = Math.Max(0, nowTicks - entry.FenceDeadlineTicks) / (double)TimeSpan.TicksPerSecond;
            yield return (sagaId, age, entry.Reason);
        }
    }

    private readonly record struct Entry(long FenceDeadlineTicks, long LastSeenTicks, string Reason);
}
