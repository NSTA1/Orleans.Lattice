using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice;

/// <summary>Latest complete blocked-consumer population per tree, retained for the process lifetime.</summary>
internal static class WalGcBlockedConsumerCensus
{
    private static readonly ConcurrentDictionary<string, long> Counts = new(StringComparer.Ordinal);

    internal static void Prime(string treeName) => Counts.TryAdd(treeName, 0);

    internal static void Record(string treeName, long count) => Counts[treeName] = count;

    private static IEnumerable<Measurement<long>> Observe()
    {
        foreach (var entry in Counts)
        {
            yield return new Measurement<long>(
                entry.Value,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, entry.Key),
                LatticeTenantLabel.ForTree(entry.Key));
        }
    }

    // Publication can invoke Observe re-entrantly; its state must already exist.
    internal static readonly ObservableGauge<long> Gauge =
        LatticeMetrics.Meter.CreateObservableGauge(
            LatticeMetrics.WalGcBlockedConsumersName,
            Observe,
            unit: "{consumer}",
            description: "Latest uncapped count of distinct durable consumers blocking a tree's cursor floor. Denominator for orleans.lattice.wal.gc.blocked_leaf_reactivations: together they expose convergence. Zero-primed on a tree's first pass; -1 means the census could not be established. The scan continues after every partition is blocked and the eight-id report is full, without changing trim entitlement. Counts consumers, not leaves: partition pins are distinct consumers.");
}
