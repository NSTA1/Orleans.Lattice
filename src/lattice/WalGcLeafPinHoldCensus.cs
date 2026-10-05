using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice;

/// <summary>
/// How long each WAL partition has been held by a leaf's durable materialiser pin,
/// as of this silo's latest garbage-collection pass, retained for the process
/// lifetime (issue #4622).
/// </summary>
/// <remarks>
/// A partition an unusable pin holds - a leaf that has never checkpointed it, or a
/// checkpointed leaf with no snapshot coverage - admits nothing. With a retention
/// window configured, a partition is held too when a leaf pin the durable offset
/// floor does not cover caps its retention ceiling below the window, because the
/// ceiling never overtakes a leaf.
/// Either way its WAL grows for as long as the hold stands. Only a hold by a
/// registry-absent leaf is reported to the blocked-leaf remedy; a hold by a live
/// leaf that has wedged is visible nowhere else, which is what this gauge is for.
/// Every partition is zero-primed on a tree's first pass, a pass that could not
/// read the pin census publishes <c>-1</c> (unknown) without resetting the age,
/// and a pass that finds the partition free resets it to <c>0</c>.
/// </remarks>
internal static class WalGcLeafPinHoldCensus
{
    private static readonly ConcurrentDictionary<(string Tree, int Partition), HoldState> Holds = new();

    /// <summary>Zero-primes every partition of <paramref name="treeName"/>.</summary>
    internal static void Prime(string treeName, int partitions)
    {
        for (var partition = 0; partition < partitions; partition++)
        {
            Holds.TryAdd((treeName, partition), default);
        }
    }

    /// <summary>
    /// Records which of <paramref name="treeName"/>'s partitions a pass found held,
    /// measuring each held partition's age from the first consecutive pass that
    /// found it held.
    /// </summary>
    internal static void Record(string treeName, bool[] held, DateTimeOffset now)
    {
        for (var partition = 0; partition < held.Length; partition++)
        {
            var isHeld = held[partition];
            Holds.AddOrUpdate(
                (treeName, partition),
                _ => isHeld ? new HoldState(now, 0) : default,
                (_, prior) =>
                {
                    if (!isHeld)
                    {
                        return default;
                    }

                    var since = prior.HeldSince ?? now;
                    return new HoldState(since, (long)Math.Max(0d, (now - since).TotalSeconds));
                });
        }
    }

    /// <summary>
    /// Records that a pass could not establish which partitions are held: each
    /// publishes <c>-1</c>, and a standing hold keeps its start.
    /// </summary>
    internal static void RecordUnknown(string treeName, int partitions)
    {
        for (var partition = 0; partition < partitions; partition++)
        {
            Holds.AddOrUpdate(
                (treeName, partition),
                _ => new HoldState(null, -1),
                (_, prior) => prior with { AgeSeconds = -1 });
        }
    }

    /// <summary>The age the latest pass published for one partition, for tests.</summary>
    internal static long? Read(string treeName, int partition)
        => Holds.TryGetValue((treeName, partition), out var state) ? state.AgeSeconds : null;

    private static IEnumerable<Measurement<long>> Observe()
    {
        foreach (var entry in Holds)
        {
            yield return new Measurement<long>(
                entry.Value.AgeSeconds,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, entry.Key.Tree),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, entry.Key.Partition),
                LatticeTenantLabel.ForTree(entry.Key.Tree));
        }
    }

    // Publication can invoke Observe re-entrantly; its state must already exist.
    internal static readonly ObservableGauge<long> Gauge =
        LatticeMetrics.Meter.CreateObservableGauge(
            LatticeMetrics.WalGcLeafPinHoldAgeName,
            Observe,
            unit: "s",
            description: "Seconds a WAL partition has been held by a leaf's durable materialiser pin, as of this silo's latest GC pass, tagged by tree and by shard carrying the partition (issue #4622). Held means an unusable pin (a leaf that never checkpointed the partition, or one without snapshot coverage), which admits nothing; or, with a retention window configured, a leaf pin the durable offset floor does not cover whose frontier is older than the window, which caps the retention ceiling at that frontier because the ceiling never overtakes a leaf. Its WAL grows for as long as the hold stands. A hold by a registry-absent leaf is also reported to the blocked-leaf remedy (orleans.lattice.wal.gc.blocked_consumers); a hold by a live leaf that never checkpoints is visible only here. Zero-primed per partition on a tree's first pass; 0 means not held; -1 means the pass could not read the pin census.");

    private readonly record struct HoldState(DateTimeOffset? HeldSince, long AgeSeconds);
}
