using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Commit-log writer decorator for <see cref="WalClockFloorClusterFixture"/>
/// (issue #4586). It forwards every append to the real
/// <see cref="WalCommitLogWriter"/>, but can be armed to refuse a tree's next
/// range-delete records below the clock floor before they reach the partition,
/// exactly as a partition whose floor overtook a long fan-out would. It records
/// every range-delete stamp it sees per tree.
/// </summary>
internal sealed class FloorRefusalInjectingCommitLogWriter(ICommitLogWriter inner) : ICommitLogWriter
{
    private static readonly ConcurrentDictionary<string, int> ArmedRangeDeleteRefusals = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, ConcurrentQueue<HybridLogicalClock>> RangeDeleteStamps = new(StringComparer.Ordinal);

    /// <summary>Refuses the next <paramref name="count"/> range-delete records of <paramref name="treeId"/>.</summary>
    public static void ArmRangeDeleteRefusals(string treeId, int count) => ArmedRangeDeleteRefusals[treeId] = count;

    /// <summary>Every range-delete stamp seen for <paramref name="treeId"/>, refused or appended, in order.</summary>
    public static IReadOnlyList<HybridLogicalClock> RangeDeleteStampsFor(string treeId) =>
        RangeDeleteStamps.TryGetValue(treeId, out var stamps) ? stamps.ToArray() : Array.Empty<HybridLogicalClock>();

    public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken = default) =>
        TryRefuse(entry) is { } refusal ? Task.FromException<long>(refusal) : inner.AppendAsync(entry, cancellationToken);

    public Task<IReadOnlyList<long>> AppendManyAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)
    {
        foreach (var entry in entries)
        {
            if (TryRefuse(entry) is { } refusal)
            {
                return Task.FromException<IReadOnlyList<long>>(refusal);
            }
        }

        return inner.AppendManyAsync(entries, cancellationToken);
    }

    public Task DrainAsync(CancellationToken cancellationToken = default) =>
        inner is WalCommitLogWriter wal ? wal.DrainAsync(cancellationToken) : Task.CompletedTask;

    private static WalStampBelowFloorException? TryRefuse(WalRecord entry)
    {
        if (entry.Op != MutationKind.DeleteRange)
        {
            return null;
        }

        RangeDeleteStamps.GetOrAdd(entry.TreeId, static _ => new ConcurrentQueue<HybridLogicalClock>()).Enqueue(entry.Timestamp);
        while (ArmedRangeDeleteRefusals.TryGetValue(entry.TreeId, out var remaining) && remaining > 0)
        {
            if (ArmedRangeDeleteRefusals.TryUpdate(entry.TreeId, remaining - 1, remaining))
            {
                var floor = entry.Timestamp with { Counter = entry.Timestamp.Counter + 1 };
                return new WalStampBelowFloorException(entry.TreeId, 0, entry.Timestamp, floor);
            }
        }

        return null;
    }
}
