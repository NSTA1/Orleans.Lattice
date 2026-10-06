using System.Collections.Immutable;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// This cluster's cross-tree purge frontier, as an origin (issue #4733): per
/// tree, the highest decision sequence at or below which the tree's
/// transaction registry stores no cross-tree decision and never will again.
/// <c>frontier(T) = min(counter(T), lowest pending - 1, lowest stored - 1)</c>,
/// the counter read before the stores, so a sequence recorded after the read
/// is above it. Shippers advertise it to their peers, whose decided barrier
/// tombstones drop once it passes the operation on every participant. A
/// cluster-wide grain keyed by <see cref="Key"/>.
/// </summary>
[Alias(ReplicationTypeAliases.ICrossTreePurgeFrontierSourceGrain)]
internal interface ICrossTreePurgeFrontierSourceGrain : IGrainWithStringKey
{
    /// <summary>The single key.</summary>
    public const string Key = "cluster";

    /// <summary>Durably adds <paramref name="trees"/> to the trees the frontier covers. Idempotent.</summary>
    Task RegisterTreesAsync(IReadOnlyCollection<string> trees);

    /// <summary>
    /// Every covered tree's frontier, recomputed at most every refresh
    /// interval. A tree whose frontier is still negative (a decision stored
    /// before sequencing) is omitted.
    /// </summary>
    Task<ImmutableDictionary<string, long>> GetAsync();
}
