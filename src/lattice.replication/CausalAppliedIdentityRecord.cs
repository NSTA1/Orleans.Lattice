namespace Orleans.Lattice.Replication;

/// <summary>
/// The bounded, in-memory record of write identities <c>(origin, HLC)</c>
/// applied to one tree, kept by the tree's high-water-mark grain (issue #4586).
/// A causal dependency names exactly one write, so a dependency whose identity
/// is recorded here is met at once. The record is a fast path only: an identity
/// it has forgotten - evicted past the capacity, or lost to a reactivation - is
/// decided instead by the origin's shipped low watermark (see
/// <see cref="CausalFrontierCore"/>). Eviction removes the oldest identity of
/// the origin, which is the one the low watermark covers first.
/// </summary>
internal sealed class CausalAppliedIdentityRecord
{
    private static readonly IComparer<HybridLogicalClock> HlcOrder =
        Comparer<HybridLogicalClock>.Create(static (left, right) => left.CompareTo(right));

    private readonly Dictionary<string, SortedSet<HybridLogicalClock>> _applied = new(StringComparer.Ordinal);

    /// <summary>Records that <paramref name="originClusterId"/>'s write at <paramref name="applied"/> was applied.</summary>
    public void Record(string originClusterId, HybridLogicalClock applied, int capacity)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        if (!_applied.TryGetValue(originClusterId, out var identities))
        {
            identities = new SortedSet<HybridLogicalClock>(HlcOrder);
            _applied[originClusterId] = identities;
        }

        identities.Add(applied);
        var bounded = Math.Max(1, capacity);
        while (identities.Count > bounded)
        {
            identities.Remove(identities.Min);
        }
    }

    /// <summary>Whether <paramref name="originClusterId"/>'s write at <paramref name="required"/> is recorded as applied.</summary>
    public bool Contains(string originClusterId, HybridLogicalClock required) =>
        _applied.TryGetValue(originClusterId, out var identities) && identities.Contains(required);

    /// <summary>How many identities of <paramref name="originClusterId"/> are recorded.</summary>
    public int Count(string originClusterId) =>
        _applied.TryGetValue(originClusterId, out var identities) ? identities.Count : 0;
}
