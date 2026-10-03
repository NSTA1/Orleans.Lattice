namespace Orleans.Lattice.Replication;

/// <summary>
/// Membership tests over a set of <see cref="LeafReReplayRange"/> windows, shared
/// by the leaf re-replay and scoped-snapshot paths so both select the same keys.
/// </summary>
internal static class LeafReReplayRanges
{
    /// <summary>
    /// Returns <see langword="true"/> when any range in <paramref name="ranges"/>
    /// contains <paramref name="key"/>.
    /// </summary>
    /// <param name="ranges">The ranges to test.</param>
    /// <param name="key">The key to look for.</param>
    internal static bool AnyContains(IReadOnlyList<LeafReReplayRange> ranges, string? key)
    {
        for (var i = 0; i < ranges.Count; i++)
        {
            if (ranges[i].Contains(key))
            {
                return true;
            }
        }
        return false;
    }
}
