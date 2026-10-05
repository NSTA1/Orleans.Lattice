namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure bookkeeping of the copies an atomic-write saga re-bound away from
/// (issue #4689, spec/shard-ownership/ShardOwnershipCutover.tla). A copy the
/// alias moved off that mirrors nowhere, such as the copy a shadow-cutover
/// restore retains, keeps the prepares the saga took on it, and the saga must
/// discard them before it decides. The model's <c>left</c> is this record;
/// <see cref="Record"/> is its re-bind update,
/// <c>left' = (left \cup {bound}) \ {alias}</c>.
/// <para>
/// Copy-on-write: the input is never mutated, so a caller that fails to persist
/// the result can restore the previous record. Physical tree ids compare
/// ordinally.
/// </para>
/// </summary>
internal static class SagaAbandonedCopies
{
    /// <summary>
    /// The record after the saga re-binds from <paramref name="leavingCopy"/>,
    /// where it had touched <paramref name="leavingShards"/>, onto
    /// <paramref name="newBoundCopy"/>. The copy left is added, its shards
    /// joined to any it already recorded, and the copy re-bound onto is removed,
    /// since the saga's prepares there are its own again.
    /// </summary>
    /// <param name="previous">The record before the re-bind, or <see langword="null"/>.</param>
    /// <param name="leavingCopy">The copy the saga was bound to, or <see langword="null"/> for an unbound saga.</param>
    /// <param name="leavingShards">The shards the saga had touched on <paramref name="leavingCopy"/>.</param>
    /// <param name="newBoundCopy">The copy the saga re-binds onto.</param>
    /// <returns>The new record, or <see langword="null"/> when it is empty.</returns>
    public static Dictionary<string, List<int>>? Record(
        IReadOnlyDictionary<string, List<int>>? previous,
        string? leavingCopy,
        IReadOnlyCollection<int> leavingShards,
        string newBoundCopy)
    {
        ArgumentNullException.ThrowIfNull(leavingShards);
        ArgumentNullException.ThrowIfNull(newBoundCopy);

        var next = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        if (previous is not null)
        {
            foreach (var (copy, shards) in previous)
            {
                next[copy] = [.. shards];
            }
        }

        if (leavingCopy is not null && !string.Equals(leavingCopy, newBoundCopy, StringComparison.Ordinal))
        {
            var joined = new SortedSet<int>(leavingShards);
            if (next.TryGetValue(leavingCopy, out var recorded))
            {
                joined.UnionWith(recorded);
            }

            next[leavingCopy] = [.. joined];
        }

        next.Remove(newBoundCopy);
        return next.Count == 0 ? null : next;
    }
}
