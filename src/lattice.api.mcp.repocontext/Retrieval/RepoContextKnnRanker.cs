namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The pure ranking kernel of the in-box exact k-nearest-neighbour search:
/// given a query vector and a set of candidate vectors, it returns the closest
/// <c>k</c> in descending score order. It holds no state and touches no store,
/// so ranking correctness is unit-testable in isolation from the cluster.
/// </summary>
/// <remarks>
/// <para>
/// The kernel is fail-closed on embedding space: a candidate whose space does
/// not match <c>querySpace</c> (a different model, dimension, or
/// normalization) is skipped, so a mixed-space candidate set never yields a
/// meaningless score. When the query space is L2-normalized and the candidate is
/// too, the dot product equals the cosine similarity and is used directly;
/// otherwise the full cosine similarity is computed.
/// </para>
/// <para>
/// Only <c>k</c> results are retained during the scan (an insertion into a small
/// bounded list), so a large candidate set does not materialize a full sorted
/// copy.
/// </para>
/// </remarks>
internal static class RepoContextKnnRanker
{
    /// <summary>
    /// Ranks <paramref name="candidates"/> against <paramref name="query"/> and
    /// returns the closest <paramref name="k"/> matches in descending score order.
    /// Candidates whose embedding space does not match
    /// <paramref name="querySpace"/> are skipped.
    /// </summary>
    /// <param name="query">The query vector.</param>
    /// <param name="querySpace">The embedding space the query was produced in.</param>
    /// <param name="candidates">The candidate vectors to rank.</param>
    /// <param name="k">The maximum number of matches to return. Must be positive.</param>
    /// <returns>The closest matches, at most <paramref name="k"/>, in descending
    /// score order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="candidates"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="k"/> is not positive.</exception>
    internal static IReadOnlyList<RepoContextVectorMatch> Rank(
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag querySpace,
        IEnumerable<RepoContextVectorCandidate> candidates,
        int k)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(k);

        var querySpan = query.Span;
        var normalized = querySpace.Normalization == VectorNormalization.UnitL2;

        var top = new List<RepoContextVectorMatch>(k);
        foreach (var candidate in candidates)
        {
            if (!VectorSpaceGuard.Matches(candidate.Space, querySpace))
            {
                continue;
            }

            if (candidate.Vector.Length != querySpan.Length)
            {
                continue;
            }

            var score = normalized
                ? VectorMath.Dot(querySpan, candidate.Vector)
                : VectorMath.Cosine(querySpan, candidate.Vector);

            Insert(top, new RepoContextVectorMatch(candidate.VectorId, candidate.SourceKey, score), k);
        }

        return top;
    }

    private static void Insert(List<RepoContextVectorMatch> top, RepoContextVectorMatch match, int k)
    {
        if (top.Count == k && match.Score <= top[^1].Score)
        {
            return;
        }

        var index = top.BinarySearch(match, DescendingScoreComparer.Instance);
        if (index < 0)
        {
            index = ~index;
        }

        top.Insert(index, match);
        if (top.Count > k)
        {
            top.RemoveAt(top.Count - 1);
        }
    }

    private sealed class DescendingScoreComparer : IComparer<RepoContextVectorMatch>
    {
        internal static readonly DescendingScoreComparer Instance = new();

        public int Compare(RepoContextVectorMatch x, RepoContextVectorMatch y) =>
            y.Score.CompareTo(x.Score);
    }
}
