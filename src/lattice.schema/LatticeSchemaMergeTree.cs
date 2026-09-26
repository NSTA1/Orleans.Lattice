using Orleans.Runtime;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Ambient association of an in-flight merge with the tree whose policy governs
/// it. The core post-merge seam hands the observer a <see cref="LatticeMergeContext"/>
/// that can carry the tree id; this scope is the ambient fallback used by paths
/// that stamp the tree id through <see cref="RequestContext"/>. <see cref="Current"/>
/// reads the scoped value back.
/// </summary>
/// <remarks>
/// Kept for compatibility with tests and older call paths that still provide the
/// tree id through ambient request context. See <see cref="LatticeSchemaMergeObserver"/>.
/// </remarks>
internal static class LatticeSchemaMergeTree
{
    /// <summary>The tree id associated with the current merge, or <c>null</c> when none is set.</summary>
    public static string? Current =>
        RequestContext.Get(SchemaConstants.MergeTreeIdRequestContextKey) as string;

    /// <summary>
    /// Associates <paramref name="treeId"/> with the current merge for the
    /// lifetime of the returned scope, restoring the prior value on disposal.
    /// </summary>
    /// <param name="treeId">The tree id to associate. Must not be <c>null</c> or empty.</param>
    /// <returns>A scope that clears the association on disposal.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public static IDisposable Enter(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var previous = RequestContext.Get(SchemaConstants.MergeTreeIdRequestContextKey) as string;
        RequestContext.Set(SchemaConstants.MergeTreeIdRequestContextKey, treeId);
        return new Scope(previous);
    }

    private sealed class Scope(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (previous is null)
            {
                RequestContext.Remove(SchemaConstants.MergeTreeIdRequestContextKey);
            }
            else
            {
                RequestContext.Set(SchemaConstants.MergeTreeIdRequestContextKey, previous);
            }
        }
    }
}
