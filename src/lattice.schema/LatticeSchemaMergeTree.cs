using Orleans.Runtime;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Ambient association of an in-flight merge with the tree whose policy governs
/// it. The core post-merge seam (#1198) hands the observer a
/// <see cref="LatticeMergeContext"/> that carries the merged key but not the tree
/// id, so the observer cannot by itself resolve the per-tree policy. This scope
/// stamps the tree id onto the ambient <see cref="RequestContext"/> for the
/// lifetime of a <c>using</c> block; <see cref="Current"/> reads it back.
/// </summary>
/// <remarks>
/// Until the core merge context carries the tree id, production wiring of this
/// scope requires a core hook; the type exists so the observer's resolution is
/// exercised (in tests) and ready for that hook. See
/// <see cref="LatticeSchemaMergeObserver"/>.
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
