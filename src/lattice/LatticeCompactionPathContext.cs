using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient context that carries the compaction walk-path label
/// (<c>walk</c> for the legacy leaf-chain traversal, <c>dirty-set</c>
/// for the shard-root dirty-leaves fast path) into
/// <see cref="BPlusTree.Grains.BPlusLeafGrain"/>'s
/// <c>CompactTombstonesAsync</c> so per-leaf instruments
/// (<see cref="LatticeMetrics.CompactionLeavesVisited"/>) can be tagged
/// with the originating walk path. Public callers do not interact with
/// this type.
/// </summary>
/// <remarks>
/// Flows on the inbound call path through Orleans
/// <see cref="RequestContext"/> keyed
/// <see cref="LatticeEventConstants.CompactionPathRequestContextKey"/>.
/// <see cref="BPlusTree.Grains.TombstoneCompactionGrain"/> wraps each
/// shard-walk with a <see cref="BeginScope"/> so every leaf call inside
/// the pass observes the path label. Outside any scope
/// <see cref="Current"/> returns <see langword="null"/> and tag emission
/// falls back to the no-tag shape so existing dashboards continue to
/// match.
/// </remarks>
internal static class LatticeCompactionPathContext
{
    /// <summary>
    /// Gets the current ambient walk-path label, or
    /// <see langword="null"/> when no path scope is active.
    /// </summary>
    public static string? Current
        => RequestContext.Get(LatticeEventConstants.CompactionPathRequestContextKey) as string;

    /// <summary>
    /// Marks the ambient context with the given walk-path label for the
    /// lifetime of the returned scope, restoring the prior value on
    /// disposal. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable BeginScope(string pathKind)
    {
        ArgumentNullException.ThrowIfNull(pathKind);
        var previous = RequestContext.Get(LatticeEventConstants.CompactionPathRequestContextKey) as string;
        RequestContext.Set(LatticeEventConstants.CompactionPathRequestContextKey, pathKind);
        return new Scope(previous);
    }

    private sealed class Scope(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (previous is null)
            {
                RequestContext.Remove(LatticeEventConstants.CompactionPathRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.CompactionPathRequestContextKey, previous);
            }
        }
    }
}
