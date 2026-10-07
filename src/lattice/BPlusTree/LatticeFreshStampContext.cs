using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Marks an <see cref="LatticeHlcOverrideContext"/> stamp as freshly minted for
/// the operation in scope rather than carried from an existing write (issue
/// #4586). The commit-log writer treats every override stamp as carried, and so
/// exempts it from a replicated tree's WAL clock floor, unless this scope is
/// active. Only two override producers mint a fresh stamp: a range delete's
/// single issue stamp and a caller-supplied idempotency key. Flows through
/// <see cref="RequestContext"/> like the override itself.
/// </summary>
internal static class LatticeFreshStampContext
{
    /// <summary>Whether the HLC override in scope was minted for this operation.</summary>
    public static bool IsActive =>
        RequestContext.Get(LatticeEventConstants.FreshStampRequestContextKey) is true;

    /// <summary>
    /// Marks the override in scope as fresh until the returned scope is
    /// disposed, then restores the previous marking.
    /// </summary>
    public static IDisposable Begin()
    {
        var previous = IsActive;
        RequestContext.Set(LatticeEventConstants.FreshStampRequestContextKey, true);
        return new Scope(previous);
    }

    private sealed class Scope(bool previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (previous)
            {
                RequestContext.Set(LatticeEventConstants.FreshStampRequestContextKey, true);
            }
            else
            {
                RequestContext.Remove(LatticeEventConstants.FreshStampRequestContextKey);
            }
        }
    }
}
