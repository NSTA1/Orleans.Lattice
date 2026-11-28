using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Marks a call as a bulk append's catch-up of its rightmost leaf (issue #6166), so the
/// leaf's starvation drive tries its consistent cut before it replays.
/// </summary>
internal static class LatticeBulkCatchUpContext
{
    /// <summary>Whether the calling flow is inside a bulk append's catch-up.</summary>
    public static bool IsActive => RequestContext.Get(LatticeEventConstants.BulkCatchUpRequestContextKey) is true;

    /// <summary>Marks the calling flow until the returned scope is disposed.</summary>
    public static Scope BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.BulkCatchUpRequestContextKey);
        RequestContext.Set(LatticeEventConstants.BulkCatchUpRequestContextKey, true);
        return new Scope(previous);
    }

    /// <summary>Restores the marker the scope replaced.</summary>
    public readonly struct Scope(object? previous) : IDisposable
    {
        /// <inheritdoc />
        public void Dispose()
        {
            if (previous is null)
            {
                RequestContext.Remove(LatticeEventConstants.BulkCatchUpRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.BulkCatchUpRequestContextKey, previous);
            }
        }
    }
}