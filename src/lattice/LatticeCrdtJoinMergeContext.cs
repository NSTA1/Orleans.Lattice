using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient marker for a whole-row merge that carries another copy's
/// rows of the same keys into a copy that may have taken CRDT contributions of
/// its own - the online-resize and online-snapshot mirror of a source's applied
/// rows, and that copy's drain (issue #4618). Under it a destination leaf joins
/// an incoming CRDT row into a live CRDT row it holds, through the key's
/// registered <see cref="CrdtShape"/>, instead of keeping only the
/// last-writer-wins winner, exactly as a split's migration import does
/// (issue #4613): the destination folds a mirrored saga terminal at its own
/// stamp, so a source row stamped below that fold can still carry a
/// contribution the fold lacks.
/// <para>
/// Other whole-row merges - replication applies, tree merges, restores - keep
/// their last-writer-wins contract: a restore must not union the state it
/// restores with the state it replaces, and a replicated last-writer-wins row is
/// not a CRDT state.
/// </para>
/// </summary>
/// <remarks>
/// The marker flows on the inbound merge through an Orleans
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.CrdtJoinMergeRequestContextKey"/>, so it
/// reaches the destination leaf through the destination shard root unchanged.
/// It is a reserved key an external client cannot assert
/// (<c>LatticeCapabilityStrippingCallFilter</c>).
/// </remarks>
internal static class LatticeCrdtJoinMergeContext
{
    /// <summary>Gets whether the ambient merge joins CRDT rows.</summary>
    public static bool Current => RequestContext.Get(LatticeEventConstants.CrdtJoinMergeRequestContextKey) is true;

    /// <summary>
    /// Marks the ambient merge as joining CRDT rows for the lifetime of the
    /// returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.CrdtJoinMergeRequestContextKey);
        RequestContext.Set(LatticeEventConstants.CrdtJoinMergeRequestContextKey, true);
        return new Scope(previous);
    }

    private sealed class Scope(object? previous) : IDisposable
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
                RequestContext.Remove(LatticeEventConstants.CrdtJoinMergeRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.CrdtJoinMergeRequestContextKey, previous);
            }
        }
    }
}
