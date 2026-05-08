using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient prepare-phase flag used to stamp
/// <see cref="LatticeMutation.IsPrepared"/> for the duration of a saga's
/// prepare-phase write. Public callers do not interact with this type —
/// the <c>AtomicWriteGrain</c> coordinator wraps its prepare-phase
/// per-key writes in <see cref="BeginScope"/> so the leaf grain's commit
/// pipeline routes the mutation into the per-leaf in-memory
/// pending-transaction map rather than into the visible projection.
/// Reads filter pending entries out of view; a subsequent terminal
/// <see cref="MutationKind.TxCommit"/> or
/// <see cref="MutationKind.TxAbort"/> mutation flips or drops the
/// pending entries on the leaf.
/// </summary>
/// <remarks>
/// <para>
/// The flag flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.PreparedRequestContextKey"/>. The
/// <c>AtomicWriteGrain</c> coordinator wraps its prepare-phase per-key
/// calls in a <see cref="BeginScope"/> <c>using</c> block; emits made
/// inside the scope are stamped with <see cref="LatticeMutation.IsPrepared"/>
/// = <c>true</c>, while emits outside the scope default to <c>false</c>.
/// The flag is preserved across grain calls because
/// <see cref="RequestContext"/> propagates automatically.
/// </para>
/// <para>
/// The default outside any scope is <c>false</c>, matching the
/// documented "wire-compatible default" semantics for observers
/// persisted before this field existed.
/// </para>
/// </remarks>
internal static class LatticePreparedContext
{
    /// <summary>
    /// Gets the current ambient prepare-phase flag. Returns <c>true</c>
    /// when a prepare scope is active on the <see cref="RequestContext"/>;
    /// otherwise returns <c>false</c>.
    /// </summary>
    public static bool Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.PreparedRequestContextKey);
            return raw is bool active && active;
        }
    }

    /// <summary>
    /// Marks the ambient context as a prepare scope for the lifetime of
    /// the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    public static IDisposable BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.PreparedRequestContextKey) as bool?;
        RequestContext.Set(LatticeEventConstants.PreparedRequestContextKey, true);
        return new Scope(previous);
    }

    private sealed class Scope(bool? previous) : IDisposable
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
                RequestContext.Remove(LatticeEventConstants.PreparedRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.PreparedRequestContextKey, previous.Value);
            }
        }
    }
}
