using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient create intent for one leaf (issue #4654).
/// <para>
/// A leaf's state row is its only link to its tree, key range, checkpoint and
/// kept-snapshot record. An activation that finds no row is either a leaf being
/// created or one whose row was lost, and nothing on the leaf can tell the two
/// apart once its row record has been lost too. So the leaf does not guess: it
/// writes a first row, and serves data, only when the call that reaches it carries
/// an intent naming it, set by a path that is creating it - shard bootstrap, a leaf
/// split, a bulk load, or the recovery reseed of a purged tree. Without one, a
/// rowless leaf fails closed.
/// </para>
/// </summary>
/// <remarks>
/// The intent flows through an Orleans <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.NewLeafIntentRequestContextKey"/>. Its value
/// names one leaf, so an intent that propagates beyond the call it was meant for
/// confers nothing on any other leaf. It is a reserved key that an external client
/// cannot assert (<c>LatticeCapabilityStrippingCallFilter</c>).
/// </remarks>
internal static class LatticeNewLeafIntentContext
{
    /// <summary>
    /// Gets whether the ambient context carries a create intent naming
    /// <paramref name="leafId"/>.
    /// </summary>
    /// <param name="leafId">The leaf asking.</param>
    public static bool IsFor(GrainId leafId) =>
        RequestContext.Get(LatticeEventConstants.NewLeafIntentRequestContextKey) is string { Length: > 0 } named
        && string.Equals(named, leafId.ToString(), StringComparison.Ordinal);

    /// <summary>
    /// Asserts a create intent for <paramref name="leafId"/> for the lifetime of
    /// the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    /// <param name="leafId">The leaf being created.</param>
    public static IDisposable BeginScope(GrainId leafId)
    {
        var previous = RequestContext.Get(LatticeEventConstants.NewLeafIntentRequestContextKey);
        RequestContext.Set(LatticeEventConstants.NewLeafIntentRequestContextKey, leafId.ToString());
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
                RequestContext.Remove(LatticeEventConstants.NewLeafIntentRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.NewLeafIntentRequestContextKey, previous);
            }
        }
    }
}
