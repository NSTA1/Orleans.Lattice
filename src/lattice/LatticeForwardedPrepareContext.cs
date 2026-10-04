using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient marker for a saga prepare-phase write that was
/// <em>forwarded</em>: carried shard to shard by a shadow-forward (an active
/// split's hot path, or an online resize's shadow copy) or replayed onto a
/// split destination by the retroactive sweep, rather than issued by the saga
/// coordinator itself. The marker's value is the id of the tree whose registry
/// records the saga's decision - the <b>logical</b> tree, which differs from the
/// destination leaf's own physical tree id once a tree has been resized.
/// <para>
/// The distinction matters because only a forwarded prepare can reach a leaf
/// after its saga decided. A coordinator's own prepare is acknowledged before
/// the saga decides, but a forward abandoned at its deadline, or a duplicate,
/// can still be delivered later, and the sweep reads the source's buckets and
/// replays them while the saga may be deciding. Such a prepare is stamped on
/// arrival, newer than any write acknowledged since, so bucketing it would let
/// the saga's value override those writes. The destination leaf therefore asks
/// the registry for the saga's decision before bucketing a forwarded prepare
/// (issue #4445). Restricting that probe to forwarded prepares keeps the
/// coordinator's prepare path free of an extra registry round trip.
/// </para>
/// </summary>
/// <remarks>
/// The marker flows on the inbound write path through an Orleans
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.ForwardedPrepareRequestContextKey"/>, so it
/// reaches the destination leaf through the destination shard root unchanged.
/// It is meaningful only alongside <see cref="LatticePreparedContext"/>; the
/// leaf ignores it on a non-prepared write. It is a reserved key that an
/// external client cannot assert (<c>LatticeCapabilityStrippingCallFilter</c>).
/// </remarks>
internal static class LatticeForwardedPrepareContext
{
    /// <summary>
    /// Gets whether the ambient context marks the current write as a forwarded
    /// prepare. Returns <c>false</c> outside any scope.
    /// </summary>
    public static bool Current => RegistryTreeId is not null;

    /// <summary>
    /// Gets the id of the tree whose registry records the forwarded prepare's
    /// saga decision, or <see langword="null"/> outside any scope.
    /// </summary>
    public static string? RegistryTreeId =>
        RequestContext.Get(LatticeEventConstants.ForwardedPrepareRequestContextKey) is string { Length: > 0 } treeId
            ? treeId
            : null;

    /// <summary>
    /// Marks the ambient context as a forwarded prepare whose saga decision is
    /// recorded under <paramref name="registryTreeId"/>, for the lifetime of the
    /// returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is idempotent.
    /// </summary>
    /// <param name="registryTreeId">The logical tree id the saga records its decision under.</param>
    public static IDisposable BeginScope(string registryTreeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(registryTreeId);
        var previous = RequestContext.Get(LatticeEventConstants.ForwardedPrepareRequestContextKey);
        RequestContext.Set(LatticeEventConstants.ForwardedPrepareRequestContextKey, registryTreeId);
        return new Scope(previous);
    }

    /// <summary>
    /// Opens a <see cref="BeginScope"/> only when the ambient write is a saga
    /// prepare (<see cref="LatticePreparedContext.Current"/> with a non-empty
    /// <see cref="LatticeTransactionContext.Current"/>), so a forward of an
    /// ordinary write adds nothing to the outbound request context. A marker
    /// already in force (a forward of a forward) keeps its tree id. Returns
    /// <see langword="null"/> otherwise, which a <c>using</c> statement accepts.
    /// </summary>
    /// <param name="registryTreeId">The logical tree id the saga records its decision under.</param>
    public static IDisposable? BeginScopeIfPrepared(string registryTreeId)
        => LatticePreparedContext.Current && LatticeTransactionContext.Current != Guid.Empty
            ? BeginScope(RegistryTreeId ?? registryTreeId)
            : null;

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
                RequestContext.Remove(LatticeEventConstants.ForwardedPrepareRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.ForwardedPrepareRequestContextKey, previous);
            }
        }
    }
}
