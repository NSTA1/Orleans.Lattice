using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient origin-cluster context used to stamp <see cref="LwwValue{T}.OriginClusterId"/>
/// onto mutations authored by the current logical call.
/// </summary>
/// <remarks>
/// <para>
/// Origin-cluster metadata flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.ocid"</c>. Callers that
/// forward a remote mutation into a local lattice (for example, an inbound
/// replication handler) wrap the call in
/// <see cref="With(string?)"/> so the grain write path reads the context
/// at commit time and stamps it onto the freshly-constructed
/// <see cref="LwwValue{T}"/> / tombstone. Local writes leave the context
/// unset, producing a <c>null</c> origin (which convention treats as
/// <em>local</em>).
/// </para>
/// <para>
/// The stamp is then preserved end-to-end across every lifecycle path the
/// library guarantees for <see cref="LwwValue{T}.ExpiresAtTicks"/> — shard
/// split shadow-forward, saga prepare / compensate, tree snapshot / restore,
/// bulk-load, and merge — and is surfaced to post-commit
/// <see cref="IMutationObserver"/> consumers through
/// <see cref="LatticeMutation.OriginClusterId"/> for loop-prevention.
/// </para>
/// </remarks>
public static class LatticeOriginContext
{
    /// <summary>
    /// Gets or sets the origin-cluster identifier on the ambient
    /// <see cref="RequestContext"/>. Setting <c>null</c> removes the key
    /// rather than storing a null value, matching the "local" default.
    /// </summary>
    public static string? Current
    {
        get => RequestContext.Get(LatticeEventConstants.OriginClusterIdRequestContextKey) as string;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.OriginClusterIdRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.OriginClusterIdRequestContextKey, value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="originClusterId"/> for
    /// the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="originClusterId">
    /// The cluster identifier to stamp onto mutations authored inside the
    /// scope, or <c>null</c> to explicitly clear the ambient context.
    /// </param>
    public static IDisposable With(string? originClusterId)
    {
        var previous = Current;
        Current = originClusterId;
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
            Current = previous;
        }
    }
}
