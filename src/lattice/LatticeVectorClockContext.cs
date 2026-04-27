using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient vector-clock context used to stamp
/// <see cref="LwwValue{T}.VectorClock"/> onto mutations authored by the
/// current logical call.
/// </summary>
/// <remarks>
/// <para>
/// Vector-clock metadata flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.vc"</c>. Callers that
/// forward a remote mutation into a local lattice (for example, an inbound
/// replication handler) wrap the call in <see cref="With(VersionVector?)"/>
/// so the grain write path reads the context at commit time and stamps
/// it onto the freshly-constructed <see cref="LwwValue{T}"/> / tombstone.
/// Local writes leave the context unset, producing a <c>null</c> frontier
/// (which convention treats as <em>empty</em>).
/// </para>
/// <para>
/// The frontier is then preserved end-to-end across every lifecycle path
/// the library guarantees for <see cref="LwwValue{T}.OriginClusterId"/> —
/// shard-split shadow-forward, saga prepare / compensate, tree snapshot /
/// restore, bulk-load, compaction, and merge — so a captured frontier
/// travels with the value and survives transfer between shards or trees.
/// </para>
/// <para>
/// The library itself does not interpret or merge the frontier;
/// replication-aware consumers populate and consult it as needed.
/// </para>
/// </remarks>
public static class LatticeVectorClockContext
{
    /// <summary>
    /// Gets or sets the vector-clock frontier on the ambient
    /// <see cref="RequestContext"/>. Setting <c>null</c> removes the key
    /// rather than storing a null value, matching the "empty" default.
    /// </summary>
    public static VersionVector? Current
    {
        get => RequestContext.Get(LatticeEventConstants.VectorClockRequestContextKey) as VersionVector;
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.VectorClockRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.VectorClockRequestContextKey, value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="vectorClock"/> for
    /// the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="vectorClock">
    /// The vector-clock frontier to stamp onto mutations authored inside
    /// the scope, or <c>null</c> to explicitly clear the ambient context.
    /// </param>
    public static IDisposable With(VersionVector? vectorClock)
    {
        var previous = Current;
        Current = vectorClock;
        return new Scope(previous);
    }

    private sealed class Scope(VersionVector? previous) : IDisposable
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
