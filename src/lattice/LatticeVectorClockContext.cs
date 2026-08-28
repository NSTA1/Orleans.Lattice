using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient vector-clock context used to stamp
/// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.VectorClock"/> onto mutations authored by the
/// current logical call.
/// </summary>
/// <remarks>
/// <para>
/// Vector-clock metadata flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.vc"</c>. Callers that
/// forward a remote mutation into a local lattice (for example, an inbound
/// replication handler) wrap the call in <see cref="With(VersionVector?)"/>
/// so the grain write path reads the context at commit time and stamps
/// it onto the freshly-constructed <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> / tombstone.
/// Local writes leave the context unset, producing a <c>null</c> frontier
/// (which convention treats as <em>empty</em>).
/// </para>
/// <para>
/// The frontier is then preserved end-to-end across every lifecycle path
/// the library guarantees for <see cref="Orleans.Lattice.Primitives.LwwValue{T}.OriginClusterId"/> -
/// shard-split shadow-forward, saga prepare / compensate, tree snapshot /
/// restore, bulk-load, compaction, and merge - so a captured frontier
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
    /// <para>
    /// <b>The setter takes a defensive copy.</b> A <see cref="VersionVector"/> is
    /// a mutable CRDT, and the frontier established here is stamped directly onto
    /// the persisted <see cref="Orleans.Lattice.Primitives.LwwValue{T}.VectorClock"/>
    /// of every entry written inside the scope. Without the copy, whatever
    /// instance the caller supplied would become the durable state of many
    /// entries at once - and on the inbound replication path that instance
    /// arrives inside an <c>[Immutable]</c> carrier, whose same-silo deep copy
    /// Orleans <em>elides</em>, so it is the co-located <em>sender's</em> object.
    /// A later mutation on either side would then silently rewrite the frontier
    /// of unrelated committed entries, and only when caller and callee share a
    /// silo, so no cross-silo test would ever show it. Copying here fixes the
    /// whole class at the single seam where an externally-owned frontier becomes
    /// platform state, rather than at the many sites that read it back.
    /// </para>
    /// <para>
    /// The cost is one clone per scope, not per write - and none at all on the
    /// dominant path, since a purely local write leaves the frontier
    /// <see langword="null"/>. The getter deliberately does <em>not</em> copy, so
    /// reading the frontier at each write site stays allocation-free; the
    /// returned instance is platform-owned and must be treated as read-only.
    /// </para>
    /// </summary>
    public static VersionVector? Current
    {
        get => RequestContext.Get(LatticeEventConstants.VectorClockRequestContextKey) as VersionVector;
        set => SetOwned(value?.Clone());
    }

    /// <summary>
    /// Stores an already platform-owned frontier without a further copy. Used by
    /// the scope restore path, whose captured value came out of
    /// <see cref="Current"/> and was therefore copied on the way in.
    /// </summary>
    private static void SetOwned(VersionVector? value)
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

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="vectorClock"/> for
    /// the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="vectorClock">
    /// The vector-clock frontier to stamp onto mutations authored inside
    /// the scope, or <c>null</c> to explicitly clear the ambient context.
    /// Copied on entry (see <see cref="Current"/>), so the caller keeps sole
    /// ownership of the instance it passed and may reuse or mutate it freely.
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
            // The captured frontier came out of Current and was copied on the way
            // in, so restoring it needs no second copy.
            SetOwned(previous);
        }
    }
}
