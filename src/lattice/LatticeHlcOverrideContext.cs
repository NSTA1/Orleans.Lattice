using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient hybrid-logical-clock override used by source-HLC-preserving
/// apply paths to stamp <see cref="LwwValue{T}.Timestamp"/> verbatim
/// onto mutations authored by the current logical call.
/// </summary>
/// <remarks>
/// <para>
/// HLC override metadata flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.hlc"</c>. The leaf
/// grain checks the context at every commit site
/// (<c>SetCoreAsync</c> / <c>DeleteAsync</c> / <c>DeleteRangeAsync</c>)
/// — when a value is present, the leaf advances its local clock past
/// the override via <see cref="HybridLogicalClock.Merge(HybridLogicalClock, HybridLogicalClock)"/>
/// (preserving local monotonicity for any subsequent foreground tick)
/// and persists the <em>override verbatim</em> on the freshly-constructed
/// <see cref="LwwValue{T}"/>'s timestamp slot so receiver-side LWW
/// resolution sees the source-side HLC bit-identically. When the
/// context is unset (the common case for any direct foreground
/// caller), the leaf falls back to the standard
/// <see cref="HybridLogicalClock.Tick(HybridLogicalClock)"/> path.
/// </para>
/// <para>
/// The supported authoring paths are the receiver-side
/// cross-cluster atomic-visibility apply seam
/// (<see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/> /
/// <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>)
/// and the per-entry merge apply seam
/// (<see cref="IReplicationApplyGrain.ApplyMergeManyAsync"/>),
/// where each per-key call is wrapped in a
/// <see cref="With(HybridLogicalClock?)"/> scope alongside
/// <see cref="LatticeOriginContext"/> and
/// <see cref="LatticeVectorClockContext"/> so the leaf re-stamps the
/// authoring cluster's
/// <c>(Timestamp, OriginClusterId, VectorClock)</c> tuple verbatim.
/// </para>
/// </remarks>
public static class LatticeHlcOverrideContext
{
    /// <summary>
    /// Gets or sets the HLC override on the ambient
    /// <see cref="RequestContext"/>. Setting <c>null</c> removes the key
    /// rather than storing a null value, matching the "no override —
    /// foreground tick" default.
    /// </summary>
    public static HybridLogicalClock? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.HlcOverrideRequestContextKey);
            return raw is HybridLogicalClock hlc ? hlc : null;
        }
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.HlcOverrideRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.HlcOverrideRequestContextKey, value.Value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="sourceHlc"/> for
    /// the lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="sourceHlc">
    /// The authoring cluster's HLC to stamp onto mutations authored
    /// inside the scope, or <c>null</c> to explicitly clear the ambient
    /// context (so the leaf falls back to a fresh local tick).
    /// </param>
    public static IDisposable With(HybridLogicalClock? sourceHlc)
    {
        var previous = Current;
        Current = sourceHlc;
        return new Scope(previous);
    }

    private sealed class Scope(HybridLogicalClock? previous) : IDisposable
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
