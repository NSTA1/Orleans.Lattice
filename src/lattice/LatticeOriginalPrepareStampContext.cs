using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient carriers that let a saga's value be applied at the saga's
/// own prepare stamp rather than at a fresh, dominating stamp (issue #4522).
/// <para>
/// A saga's terminal used to install its committed values under a stamp minted
/// at terminal time, above every row the leaf held. A write acknowledged after
/// the prepare - a later local write, or one that reached the leaf as a
/// cross-shard migration import - was then overwritten by the saga's older
/// value. The fix applies the saga's value under last-writer-wins at the
/// prepare stamp P: it is installed only over a row stamped below P, and it is
/// stored AT P, so any write stamped above P survives whatever order it lands
/// in. That requires every copy of a prepare to carry its original P, which is
/// what these carriers convey.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>The prepared route.</b> The routing tier names the shard it dispatches a
/// prepared write to. A leaf of that shard mints the prepare's stamp itself,
/// so the stamp is original and the prepare is <i>marked</i>. Any other leaf -
/// the destination of a forward, which inherits the forwarding shard's route -
/// does not match, so a forward is never mistaken for an original prepare.
/// </description></item>
/// <item><description>
/// <b>The original stamps.</b> A key to stamp map. A forwarder (a split's live
/// shadow-forward, or its retroactive sweep) carries each marked prepare's P
/// to the destination, which buckets the prepare AT P and marks it. A terminal
/// delivery carries the P of each committed-values backstop key, and the leaf
/// applies that key under last-writer-wins at P.
/// </description></item>
/// </list>
/// <para>
/// A prepare reached by neither carrier - one written by an older silo, or
/// replayed from a write-ahead-log record that predates the marker - is
/// <i>unmarked</i>. Its stamp may be the destination's own clock rather than the
/// original P, so it keeps the pre-#4522 terminal drain, including the
/// migrated-row carve-out that protects it from a pre-saga migrated value
/// stamped above it. Both keys are reserved and stripped from external clients
/// (<c>LatticeCapabilityStrippingCallFilter</c>).
/// </para>
/// </summary>
internal static class LatticeOriginalPrepareStampContext
{
    /// <summary>
    /// The shard the routing tier dispatched the ambient prepared write to, as
    /// <c>{physicalTreeId}/{shardIndex}</c>, or <see langword="null"/>.
    /// </summary>
    public static string? PreparedRoute =>
        RequestContext.Get(LatticeEventConstants.PreparedRouteRequestContextKey) as string;

    /// <summary>
    /// Stamps <paramref name="shardKey"/> as the shard the next prepared write is
    /// dispatched to. Called by the routing tier immediately before each
    /// per-shard dispatch, the same way it stamps the routed identity: an Orleans
    /// call captures the request context when it is issued, so each dispatch
    /// carries the shard it was made to.
    /// </summary>
    /// <param name="shardKey">The target shard's grain key.</param>
    public static void StampPreparedRoute(string shardKey) =>
        RequestContext.Set(LatticeEventConstants.PreparedRouteRequestContextKey, shardKey);

    /// <summary>
    /// Returns the original prepare stamp carried for <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="stamp">The carried stamp, when present.</param>
    /// <returns>Whether a stamp is carried for the key.</returns>
    public static bool TryGetStamp(string key, out HybridLogicalClock stamp)
    {
        if (RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey)
                is Dictionary<string, HybridLogicalClock> stamps
            && stamps.TryGetValue(key, out stamp))
        {
            return true;
        }

        stamp = default;
        return false;
    }

    /// <summary>
    /// Whether any original stamp is carried, so a caller can skip per-key
    /// lookups on the common path where none is.
    /// </summary>
    public static bool HasStamps =>
        RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey) is Dictionary<string, HybridLogicalClock> { Count: > 0 };

    /// <summary>
    /// Carries <paramref name="stamps"/> for the lifetime of the returned scope,
    /// restoring the prior value on dispose. A <see langword="null"/> or empty
    /// map clears the carrier for the scope, so a nested call made on behalf of
    /// another write never inherits stamps that were not meant for it.
    /// </summary>
    /// <param name="stamps">The key to original-prepare-stamp map, or <see langword="null"/>.</param>
    /// <returns>A scope that restores the previous value.</returns>
    public static IDisposable With(Dictionary<string, HybridLogicalClock>? stamps)
    {
        var previous = RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey);
        if (stamps is { Count: > 0 })
            RequestContext.Set(LatticeEventConstants.OriginalPrepareStampsRequestContextKey, stamps);
        else
            RequestContext.Remove(LatticeEventConstants.OriginalPrepareStampsRequestContextKey);
        return new Scope(LatticeEventConstants.OriginalPrepareStampsRequestContextKey, previous);
    }

    /// <summary>
    /// Clears the prepared-route marker for the lifetime of the returned scope,
    /// so a forward never carries a route it could match by accident.
    /// </summary>
    /// <returns>A scope that restores the previous value.</returns>
    public static IDisposable WithoutPreparedRoute()
    {
        var previous = RequestContext.Get(LatticeEventConstants.PreparedRouteRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.PreparedRouteRequestContextKey);
        return new Scope(LatticeEventConstants.PreparedRouteRequestContextKey, previous);
    }

    private sealed class Scope(string key, object? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (previous is null)
                RequestContext.Remove(key);
            else
                RequestContext.Set(key, previous);
        }
    }
}
