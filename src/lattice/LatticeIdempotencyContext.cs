using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient idempotency-key scope used to pin the
/// <see cref="LwwValue{T}.Timestamp"/> of every mutation authored
/// inside the scope to a caller-supplied
/// <see cref="LatticeIdempotencyKey"/>. Authoring cluster identity
/// is owned exclusively by the silo via
/// <see cref="LatticeOriginContext"/> /
/// <see cref="ILatticeOriginClusterIdResolver"/> and is
/// deliberately not part of the caller-supplied key.
/// </summary>
/// <remarks>
/// <para>
/// The context flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed <c>"ol.idk"</c>. When
/// <see cref="Current"/> is set, the public
/// <see cref="ILattice"/> mutating entry-points (<c>SetAsync</c>,
/// <c>DeleteAsync</c>, <c>DeleteRangeAsync</c>, <c>SetIfVersionAsync</c>,
/// <c>GetOrSetAsync</c>, and every CRDT accessor mutation that flows
/// through them) project the key's <see cref="LatticeIdempotencyKey.Timestamp"/>
/// into the existing <see cref="LatticeHlcOverrideContext"/> ambient
/// scope so the leaf-grain stamping path picks it up via the standard
/// mechanism. The result is that retries of the same operation under
/// the same key produce <see cref="LwwValue{T}"/> instances with
/// identical <see cref="LwwValue{T}.Timestamp"/> values - which the
/// WAL-append HWM dedup, the LWW merge tie-break, and the
/// <see cref="PnCounterAccessor"/> counter-side dedup guard collapse
/// to a single observable mutation.
/// </para>
/// <para>
/// When the context is unset (the default), the leaf grain falls back
/// to the standard <see cref="Orleans.Lattice.HybridLogicalClock.Tick(Orleans.Lattice.HybridLogicalClock)"/>
/// path and stamps the ambient <see cref="LatticeOriginContext"/>.
/// The dedup-on-retry behaviour is therefore strictly opt-in and adds
/// zero ambient cost when the scope is not entered.
/// </para>
/// </remarks>
public static class LatticeIdempotencyContext
{
    /// <summary>
    /// True when an idempotency scope is currently set on the ambient
    /// <see cref="RequestContext"/>. Cheaper than reading
    /// <see cref="Current"/> because the result is a <c>bool</c> rather
    /// than a boxed <see cref="LatticeIdempotencyKey"/> nullable; used
    /// by the public <see cref="ILattice"/> mutating entry-points to
    /// short-circuit the retry/scope plumbing on the (default) cold
    /// path so callers who never enter a scope pay no extra cost.
    /// </summary>
    public static bool IsActive =>
        RequestContext.Get(LatticeEventConstants.IdempotencyKeyRequestContextKey) is LatticeIdempotencyKey;

    /// <summary>
    /// Gets or sets the idempotency key on the ambient
    /// <see cref="RequestContext"/>. Setting <c>null</c> removes the
    /// key rather than storing a null value, matching the
    /// "no idempotency scope - fresh tick per call" default.
    /// </summary>
    public static LatticeIdempotencyKey? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.IdempotencyKeyRequestContextKey);
            return raw is LatticeIdempotencyKey key ? key : null;
        }
        set
        {
            if (value is null)
            {
                RequestContext.Remove(LatticeEventConstants.IdempotencyKeyRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.IdempotencyKeyRequestContextKey, value.Value);
            }
        }
    }

    /// <summary>
    /// Sets <see cref="Current"/> to <paramref name="key"/> for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <param name="key">
    /// The idempotency key to stamp onto mutations authored inside
    /// the scope, or <c>null</c> to explicitly clear the ambient
    /// context (so the leaf falls back to a fresh local tick).
    /// </param>
    public static IDisposable With(LatticeIdempotencyKey? key)
    {
        var previous = Current;
        Current = key;
        return new Scope(previous);
    }

    /// <summary>
    /// Convenience shorthand for <c>With(LatticeIdempotencyKey.Fresh())</c>:
    /// mints a fresh <see cref="LatticeIdempotencyKey"/> and opens a
    /// scope around it in one call. Use this when the caller only
    /// needs the scope's collapsing behaviour for retries inside the
    /// block and never reads the key back. Callers that need to
    /// inspect the key after the scope (e.g. to assert the stored
    /// HLC matches it) should mint it explicitly with
    /// <see cref="LatticeIdempotencyKey.Fresh"/> and pass it to
    /// <see cref="With"/>.
    /// </summary>
    public static IDisposable NewScope() => With(LatticeIdempotencyKey.Fresh());

    private sealed class Scope(LatticeIdempotencyKey? previous) : IDisposable
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
