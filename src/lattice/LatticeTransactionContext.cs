using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient transaction-id context used to stamp
/// <see cref="LatticeMutation.TransactionId"/> onto mutations authored by
/// the current logical call. Public callers do not interact with this
/// type — observers read <see cref="LatticeMutation.TransactionId"/>
/// directly.
/// </summary>
/// <remarks>
/// <para>
/// The transaction id flows on the inbound write path via an Orleans
/// <see cref="RequestContext"/> entry keyed
/// <see cref="LatticeEventConstants.TransactionIdRequestContextKey"/>.
/// Public <see cref="ILattice"/> write entry-points call
/// <see cref="EnsureCurrent"/> at method start so a fresh
/// <see cref="System.Guid"/> is generated for callers that have not
/// provided one. The <c>AtomicWriteGrain</c> stamps the saga's persisted
/// transaction id explicitly via <see cref="Set(Guid)"/>;
/// <see cref="EnsureCurrent"/> preserves an existing non-empty value
/// rather than overwriting.
/// </para>
/// <para>
/// The publish helpers (<c>BPlusLeafGrain.MutationObserver</c> /
/// <c>ShardRootGrain.MutationObserver</c>) read <see cref="Current"/>
/// when constructing the <see cref="LatticeMutation"/> payload. Reads
/// fall back to <see cref="System.Guid.Empty"/> when the context is
/// missing — the convergence-only paths that publish without a leading
/// public entry-point produce an empty id rather than fabricate a fresh
/// one, which matches the documented "wire-compatible default" semantics
/// for legacy persisted observers.
/// </para>
/// </remarks>
internal static class LatticeTransactionContext
{
    /// <summary>
    /// Gets the current ambient transaction id, or <see cref="Guid.Empty"/>
    /// when none is set on the <see cref="RequestContext"/>.
    /// </summary>
    public static Guid Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.TransactionIdRequestContextKey);
            return raw is Guid g ? g : Guid.Empty;
        }
    }

    /// <summary>
    /// Returns the current ambient transaction id; if none is set, mints
    /// a fresh <see cref="Guid"/> and stores it on the
    /// <see cref="RequestContext"/> so subsequent grain calls — and the
    /// publish helpers that read <see cref="Current"/> — see the same id.
    /// </summary>
    public static Guid EnsureCurrent()
    {
        var raw = RequestContext.Get(LatticeEventConstants.TransactionIdRequestContextKey);
        if (raw is Guid existing && existing != Guid.Empty)
        {
            return existing;
        }

        var fresh = Guid.NewGuid();
        RequestContext.Set(LatticeEventConstants.TransactionIdRequestContextKey, fresh);
        return fresh;
    }

    /// <summary>
    /// Sets the ambient transaction id to <paramref name="transactionId"/>,
    /// overwriting any existing value. Used by <c>AtomicWriteGrain</c> to
    /// stamp the saga's persisted id onto every per-key write it makes.
    /// Setting <see cref="Guid.Empty"/> removes the key rather than
    /// storing the default value.
    /// </summary>
    public static void Set(Guid transactionId)
    {
        if (transactionId == Guid.Empty)
        {
            RequestContext.Remove(LatticeEventConstants.TransactionIdRequestContextKey);
            return;
        }

        RequestContext.Set(LatticeEventConstants.TransactionIdRequestContextKey, transactionId);
    }
}
