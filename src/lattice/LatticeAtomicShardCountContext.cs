using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient signal used by the atomic-write saga coordinator
/// to communicate the saga's authoritative <i>touched-shard count</i>
/// down into the per-shard terminal-mutation publish helpers, so the
/// emitted <see cref="LatticeMutation"/> and downstream
/// <c>WalRecord</c> carry <see cref="LatticeMutation.AtomicShardCount"/>
/// for receiver-side cross-cluster all-or-nothing visibility gating.
/// </summary>
/// <remarks>
/// <para>
/// The saga's <c>BroadcastTerminalsAsync</c> sets this ambient to
/// <c>state.State.TouchedShards.Count</c> at the head of each
/// per-shard call. The per-shard
/// <c>ShardRootGrain.AppendTxTerminalAsync</c> reads
/// <see cref="Current"/> while assembling the terminal mutation and
/// stamps it onto <see cref="LatticeMutation.AtomicShardCount"/>.
/// Single-key non-saga writes and prepare-phase per-key writes leave
/// the ambient unset; the receiver-side cross-cluster gate treats
/// <c>0</c> as "no gating information" and falls back to the legacy
/// "mark on first terminal" semantics.
/// </para>
/// <para>
/// Late-pass shards discovered after the initial fan-out (orphan-window
/// closure inside <c>BroadcastTerminalsAsync</c>) observe a strictly
/// non-decreasing count because the saga only ever unions new
/// participants into <c>TouchedShards</c>. The receiver-side tally
/// takes <c>max(seen, incoming)</c> so a late-shipped terminal carrying
/// the updated count correctly raises the gate's expected total without
/// risking under-counting.
/// </para>
/// </remarks>
internal static class LatticeAtomicShardCountContext
{
    /// <summary>
    /// Gets or sets the ambient saga touched-shard count on the
    /// current <see cref="RequestContext"/>. Setting <c>null</c> or
    /// <c>0</c> removes the entry rather than storing a zero value,
    /// matching the "not in a saga" default.
    /// </summary>
    public static int? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.AtomicShardCountRequestContextKey);
            return raw is int count && count > 0 ? count : null;
        }
        set
        {
            if (value is null || value.Value <= 0)
            {
                RequestContext.Remove(LatticeEventConstants.AtomicShardCountRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.AtomicShardCountRequestContextKey,
                    value.Value);
            }
        }
    }

    /// <summary>
    /// Stamps <paramref name="count"/> as the ambient saga
    /// touched-shard count for the lifetime of the returned scope,
    /// restoring the prior value on <see cref="IDisposable.Dispose"/>.
    /// Safe to nest; disposal is idempotent. Passing <c>null</c> or
    /// a non-positive count explicitly clears the ambient.
    /// </summary>
    public static IDisposable With(int? count)
    {
        var previous = Current;
        Current = count;
        return new Scope(previous);
    }

    private sealed class Scope(int? previous) : IDisposable
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
