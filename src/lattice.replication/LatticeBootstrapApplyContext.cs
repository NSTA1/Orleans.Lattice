using Orleans.Runtime;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal ambient flag that marks the calling
/// <see cref="IReplicationApplier"/> invocation as part of a
/// receiver-side bootstrap drain. While the flag is set, the applier
/// suppresses the per-origin high-water-mark dedup check and HWM
/// advance for the entry so the drain can deliver per-key prepared and
/// committed-projection rows whose source HLCs are not globally
/// ordered (the snapshot exporter walks shards and leaves in arbitrary
/// order, not in HLC order).
/// </summary>
/// <remarks>
/// <para>
/// In the steady state the producer's incremental WAL stream is
/// HLC-monotonic per origin, so the per-origin HWM check correctly
/// suppresses re-delivery. During bootstrap the same monotonicity
/// does not hold: prepared rows captured from a leaf's pending-tx
/// bucket carry the per-saga prepare-time HLC, and per-leaf scans
/// across shards can yield rows whose HLCs are interleaved relative
/// to the per-origin HWM. Without the suppression below, the first
/// shard's row for a saga can advance the HWM past a subsequent
/// shard's row for the same saga - dropping the second row as
/// "dedup" and leaving the saga's pending-tx bucket missing keys.
/// The matching terminal record then flips a partial bucket into a
/// strict-subset view, violating the per-saga all-or-nothing
/// invariant that the bootstrap-boundary contract owes the receiver.
/// </para>
/// <para>
/// The receiver-side dedup primitives that remain in force during a
/// bootstrap drain are sufficient to preserve idempotency:
/// </para>
/// <list type="bullet">
/// <item>
/// Leaf-level LWW merge in
/// <c>BPlusLeafGrain.AddPreparedMutation</c> and the canonical
/// <c>MergeManyAsync</c> path, so re-delivery of the same
/// <c>(txid, key)</c> prepare or the same committed projection is a
/// no-op.
/// </item>
/// <item>
/// Per-leaf <c>_recentlyTerminal</c> guard, so a re-arriving terminal
/// for a saga whose drain has already drained the bucket is dropped.
/// </item>
/// <item>
/// Per-tree <c>ITxRegistryGrain</c> "repeat-same-outcome no-op", so a
/// commit/abort mark on a transaction id that is already in the
/// requested terminal state is a no-op.
/// </item>
/// </list>
/// <para>
/// The handoff at the end of the drain
/// (<see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>)
/// monotonically establishes the per-origin HWM at the snapshot's
/// <c>AsOfHlc</c>, so steady-state dedup is preserved across the
/// transition. Range deletes, terminal records, and tombstone-reap
/// envelopes do not interact with the HWM check and are unaffected by
/// this scope.
/// </para>
/// </remarks>
internal static class LatticeBootstrapApplyContext
{
    /// <summary>
    /// RequestContext key used to propagate the flag. Internal to the
    /// replication package; the core library is unaware of the
    /// bootstrap protocol.
    /// </summary>
    internal const string RequestContextKey = "ol.bootapp";

    /// <summary>
    /// <see langword="true"/> when a bootstrap-drain scope is active
    /// on the current <see cref="RequestContext"/>; otherwise
    /// <see langword="false"/>.
    /// </summary>
    public static bool IsActive
    {
        get
        {
            var raw = RequestContext.Get(RequestContextKey);
            return raw is bool active && active;
        }
    }

    /// <summary>
    /// Marks the ambient context as a bootstrap-drain scope for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    /// <remarks>
    /// Returns a <see cref="Scope"/> value type by concrete type rather
    /// than via the <see cref="IDisposable"/> interface so the
    /// <c>using</c> statement binds <see cref="Scope.Dispose"/> directly
    /// without boxing - matching the allocation profile of the other
    /// RequestContext-scope helpers in the package
    /// (<c>LatticeAtomicBatchContext</c>, <c>LatticeRegistrySnapshotContext</c>,
    /// etc.). The result is one scope allocation per <c>using</c>
    /// block, paid on the stack frame.
    /// </remarks>
    public static Scope BeginScope()
    {
        var previous = RequestContext.Get(RequestContextKey) as bool?;
        RequestContext.Set(RequestContextKey, true);
        return new Scope(previous);
    }

    /// <summary>
    /// Disposable scope that restores the previous bootstrap-drain
    /// flag on the ambient <see cref="RequestContext"/>. Allocated on
    /// the caller's stack frame by the <c>using</c> statement so
    /// opening a scope does not generate heap pressure on the drain
    /// hot path. Disposal is idempotent; double-dispose is safe.
    /// </summary>
    public struct Scope : IDisposable
    {
        private readonly bool? _previous;
        private bool _disposed;

        internal Scope(bool? previous)
        {
            _previous = previous;
            _disposed = false;
        }

        /// <summary>
        /// Restores the previous bootstrap-drain flag on the ambient
        /// <see cref="RequestContext"/>. Idempotent; a second call is a
        /// no-op rather than overwriting the now-restored flag.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_previous is null)
            {
                RequestContext.Remove(RequestContextKey);
            }
            else
            {
                RequestContext.Set(RequestContextKey, _previous.Value);
            }
        }
    }
}
