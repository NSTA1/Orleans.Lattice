using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal scope that strips every user-saga and replication ambient
/// <see cref="RequestContext"/> entry for the lifetime of the returned
/// scope, restoring the prior values on <see cref="IDisposable.Dispose"/>.
/// Used at the boundary between a user-tree call and an internal
/// system-tree call (e.g. registering the user tree in
/// <see cref="LatticeConstants.RegistryTreeId"/> from inside a saga's
/// prepare phase) so the system-tree write does not inherit the
/// user-tree saga's prepared/transaction context.
/// <para>
/// Orleans propagates <see cref="RequestContext"/> entries automatically
/// across grain calls. Without this boundary, a receiver-side
/// <c>ApplyPreparedSetAsync</c> running under
/// <see cref="LatticePreparedContext"/> + <see cref="LatticeTransactionContext"/>
/// would route a tree-registry registration into the system-tree leaf's
/// per-transaction pending bucket - which never sees a matching
/// terminal (the system tree has its own per-tree <c>TxRegistry</c>),
/// leaving an orphan pending bucket that gates subsequent reads.
/// </para>
/// <para>
/// This scope is unrelated to <see cref="LatticeRegistrySnapshotContext"/>
/// (which carries a read-side registry snapshot for scans) and
/// <see cref="LatticeMaintenanceContext"/> (which tags background
/// maintenance writes). Both are preserved across the boundary because
/// they describe the *containing* call's intent rather than user-saga
/// linearization.
/// </para>
/// </summary>
internal static class LatticeSystemTreeBoundary
{
    /// <summary>
    /// Enters a system-tree call boundary: every user-saga and
    /// replication ambient <see cref="RequestContext"/> entry is
    /// cleared for the lifetime of the returned scope and restored on
    /// dispose. Safe to nest; disposal is idempotent. Returns
    /// <see cref="EmptyDisposable.Instance"/> when no user-saga context
    /// is active to avoid the per-call allocation on the foreground
    /// fast path.
    /// </summary>
    public static IDisposable Enter()
    {
        // Snapshot every key we intend to clear. If none are present
        // there is nothing to restore and we can return a shared
        // no-allocation disposable.
        var txId = RequestContext.Get(LatticeEventConstants.TransactionIdRequestContextKey);
        var prepared = RequestContext.Get(LatticeEventConstants.PreparedRequestContextKey);
        var origin = RequestContext.Get(LatticeEventConstants.OriginClusterIdRequestContextKey);
        var hlcOverride = RequestContext.Get(LatticeEventConstants.HlcOverrideRequestContextKey);
        var vectorClock = RequestContext.Get(LatticeEventConstants.VectorClockRequestContextKey);
        var atomicBatch = RequestContext.Get(LatticeEventConstants.AtomicBatchRequestContextKey);
        var atomicShardCount = RequestContext.Get(LatticeEventConstants.AtomicShardCountRequestContextKey);
        var applyOffset = RequestContext.Get(LatticeEventConstants.ApplyOffsetRequestContextKey);

        if (txId is null
            && prepared is null
            && origin is null
            && hlcOverride is null
            && vectorClock is null
            && atomicBatch is null
            && atomicShardCount is null
            && applyOffset is null)
        {
            return EmptyDisposable.Instance;
        }

        RequestContext.Remove(LatticeEventConstants.TransactionIdRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.PreparedRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.OriginClusterIdRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.HlcOverrideRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.VectorClockRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.AtomicBatchRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.AtomicShardCountRequestContextKey);
        RequestContext.Remove(LatticeEventConstants.ApplyOffsetRequestContextKey);

        return new Scope(txId, prepared, origin, hlcOverride, vectorClock, atomicBatch, atomicShardCount, applyOffset);
    }

    private sealed class EmptyDisposable : IDisposable
    {
        public static readonly EmptyDisposable Instance = new();
        public void Dispose() { }
    }

    private sealed class Scope(
        object? txId,
        object? prepared,
        object? origin,
        object? hlcOverride,
        object? vectorClock,
        object? atomicBatch,
        object? atomicShardCount,
        object? applyOffset) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Restore(LatticeEventConstants.TransactionIdRequestContextKey, txId);
            Restore(LatticeEventConstants.PreparedRequestContextKey, prepared);
            Restore(LatticeEventConstants.OriginClusterIdRequestContextKey, origin);
            Restore(LatticeEventConstants.HlcOverrideRequestContextKey, hlcOverride);
            Restore(LatticeEventConstants.VectorClockRequestContextKey, vectorClock);
            Restore(LatticeEventConstants.AtomicBatchRequestContextKey, atomicBatch);
            Restore(LatticeEventConstants.AtomicShardCountRequestContextKey, atomicShardCount);
            Restore(LatticeEventConstants.ApplyOffsetRequestContextKey, applyOffset);
        }

        private static void Restore(string key, object? value)
        {
            if (value is null)
            {
                return;
            }
            RequestContext.Set(key, value);
        }
    }
}
