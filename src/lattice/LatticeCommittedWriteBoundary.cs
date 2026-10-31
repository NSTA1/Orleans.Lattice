using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal scope that detaches a side write from the write that caused it, so
/// the side write lands as an ordinary committed write of its own (issue #5297).
/// For the lifetime of the returned scope every ambient
/// <see cref="RequestContext"/> entry that shapes how a write is applied is
/// cleared - the saga transaction id and prepare flag (and every prepare,
/// forward and atomic-batch stamp), the replication origin, vector clock, carried
/// clock and apply offset, the author delta, the CRDT join flag, a conditional
/// predicate and an idempotency key - and the prior values are restored on
/// <see cref="IDisposable.Dispose"/>.
/// <para>
/// Orleans propagates <see cref="RequestContext"/> entries across grain calls.
/// A side write issued while a replicated atomic-batch prepare is being judged
/// would otherwise inherit that prepare's context: the destination leaf stages it
/// in a pending bucket under the batch's transaction id, which no terminal ever
/// drains on the side write's tree, so it is never committed. A schema
/// dead-letter append is the case this exists for: the contract is that the
/// record is durable once the diversion is reported, whatever the diverted
/// write's own transaction does.
/// </para>
/// <para>
/// Identity and intent are preserved: the access-gate system-origin flag, the
/// internal-origin marker, the caller's credential and active tenant, the
/// maintenance tag, the read-side registry snapshot and the call-start stamp
/// describe the containing call rather than the write being applied. The
/// bootstrap-shadow route is keyed by the logical tree it reroutes, so it is
/// inert for any other tree and is left in place.
/// </para>
/// <para>
/// Returns <see cref="NoOpDisposable.Instance"/> without allocating when none of
/// the entries is present.
/// </para>
/// </summary>
internal static class LatticeCommittedWriteBoundary
{
    private static readonly string[] DetachedKeys =
    [
        LatticeEventConstants.TransactionIdRequestContextKey,
        LatticeEventConstants.PreparedRequestContextKey,
        LatticeEventConstants.ForwardedPrepareRequestContextKey,
        LatticeEventConstants.PreparedRouteRequestContextKey,
        LatticeEventConstants.OriginalPrepareStampsRequestContextKey,
        LatticeEventConstants.AtomicBoundPhysicalTreeRequestContextKey,
        LatticeEventConstants.SagaDecisionDeadlineRequestContextKey,
        LatticeEventConstants.AtomicBatchRequestContextKey,
        LatticeEventConstants.AtomicBatchIndexMapRequestContextKey,
        LatticeEventConstants.AtomicBatchDeltaMapRequestContextKey,
        LatticeEventConstants.AtomicBatchDeleteSetRequestContextKey,
        LatticeEventConstants.AtomicShardCountRequestContextKey,
        LatticeEventConstants.CrossTreeTerminalRequestContextKey,
        LatticeEventConstants.DeltaRequestContextKey,
        LatticeEventConstants.OriginClusterIdRequestContextKey,
        LatticeEventConstants.VectorClockRequestContextKey,
        LatticeEventConstants.HlcOverrideRequestContextKey,
        LatticeEventConstants.FreshStampRequestContextKey,
        LatticeEventConstants.ApplyOffsetRequestContextKey,
        LatticeEventConstants.ApplyOffsetPartitionRequestContextKey,
        LatticeEventConstants.CrdtJoinMergeRequestContextKey,
        LatticeEventConstants.PredicateRequestContextKey,
        LatticeEventConstants.IdempotencyKeyRequestContextKey,
    ];

    /// <summary>
    /// Enters the boundary: clears every write-shaping ambient entry until the
    /// returned scope is disposed. Safe to nest; disposal is idempotent.
    /// </summary>
    public static IDisposable Enter()
    {
        object?[]? saved = null;
        for (var i = 0; i < DetachedKeys.Length; i++)
        {
            var value = RequestContext.Get(DetachedKeys[i]);
            if (value is null)
            {
                continue;
            }

            saved ??= new object?[DetachedKeys.Length];
            saved[i] = value;
            RequestContext.Remove(DetachedKeys[i]);
        }

        return saved is null ? NoOpDisposable.Instance : new Scope(saved);
    }

    private sealed class Scope(object?[] saved) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            for (var i = 0; i < DetachedKeys.Length; i++)
            {
                if (saved[i] is { } value)
                {
                    RequestContext.Set(DetachedKeys[i], value);
                }
            }
        }
    }
}
