namespace Orleans.Lattice.Replication;

/// <summary>
/// Commit-time doorbell nudge seam. The durable replication log is now
/// written exclusively by the foreground leaf commit-log writer in the
/// core assembly; this sink no longer appends anything. Its sole job is
/// to wake the background log-tailing shipper(s) for a committed tree -
/// reactivating one that had been deactivated - so the shipper's phase timer
/// drains the new entries; the sink never ships anything itself. The default
/// registration is the doorbell-ringing <see cref="ShardedReplogSink"/>.
/// <para>
/// Implementations are invoked synchronously inside the grain's
/// scheduler via the core <see cref="IMutationObserver"/> hook, so every
/// millisecond spent inside <see cref="WriteAsync"/> is added to the
/// caller's write latency. Implementations must complete quickly or
/// enqueue the work onto a background drain.
/// </para>
/// </summary>
internal interface IReplogSink
{
    /// <summary>
    /// Nudges the background shipper(s) for a committed tree.
    /// </summary>
    /// <param name="treeId">The identifier of the tree that just committed.</param>
    /// <param name="cancellationToken">Cancellation token propagated from the grain call.</param>
    Task WriteAsync(string treeId, CancellationToken cancellationToken);
}
