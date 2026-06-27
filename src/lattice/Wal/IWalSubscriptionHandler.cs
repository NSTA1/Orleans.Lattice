using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Wal;

/// <summary>
/// Consumer-supplied callback that receives the ordered entries surfaced by
/// <see cref="IWalSubscriber.DrainAsync"/>. The handler owns the
/// consumer-specific apply: a view projection, an atomic-batch staging buffer
/// keyed by <see cref="WalSubscriptionEntry.TransactionId"/>, a replication
/// shipper, or any other sink. The generic tailing mechanics (cursored read,
/// fall-off-log detection, ShardIndex / maintenance filtering, dynamic shard
/// onboarding, back-pressure, WAL pinning) live in the subscriber.
/// <para>
/// <see cref="OnEntry"/> is invoked synchronously on the consumer's scheduler
/// in ascending offset order within a partition; it must not start
/// asynchronous work that outlives the call. A handler that needs to do async
/// apply work buffers the surfaced entries and flushes them after
/// <see cref="IWalSubscriber.DrainAsync"/> returns.
/// </para>
/// </summary>
internal interface IWalSubscriptionHandler
{
    /// <summary>
    /// Receives a single surfaced WAL entry. Called once per entry that passes
    /// the subscriber's ShardIndex and maintenance filters, strictly in
    /// ascending offset order within each partition.
    /// </summary>
    /// <param name="entry">The surfaced entry. Passed by reference to avoid copying the record struct.</param>
    void OnEntry(in WalSubscriptionEntry entry);

    /// <summary>
    /// The lowest <see cref="HybridLogicalClock"/> of any partially-buffered
    /// atomic batch the handler is currently holding, or <see langword="null"/>
    /// when the handler has no incomplete batch buffered. Read once at the end of
    /// a drain pass; when non-<see langword="null"/> the subscriber reports it as
    /// the handler's blocked-floor pin so the WAL garbage collector does not trim
    /// past an entry the handler still needs to reassemble its batch. The default
    /// implementation reports no pin.
    /// </summary>
    HybridLogicalClock? BlockedAtHlc => null;
}
