namespace Orleans.Lattice.Wal;

/// <summary>
/// Internal seam over the per-shard write-ahead log tailing loop. Generalises
/// the bespoke "tail every source WAL partition from a durable checkpoint and
/// fan ordered entries out to a consumer" primitive that the materialised-view
/// maintainer first built directly on <see cref="BPlusTree.Grains.ICommitLogReader"/>
/// + <see cref="IWalCursorRegistry"/> into one reusable component every WAL
/// consumer (views, the replication producer, a future change-feed / audit
/// sink) can share.
/// <para>
/// A consumer owns its durable per-partition checkpoint and drives the loop by
/// calling <see cref="DrainAsync"/> from its own timer / reminder cadence,
/// passing the checkpoint in a <see cref="WalSubscriptionContext"/> and an
/// <see cref="IWalSubscriptionHandler"/> for the consumer-specific apply. Each
/// call reads a bounded batch per partition, surfaces the ordered entries that
/// pass the configured ShardIndex / maintenance filters, pins the WAL via the
/// cursor registry, and returns the advanced offsets and high-water mark for
/// the consumer to persist.
/// </para>
/// <para>
/// Resolved from DI as a singleton. The seam is internal: it is built on the
/// internal <see cref="BPlusTree.Grains.ICommitLogReader"/> and is not part of
/// the public API contract.
/// </para>
/// </summary>
internal interface IWalSubscriber
{
    /// <summary>
    /// Runs one drain pass over the source tree's WAL described by
    /// <paramref name="context"/>, surfacing ordered entries to
    /// <paramref name="handler"/>.
    /// <para>
    /// Behaviour provided once for every consumer:
    /// </para>
    /// <list type="bullet">
    /// <item><description>
    /// <b>Fall-off-log detection.</b> Before reading, each partition's oldest
    /// still-readable offset is compared against the checkpoint; if the WAL was
    /// trimmed past the resume point the pass surfaces nothing, advances nothing,
    /// and returns <see cref="WalDrainResult.FellOffLog"/> so the consumer can
    /// rebuild / bootstrap.
    /// </description></item>
    /// <item><description>
    /// <b>Cursored, bounded read.</b> Each partition is read from its checkpoint
    /// in ascending offset order, capped at <see cref="WalSubscriptionContext.BatchSize"/>.
    /// </description></item>
    /// <item><description>
    /// <b>ShardIndex partition filtering and maintenance filtering.</b> Entries
    /// that do not match <see cref="WalSubscriptionContext.ShardIndexFilter"/>, and
    /// maintenance entries under <see cref="WalMaintenancePolicy.Skip"/>, are not
    /// surfaced but the cursor still advances past them.
    /// </description></item>
    /// <item><description>
    /// <b>Dynamic shard onboarding.</b> The pass reads every partition in
    /// <c>[0, context.Partitions)</c>, so a partition count that grew since the
    /// last drain is picked up automatically.
    /// </description></item>
    /// <item><description>
    /// <b>WAL pinning.</b> When <see cref="WalSubscriptionContext.PinWal"/> is set,
    /// the drained HLC cursor and the handler's
    /// <see cref="IWalSubscriptionHandler.BlockedAtHlc"/> pin are reported to the
    /// cursor registry under <see cref="WalSubscriptionContext.ConsumerId"/>.
    /// </description></item>
    /// </list>
    /// </summary>
    /// <param name="context">The drain configuration and durable checkpoint.</param>
    /// <param name="handler">The consumer's per-entry apply callback.</param>
    /// <param name="cancellationToken">Cancellation observed between page reads and surfaced entries.</param>
    /// <returns>The advanced offsets, high-water mark, and fall-off-log signal for the consumer to persist and act on.</returns>
    Task<WalDrainResult> DrainAsync(
        WalSubscriptionContext context,
        IWalSubscriptionHandler handler,
        CancellationToken cancellationToken = default);
}
