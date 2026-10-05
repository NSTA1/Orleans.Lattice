using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree.Grains;
namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree dead-letter queue grain. Holds bounded FIFO storage for
/// <see cref="WalRecord"/> records the inbound apply pipeline could
/// not install after exhausting
/// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> consecutive
/// retries on the same
/// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple.
/// <para>
/// Grain key format: the tree id verbatim. A replicated tree therefore
/// has at most one DLQ activation regardless of how many origin
/// clusters publish to it; the entry's
/// <see cref="WalRecord.OriginClusterId"/> remains carried inside
/// the parked <see cref="DeadLetterEntry.Entry"/> for diagnostic
/// fan-out by the inspection seam.
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationDeadLetterGrain)]
internal interface IReplicationDeadLetterGrain : IGrainWithStringKey
{
    /// <summary>
    /// Parks <paramref name="entry"/> on the queue with the supplied
    /// <paramref name="failureReason"/> and <paramref name="retryCount"/>.
    /// Returns the assigned <see cref="DeadLetterEntry.EntryId"/>. Idempotent
    /// by the entry's <c>(origin, timestamp, key, op)</c> identity: parking an
    /// entry that is already parked returns its existing id. When the queue
    /// already holds
    /// <see cref="LatticeReplicationOptions.DeadLetterQueueCapacity"/> entries
    /// the enqueue is refused with
    /// <see cref="ReplicationDeadLetterQueueFullException"/> - the queue never
    /// evicts, because every parked entry is the only copy of an acknowledged
    /// write (#4603) - and the caller must keep the entry unacknowledged.
    /// <para>
    /// <paramref name="reasonTag"/> is the canonical reason value
    /// stamped on the
    /// <c>orleans.lattice.replication.dead_letter.enqueued</c> counter
    /// for this enqueue. Callers pick from the <c>Reason*</c>
    /// constants on <see cref="LatticeReplicationMetrics"/>
    /// (<see cref="LatticeReplicationMetrics.ReasonSchema"/>,
    /// <see cref="LatticeReplicationMetrics.ReasonHlcSkew"/>,
    /// <see cref="LatticeReplicationMetrics.ReasonOversized"/>,
    /// <see cref="LatticeReplicationMetrics.ReasonUnknown"/>) so the
    /// <c>reason</c> dimension stays stable across publishers.
    /// </para>
    /// </summary>
    Task<long> EnqueueAsync(WalRecord entry, string failureReason, int retryCount, string reasonTag, CancellationToken cancellationToken);

    /// <summary>Returns every parked entry in ascending entry-id order. Empty list when the queue is empty.</summary>
    Task<IReadOnlyList<DeadLetterEntry>> ListAsync(CancellationToken cancellationToken);

    /// <summary>Returns the number of entries currently parked.</summary>
    Task<int> CountAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Removes the entry with the supplied id without applying it. Returns
    /// <c>true</c> on removal; <c>false</c> otherwise. A foreign-origin entry's
    /// write is then lost on this cluster for good, so its identity is first
    /// recorded as lost on the tree's high-water-mark grain
    /// (<see cref="IReplicationHighWaterMarkGrain.RecordLostAsync"/>), and an
    /// entry that depends on it is dead-lettered rather than released (#4603).
    /// </summary>
    Task<bool> DiscardAsync(long entryId, CancellationToken cancellationToken);

    /// <summary>
    /// Removes the entry with the supplied id after a successful
    /// replay. Behaves identically to <see cref="DiscardAsync(long, CancellationToken)"/>
    /// but tags the <c>orleans.lattice.replication.dead_letter.removed</c>
    /// counter with <c>reason=replayed</c> rather than
    /// <c>reason=discarded</c>.
    /// </summary>
    Task<bool> RemoveReplayedAsync(long entryId, CancellationToken cancellationToken);

    /// <summary>Returns the parked entry with the supplied id, or <c>null</c> when no such entry is parked.</summary>
    Task<DeadLetterEntry?> TryGetAsync(long entryId, CancellationToken cancellationToken);

    /// <summary>
    /// Whether the queue holds a parked write of <paramref name="originClusterId"/>
    /// at <paramref name="timestamp"/> (issue #4586). An entry counts as held from
    /// its durable enqueue until its durable removal. Interleaves, so the origin
    /// frontier grain can ask while this grain waits on it.
    /// </summary>
    [AlwaysInterleave]
    Task<bool> IsHoldingAsync(string originClusterId, HybridLogicalClock timestamp, CancellationToken cancellationToken);
}

