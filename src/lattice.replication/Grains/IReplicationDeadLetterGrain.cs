namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree dead-letter queue grain. Holds bounded FIFO storage for
/// <see cref="ReplogEntry"/> records the inbound apply pipeline could
/// not install after exhausting
/// <see cref="LatticeReplicationOptions.MaxApplyRetries"/> consecutive
/// retries on the same
/// <c>(treeId, originClusterId, timestamp, key, op)</c> tuple.
/// <para>
/// Grain key format: the tree id verbatim. A replicated tree therefore
/// has at most one DLQ activation regardless of how many origin
/// clusters publish to it; the entry''s
/// <see cref="ReplogEntry.OriginClusterId"/> remains carried inside
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
    /// Returns the assigned <see cref="DeadLetterEntry.EntryId"/>. When
    /// the queue is at
    /// <see cref="LatticeReplicationOptions.DeadLetterQueueCapacity"/>
    /// the oldest entry is evicted to make room (FIFO).
    /// </summary>
    Task<long> EnqueueAsync(ReplogEntry entry, string failureReason, int retryCount, CancellationToken cancellationToken);

    /// <summary>Returns every parked entry in ascending entry-id order. Empty list when the queue is empty.</summary>
    Task<IReadOnlyList<DeadLetterEntry>> ListAsync(CancellationToken cancellationToken);

    /// <summary>Returns the number of entries currently parked.</summary>
    Task<int> CountAsync(CancellationToken cancellationToken);

    /// <summary>Removes the entry with the supplied id. Returns <c>true</c> on removal; <c>false</c> otherwise.</summary>
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
}

