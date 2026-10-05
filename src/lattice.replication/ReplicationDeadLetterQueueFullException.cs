namespace Orleans.Lattice.Replication;

/// <summary>
/// Thrown by the per-tree replication dead-letter queue when it already holds
/// <see cref="LatticeReplicationOptions.DeadLetterQueueCapacity"/> entries
/// (#4603). The queue no longer evicts its oldest entry to make room: every
/// parked entry was acknowledged without being applied, so evicting it would
/// lose the write for good. A caller that cannot park an entry keeps it
/// unacknowledged instead - the applier defers it, so the sender keeps its
/// cursor and re-ships it, and the shipper does not advance past it - until an
/// operator replays or discards parked entries.
/// <para>
/// Derives directly from <see cref="Exception"/> so the same-silo deep copy of a
/// grain result needs no registered copier.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationDeadLetterQueueFullException)]
internal sealed class ReplicationDeadLetterQueueFullException : Exception
{
    /// <summary>The tree whose dead-letter queue is full.</summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>The configured capacity the queue has reached.</summary>
    [Id(1)]
    public int Capacity { get; }

    /// <summary>Initialises a new instance with a default message.</summary>
    public ReplicationDeadLetterQueueFullException()
        : base("The replication dead-letter queue is full.")
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance with <paramref name="message"/>.</summary>
    public ReplicationDeadLetterQueueFullException(string message)
        : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance for <paramref name="treeId"/> at <paramref name="capacity"/>.</summary>
    public ReplicationDeadLetterQueueFullException(string treeId, int capacity)
        : base($"The replication dead-letter queue for tree '{treeId}' is full ({capacity} entries); "
            + "the entry is kept unacknowledged until an operator replays or discards parked entries.")
    {
        TreeId = treeId;
        Capacity = capacity;
    }
}
