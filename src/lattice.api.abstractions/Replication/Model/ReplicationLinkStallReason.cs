namespace Orleans.Lattice.Api.Replication;

/// <summary>The operator-actionable cause of a stalled replication link.</summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationLinkStallReason)]
public enum ReplicationLinkStallReason
{
    /// <summary>The peer lost records to a WAL trim and requires a snapshot re-seed.</summary>
    ReseedRequired = 0,

    /// <summary>The peer's dead-letter queue is full and is withholding the next entry.</summary>
    DeadLetterQueueFull = 1,
}
