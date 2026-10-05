namespace Orleans.Lattice.Replication.Grains;

/// <summary>A durable receiver-side poisoned saga entry.</summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReceiverSagaPoisonRecord)]
internal readonly record struct ReceiverSagaPoisonRecord(
    [property: Id(0)] string OriginClusterId,
    [property: Id(1)] Guid TransactionId,
    [property: Id(2)] string Reason);
