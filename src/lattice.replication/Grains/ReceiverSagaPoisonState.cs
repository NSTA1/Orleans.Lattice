namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="ReceiverSagaPoisonGrain"/>. Entries are
/// keyed logically by origin cluster id plus transaction id.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReceiverSagaPoisonState)]
internal sealed class ReceiverSagaPoisonState
{
    /// <summary>The poisoned saga entries.</summary>
    [Id(0)] public List<ReceiverSagaPoisonRecord> Entries { get; set; } = new();

    /// <summary>
    /// Origin cluster ids for which at least one poison was recorded but a
    /// bootstrap kickoff has not yet been accepted.
    /// </summary>
    [Id(1)] public List<string> ReseedOwedOrigins { get; set; } = new();
}
