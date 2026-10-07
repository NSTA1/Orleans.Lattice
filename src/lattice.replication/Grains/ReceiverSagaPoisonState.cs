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

    /// <summary>
    /// Sagas whose poison a completed re-seed retired (issue #4692), oldest
    /// first and bounded. A saga here that has to be poisoned again failed for a
    /// reason the re-seed could not remove, so it is quarantined instead.
    /// </summary>
    [Id(2)] public List<ReceiverSagaPoisonRecord> Retired { get; set; } = new();

    /// <summary>
    /// Quarantined sagas (issue #4692): a record of one is parked without being
    /// applied, and the saga is never re-seeded again for that cause.
    /// </summary>
    [Id(3)] public List<ReceiverSagaPoisonRecord> Quarantined { get; set; } = new();
}
