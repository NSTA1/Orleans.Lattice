namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The poisoned and quarantined sagas among a set of transaction ids from one
/// origin (issue #4692), read in one call by the receiver's record filter.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReceiverSagaPoisonClassification)]
internal sealed record ReceiverSagaPoisonClassification
{
    /// <summary>No saga poisoned or quarantined.</summary>
    public static ReceiverSagaPoisonClassification Empty { get; } = new();

    /// <summary>The poisoned sagas: their terminals are withheld until a re-seed retires the poison.</summary>
    [Id(0)] public IReadOnlyCollection<Guid> Poisoned { get; init; } = Array.Empty<Guid>();

    /// <summary>The quarantined sagas: their records are parked without being applied.</summary>
    [Id(1)] public IReadOnlyCollection<Guid> Quarantined { get; init; } = Array.Empty<Guid>();
}
