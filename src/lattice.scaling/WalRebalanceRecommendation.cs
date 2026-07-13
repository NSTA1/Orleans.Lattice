namespace Orleans.Lattice.Scaling;

/// <summary>
/// A forward-compatible suggestion to relocate one WAL partition's log from its
/// current <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> key to a
/// different one, to relieve storage pressure on the storage axis of a
/// <see cref="ScalingSignal"/>.
/// <para>
/// The scaffold defines only the minimal shape; the storage-axis issue (#1187)
/// fully defines the recommendation semantics and populates it. The scaffold
/// facade never emits a recommendation
/// (<see cref="StoragePressure.Recommendation"/> is <see langword="null"/>).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ScalingTypeAliases.WalRebalanceRecommendation)]
[Immutable]
public readonly record struct WalRebalanceRecommendation
{
    /// <summary>The tree whose WAL partition the recommendation applies to.</summary>
    [Id(0)] public string Tree { get; init; }

    /// <summary>The WAL partition index within <see cref="Tree"/> to relocate.</summary>
    [Id(1)] public int Partition { get; init; }

    /// <summary>
    /// The catalogue key that currently backs the
    /// (<see cref="Tree"/>, <see cref="Partition"/>) log.
    /// </summary>
    [Id(2)] public string CurrentProviderKey { get; init; }

    /// <summary>
    /// The suggested catalogue key to move the
    /// (<see cref="Tree"/>, <see cref="Partition"/>) log to.
    /// </summary>
    [Id(3)] public string TargetProviderKey { get; init; }

    /// <summary>
    /// Human-readable explanation of why the move is recommended (for example,
    /// which retention threshold the current key crossed).
    /// </summary>
    [Id(4)] public string Rationale { get; init; }
}
