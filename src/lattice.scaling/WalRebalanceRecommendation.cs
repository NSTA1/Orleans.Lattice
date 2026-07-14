namespace Orleans.Lattice.Scaling;

/// <summary>
/// A suggestion to relocate one WAL partition's log from its current
/// <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> key to a different
/// one, to relieve storage pressure on the storage axis of a
/// <see cref="ScalingSignal"/>. Produced by the storage-axis collector (#1187)
/// when a hot account is found; <see cref="StoragePressure.Recommendation"/> is
/// <see langword="null"/> when no rebalance is warranted.
/// <para>
/// This is advisory and signal-only: acting on it means calling the
/// <see cref="Orleans.Lattice.ILatticeAdmin"/> move surface
/// (<see cref="Orleans.Lattice.ILatticeAdmin.PlanWalMoveAsync(string, int, string, System.Threading.CancellationToken)"/>
/// then
/// <see cref="Orleans.Lattice.ILatticeAdmin.ExecuteWalMoveAsync(string, int, string, Orleans.Lattice.WalMoveOptions, System.Threading.CancellationToken)"/>,
/// and later
/// <see cref="Orleans.Lattice.ILatticeAdmin.ReclaimMovedWalSourceAsync(string, int, string, System.Threading.CancellationToken)"/>).
/// The collector never performs the move itself.
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
    /// (<see cref="Tree"/>, <see cref="Partition"/>) log to when
    /// <see cref="HasHeadroom"/> is <see langword="true"/>. An empty string when
    /// <see cref="HasHeadroom"/> is <see langword="false"/> - every registered
    /// key is already hot, so there is no target to move to and the operator must
    /// provision or register another account first.
    /// </summary>
    [Id(3)] public string TargetProviderKey { get; init; }

    /// <summary>
    /// Human-readable explanation of why the move is recommended (for example,
    /// which retention threshold the current key crossed).
    /// </summary>
    [Id(4)] public string Rationale { get; init; }

    /// <summary>
    /// <see langword="true"/> when <see cref="TargetProviderKey"/> names a
    /// registered account with spare headroom to accept the partition;
    /// <see langword="false"/> when no registered key has headroom (every account
    /// is hot), in which case the remedy is to provision or register another
    /// account before any move can help.
    /// </summary>
    [Id(5)] public bool HasHeadroom { get; init; }

    /// <summary>
    /// Why the current account is hot - throughput-bound or capacity-bound - so
    /// the operator knows whether spreading throughput (a move) or adding
    /// retention will actually help. See <see cref="WalPressureClassification"/>.
    /// </summary>
    [Id(6)] public WalPressureClassification Classification { get; init; }
}
