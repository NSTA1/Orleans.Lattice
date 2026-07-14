namespace Orleans.Lattice.Scaling;

/// <summary>
/// Normalised storage-axis pressure for the cluster, one of the two axes of a
/// <see cref="ScalingSignal"/>. Reports whether retained write-ahead-log bytes
/// have crossed a configured threshold, the aggregate retained bytes, a
/// per-catalogue-key breakdown, and an optional rebalance suggestion.
/// <para>
/// This is a read-only point-in-time snapshot collected by the storage-axis
/// pressure collector. Before the first sample completes the facade returns a
/// not-over-threshold, zero-byte instance with an empty account list and no
/// recommendation.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ScalingTypeAliases.StoragePressure)]
[Immutable]
public readonly record struct StoragePressure
{
    [Id(2)] private readonly IReadOnlyList<WalAccountPressure>? _accounts;

    /// <summary>
    /// <see langword="true"/> when aggregate retained WAL bytes have crossed the
    /// configured storage-pressure threshold, signalling the storage axis is
    /// under-provisioned. <see langword="false"/> when retained bytes are within
/// the configured threshold.
    /// </summary>
    [Id(0)] public bool OverThreshold { get; init; }

    /// <summary>
    /// Total bytes of write-ahead log currently retained across every
    /// catalogue key in the cluster.
    /// </summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>
    /// Per-catalogue-key WAL storage breakdown. Never <see langword="null"/>:
    /// defaults to an empty list (including for a default-constructed value), so
    /// callers can enumerate without a null check.
    /// </summary>
    public IReadOnlyList<WalAccountPressure> Accounts
    {
        get => _accounts ?? Array.Empty<WalAccountPressure>();
        init => _accounts = value;
    }

    /// <summary>
    /// Optional suggestion to relocate a WAL partition's log to relieve storage
    /// pressure, or <see langword="null"/> when no rebalance is recommended.
    /// </summary>
    [Id(3)] public WalRebalanceRecommendation? Recommendation { get; init; }
}
