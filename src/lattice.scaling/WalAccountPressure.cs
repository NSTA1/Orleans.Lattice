namespace Orleans.Lattice.Scaling;

/// <summary>
/// Per-account write-ahead-log storage pressure for a single
/// <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> key, contributing
/// to the storage axis of a <see cref="ScalingSignal"/>. The storage-axis
/// collector (#1187) populates one of these per catalogue key that backs a WAL
/// partition, with a throughput-bound vs capacity-bound
/// <see cref="Classification"/>.
/// </summary>
[GenerateSerializer]
[Alias(ScalingTypeAliases.WalAccountPressure)]
[Immutable]
public readonly record struct WalAccountPressure
{
    /// <summary>
    /// The <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> catalogue
    /// key (the "account") whose retained WAL bytes this entry reports.
    /// </summary>
    [Id(0)] public string ProviderKey { get; init; }

    /// <summary>
    /// Bytes of write-ahead log currently retained against
    /// <see cref="ProviderKey"/> across every partition it backs.
    /// </summary>
    [Id(1)] public long WalRetainedBytes { get; init; }

    /// <summary>
    /// Worst-case <see cref="Orleans.Lattice.WalSaturationState"/> classification
    /// observed on partitions backed by <see cref="ProviderKey"/>. Serves as the
    /// per-account saturation indicator the storage-axis collector (#1187) uses
    /// to decide whether the account is over its retention threshold.
    /// </summary>
    [Id(2)] public WalSaturationState Saturation { get; init; }

    /// <summary>
    /// Why this account is under pressure - throughput-bound (backend write rate
    /// topped out), capacity-bound (retained bytes over the advisory threshold),
    /// or <see cref="WalPressureClassification.None"/> when healthy. A healthy
    /// account (the default) reports <see cref="WalPressureClassification.None"/>.
    /// </summary>
    [Id(3)] public WalPressureClassification Classification { get; init; }

    /// <summary>
    /// <see langword="true"/> when this account's retained WAL bytes have crossed
    /// the advisory fraction of
    /// <see cref="Orleans.Lattice.LatticeOptions.WalMaxRetainedBytes"/> - the
    /// capacity-bound trigger. <see langword="false"/> for a healthy or a purely
    /// throughput-bound account.
    /// </summary>
    [Id(4)] public bool OverThreshold { get; init; }
}
