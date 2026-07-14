namespace Orleans.Lattice.Scaling;

/// <summary>
/// The provider placement of a single WAL partition within a tree: the partition
/// index and the <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> key
/// currently backing its log. Part of the internal storage-axis sample produced
/// by <see cref="IWalStorageStateSource"/>; never an Orleans wire type.
/// </summary>
internal readonly record struct WalPartitionSample
{
    /// <summary>The WAL partition index within its owning tree.</summary>
    public int Partition { get; init; }

    /// <summary>The catalogue key that currently backs this partition's log.</summary>
    public string ProviderKey { get; init; }
}

/// <summary>
/// Per-tree slice of the internal storage-axis sample: the tree's retained WAL
/// bytes, its current backend saturation regime and how long it has held it, and
/// the per-partition provider placement. Produced by
/// <see cref="IWalStorageStateSource"/> and reduced into per-account
/// <see cref="WalAccountPressure"/> by <see cref="StoragePressureCollector"/>.
/// Not an Orleans wire type - it never leaves the silo.
/// </summary>
internal readonly record struct WalTreeSample
{
    /// <summary>Logical tree id this slice describes.</summary>
    public string TreeId { get; init; }

    /// <summary>
    /// Sum of retained WAL bytes across every partition of this tree, attributed
    /// across the tree's partitions when reduced to per-account totals.
    /// </summary>
    public long WalRetainedBytes { get; init; }

    /// <summary>
    /// <see langword="true"/> when the tree's WAL byte figure is a lower bound (a
    /// provider without byte accounting), so the aggregate is a lower bound too.
    /// </summary>
    public bool Partial { get; init; }

    /// <summary>
    /// The tree's current backend WAL saturation regime
    /// (<see cref="Orleans.Lattice.IWalSaturationSignal.GetCurrentState(string)"/>),
    /// the throughput-bound signal.
    /// </summary>
    public WalSaturationState Saturation { get; init; }

    /// <summary>
    /// How long the tree has been continuously observed at a non-healthy
    /// saturation state. Compared against
    /// <see cref="LatticeScalingSignalOptions.AccountSaturationWindow"/> to debounce
    /// a transient blip into a throughput-bound classification. <c>Zero</c> when
    /// the tree is healthy.
    /// </summary>
    public TimeSpan SaturatedFor { get; init; }

    private readonly IReadOnlyList<WalPartitionSample>? _partitions;

    /// <summary>
    /// Per-partition provider placement for this tree. Never <see langword="null"/>:
    /// defaults to an empty list so callers can enumerate without a null check.
    /// </summary>
    public IReadOnlyList<WalPartitionSample> Partitions
    {
        get => _partitions ?? Array.Empty<WalPartitionSample>();
        init => _partitions = value;
    }
}

/// <summary>
/// The cluster-aggregate storage-axis sample the collector reduces into a
/// <see cref="StoragePressure"/>: one <see cref="WalTreeSample"/> per registered
/// tree plus the set of registered catalogue keys (so an account with headroom -
/// or the "every key is hot" condition - can be detected). Not an Orleans wire
/// type; it never leaves the silo.
/// </summary>
internal readonly record struct WalStorageSample
{
    private readonly IReadOnlyList<WalTreeSample>? _trees;
    private readonly IReadOnlyCollection<string>? _catalogKeys;

    /// <summary>
    /// Per-tree WAL slices. Never <see langword="null"/>: defaults to an empty
    /// list, which the collector maps to a zero, not-over-threshold pressure.
    /// </summary>
    public IReadOnlyList<WalTreeSample> Trees
    {
        get => _trees ?? Array.Empty<WalTreeSample>();
        init => _trees = value;
    }

    /// <summary>
    /// Every <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> key
    /// registered on the silo (always including the default key). Used to find a
    /// move target with headroom, and to detect that every registered key is hot.
    /// Never <see langword="null"/>: defaults to an empty collection.
    /// </summary>
    public IReadOnlyCollection<string> CatalogKeys
    {
        get => _catalogKeys ?? Array.Empty<string>();
        init => _catalogKeys = value;
    }
}
