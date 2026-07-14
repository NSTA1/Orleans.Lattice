namespace Orleans.Lattice.Scaling;

/// <summary>
/// Classifies why a single write-ahead-log account
/// (an <see cref="Orleans.Lattice.IWalStorageProviderCatalog"/> key) is under
/// storage pressure, so an operator can tell a <i>throughput</i> problem from a
/// <i>capacity</i> problem at a glance. Surfaced on
/// <see cref="WalAccountPressure.Classification"/> and echoed on
/// <see cref="WalRebalanceRecommendation.Classification"/>.
/// <para>
/// The distinction drives the remedy: a throughput-bound account is relieved by
/// spreading its hot partitions across more accounts (a WAL move), whereas a
/// capacity-bound account is relieved by trimming retained bytes or provisioning
/// more retention headroom. The storage axis only ever <i>reports</i> this - it
/// never changes the compute scale value.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ScalingTypeAliases.WalPressureClassification)]
public enum WalPressureClassification
{
    /// <summary>
    /// The account is healthy: neither backend-bound saturated nor over its
    /// retained-bytes advisory threshold. The default for a default-constructed
    /// <see cref="WalAccountPressure"/>.
    /// </summary>
    None = 0,

    /// <summary>
    /// The account is <b>throughput-bound</b>: a single hot account has topped
    /// out its backend write rate (its per-tree
    /// <see cref="Orleans.Lattice.WalSaturationState"/> is
    /// <see cref="Orleans.Lattice.WalSaturationState.Throttled"/> or
    /// <see cref="Orleans.Lattice.WalSaturationState.Saturated"/>, in practice
    /// around 22-24 thousand entries per second for one storage account).
    /// Adding retention headroom does not help; the fix is to move one of the
    /// account's hot partitions to another account.
    /// </summary>
    ThroughputBound = 1,

    /// <summary>
    /// The account is <b>capacity-bound</b>: its retained WAL bytes have grown
    /// past the advisory fraction of
    /// <see cref="Orleans.Lattice.LatticeOptions.WalMaxRetainedBytes"/>. The fix
    /// is to reclaim retained bytes or provision more retention, not to spread
    /// write throughput.
    /// </summary>
    CapacityBound = 2,
}
