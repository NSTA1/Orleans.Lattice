namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// A tenant's usage against its quota ceilings, as the Explorer presents it:
/// per dimension the consumption, the ceilings, and the overage, qualified by
/// the scope the figures were read under.
/// <para>
/// The dimensions are named properties rather than a collection, so a panel
/// polling this reading allocates nothing per dimension. Iterate them through
/// the shared, cached <see cref="Dimensions"/> list and the
/// <see cref="this[ExplorerTenantQuotaDimensionKind]"/> indexer when a uniform
/// pass is wanted.
/// </para>
/// <para>
/// Every figure in one reading comes from a single coherent snapshot: the
/// control API deliberately never pairs a just-changed ceiling with a
/// not-yet-resampled usage, because an incoherent reading invents breaches that
/// admission is not enforcing. The accepted cost is that a quota edit can lag by
/// one index cycle and shows on <see cref="ExplorerTenantDetail.Quotas"/> first
/// - staleness a panel can caption, whereas an invented violation it cannot.
/// </para>
/// </summary>
public sealed record ExplorerTenantQuotaUsage
{
    /// <summary>
    /// The dimensions in display order, as one shared cached list, so iterating
    /// a reading allocates nothing.
    /// </summary>
    public static IReadOnlyList<ExplorerTenantQuotaDimensionKind> Dimensions { get; } =
    [
        ExplorerTenantQuotaDimensionKind.Bytes,
        ExplorerTenantQuotaDimensionKind.Keys,
        ExplorerTenantQuotaDimensionKind.MemoryBytes,
        ExplorerTenantQuotaDimensionKind.TreeCount,
        ExplorerTenantQuotaDimensionKind.OpsPerSecond,
    ];

    /// <summary>The tenant the reading describes.</summary>
    public required string TenantId { get; init; }

    /// <summary><see langword="true"/> when this is the reserved default tenant.</summary>
    public bool IsDefault { get; init; }

    /// <summary>
    /// The scope the figures were read and are enforced under. A
    /// <see cref="ExplorerTenantQuotaEnforcement.PerCluster"/> reading is this
    /// cluster's local view, not a global total, and a panel must caption it as
    /// such.
    /// </summary>
    public ExplorerTenantQuotaEnforcement EnforcementScope { get; init; }

    /// <summary>
    /// <see langword="true"/> when the reading carries consumption figures at
    /// all. <see langword="false"/> for a registered tenant whose warm view has
    /// not compiled yet: the ceilings below are still authoritative, and the
    /// consumption figures are absent rather than fabricated as zeros.
    /// </summary>
    public bool HasUsage { get; init; }

    /// <summary>Stored bytes against the stored-bytes ceiling.</summary>
    public ExplorerTenantQuotaDimension Bytes { get; init; }

    /// <summary>Live keys against the live-key ceiling.</summary>
    public ExplorerTenantQuotaDimension Keys { get; init; }

    /// <summary>Resident memory against the resident-memory ceiling.</summary>
    public ExplorerTenantQuotaDimension MemoryBytes { get; init; }

    /// <summary>Owned trees against the owned-tree ceiling.</summary>
    public ExplorerTenantQuotaDimension TreeCount { get; init; }

    /// <summary>
    /// The operation-rate ceiling. The sampler takes no rate sample, so this
    /// dimension is normally unmeasured even on a warm reading; render it as
    /// "not measured" rather than as an idle bar.
    /// </summary>
    public ExplorerTenantQuotaDimension OpsPerSecond { get; init; }

    /// <summary>
    /// The burst allowance, as a percentage above each steady-state ceiling that
    /// admission tolerates.
    /// </summary>
    public int BurstPercent { get; init; }

    /// <summary>
    /// The ceilings the reading was taken against, from the same coherent
    /// snapshot as the figures above.
    /// </summary>
    public ExplorerTenantQuotaLimits Limits { get; init; }

    /// <summary>
    /// The figures for <paramref name="kind"/>.
    /// </summary>
    /// <param name="kind">The dimension to read.</param>
    /// <returns>That dimension's consumption, ceilings, and overage.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a defined dimension.</exception>
    public ExplorerTenantQuotaDimension this[ExplorerTenantQuotaDimensionKind kind] => kind switch
    {
        ExplorerTenantQuotaDimensionKind.Bytes => Bytes,
        ExplorerTenantQuotaDimensionKind.Keys => Keys,
        ExplorerTenantQuotaDimensionKind.MemoryBytes => MemoryBytes,
        ExplorerTenantQuotaDimensionKind.TreeCount => TreeCount,
        ExplorerTenantQuotaDimensionKind.OpsPerSecond => OpsPerSecond,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };
}
