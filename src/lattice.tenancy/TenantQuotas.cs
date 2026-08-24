namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The resource quotas and burst allowance for a tenant. Each dimension is a
/// nullable ceiling where <c>null</c> means <em>unbounded</em> (no limit on that
/// dimension); <see cref="BurstPercent"/> is the transient headroom, as a
/// percentage of the bounded ceilings, a tenant may momentarily exceed before
/// admission control engages (<c>0</c> means no burst).
/// </summary>
/// <remarks>
/// This is the tenant's declared allocation, authored by an operator and stored
/// in the registry; enforcement of it against live usage is a separate concern
/// layered on top of the registry. The reserved <see cref="TenantId.Default"/>
/// tenant carries <see cref="Unbounded"/>.
/// </remarks>
[GenerateSerializer]
[Alias(TenantTypeAliases.TenantQuotas)]
[Immutable]
public readonly record struct TenantQuotas
{
    /// <summary>The maximum total stored value bytes, or <c>null</c> for unbounded.</summary>
    [Id(0)]
    public long? MaxBytes { get; init; }

    /// <summary>The maximum total live key count, or <c>null</c> for unbounded.</summary>
    [Id(1)]
    public long? MaxKeys { get; init; }

    /// <summary>The maximum resident memory in bytes, or <c>null</c> for unbounded.</summary>
    [Id(2)]
    public long? MaxMemoryBytes { get; init; }

    /// <summary>The maximum number of trees the tenant may own, or <c>null</c> for unbounded.</summary>
    [Id(3)]
    public long? MaxTreeCount { get; init; }

    /// <summary>The maximum sustained operations per second, or <c>null</c> for unbounded.</summary>
    [Id(4)]
    public long? MaxOpsPerSecond { get; init; }

    /// <summary>
    /// The transient burst headroom above the bounded ceilings, as a percentage
    /// (<c>0</c> for none). For example <c>20</c> permits a momentary 20% overage
    /// before admission control engages.
    /// </summary>
    [Id(5)]
    public int BurstPercent { get; init; }

    /// <summary>
    /// The unbounded quota: every dimension <c>null</c> and no burst. This is the
    /// quota of the reserved <see cref="TenantId.Default"/> tenant.
    /// </summary>
    public static TenantQuotas Unbounded => default;

    /// <summary>
    /// <c>true</c> when every resource dimension is unbounded (<c>null</c>),
    /// regardless of <see cref="BurstPercent"/>.
    /// </summary>
    public bool IsUnbounded =>
        MaxBytes is null
        && MaxKeys is null
        && MaxMemoryBytes is null
        && MaxTreeCount is null
        && MaxOpsPerSecond is null;
}
