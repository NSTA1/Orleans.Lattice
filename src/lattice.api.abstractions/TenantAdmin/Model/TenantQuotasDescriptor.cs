namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The transport-agnostic projection of a tenant's resource quotas and burst
/// allowance, authored through <see cref="ILatticeTenantAdmin.SetTenantQuotasAsync"/>
/// and reported on <see cref="TenantStatusReport.Quotas"/>. It mirrors the tenancy
/// engine's quota model without taking a dependency on it, so the control-API
/// contract stays engine-agnostic: each resource dimension is a nullable ceiling
/// where <see langword="null"/> means <em>unbounded</em> (no limit on that
/// dimension), and <see cref="BurstPercent"/> is the transient headroom, as a
/// percentage of the bounded ceilings, a tenant may momentarily exceed before
/// admission control engages (<c>0</c> means no burst).
/// </summary>
/// <remarks>
/// This is a value-typed descriptor (a <see langword="readonly"/> record struct),
/// so authoring and reporting quotas allocate nothing on the heap for the quota
/// payload itself. The reserved default tenant always carries
/// <see cref="Unbounded"/>.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantQuotasDescriptor)]
[Immutable]
public readonly record struct TenantQuotasDescriptor
{
    /// <summary>The maximum total stored value bytes, or <see langword="null"/> for unbounded.</summary>
    [Id(0)] public long? MaxBytes { get; init; }

    /// <summary>The maximum total live key count, or <see langword="null"/> for unbounded.</summary>
    [Id(1)] public long? MaxKeys { get; init; }

    /// <summary>The maximum resident memory in bytes, or <see langword="null"/> for unbounded.</summary>
    [Id(2)] public long? MaxMemoryBytes { get; init; }

    /// <summary>The maximum number of trees the tenant may own, or <see langword="null"/> for unbounded.</summary>
    [Id(3)] public long? MaxTreeCount { get; init; }

    /// <summary>The maximum sustained operations per second, or <see langword="null"/> for unbounded.</summary>
    [Id(4)] public long? MaxOpsPerSecond { get; init; }

    /// <summary>
    /// The transient burst headroom above the bounded ceilings, as a percentage
    /// (<c>0</c> for none). For example <c>20</c> permits a momentary 20% overage
    /// before admission control engages. Must be non-negative.
    /// </summary>
    [Id(5)] public int BurstPercent { get; init; }

    /// <summary>
    /// The unbounded quota: every dimension <see langword="null"/> and no burst.
    /// This is the quota of the reserved default tenant.
    /// </summary>
    public static TenantQuotasDescriptor Unbounded => default;

    /// <summary>
    /// <see langword="true"/> when every resource dimension is unbounded
    /// (<see langword="null"/>), regardless of <see cref="BurstPercent"/>.
    /// </summary>
    public bool IsUnbounded =>
        MaxBytes is null
        && MaxKeys is null
        && MaxMemoryBytes is null
        && MaxTreeCount is null
        && MaxOpsPerSecond is null;
}
