namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The transport-agnostic projection of a tenant's resource quotas and burst
/// allowance, authored through <see cref="ILatticeTenantAdmin.SetTenantQuotasAsync"/>
/// and reported on <see cref="TenantStatusReport.Quotas"/>. It mirrors the tenancy
/// engine's quota model without taking a dependency on it, so the control-API
/// contract stays engine-agnostic: each data-plane resource dimension is a nullable
/// ceiling where <see langword="null"/> means <em>unbounded</em> (no limit on that
/// dimension) and a bounded ceiling must be non-negative, and
/// <see cref="BurstPercent"/> is the transient headroom, as a
/// percentage of the bounded ceilings, a tenant may momentarily exceed before
/// admission control engages (<c>0</c> means no burst). The delegated tenant access
/// caps (<see cref="MaxGroups"/>, <see cref="MaxMembershipEdges"/>,
/// <see cref="MaxMemberSubjects"/>, <see cref="MaxTenantRules"/>) read differently:
/// for them <see langword="null"/> means <em>the default cap applies</em>, never
/// unbounded, because they protect trees every tenant shares.
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
    /// <summary>
    /// The maximum total storage footprint in bytes - the write-ahead log, snapshot,
    /// and persisted leaf-state bytes of the tenant's trees - or
    /// <see langword="null"/> for unbounded.
    /// </summary>
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
    /// The maximum number of tenant groups the tenant may own (delegated tenant
    /// access administration), or <see langword="null"/> for the default of
    /// <c>500</c>. Unlike the data-plane dimensions above, <see langword="null"/>
    /// does <b>not</b> mean unbounded: this cap protects the membership trees every
    /// tenant shares, so it always applies. A bounded value must be non-negative.
    /// </summary>
    [Id(6)] public long? MaxGroups { get; init; }

    /// <summary>
    /// The maximum number of membership edges the tenant's groups may hold, or
    /// <see langword="null"/> for the default of <c>10000</c>. <see langword="null"/>
    /// does <b>not</b> mean unbounded: this cap protects the shared membership
    /// trees, so it always applies. A bounded value must be non-negative.
    /// </summary>
    [Id(7)] public long? MaxMembershipEdges { get; init; }

    /// <summary>
    /// The maximum number of entries in the tenant's member set, or
    /// <see langword="null"/> for the default of <c>5000</c>. <see langword="null"/>
    /// does <b>not</b> mean unbounded: this cap protects the shared tenant registry,
    /// so it always applies. A bounded value must be non-negative.
    /// </summary>
    [Id(8)] public long? MaxMemberSubjects { get; init; }

    /// <summary>
    /// The maximum number of tenant-tier authorization rules the tenant may author,
    /// or <see langword="null"/> for the default of <c>1000</c>.
    /// <see langword="null"/> does <b>not</b> mean unbounded: this cap protects the
    /// policy snapshot every tenant shares, so it always applies. A bounded value
    /// must be non-negative.
    /// </summary>
    [Id(9)] public long? MaxTenantRules { get; init; }

    /// <summary>
    /// The unbounded quota: every data-plane dimension <see langword="null"/> and no
    /// burst. This is the quota of the reserved default tenant. Its delegated access
    /// caps (<see cref="MaxGroups"/>, <see cref="MaxMembershipEdges"/>,
    /// <see cref="MaxMemberSubjects"/>, <see cref="MaxTenantRules"/>) are also
    /// <see langword="null"/>, which means their defaults apply, not that they are
    /// unbounded.
    /// </summary>
    public static TenantQuotasDescriptor Unbounded => default;

    /// <summary>
    /// <see langword="true"/> when every data-plane resource dimension
    /// (<see cref="MaxBytes"/>, <see cref="MaxKeys"/>, <see cref="MaxMemoryBytes"/>,
    /// <see cref="MaxTreeCount"/>, <see cref="MaxOpsPerSecond"/>) is unbounded
    /// (<see langword="null"/>), regardless of <see cref="BurstPercent"/>. The
    /// delegated access caps are not consulted: they are never unbounded.
    /// </summary>
    public bool IsUnbounded =>
        MaxBytes is null
        && MaxKeys is null
        && MaxMemoryBytes is null
        && MaxTreeCount is null
        && MaxOpsPerSecond is null;
}
