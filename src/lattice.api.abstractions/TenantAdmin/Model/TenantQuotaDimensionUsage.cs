namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One quota dimension's usage-against-ceiling reading on a
/// <see cref="TenantQuotaUsageReport"/>: the tenant's current consumption, the
/// steady-state ceiling, the burst-adjusted admission ceiling, and both the live
/// and accrued overage above the steady-state cap. This is what a quota bar is
/// drawn from - "you are using <see cref="Usage"/> of your <see cref="Limit"/>".
/// </summary>
/// <remarks>
/// <para>
/// <b>Unbounded is not zero.</b> <see cref="Limit"/> is <see langword="null"/>
/// when the dimension has <em>no ceiling at all</em> - the state of the reserved
/// default tenant and of any dimension an operator left unbounded - which is a
/// completely different fact from a ceiling of <c>0</c> (a dimension capped at
/// nothing, where any usage is already in overage). A surface must branch on
/// <see cref="IsBounded"/> rather than treat a missing ceiling as <c>0</c>, or an
/// unlimited tenant renders as a full bar.
/// </para>
/// <para>
/// <b>Not every dimension is measured.</b> <see cref="Usage"/> is
/// <see langword="null"/> when the reading carries no consumption figure for the
/// dimension - the tenancy engine's usage accounting samples stored bytes, live
/// keys, resident memory, and owned trees, but not a sustained operation rate, so
/// the ops-per-second dimension reports its ceiling with an unmeasured usage
/// rather than a fabricated <c>0</c>. <see cref="Overage"/> and
/// <see cref="MeteredOverage"/> are likewise <c>0</c> on an unmeasured dimension
/// and carry no meaning there; check <see cref="IsMeasured"/> first.
/// </para>
/// <para>
/// A <see langword="readonly"/> record struct, so a report carries its five
/// dimensions inline and a usage read allocates nothing per dimension.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantQuotaDimensionUsage)]
[Immutable]
public readonly record struct TenantQuotaDimensionUsage
{
    /// <summary>
    /// The tenant's current consumption on this dimension, or
    /// <see langword="null"/> when the dimension is not measured by this reading
    /// (see the remarks on <see cref="TenantQuotaDimensionUsage"/>).
    /// </summary>
    [Id(0)] public long? Usage { get; init; }

    /// <summary>
    /// The steady-state ceiling for this dimension, or <see langword="null"/> for
    /// <em>unbounded</em> (no ceiling). Never conflate <see langword="null"/> with
    /// <c>0</c>: a ceiling of <c>0</c> permits nothing at all.
    /// </summary>
    [Id(1)] public long? Limit { get; init; }

    /// <summary>
    /// The burst-adjusted ceiling admission control actually engages at -
    /// <see cref="Limit"/> plus the tenant's burst headroom percentage - or
    /// <see langword="null"/> when the dimension is unbounded. Equal to
    /// <see cref="Limit"/> when the tenant has no burst allowance.
    /// </summary>
    [Id(2)] public long? BurstLimit { get; init; }

    /// <summary>
    /// The live amount by which <see cref="Usage"/> currently exceeds the
    /// <em>steady-state</em> <see cref="Limit"/> (never the burst-adjusted one);
    /// <c>0</c> when the tenant is within quota, when the dimension is unbounded,
    /// or when it is not measured.
    /// </summary>
    [Id(3)] public long Overage { get; init; }

    /// <summary>
    /// The tenant's converged, durable <em>accrued</em> metered overage on this
    /// dimension - the billing total, distinct from the instantaneous
    /// <see cref="Overage"/>. <c>0</c> when the tenant has never been in overage
    /// or the dimension is not metered.
    /// </summary>
    [Id(4)] public long MeteredOverage { get; init; }

    /// <summary>
    /// The unbounded, unmeasured dimension: no ceiling, no usage, no overage.
    /// </summary>
    public static TenantQuotaDimensionUsage Unbounded => default;

    /// <summary>
    /// <see langword="true"/> when this dimension has a ceiling at all, so a
    /// consumer can distinguish "unbounded" from "capped at zero" without
    /// inspecting <see cref="Limit"/> for <see langword="null"/>.
    /// </summary>
    public bool IsBounded => Limit is not null;

    /// <summary>
    /// <see langword="true"/> when this reading carries a consumption figure for
    /// the dimension, so a consumer knows whether <see cref="Usage"/>,
    /// <see cref="Overage"/>, and <see cref="MeteredOverage"/> mean anything.
    /// </summary>
    public bool IsMeasured => Usage is not null;
}
