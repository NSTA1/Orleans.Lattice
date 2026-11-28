namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Filter and paging request for the admin audit readers
/// (<see cref="ILatticeAdminAuditLog"/> and
/// <c>Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAuditLog</c>). Every filter is
/// optional and they combine with AND. Records are returned newest first;
/// <see cref="PageToken"/> is the exclusive cursor - pass the
/// <see cref="AdminAuditPage.NextPageToken"/> of the previous page to fetch the next.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AdminAuditQuery)]
[Immutable]
public sealed record AdminAuditQuery
{
    /// <summary>Default page size used when <see cref="PageSize"/> is unset.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>Largest page size honoured; larger values are clamped down.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>
    /// Maximum number of records to return in a single page. Values below <c>1</c>
    /// fall back to <see cref="DefaultPageSize"/>; values above
    /// <see cref="MaxPageSize"/> are clamped to it.
    /// </summary>
    [Id(0)] public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>
    /// Exclusive continuation cursor returned as <see cref="AdminAuditPage.NextPageToken"/>
    /// by the previous page. <see langword="null"/> (the default) starts at the newest record.
    /// </summary>
    [Id(1)] public string? PageToken { get; init; }

    /// <summary>
    /// The partition to read: <c>cluster</c> or <c>t/{tenant}</c>
    /// (<see cref="LatticeAdminAuditRequest.ClusterScope"/>,
    /// <see cref="LatticeAdminAuditRequest.TenantScope"/>). <see langword="null"/> reads
    /// every partition. The operator reader honours it; the tenant reader ignores it and
    /// always reads the caller's validated tenant partition.
    /// </summary>
    [Id(2)] public string? Scope { get; init; }

    /// <summary>Inclusive lower bound on <see cref="LatticeAdminAuditRecord.TimestampUtc"/>, or <see langword="null"/>.</summary>
    [Id(3)] public DateTimeOffset? From { get; init; }

    /// <summary>Exclusive upper bound on <see cref="LatticeAdminAuditRecord.TimestampUtc"/>, or <see langword="null"/>.</summary>
    [Id(4)] public DateTimeOffset? To { get; init; }

    /// <summary>Only records whose <see cref="LatticeAdminAuditRecord.ActorSubjectId"/> equals this value (ordinal), or <see langword="null"/>.</summary>
    [Id(5)] public string? Actor { get; init; }

    /// <summary>Only records of this action, or <see langword="null"/>.</summary>
    [Id(6)] public LatticeAdminAuditAction? Action { get; init; }

    /// <summary>Only records with this outcome, or <see langword="null"/>.</summary>
    [Id(7)] public LatticeAdminAuditOutcome? Outcome { get; init; }

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
