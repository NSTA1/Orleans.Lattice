namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Paging request for the paged listings of the delegated tenant-access surface
/// (a tenant's groups, its member set, and its rules). It follows the cluster
/// access facade's paging convention: entries are enumerated in a
/// deterministic, stable ascending order and <see cref="PageToken"/> is the
/// exclusive cursor - pass the <c>NextPageToken</c> returned by the previous page
/// to fetch the next one. A group's direct members are returned whole and take no
/// paging request.
/// </summary>
/// <remarks>
/// A page token is opaque and only meaningful for the listing and the tenant that
/// issued it. The tenant is always the explicit argument of the call, never a
/// property of the request, so a token can never widen a listing to another
/// tenant.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantAccessPageRequest)]
[Immutable]
public sealed record TenantAccessPageRequest
{
    /// <summary>Default page size used when <see cref="PageSize"/> is unset.</summary>
    public const int DefaultPageSize = 100;

    /// <summary>Largest page size honoured; larger values are clamped down.</summary>
    public const int MaxPageSize = 1000;

    /// <summary>
    /// Maximum number of entries to return in a single page. Values below
    /// <c>1</c> fall back to <see cref="DefaultPageSize"/>; values above
    /// <see cref="MaxPageSize"/> are clamped to it.
    /// </summary>
    [Id(0)] public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>
    /// Exclusive continuation cursor returned as the previous page's
    /// <c>NextPageToken</c>. <see langword="null"/> (the default) starts from the
    /// beginning.
    /// </summary>
    [Id(1)] public string? PageToken { get; init; }

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
