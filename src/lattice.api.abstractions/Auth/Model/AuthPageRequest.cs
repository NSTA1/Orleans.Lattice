namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Paging request for the admin facade's paged list endpoints (the group
/// catalog, and the rule catalog whole or per tree; group-member lists are
/// returned whole and take no paging request). Mirrors the
/// <c>Orleans.Lattice.Api.State</c> catalog paging
/// convention: entries are enumerated in a deterministic, stable ascending
/// order and <see cref="PageToken"/> is the exclusive cursor - pass the
/// <c>NextPageToken</c> returned by the previous page to fetch the next one.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthPageRequest)]
[Immutable]
public sealed record AuthPageRequest
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
    /// Exclusive continuation cursor: the id of the last entry on the previous
    /// page. <see langword="null"/> (the default) starts from the beginning.
    /// </summary>
    [Id(1)] public string? PageToken { get; init; }

    /// <summary>
    /// Whether a rule-catalogue listing (<see cref="ILatticeAuthAdmin.ListRulesAsync"/>)
    /// is narrowed to the rules governing trees the caller's active tenant owns.
    /// <see langword="false"/> (the default) lists the whole catalogue, exactly as before.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The tenant is never taken from the request: it is the active tenant the
    /// server resolves for the call - the caller's <c>lattice-active-tenant</c>
    /// assertion, re-validated against the caller's own membership, or the
    /// reserved default tenant when the call asserts none. A tenant owns a rule
    /// when the rule's governed tree is one of its own trees
    /// (<c>t/{tenant}/{name}</c>, or a bare tree id for the default tenant).
    /// Cluster-wide <c>Tree:*</c> rules and rules on platform trees belong to no
    /// tenant and are never listed; read the cluster-wide bucket with
    /// <see cref="ILatticeAuthAdmin.ListRulesForTreeAsync"/> for <c>*</c>.
    /// </para>
    /// <para>
    /// Paging is unchanged: the narrowing happens before the page is cut, so every
    /// page but the last is full. The answering page reports the tenant it was
    /// narrowed to in <see cref="AuthRulePage.Tenant"/>; a server that predates
    /// this member ignores it and answers the whole catalogue with no tenant.
    /// Ignored by the group listing and the per-tree rule listing.
    /// </para>
    /// </remarks>
    [Id(2)] public bool ActiveTenantOnly { get; init; }

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
