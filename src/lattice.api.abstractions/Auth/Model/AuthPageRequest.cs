namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Paging request for the admin facade's list endpoints (users, groups, and
/// rules). Mirrors the <c>Orleans.Lattice.Api.State</c> catalog paging
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

    /// <summary>The effective, clamped page size derived from <see cref="PageSize"/>.</summary>
    public int EffectivePageSize => PageSize switch
    {
        < 1 => DefaultPageSize,
        > MaxPageSize => MaxPageSize,
        _ => PageSize,
    };
}
