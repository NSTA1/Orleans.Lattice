namespace Orleans.Lattice.Api.Operations;

/// <summary>A request for one page of the caller's operations, newest-first.</summary>
[GenerateSerializer]
[Alias(ApiOperationTypeAliases.LatticeOperationListRequest)]
[Immutable]
public sealed record LatticeOperationListRequest
{
    /// <summary>The page size used when <see cref="PageSize"/> is zero or negative.</summary>
    public const int DefaultPageSize = 50;

    /// <summary>The largest page size honoured; a larger request is clamped to it.</summary>
    public const int MaxPageSize = 500;

    /// <summary>The maximum operations to return; zero or negative selects <see cref="DefaultPageSize"/>.</summary>
    [Id(0)] public int PageSize { get; init; }

    /// <summary>The previous page's <see cref="LatticeOperationPage.NextPageToken"/>, or <see langword="null"/> to start at the newest.</summary>
    [Id(1)] public string? PageToken { get; init; }

    /// <summary>The effective page size: <see cref="PageSize"/> defaulted and clamped.</summary>
    public int EffectivePageSize => PageSize <= 0 ? DefaultPageSize : Math.Min(PageSize, MaxPageSize);
}
