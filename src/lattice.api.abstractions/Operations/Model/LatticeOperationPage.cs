namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// One page of operations, newest-first. An operation the caller may not read is
/// omitted, so a page can be shorter than requested while
/// <see cref="NextPageToken"/> is still set.
/// </summary>
[GenerateSerializer]
[Alias(ApiOperationTypeAliases.LatticeOperationPage)]
[Immutable]
public sealed record LatticeOperationPage
{
    /// <summary>The operations on this page, newest-first.</summary>
    [Id(0)] public IReadOnlyList<LatticeOperationStatus> Operations { get; init; } = [];

    /// <summary>The token for the next page, or <see langword="null"/> on the last page.</summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
