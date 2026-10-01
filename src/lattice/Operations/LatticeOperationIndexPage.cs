namespace Orleans.Lattice.Operations;

/// <summary>One page of operation ids from the per-tenant operation index.</summary>
/// <param name="OperationIds">The ids, newest-first.</param>
/// <param name="NextPageToken">The token for the next page, or <see langword="null"/> on the last page.</param>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationIndexPage)]
[Immutable]
internal sealed record LatticeOperationIndexPage(
    [property: Id(0)] IReadOnlyList<string> OperationIds,
    [property: Id(1)] string? NextPageToken);
