namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Result of a <see cref="DataRangeDeleteRequest"/>: the tree the delete ran
/// against and the total number of keys tombstoned across the whole range.
/// </summary>
/// <remarks>
/// The count reflects keys actually tombstoned by this call. For an
/// unauthorized (or anonymous) caller the gated surface denies the range delete
/// rather than reporting a partial count.
/// </remarks>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataRangeDeleteResult)]
[Immutable]
public sealed record DataRangeDeleteResult
{
    /// <summary>Logical tree identifier the delete ran against.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>Total number of keys tombstoned across the range.</summary>
    [Id(1)] public required int DeletedCount { get; init; }
}
