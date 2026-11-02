namespace Orleans.Lattice.Api.Data;

/// <summary>
/// The totals of a streamed write from <see cref="ILatticeDataApi.SetManyStreamAsync"/>:
/// how many entries the stream carried, how many were written, how many were refused,
/// and in how many batches.
/// </summary>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataSetManyStreamResult)]
[Immutable]
public sealed record DataSetManyStreamResult
{
    /// <summary>The tree the stream was written to.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>How many entries the stream carried.</summary>
    [Id(1)] public required long EntryCount { get; init; }

    /// <summary>How many entries were written.</summary>
    [Id(2)] public required long CommittedCount { get; init; }

    /// <summary>How many entries were refused.</summary>
    [Id(3)] public required long RejectedCount { get; init; }

    /// <summary>How many batches the stream was written in.</summary>
    [Id(4)] public required long BatchCount { get; init; }
}
