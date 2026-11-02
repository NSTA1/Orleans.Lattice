namespace Orleans.Lattice.Api.Data;

/// <summary>
/// The outcome of one batch of a streamed write, reported as the batch completes: how
/// many of its entries were written and which were refused. The entries a batch
/// committed are durable once it is reported.
/// </summary>
[GenerateSerializer]
[Alias(DataApiTypeAliases.DataSetManyStreamBatch)]
[Immutable]
public sealed record DataSetManyStreamBatch
{
    /// <summary>The tree the batch was written to.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The zero-based index of the batch in the stream.</summary>
    [Id(1)] public required long BatchIndex { get; init; }

    /// <summary>The ordinal of the batch's first entry.</summary>
    [Id(2)] public required long FirstOrdinal { get; init; }

    /// <summary>How many entries the batch carried.</summary>
    [Id(3)] public required int EntryCount { get; init; }

    /// <summary>How many of the batch's entries were written.</summary>
    [Id(4)] public required int CommittedCount { get; init; }

    /// <summary>Every entry of the batch that was refused, in stream order. Empty when none was.</summary>
    [Id(5)] public IReadOnlyList<DataEntryRejection> Rejections { get; init; } = Array.Empty<DataEntryRejection>();
}
