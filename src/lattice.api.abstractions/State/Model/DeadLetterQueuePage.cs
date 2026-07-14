namespace Orleans.Lattice.Api.State;

/// <summary>
/// One page of a tree's strict-mode dead-letter queue, in append (time) order.
/// <see cref="NextPageToken"/> is the cursor to pass back in the next
/// <see cref="DeadLetterQueueRequest"/> to continue enumeration; it is
/// <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.DeadLetterQueuePage)]
[Immutable]
public sealed record DeadLetterQueuePage
{
    /// <summary>The dead-letter entries on this page, in append (time) order.</summary>
    [Id(0)] public IReadOnlyList<DeadLetterEntryRecord> Entries { get; init; } = Array.Empty<DeadLetterEntryRecord>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
