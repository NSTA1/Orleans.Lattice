namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire response for the entry-scan RPC. A serializable mirror of
/// <see cref="EntryScanResult"/> carrying a snapshot-isolated page of entries
/// and the opaque continuation token that resumes the same point-in-time
/// cursor.
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.EntryScanResponse)]
[Immutable]
public sealed record EntryScanResponse
{
    /// <summary>Lookup outcome mapped from the facade result.</summary>
    [Id(0)] public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was scanned.</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The entries in this page, in the scan's key order.</summary>
    [Id(2)] public IReadOnlyList<EntryRecord> Entries { get; init; } = Array.Empty<EntryRecord>();

    /// <summary>
    /// Opaque continuation token to resume the scan against the same snapshot,
    /// or <see langword="null"/> when the scan is fully drained.
    /// </summary>
    [Id(3)] public string? ContinuationToken { get; init; }
}
