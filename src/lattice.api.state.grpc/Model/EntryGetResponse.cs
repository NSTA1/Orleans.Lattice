namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire response for the single-entry get RPC. A serializable mirror of
/// <see cref="EntryDetailResult"/> that distinguishes an unknown tree from a
/// missing key via <see cref="Status"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.EntryGetResponse)]
[Immutable]
public sealed record EntryGetResponse
{
    /// <summary>Lookup outcome mapped from the facade result.</summary>
    [Id(0)] public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    [Id(1)] public required string TreeId { get; init; }

    /// <summary>The key that was queried.</summary>
    [Id(2)] public required string Key { get; init; }

    /// <summary>The record when <see cref="Status"/> is <see cref="StateQueryStatus.Found"/>.</summary>
    [Id(3)] public EntryRecord? Entry { get; init; }
}
