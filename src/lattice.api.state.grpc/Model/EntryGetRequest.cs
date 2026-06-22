namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Wire request for the single-entry get RPC. A serializable mirror of the
/// <c>(treeId, key)</c> argument pair of
/// <see cref="ILatticeStateQuery.GetEntryAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcStateTypeAliases.EntryGetRequest)]
[Immutable]
public sealed record EntryGetRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The entry key to read.</summary>
    [Id(1)] public required string Key { get; init; }
}
