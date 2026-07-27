namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire request for the unified typed-CRDT read RPC. <see cref="Kind"/> selects
/// which primitive to decode the stored value into, so a single read RPC serves
/// every CRDT logical read. An unreadable or absent key yields the empty logical
/// value for the requested kind, never a fault.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtReadRequest)]
[Immutable]
public sealed record CrdtReadRequest
{
    /// <summary>Logical tree identifier.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The key the CRDT is stored under.</summary>
    [Id(1)] public required string Key { get; init; }

    /// <summary>The CRDT primitive to decode the stored value into.</summary>
    [Id(2)] public required CrdtKind Kind { get; init; }
}
