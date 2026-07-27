namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Wire response for the unified typed-CRDT read RPC. Carries every logical read
/// shape as an optional field; the client reads only the field matching the
/// <see cref="CrdtReadRequest.Kind"/> it asked for and ignores the rest. An
/// absent or unreadable key yields the empty value for that kind (0, false, or an
/// empty collection).
/// </summary>
/// <remarks>
/// Not <c>[Immutable]</c>: it nests mutable value buffers materialised from the
/// decoded CRDT, so it must remain copy-eligible across the gRPC boundary.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtReadResponse)]
public sealed record CrdtReadResponse
{
    /// <summary>The converged total for a PN-counter read.</summary>
    [Id(0)] public long CounterValue { get; init; }

    /// <summary>The boolean state for an OR-Flag / RW-Flag read.</summary>
    [Id(1)] public bool FlagValue { get; init; }

    /// <summary>The element / value bytes for an OR-Set, MV-Register, or Sequence read.</summary>
    [Id(2)] public List<byte[]> Elements { get; init; } = [];

    /// <summary>The per-replica clocks for a version-vector read.</summary>
    [Id(3)] public List<CrdtVectorEntry> Vector { get; init; } = [];

    /// <summary>The per-field concurrent values for an OR-Map read.</summary>
    [Id(4)] public List<CrdtMapField> Map { get; init; } = [];
}
