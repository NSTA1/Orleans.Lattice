namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// One replica's entry in a version-vector read: the writer id and its clock
/// formatted <c>"wallClockTicks:counter"</c>.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtVectorEntry)]
[Immutable]
public sealed record CrdtVectorEntry
{
    /// <summary>The replica id this clock belongs to.</summary>
    [Id(0)] public required string ReplicaId { get; init; }

    /// <summary>The replica's clock, formatted <c>"wallClockTicks:counter"</c>.</summary>
    [Id(1)] public required string Clock { get; init; }
}
