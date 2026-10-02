namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request for the <c>AdvanceTargetVersion</c>, <c>AdvanceAndMigrate</c> and <c>StartAdvanceAndMigrate</c>
/// RPCs, carrying the tree id and the new target schema version.
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.AdvanceVersionRequest)]
[Immutable]
public sealed record AdvanceVersionRequest
{
    /// <summary>The governed tree id whose target version advances.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The new target schema version. Must be greater than the current target.</summary>
    [Id(1)] public required uint NewTargetVersion { get; init; }

    /// <summary>
    /// The caller's idempotency id for the <c>StartAdvanceAndMigrate</c> RPC, or
    /// <c>null</c> to have one generated. Ignored by the blocking RPCs.
    /// </summary>
    [Id(2)] public string? OperationId { get; init; }
}
