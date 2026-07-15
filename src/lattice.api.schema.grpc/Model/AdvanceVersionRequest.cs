namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request for the <c>AdvanceTargetVersion</c> and <c>AdvanceAndMigrate</c>
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
}
