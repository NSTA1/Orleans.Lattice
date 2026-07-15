namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response for the schema control-API RPCs that report whether a stored
/// item was removed (clear policy, clear version config).
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaRemovedResponse)]
[Immutable]
public sealed record SchemaRemovedResponse
{
    /// <summary><c>true</c> when an item was removed; otherwise <c>false</c>.</summary>
    [Id(0)] public required bool Removed { get; init; }
}
