namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire response for the <c>CountDeadLetters</c> RPC.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaCountResponse)]
[Immutable]
public sealed record SchemaCountResponse
{
    /// <summary>The dead-letter entry count for the tree.</summary>
    [Id(0)] public required int Count { get; init; }
}
