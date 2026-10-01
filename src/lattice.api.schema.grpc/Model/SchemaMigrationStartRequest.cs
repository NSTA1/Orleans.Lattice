namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire request for the <c>StartMigration</c> RPC.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaMigrationStartRequest)]
[Immutable]
public sealed record SchemaMigrationStartRequest
{
    /// <summary>The governed tree id to migrate to its current target version.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The caller's idempotency id, or <c>null</c> to have one generated.</summary>
    [Id(1)] public string? OperationId { get; init; }
}
