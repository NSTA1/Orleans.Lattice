namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire request carrying only a tree id, shared by the schema control-API RPCs
/// that address a single tree with no further arguments (clear policy, get policy,
/// list / count dead letters, get / clear version config, migrate, get remediation
/// status, scan compliance, probe capabilities).
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SchemaTreeRequest)]
[Immutable]
public sealed record SchemaTreeRequest
{
    /// <summary>The governed tree id the call targets.</summary>
    [Id(0)] public required string TreeId { get; init; }
}
