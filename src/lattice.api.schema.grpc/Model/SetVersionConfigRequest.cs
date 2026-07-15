using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire request for the <c>SetVersionConfig</c> RPC.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SetVersionConfigRequest)]
[Immutable]
public sealed record SetVersionConfigRequest
{
    /// <summary>The governed tree id the version config applies to.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The version configuration to install.</summary>
    [Id(1)] public required LatticeSchemaVersionConfig Config { get; init; }
}
