using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// Wire response wrapping a <see cref="LatticeSchemaVersionConfig"/> value type
/// for the <c>AdvanceTargetVersion</c> RPC (gRPC message types must be reference
/// types).
/// </summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.VersionConfigResponse)]
[Immutable]
public sealed record VersionConfigResponse
{
    /// <summary>The updated version config.</summary>
    [Id(0)] public required LatticeSchemaVersionConfig Config { get; init; }
}
