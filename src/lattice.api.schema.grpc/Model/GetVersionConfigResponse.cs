using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire response for the <c>GetVersionConfig</c> RPC.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.GetVersionConfigResponse)]
[Immutable]
public sealed record GetVersionConfigResponse
{
    /// <summary><c>true</c> when the tree is versioned; otherwise <c>false</c>.</summary>
    [Id(0)] public required bool Found { get; init; }

    /// <summary>The version config when <see cref="Found"/> is <c>true</c>; otherwise default.</summary>
    [Id(1)] public LatticeSchemaVersionConfig Config { get; init; }
}
