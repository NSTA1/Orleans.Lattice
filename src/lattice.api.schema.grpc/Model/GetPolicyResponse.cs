using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire response for the <c>GetPolicy</c> RPC.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.GetPolicyResponse)]
[Immutable]
public sealed record GetPolicyResponse
{
    /// <summary><c>true</c> when a policy exists for the tree; otherwise <c>false</c>.</summary>
    [Id(0)] public required bool Found { get; init; }

    /// <summary>The policy when <see cref="Found"/> is <c>true</c>; otherwise <c>null</c>.</summary>
    [Id(1)] public LatticeSchemaPolicy? Policy { get; init; }
}
