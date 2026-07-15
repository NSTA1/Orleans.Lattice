using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>Wire request for the <c>SetPolicy</c> RPC.</summary>
[GenerateSerializer]
[Alias(GrpcSchemaTypeAliases.SetPolicyRequest)]
[Immutable]
public sealed record SetPolicyRequest
{
    /// <summary>The governed tree id the policy applies to.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The enforcement policy to install.</summary>
    [Id(1)] public required LatticeSchemaPolicy Policy { get; init; }
}
