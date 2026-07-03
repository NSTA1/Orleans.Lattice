using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire response wrapping a nullable rule lookup. gRPC unary responses cannot be
/// <see langword="null"/>, so <c>GetRuleAsync</c> returns this envelope with
/// <see cref="Rule"/> set to the resolved rule or <see langword="null"/> when no
/// such rule exists.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthRuleResult)]
[Immutable]
public sealed record AuthRuleResult
{
    /// <summary>The resolved rule, or <see langword="null"/> when no such rule exists.</summary>
    [Id(0)] public LatticeAuthorizationRule? Rule { get; init; }
}
