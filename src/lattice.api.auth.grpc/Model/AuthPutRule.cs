using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request wrapping an authored authorization rule. A serializable envelope
/// for <c>PutRuleAsync</c>, whose only argument is the rule to persist.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthPutRule)]
[Immutable]
public sealed record AuthPutRule
{
    /// <summary>The rule to create or replace.</summary>
    [Id(0)] public required LatticeAuthorizationRule Rule { get; init; }
}
