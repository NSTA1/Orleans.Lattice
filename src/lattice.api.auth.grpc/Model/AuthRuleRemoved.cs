namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire response for a rule removal. Carries whether a rule was actually removed
/// (<c>RemoveRuleAsync</c> returns <see langword="false"/> when no matching rule
/// existed).
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthRuleRemoved)]
[Immutable]
public sealed record AuthRuleRemoved
{
    /// <summary><see langword="true"/> when a rule was removed; <see langword="false"/> when none matched.</summary>
    [Id(0)] public bool Removed { get; init; }
}
