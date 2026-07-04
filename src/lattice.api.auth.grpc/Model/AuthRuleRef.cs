namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request naming a single authorization rule by its governed tree id and
/// rule id. A serializable envelope for the facade operations whose arguments
/// are the <c>(treeId, ruleId)</c> pair (<c>GetRuleAsync</c> /
/// <c>RemoveRuleAsync</c>).
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthRuleRef)]
[Immutable]
public sealed record AuthRuleRef
{
    /// <summary>The rule's governed tree id.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The rule id.</summary>
    [Id(1)] public required string RuleId { get; init; }
}
