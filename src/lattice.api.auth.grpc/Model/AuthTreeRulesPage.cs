namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request for a page of the rules governing one tree. A serializable
/// envelope for <c>ListRulesForTreeAsync</c>, which takes a governed tree id and
/// a paging request.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthTreeRulesPage)]
[Immutable]
public sealed record AuthTreeRulesPage
{
    /// <summary>The governed tree id whose rules to list.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>The paging request (page size and continuation cursor).</summary>
    [Id(1)] public AuthPageRequest Page { get; init; } = new();
}
