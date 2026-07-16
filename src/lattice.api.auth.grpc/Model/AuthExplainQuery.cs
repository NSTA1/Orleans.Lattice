using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request for a policy-introspection explain. A serializable envelope for
/// <c>ExplainAsync</c>, which asks whether a subject may perform an operation
/// over a scope.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthExplainQuery)]
[Immutable]
public sealed record AuthExplainQuery
{
    /// <summary>The subject to explain the decision for.</summary>
    [Id(0)] public required string SubjectId { get; init; }

    /// <summary>
    /// Whether <see cref="SubjectId"/> names a user or a group. Defaults to
    /// <see cref="LatticeSubjectSelectorKind.User"/>, so an older client that
    /// omits it is interpreted exactly as before.
    /// </summary>
    [Id(3)] public LatticeSubjectSelectorKind SubjectKind { get; init; } = LatticeSubjectSelectorKind.User;

    /// <summary>The operation to evaluate.</summary>
    [Id(1)] public LatticeOperation Operation { get; init; }

    /// <summary>The keyspace scope to evaluate (whole tree, a key, or a prefix).</summary>
    [Id(2)] public required LatticeScope Scope { get; init; }
}
