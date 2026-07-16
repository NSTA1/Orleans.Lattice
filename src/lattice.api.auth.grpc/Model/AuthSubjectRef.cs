using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request naming a single subject by id. A serializable envelope for
/// <c>EffectivePermissionsAsync</c>, whose only argument is a subject id.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthSubjectRef)]
[Immutable]
public sealed record AuthSubjectRef
{
    /// <summary>The subject id to resolve permissions for.</summary>
    [Id(0)] public required string SubjectId { get; init; }

    /// <summary>
    /// Whether <see cref="SubjectId"/> names a user or a group. Defaults to
    /// <see cref="LatticeSubjectSelectorKind.User"/>, so an older client that
    /// omits it is interpreted exactly as before.
    /// </summary>
    [Id(1)] public LatticeSubjectSelectorKind SubjectKind { get; init; } = LatticeSubjectSelectorKind.User;
}
