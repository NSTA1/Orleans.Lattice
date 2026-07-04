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
}
