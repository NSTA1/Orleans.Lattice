using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>A sign-in scheme with public configuration only; never include secrets or caller data.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AuthSchemeDescriptor), Immutable]
public sealed record AuthSchemeDescriptor
{
    /// <summary>The stable scheme id a client matches to a sign-in provider.</summary>
    [Id(0)] public required string SchemeId { get; init; }
    /// <summary>The scheme's human-readable display name.</summary>
    [Id(1)] public string DisplayName { get; init; } = string.Empty;
    /// <summary>Public parameters needed to begin sign-in.</summary>
    [Id(2)] public ImmutableDictionary<string, string> Parameters { get; init; } =
        ImmutableDictionary<string, string>.Empty;
}
