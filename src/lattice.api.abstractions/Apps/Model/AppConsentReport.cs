using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>The effective version-pinned consent; scope references never expose composed physical ids.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppConsentReport), Immutable]
public sealed record AppConsentReport
{
    /// <summary>The installed app slug whose consent was read.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The exact version to which this consent applies.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The approved operations and exception scopes.</summary>
    [Id(2)] public required AppCapabilityCeilingDescriptor Ceiling { get; init; }
    /// <summary>
    /// The consented bridge operations, or null when none were ever recorded or the
    /// server predates bridge consent.
    /// </summary>
    [Id(3)] public ImmutableArray<string>? BridgeOperations { get; init; }
}
