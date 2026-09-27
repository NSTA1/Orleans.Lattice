using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>Installed app summaries visible in the active isolation context; contains no tree ids.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppCatalog), Immutable]
public sealed record AppCatalog
{
    /// <summary>The installed app summaries; empty when no installations are visible.</summary>
    [Id(0)] public ImmutableArray<AppSummary> Apps { get; init; } = [];
}
