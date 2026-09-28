using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>An enabled app in which the caller holds at least one role in the active tenant.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.WorkspaceAppSummary), Immutable]
public sealed record WorkspaceAppSummary
{
    /// <summary>The app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The installed version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The install revision a UI frame is bound to; it changes on every upgrade.</summary>
    [Id(2)] public long InstallRevision { get; init; }
    /// <summary>The installed version's presentation, or null when it declares none.</summary>
    [Id(3)] public AppPresentationDescriptor? Presentation { get; init; }
    /// <summary>Whether the installed version ships a UI bundle.</summary>
    [Id(4)] public bool HasUi { get; init; }
    /// <summary>The names of the app roles the caller holds.</summary>
    [Id(5)] public ImmutableArray<string> Roles { get; init; } = [];
}
