using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Apps;

/// <summary>An app one source offers, joined with its installation in the active tenant.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AvailableAppSummary), Immutable]
public sealed record AvailableAppSummary
{
    /// <summary>The key of the source offering the app.</summary>
    [Id(0)] public required string SourceKey { get; init; }
    /// <summary>The app slug.</summary>
    [Id(1)] public required string Slug { get; init; }
    /// <summary>The newest version the source offers.</summary>
    [Id(2)] public required string NewestVersion { get; init; }
    /// <summary>Every version the source offers, newest first.</summary>
    [Id(3)] public ImmutableArray<string> AvailableVersions { get; init; } = [];
    /// <summary>The newest version's presentation, or null when it declares none.</summary>
    [Id(4)] public AppPresentationDescriptor? Presentation { get; init; }
    /// <summary>Whether the newest version ships a UI bundle.</summary>
    [Id(5)] public bool HasUi { get; init; }
    /// <summary>The version installed in the active tenant, or null when the app is not installed.</summary>
    [Id(6)] public string? InstalledVersion { get; init; }
    /// <summary>The installation's lifecycle state in the active tenant, or null when the app is not installed.</summary>
    [Id(7)] public AppLifecycleState? InstalledState { get; init; }
}
