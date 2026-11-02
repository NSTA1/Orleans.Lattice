namespace Orleans.Lattice.Api.Apps;

/// <summary>A tree-free summary of an installed app in the active isolation context.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppSummary), Immutable]
public sealed record AppSummary
{
    /// <summary>The installed app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The installed semantic version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>The installation's current lifecycle state.</summary>
    [Id(2)] public AppLifecycleState State { get; init; }
    /// <summary>The recorded source metadata, not an authorization claim.</summary>
    [Id(3)] public required AppProvenanceDescriptor Provenance { get; init; }
    /// <summary>
    /// The state a lifecycle run still in flight is moving the installation to, or null when no run
    /// is executing. While it is set, <see cref="State"/> reports the state before the run and is
    /// never <see cref="AppLifecycleState.Failed"/>; a listed Failed state is a run that ended failed.
    /// Null from a server that predates it.
    /// </summary>
    [Id(4)] public AppLifecycleState? PendingState { get; init; }
}
