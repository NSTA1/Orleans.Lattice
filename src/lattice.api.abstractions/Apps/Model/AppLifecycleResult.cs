namespace Orleans.Lattice.Api.Apps;

/// <summary>The outcome of an app lifecycle operation, with no physical tree identifiers.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppLifecycleResult), Immutable]
public sealed record AppLifecycleResult
{
    /// <summary>The affected app slug.</summary>
    [Id(0)] public required string Slug { get; init; }
    /// <summary>The affected installation's exact version.</summary>
    [Id(1)] public required string Version { get; init; }
    /// <summary>
    /// The resulting Installed, Enabled, Disabled, or Uninstalled state.
    /// NotInstalled and Failed are inspection-only states and never mutation results;
    /// unsuccessful mutations throw rather than return a success-shaped result.
    /// </summary>
    [Id(2)] public AppLifecycleState State { get; init; }
    /// <summary>Whether this operation changed the installation.</summary>
    [Id(3)] public bool Changed { get; init; }
    /// <summary>
    /// Whether the operation was accepted but is still running when this result was returned.
    /// A lifecycle run that outlasts the facade's accept window keeps running to completion and
    /// records its outcome; <see cref="State"/> then reports the state before the run and
    /// <see cref="Changed"/> is <see langword="false"/>. Poll
    /// <see cref="ILatticeAppsControl.DescribeAsync"/> or <see cref="ILatticeAppsControl.ListAsync"/>
    /// until the app leaves that state; a run that failed reports
    /// <see cref="AppLifecycleState.Failed"/> there.
    /// </summary>
    [Id(4)] public bool Pending { get; init; }
}
