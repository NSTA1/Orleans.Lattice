namespace Orleans.Lattice.Apps;

/// <summary>
/// The activation evidence recorded against one tenant's app: the most recent run's outcome
/// and the manifest whose trees and rules are currently applied.
/// </summary>
[GenerateSerializer, Alias(AppActivationTypeAliases.AppActivationStatus)]
public sealed record AppActivationStatus
{
    /// <summary>The tenant the app is installed for.</summary>
    [Id(0)] public required TenantId Tenant { get; init; }

    /// <summary>The app the status describes.</summary>
    [Id(1)] public required AppSlug Slug { get; init; }

    /// <summary>The outcome of the most recent activation run.</summary>
    [Id(2)] public required AppActivationOutcome LastOutcome { get; init; }

    /// <summary>
    /// The manifest the app's trees were last provisioned from, or <c>null</c> when nothing is
    /// applied (never activated, or uninstalled). A later upgrade is validated against it and
    /// soft-deletes the structural trees it drops.
    /// </summary>
    [Id(3)] public AppManifest? AppliedManifest { get; init; }

    /// <summary>
    /// Effective tree ids mapped to whether this install authored or may have attempted their
    /// enable. A false value records a pre-existing enrolment, which uninstall and upgrades
    /// must leave intact. Written before changes so interrupted activations remain cleanable.
    /// </summary>
    [Id(4)] public IReadOnlyDictionary<string, bool> ReplicationTrees { get; init; } =
        System.Collections.ObjectModel.ReadOnlyDictionary<string, bool>.Empty;
}
