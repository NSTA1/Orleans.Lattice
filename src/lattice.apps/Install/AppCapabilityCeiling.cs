using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps;

/// <summary>
/// Install-time capability ceiling pinned per app id and version. Every compiled rule must
/// be intersected with this ceiling. Scope defaults structurally to the app's own
/// <c>a/{app}/</c> namespace, so ordinary app-owned trees need no exception consent.
/// This record describes approval; it does not itself authorize or compile rules.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppCapabilityCeiling)]
public sealed record AppCapabilityCeiling
{
    /// <summary>Maximum operations available to compiled rules; the default grants none.</summary>
    [Id(0)] public LatticeOperation AllowedOperations { get; init; }

    /// <summary>
    /// Explicit operator-approved scopes outside the app's structural namespace, empty by default.
    /// Examples include another app's trees and adopted pre-app physical trees.
    /// </summary>
    [Id(1)] public IReadOnlyList<LatticeScope> ApprovedExceptionScopes { get; init; } = Array.Empty<LatticeScope>();

    /// <summary>Creates a ceiling limited to the app's structural namespace, with no exceptions.</summary>
    public static AppCapabilityCeiling Structural(LatticeOperation allowed) => new() { AllowedOperations = allowed };
}
