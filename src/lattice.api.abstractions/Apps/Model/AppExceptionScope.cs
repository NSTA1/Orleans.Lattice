using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// An explicitly approved scope outside the structural app namespace. App trees
/// use a slug and local name, never a composed physical id. Legacy tree ids are
/// operator-declared adoption targets, not namespace-composed identifiers.
/// </summary>
/// <remarks>
/// Exactly one named target shape is valid: App plus Tree, or AdoptedTreeId alone.
/// Implementations reject mixed shapes, reserved or tenant-qualified adoption
/// targets, invalid kinds, and inconsistent keys before changing consent.
/// </remarks>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppExceptionScope), Immutable]
public sealed record AppExceptionScope
{
    /// <summary>Whole-tree, exact-key, or key-prefix extent.</summary>
    [Id(0)] public LatticeScopeKind Kind { get; init; } = LatticeScopeKind.Tree;
    /// <summary>The explicit target app slug; null for a legacy target.</summary>
    [Id(1)] public string? App { get; init; }
    /// <summary>The target app's local tree name; never a composed physical id.</summary>
    [Id(2)] public string? Tree { get; init; }
    /// <summary>An operator-declared legacy adoption id; never an app- or tenant-composed id.</summary>
    [Id(3)] public string? AdoptedTreeId { get; init; }
    /// <summary>The exact key or prefix; null for a whole-tree scope.</summary>
    [Id(4)] public string? KeyOrPrefix { get; init; }
}
