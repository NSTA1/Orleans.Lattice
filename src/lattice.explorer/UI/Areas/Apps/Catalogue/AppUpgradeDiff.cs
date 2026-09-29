using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>What changes between an installed version and the one being reviewed.</summary>
/// <param name="Slug">The app slug.</param>
/// <param name="FromVersion">The installed version.</param>
/// <param name="ToVersion">The reviewed version.</param>
internal sealed record AppUpgradeDiff(string Slug, string FromVersion, string ToVersion)
{
    /// <summary>Trees the new version declares that the installed one does not.</summary>
    public ImmutableArray<string> TreesAdded { get; init; } = [];

    /// <summary>Trees the installed version declares that the new one drops.</summary>
    public ImmutableArray<string> TreesRemoved { get; init; } = [];

    /// <summary>Roles the new version adds.</summary>
    public ImmutableArray<string> RolesAdded { get; init; } = [];

    /// <summary>Roles the new version drops.</summary>
    public ImmutableArray<string> RolesRemoved { get; init; } = [];

    /// <summary>Roles whose operations or scopes change.</summary>
    public ImmutableArray<string> RolesChanged { get; init; } = [];

    /// <summary>Operations the new version needs that the recorded ceiling does not approve.</summary>
    public LatticeOperation CeilingAdded { get; init; }

    /// <summary>Exception scopes the new version needs that are not approved.</summary>
    public ImmutableArray<AppExceptionScope> ScopesAdded { get; init; } = [];

    /// <summary>Bridge grants the new version requests that are not consented.</summary>
    public ImmutableArray<AppUiBridgeGrantDescriptor> BridgeAdded { get; init; } = [];

    /// <summary>Bridge grants the installed version requested that the new one no longer does.</summary>
    public ImmutableArray<AppUiBridgeGrantDescriptor> BridgeRemoved { get; init; } = [];

    /// <summary>Whether the upgrade needs the operator to consent again before it can activate.</summary>
    public bool RequiresReconsent => CeilingAdded != LatticeOperation.None || !ScopesAdded.IsEmpty || !BridgeAdded.IsEmpty;

    /// <summary>Whether nothing that matters changes.</summary>
    public bool IsEmpty =>
        TreesAdded.IsEmpty && TreesRemoved.IsEmpty && RolesAdded.IsEmpty && RolesRemoved.IsEmpty
        && RolesChanged.IsEmpty && BridgeRemoved.IsEmpty && !RequiresReconsent;
}
