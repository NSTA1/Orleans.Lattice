namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// A top-level application area shown in the shell's area switcher, above the
/// per-tree detail tab strip. Areas are the app-level navigation tier: the
/// existing tree browser (<see cref="Explore"/>), the backup management surface
/// (<see cref="Backups"/>), and the membership / access-control administration
/// surface (<see cref="Access"/>). This is deliberately distinct from
/// <c>DetailTab</c>, which is the per-tree detail tier within the Explore area.
/// </summary>
public enum AppArea
{
    /// <summary>The tree / view browser: navigation panel plus the detail tabs. The default area.</summary>
    Explore,

    /// <summary>The backup and restore management surface.</summary>
    Backups,

    /// <summary>The membership and access-control administration surface.</summary>
    Access,
}
