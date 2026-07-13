namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The registration record for one top-level area in the shell's area switcher:
/// its <see cref="AppArea"/> id, its display label, and the advisory rule that
/// decides whether the area entry is enabled for the current capability map. A
/// new area (for example a future access-control surface) registers by adding a
/// descriptor to <see cref="AppAreas.Ordered"/>; the shell needs no per-area
/// code. Enabling is advisory only - the server still enforces access when the
/// area's actions run.
/// </summary>
public sealed record AppAreaDescriptor
{
    /// <summary>The area this descriptor registers.</summary>
    public required AppArea Area { get; init; }

    /// <summary>The human-readable label shown on the area switcher.</summary>
    public required string Label { get; init; }

    /// <summary>
    /// The advisory rule that reports whether the area is enabled for a given
    /// capability map. A disabled area is rendered greyed-out and visible, not
    /// hidden. Must not be <see langword="null"/>.
    /// </summary>
    public required Func<ExplorerCapabilities, bool> IsEnabled { get; init; }
}
