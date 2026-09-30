using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Layout.Appearance;

/// <summary>
/// The appearance menu's contribution to the Explorer's preference contract:
/// the material, the contrast overlay and the density, each remembered per user.
/// </summary>
/// <remarks>
/// <para>
/// The names are the three the Explorer already remembers and documents in
/// <c>what-the-explorer-remembers.md</c> (<c>appearance.theme</c>,
/// <c>appearance.contrast</c>, <c>appearance.density</c>), with a compatible
/// value vocabulary (<see cref="ShellAppearanceNames"/>), so no key is added to
/// the contract and an operator's choice carries across the cutover. Each is
/// declared once here, as the contract requires, and registered on the catalog
/// by <see cref="ShellAppearance"/>.
/// </para>
/// <para>
/// They are <see cref="ExplorerPreferenceScope.User"/>: a material is a property
/// of the person and the room, not of the cluster they are pointed at.
/// </para>
/// </remarks>
internal static class ShellAppearancePreferenceKeys
{
    /// <summary>The material: <c>system</c>, <c>light</c> (Paper) or <c>dark</c> (Board).</summary>
    public static ExplorerPreferenceKey Theme { get; } = new(
        "appearance.theme",
        "the theme you chose",
        ExplorerPreferenceScope.User);

    /// <summary>The contrast overlay: <c>system</c>, <c>standard</c> or <c>more</c>.</summary>
    public static ExplorerPreferenceKey Contrast { get; } = new(
        "appearance.contrast",
        "the contrast you chose",
        ExplorerPreferenceScope.User);

    /// <summary>The density: <c>comfortable</c> or <c>compact</c>.</summary>
    public static ExplorerPreferenceKey Density { get; } = new(
        "appearance.density",
        "the display density you chose",
        ExplorerPreferenceScope.User);

    /// <summary>The three keys, in the order the menu presents them.</summary>
    public static IReadOnlyList<ExplorerPreferenceKey> All { get; } = [Theme, Contrast, Density];
}
