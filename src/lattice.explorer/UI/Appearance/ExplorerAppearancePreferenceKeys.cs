using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// The appearance feature's contribution to the Explorer's preference contract:
/// three keys, declared once, registered onto the shell's catalog by
/// <see cref="ExplorerAppearanceServiceCollectionExtensions.AddExplorerAppearance"/>.
/// </summary>
/// <remarks>
/// <para>
/// All three are <see cref="ExplorerPreferenceScope.User"/> rather than the
/// contract's usual <see cref="ExplorerPreferenceScope.UserAndCluster"/>. A
/// palette is a property of the person and the room they are sitting in, not of
/// the cluster they happen to be pointed at, so pointing the Explorer at another
/// cluster must not throw an operator back into a palette they cannot read.
/// </para>
/// <para>
/// Because they are declared here rather than in
/// <see cref="ExplorerPreferenceKeys"/>, the reset-view page discloses and
/// clears them with no change to that page - which is the whole point of an
/// enumerated contract.
/// </para>
/// </remarks>
public static class ExplorerAppearancePreferenceKeys
{
    /// <summary>The palette the operator chose, as an <see cref="ExplorerAppearanceNames.ThemeName"/>.</summary>
    public static ExplorerPreferenceKey Theme { get; } = new(
        "appearance.theme",
        "the theme you chose",
        ExplorerPreferenceScope.User);

    /// <summary>The contrast overlay the operator chose, as an <see cref="ExplorerAppearanceNames.ContrastName"/>.</summary>
    public static ExplorerPreferenceKey Contrast { get; } = new(
        "appearance.contrast",
        "the contrast you chose",
        ExplorerPreferenceScope.User);

    /// <summary>The density the operator chose, as an <see cref="ExplorerAppearanceNames.DensityName"/>.</summary>
    public static ExplorerPreferenceKey Density { get; } = new(
        "appearance.density",
        "the display density you chose",
        ExplorerPreferenceScope.User);

    /// <summary>The three keys, in the order the settings control presents them.</summary>
    public static IReadOnlyList<ExplorerPreferenceKey> All { get; } =
    [
        Theme,
        Contrast,
        Density,
    ];
}
