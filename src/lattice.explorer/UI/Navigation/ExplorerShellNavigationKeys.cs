using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The preference keys the shell's navigation surface declares, over and above
/// the route-shaped keys in <see cref="ExplorerPreferenceKeys"/>.
/// </summary>
/// <remarks>
/// <para>
/// Where the shell's own keys record <em>where you were</em>, this one records
/// <em>how you want the rail to read</em>. It is therefore scoped to the user
/// rather than to the user and cluster: a preference about how much of the
/// product you want to see is the same preference wherever you point the
/// Explorer.
/// </para>
/// <para>
/// The key is registered by the rail itself when it mounts, rather than by a
/// head, so a deployment gains it by rendering the shell and the reset-view
/// affordance discloses and clears it with no further wiring.
/// </para>
/// </remarks>
public static class ExplorerShellNavigationKeys
{
    /// <summary>
    /// Whether the rail hides the areas the caller cannot open, rather than
    /// demoting them below a divider.
    /// </summary>
    /// <remarks>
    /// Defaults to <see langword="false"/> - show them. A user who cannot see
    /// that a Backups area exists cannot ask to be granted it, so the shell's
    /// out-of-the-box answer is to keep a refusal visible and explained, and to
    /// let someone who has decided they do not care opt out.
    /// </remarks>
    public static ExplorerPreferenceKey HideInaccessibleAreas { get; } = new(
        "shell.hide-inaccessible",
        "whether areas you cannot open are hidden",
        ExplorerPreferenceScope.User);
}
