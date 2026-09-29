namespace Orleans.Lattice.Explorer.UI.Design.Tokens;

/// <summary>
/// The stable names and labels of each <see cref="LtStateRole"/>: the key that
/// selects its tokens in <c>lattice-operate.css</c> and the text that always
/// accompanies its colour and glyph.
/// </summary>
internal static class LtStateRoles
{
    /// <summary>Every state role, in declaration order.</summary>
    public static IReadOnlyList<LtStateRole> All { get; } = Enum.GetValues<LtStateRole>();

    /// <summary>
    /// The role's token key: the <c>data-lt-state</c> attribute value and the
    /// <c>--lt-op-state-{key}</c> custom property stem.
    /// </summary>
    /// <param name="role">The state role.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="role"/> is not a declared role.</exception>
    public static string Key(LtStateRole role) => role switch
    {
        LtStateRole.Installed => "installed",
        LtStateRole.Enabled => "enabled",
        LtStateRole.Disabled => "disabled",
        LtStateRole.Uninstalled => "uninstalled",
        LtStateRole.Drift => "drift",
        LtStateRole.Healthy => "healthy",
        LtStateRole.Lagging => "lagging",
        LtStateRole.Stalled => "stalled",
        LtStateRole.Failed => "failed",
        LtStateRole.Unknown => "unknown",
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unknown state role."),
    };

    /// <summary>The role's default visible label.</summary>
    /// <param name="role">The state role.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="role"/> is not a declared role.</exception>
    public static string Label(LtStateRole role) => role switch
    {
        LtStateRole.Installed => "Installed",
        LtStateRole.Enabled => "Enabled",
        LtStateRole.Disabled => "Disabled",
        LtStateRole.Uninstalled => "Not installed",
        LtStateRole.Drift => "Consent drift",
        LtStateRole.Healthy => "Healthy",
        LtStateRole.Lagging => "Lagging",
        LtStateRole.Stalled => "Stalled",
        LtStateRole.Failed => "Failed",
        LtStateRole.Unknown => "Unknown",
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unknown state role."),
    };
}
