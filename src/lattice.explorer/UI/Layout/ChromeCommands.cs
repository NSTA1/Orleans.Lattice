using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout.Appearance;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The navigation chrome's own palette commands - go to Home or to an area, and
/// every appearance choice - each of which is also a visible control in the
/// chrome carrying <c>data-lt-command</c>.
/// </summary>
internal static class ChromeCommands
{
    /// <summary>The command, and the spine stop, that go to Home.</summary>
    public const string GoHomeId = "go.home";

    /// <summary>The command that opens the appearance menu, and the menu's button.</summary>
    public const string AppearanceMenuId = "appearance.menu";

    /// <summary>The command, and the spine stop, that go to the area with <paramref name="key"/>.</summary>
    /// <param name="key">The area key.</param>
    public static string GoToAreaId(string key) => "go." + key;

    /// <summary>The command, and the menu button, that choose <paramref name="theme"/>.</summary>
    /// <param name="theme">The material.</param>
    public static string ThemeId(ShellTheme theme) => theme switch
    {
        ShellTheme.Paper => "appearance.theme.paper",
        ShellTheme.Board => "appearance.theme.board",
        _ => "appearance.theme.system",
    };

    /// <summary>The command, and the menu button, that choose <paramref name="contrast"/>.</summary>
    /// <param name="contrast">The contrast.</param>
    public static string ContrastId(ShellContrast contrast) => contrast switch
    {
        ShellContrast.Standard => "appearance.contrast.standard",
        ShellContrast.More => "appearance.contrast.more",
        _ => "appearance.contrast.system",
    };

    /// <summary>The command, and the menu button, that choose <paramref name="density"/>.</summary>
    /// <param name="density">The density.</param>
    public static string DensityId(LtDensity density) =>
        density == LtDensity.Compact ? "appearance.density.compact" : "appearance.density.comfortable";

    /// <summary>
    /// The chrome's commands for the current stops: Home, each visible area, and
    /// every appearance choice.
    /// </summary>
    /// <param name="location">Where the user is, and the shown stops.</param>
    /// <param name="appearance">The appearance state the appearance commands set.</param>
    public static IReadOnlyList<ExplorerCommand> Build(ExplorerLocation location, ShellAppearance appearance)
    {
        ArgumentNullException.ThrowIfNull(location);
        ArgumentNullException.ThrowIfNull(appearance);

        var tenant = location.Address.Tenant;
        var commands = new List<ExplorerCommand>
        {
            new(GoHomeId, "Go to Home") { Target = ExplorerAddress.Home.WithTenant(tenant), Detail = "The estate overview" },
        };

        foreach (var entry in location.Entries)
        {
            if (entry.IsVisible)
            {
                commands.Add(new ExplorerCommand(GoToAreaId(entry.Area.Key), "Go to " + entry.Area.DisplayName)
                {
                    Target = ExplorerAddress.ForArea(entry.Area.Key).WithTenant(tenant),
                });
            }
        }

        commands.Add(Theme(appearance, ShellTheme.System, "Use the system theme"));
        commands.Add(Theme(appearance, ShellTheme.Paper, "Use the Paper theme"));
        commands.Add(Theme(appearance, ShellTheme.Board, "Use the Board theme"));
        commands.Add(Contrast(appearance, ShellContrast.System, "Use the system contrast"));
        commands.Add(Contrast(appearance, ShellContrast.Standard, "Use standard contrast"));
        commands.Add(Contrast(appearance, ShellContrast.More, "Use high contrast"));
        commands.Add(Density(appearance, LtDensity.Comfortable, "Use the comfortable density"));
        commands.Add(Density(appearance, LtDensity.Compact, "Use the compact density"));

        return commands;
    }

    private static ExplorerCommand Theme(ShellAppearance appearance, ShellTheme theme, string title) =>
        new(ThemeId(theme), title) { InvokeAsync = cancellationToken => new ValueTask(appearance.SetThemeAsync(theme, cancellationToken)) };

    private static ExplorerCommand Contrast(ShellAppearance appearance, ShellContrast contrast, string title) =>
        new(ContrastId(contrast), title) { InvokeAsync = cancellationToken => new ValueTask(appearance.SetContrastAsync(contrast, cancellationToken)) };

    private static ExplorerCommand Density(ShellAppearance appearance, LtDensity density, string title) =>
        new(DensityId(density), title) { InvokeAsync = cancellationToken => new ValueTask(appearance.SetDensityAsync(density, cancellationToken)) };
}
