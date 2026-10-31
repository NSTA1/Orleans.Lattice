namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

/// <summary>
/// The <c>data-lt-command</c> of each appearance control: the header's appearance menu, the
/// compact <b>Menu</b> sheet and the palette command that make the same choice all carry it.
/// </summary>
public static class AppearanceCommands
{
    /// <summary>Follow the system's light or dark preference.</summary>
    public const string SystemTheme = "appearance.theme.system";

    /// <summary>The Paper (light) material.</summary>
    public const string Paper = "appearance.theme.paper";

    /// <summary>The Board (dark) material.</summary>
    public const string Board = "appearance.theme.board";

    /// <summary>Follow the system's contrast preference.</summary>
    public const string SystemContrast = "appearance.contrast.system";

    /// <summary>Standard contrast.</summary>
    public const string StandardContrast = "appearance.contrast.standard";

    /// <summary>The high-contrast overlay.</summary>
    public const string MoreContrast = "appearance.contrast.more";

    /// <summary>The comfortable density.</summary>
    public const string Comfortable = "appearance.density.comfortable";

    /// <summary>The compact density.</summary>
    public const string Compact = "appearance.density.compact";
}
