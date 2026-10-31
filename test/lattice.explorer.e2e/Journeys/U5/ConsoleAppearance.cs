using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U5;

/// <summary>
/// One appearance, as the Explorer's appearance controls name it and the story's ledger records
/// it (<c>theme/contrast/density</c>, see <see cref="Contracts.ContosoAppearance"/>): theme
/// <c>system</c>, <c>paper</c> or <c>board</c>; contrast <c>system</c>, <c>standard</c> or
/// <c>more</c>; density <c>comfortable</c> or <c>compact</c>.
/// </summary>
/// <param name="Theme">The theme choice.</param>
/// <param name="Contrast">The contrast choice.</param>
/// <param name="Density">The density choice.</param>
public sealed partial record ConsoleAppearance(string Theme, string Contrast, string Density)
{
    /// <summary>The default: System theme, System contrast, Comfortable density.</summary>
    public static ConsoleAppearance Default { get; } = new("system", "system", "comfortable");

    /// <summary>Every theme choice, in the order the appearance controls draw them.</summary>
    public static IReadOnlyList<string> Themes { get; } = ["system", "paper", "board"];

    /// <summary>Every contrast choice, in the order the appearance controls draw them.</summary>
    public static IReadOnlyList<string> Contrasts { get; } = ["system", "standard", "more"];

    /// <summary>Every density choice, in the order the appearance controls draw them.</summary>
    public static IReadOnlyList<string> Densities { get; } = ["comfortable", "compact"];

    /// <summary>
    /// The material the document is drawn in: <c>light</c> for Paper, <c>dark</c> for Board, and
    /// for System whatever the operating system prefers (<paramref name="systemPrefersDark"/>).
    /// </summary>
    /// <param name="systemPrefersDark">Whether the operating system prefers a dark scheme.</param>
    public string Material(bool systemPrefersDark = false) => Theme switch
    {
        "paper" => "light",
        "board" => "dark",
        _ => systemPrefersDark ? "dark" : "light",
    };

    /// <summary>The ledger form: <c>theme/contrast/density</c>.</summary>
    public override string ToString() => $"{Theme}/{Contrast}/{Density}";

    /// <summary>The control command that chooses <paramref name="theme"/>.</summary>
    /// <param name="theme">A theme choice.</param>
    public static string ThemeCommandOf(string theme) => theme switch
    {
        "system" => AppearanceCommands.SystemTheme,
        "paper" => AppearanceCommands.Paper,
        "board" => AppearanceCommands.Board,
        _ => throw new ArgumentOutOfRangeException(nameof(theme), theme, "Not a theme choice."),
    };

    /// <summary>The control command that chooses <paramref name="contrast"/>.</summary>
    /// <param name="contrast">A contrast choice.</param>
    public static string ContrastCommandOf(string contrast) => contrast switch
    {
        "system" => AppearanceCommands.SystemContrast,
        "standard" => AppearanceCommands.StandardContrast,
        "more" => AppearanceCommands.MoreContrast,
        _ => throw new ArgumentOutOfRangeException(nameof(contrast), contrast, "Not a contrast choice."),
    };

    /// <summary>The control command that chooses <paramref name="density"/>.</summary>
    /// <param name="density">A density choice.</param>
    public static string DensityCommandOf(string density) => density switch
    {
        "comfortable" => AppearanceCommands.Comfortable,
        "compact" => AppearanceCommands.Compact,
        _ => throw new ArgumentOutOfRangeException(nameof(density), density, "Not a density choice."),
    };

    /// <summary>A pattern any attribute value matches: <c>Not.ToHaveAttribute</c> with it asserts the attribute is absent.</summary>
    [GeneratedRegex(".")]
    internal static partial Regex AnyValue();
}
