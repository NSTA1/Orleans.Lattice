using Orleans.Lattice.Explorer.Shell.Design.Tokens;

namespace Orleans.Lattice.Explorer.Shell.Layout.Appearance;

/// <summary>
/// The names of every appearance value: what is stored in the preference
/// contract, and what the document attributes carry.
/// </summary>
/// <remarks>
/// <para>
/// The stored names are the ones the Explorer has always stored under the same
/// <c>appearance.*</c> keys (<c>system</c>, <c>light</c>, <c>dark</c>;
/// <c>system</c>, <c>standard</c>, <c>more</c>), so an operator's choice survives
/// the move to the rewritten Explorer. Paper is stored as <c>light</c> and Board
/// as <c>dark</c>, which is also what <c>data-bs-theme</c> carries.
/// </para>
/// <para>
/// The rewritten Explorer has two densities. A stored density it has no
/// equivalent for (<c>cosy</c>, or <c>layout</c> for "follow the layout") reads
/// as comfortable rather than being forgotten.
/// </para>
/// </remarks>
internal static class ShellAppearanceNames
{
    /// <summary>The stored name for following the operating system.</summary>
    public const string FollowSystem = "system";

    /// <summary>The stored name, and <c>data-bs-theme</c> value, of Paper.</summary>
    public const string Light = "light";

    /// <summary>The stored name, and <c>data-bs-theme</c> value, of Board.</summary>
    public const string Dark = "dark";

    /// <summary>The stored name, and <c>data-lt-contrast</c> value, of the standard palette.</summary>
    public const string Standard = "standard";

    /// <summary>The stored name, and <c>data-lt-contrast</c> value, of the high-contrast overlay.</summary>
    public const string More = "more";

    /// <summary>The stored name of the comfortable density.</summary>
    public static readonly string Comfortable = LtDensities.AttributeValue(LtDensity.Comfortable);

    /// <summary>The stored name, and <c>data-lt-density</c> value, of the compact density.</summary>
    public static readonly string Compact = LtDensities.AttributeValue(LtDensity.Compact);

    private static readonly string[] LegacyComfortableNames = ["cosy", "layout"];

    /// <summary>The stored name of <paramref name="theme"/>.</summary>
    /// <param name="theme">The theme.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="theme"/> is not declared.</exception>
    public static string Name(ShellTheme theme) => theme switch
    {
        ShellTheme.System => FollowSystem,
        ShellTheme.Paper => Light,
        ShellTheme.Board => Dark,
        _ => throw new ArgumentOutOfRangeException(nameof(theme), theme, "Unknown theme."),
    };

    /// <summary>The stored name of <paramref name="contrast"/>.</summary>
    /// <param name="contrast">The contrast.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="contrast"/> is not declared.</exception>
    public static string Name(ShellContrast contrast) => contrast switch
    {
        ShellContrast.System => FollowSystem,
        ShellContrast.Standard => Standard,
        ShellContrast.More => More,
        _ => throw new ArgumentOutOfRangeException(nameof(contrast), contrast, "Unknown contrast."),
    };

    /// <summary>The stored name of <paramref name="density"/>.</summary>
    /// <param name="density">The density.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="density"/> is not declared.</exception>
    public static string Name(LtDensity density) => LtDensities.AttributeValue(density);

    /// <summary>Parses a stored theme name, case-insensitively.</summary>
    /// <param name="name">The stored name.</param>
    /// <param name="theme">The theme, or <see cref="ShellTheme.System"/> when unknown.</param>
    public static bool TryParseTheme(string? name, out ShellTheme theme)
    {
        if (Is(name, FollowSystem) || Is(name, Light) || Is(name, Dark))
        {
            theme = Is(name, Light) ? ShellTheme.Paper : Is(name, Dark) ? ShellTheme.Board : ShellTheme.System;
            return true;
        }

        theme = ShellTheme.System;
        return false;
    }

    /// <summary>Parses a stored contrast name, case-insensitively.</summary>
    /// <param name="name">The stored name.</param>
    /// <param name="contrast">The contrast, or <see cref="ShellContrast.System"/> when unknown.</param>
    public static bool TryParseContrast(string? name, out ShellContrast contrast)
    {
        if (Is(name, FollowSystem) || Is(name, Standard) || Is(name, More))
        {
            contrast = Is(name, Standard) ? ShellContrast.Standard : Is(name, More) ? ShellContrast.More : ShellContrast.System;
            return true;
        }

        contrast = ShellContrast.System;
        return false;
    }

    /// <summary>
    /// Parses a stored density name, case-insensitively. A legacy name with no
    /// equivalent here reads as comfortable.
    /// </summary>
    /// <param name="name">The stored name.</param>
    /// <param name="density">The density, or <see cref="LtDensity.Comfortable"/> when unknown.</param>
    public static bool TryParseDensity(string? name, out LtDensity density)
    {
        density = LtDensity.Comfortable;
        if (name is null)
        {
            return false;
        }

        if (string.Equals(name, Compact, StringComparison.OrdinalIgnoreCase))
        {
            density = LtDensity.Compact;
            return true;
        }

        return string.Equals(name, Comfortable, StringComparison.OrdinalIgnoreCase)
            || LegacyComfortableNames.Contains(name, StringComparer.OrdinalIgnoreCase);
    }

    private static bool Is(string? name, string expected) => string.Equals(name, expected, StringComparison.OrdinalIgnoreCase);
}
