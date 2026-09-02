using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.UI.Appearance;

/// <summary>
/// The one spelling of every appearance value, shared by the three places that
/// have to agree on it: the durable preference contract, the document attributes
/// the token layer keys off, and the first-paint bootstrap script that applies
/// those attributes before the application exists.
/// </summary>
/// <remarks>
/// <para>
/// Two different vocabularies are deliberately distinguished. A <em>name</em> is
/// what is stored and what the bootstrap script reads back; every choice has one,
/// including the "follow the environment" choices, because "I have chosen to
/// follow the system" and "I have never chosen anything" must round-trip as
/// different states. An <em>attribute value</em> is what goes on the document;
/// following the environment has none, because the token layer's own
/// <c>prefers-contrast</c> handling and the script's <c>prefers-color-scheme</c>
/// query only apply when the attribute is absent.
/// </para>
/// <para>
/// Every member returns an interned literal or <see langword="null"/>, so
/// resolution on a render path allocates nothing.
/// </para>
/// </remarks>
public static class ExplorerAppearanceNames
{
    /// <summary>The stored name of <see cref="ExplorerThemeChoice.FollowSystem"/> and <see cref="ExplorerContrastChoice.FollowSystem"/>.</summary>
    public const string FollowSystemName = "system";

    /// <summary>The stored name of <see cref="ExplorerDensityChoice.FollowLayout"/>.</summary>
    public const string FollowLayoutName = "layout";

    /// <summary>The stored name and <c>data-theme</c> value of <see cref="ExplorerThemeChoice.Light"/>.</summary>
    public const string LightName = "light";

    /// <summary>The stored name and <c>data-theme</c> value of <see cref="ExplorerThemeChoice.Dark"/>.</summary>
    public const string DarkName = "dark";

    /// <summary>The stored name and <c>data-contrast</c> value of <see cref="ExplorerContrastChoice.Standard"/>.</summary>
    public const string StandardName = "standard";

    /// <summary>The stored name and <c>data-contrast</c> value of <see cref="ExplorerContrastChoice.More"/>.</summary>
    public const string MoreName = "more";

    /// <summary>The stored name of <paramref name="theme"/>.</summary>
    /// <param name="theme">The theme choice to name.</param>
    /// <returns>The stable stored name.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="theme"/> is not a declared choice.</exception>
    public static string ThemeName(ExplorerThemeChoice theme) => theme switch
    {
        ExplorerThemeChoice.FollowSystem => FollowSystemName,
        ExplorerThemeChoice.Light => LightName,
        ExplorerThemeChoice.Dark => DarkName,
        _ => throw new ArgumentOutOfRangeException(nameof(theme), theme, "Unknown theme choice."),
    };

    /// <summary>
    /// The <c>data-theme</c> value for <paramref name="theme"/>, or
    /// <see langword="null"/> when the environment is to decide and the attribute
    /// must therefore be absent.
    /// </summary>
    /// <param name="theme">The theme choice to express as an attribute.</param>
    /// <returns>The attribute value, or <see langword="null"/> to remove the attribute.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="theme"/> is not a declared choice.</exception>
    public static string? ThemeAttribute(ExplorerThemeChoice theme) => theme switch
    {
        ExplorerThemeChoice.FollowSystem => null,
        ExplorerThemeChoice.Light => LightName,
        ExplorerThemeChoice.Dark => DarkName,
        _ => throw new ArgumentOutOfRangeException(nameof(theme), theme, "Unknown theme choice."),
    };

    /// <summary>Parses a stored theme name. Matching is ordinal and case-insensitive.</summary>
    /// <param name="name">The stored name to parse.</param>
    /// <param name="theme">The parsed choice, or <see cref="ExplorerThemeChoice.FollowSystem"/> when parsing fails.</param>
    /// <returns><see langword="true"/> when <paramref name="name"/> is a known theme name.</returns>
    public static bool TryParseThemeName(string? name, out ExplorerThemeChoice theme)
    {
        if (string.Equals(name, FollowSystemName, StringComparison.OrdinalIgnoreCase))
        {
            theme = ExplorerThemeChoice.FollowSystem;
            return true;
        }

        if (string.Equals(name, LightName, StringComparison.OrdinalIgnoreCase))
        {
            theme = ExplorerThemeChoice.Light;
            return true;
        }

        if (string.Equals(name, DarkName, StringComparison.OrdinalIgnoreCase))
        {
            theme = ExplorerThemeChoice.Dark;
            return true;
        }

        theme = ExplorerThemeChoice.FollowSystem;
        return false;
    }

    /// <summary>The stored name of <paramref name="contrast"/>.</summary>
    /// <param name="contrast">The contrast choice to name.</param>
    /// <returns>The stable stored name.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="contrast"/> is not a declared choice.</exception>
    public static string ContrastName(ExplorerContrastChoice contrast) => contrast switch
    {
        ExplorerContrastChoice.FollowSystem => FollowSystemName,
        ExplorerContrastChoice.Standard => StandardName,
        ExplorerContrastChoice.More => MoreName,
        _ => throw new ArgumentOutOfRangeException(nameof(contrast), contrast, "Unknown contrast choice."),
    };

    /// <summary>
    /// The <c>data-contrast</c> value for <paramref name="contrast"/>, or
    /// <see langword="null"/> when the operating system's <c>prefers-contrast</c>
    /// hint is to decide and the attribute must therefore be absent.
    /// </summary>
    /// <param name="contrast">The contrast choice to express as an attribute.</param>
    /// <returns>The attribute value, or <see langword="null"/> to remove the attribute.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="contrast"/> is not a declared choice.</exception>
    public static string? ContrastAttribute(ExplorerContrastChoice contrast) => contrast switch
    {
        ExplorerContrastChoice.FollowSystem => null,
        ExplorerContrastChoice.Standard => StandardName,
        ExplorerContrastChoice.More => MoreName,
        _ => throw new ArgumentOutOfRangeException(nameof(contrast), contrast, "Unknown contrast choice."),
    };

    /// <summary>Parses a stored contrast name. Matching is ordinal and case-insensitive.</summary>
    /// <param name="name">The stored name to parse.</param>
    /// <param name="contrast">The parsed choice, or <see cref="ExplorerContrastChoice.FollowSystem"/> when parsing fails.</param>
    /// <returns><see langword="true"/> when <paramref name="name"/> is a known contrast name.</returns>
    public static bool TryParseContrastName(string? name, out ExplorerContrastChoice contrast)
    {
        if (string.Equals(name, FollowSystemName, StringComparison.OrdinalIgnoreCase))
        {
            contrast = ExplorerContrastChoice.FollowSystem;
            return true;
        }

        if (string.Equals(name, StandardName, StringComparison.OrdinalIgnoreCase))
        {
            contrast = ExplorerContrastChoice.Standard;
            return true;
        }

        if (string.Equals(name, MoreName, StringComparison.OrdinalIgnoreCase))
        {
            contrast = ExplorerContrastChoice.More;
            return true;
        }

        contrast = ExplorerContrastChoice.FollowSystem;
        return false;
    }

    /// <summary>
    /// The stored name of <paramref name="density"/>. Every explicit density
    /// reuses the token layer's own name through
    /// <see cref="LatticeDensities.Name"/>, so there is one spelling rather than
    /// two that can drift.
    /// </summary>
    /// <param name="density">The density choice to name.</param>
    /// <returns>The stable stored name.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="density"/> is not a declared choice.</exception>
    public static string DensityName(ExplorerDensityChoice density) => density switch
    {
        ExplorerDensityChoice.FollowLayout => FollowLayoutName,
        ExplorerDensityChoice.Comfortable => LatticeDensities.ComfortableName,
        ExplorerDensityChoice.Cosy => LatticeDensities.CosyName,
        ExplorerDensityChoice.Compact => LatticeDensities.CompactName,
        _ => throw new ArgumentOutOfRangeException(nameof(density), density, "Unknown density choice."),
    };

    /// <summary>
    /// The <c>data-lx-density</c> value for <paramref name="density"/>, or
    /// <see langword="null"/> when each adaptive root is to keep the density it
    /// derives from its own breakpoint.
    /// </summary>
    /// <param name="density">The density choice to express as an attribute.</param>
    /// <returns>The attribute value, or <see langword="null"/> to remove the attribute.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="density"/> is not a declared choice.</exception>
    public static string? DensityAttribute(ExplorerDensityChoice density) =>
        density == ExplorerDensityChoice.FollowLayout ? null : DensityName(density);

    /// <summary>Parses a stored density name. Matching is ordinal and case-insensitive.</summary>
    /// <param name="name">The stored name to parse.</param>
    /// <param name="density">The parsed choice, or <see cref="ExplorerDensityChoice.FollowLayout"/> when parsing fails.</param>
    /// <returns><see langword="true"/> when <paramref name="name"/> is a known density name.</returns>
    public static bool TryParseDensityName(string? name, out ExplorerDensityChoice density)
    {
        if (string.Equals(name, FollowLayoutName, StringComparison.OrdinalIgnoreCase))
        {
            density = ExplorerDensityChoice.FollowLayout;
            return true;
        }

        if (LatticeDensities.TryParseName(name, out var token))
        {
            density = token switch
            {
                LatticeDensity.Comfortable => ExplorerDensityChoice.Comfortable,
                LatticeDensity.Compact => ExplorerDensityChoice.Compact,
                _ => ExplorerDensityChoice.Cosy,
            };
            return true;
        }

        density = ExplorerDensityChoice.FollowLayout;
        return false;
    }
}
