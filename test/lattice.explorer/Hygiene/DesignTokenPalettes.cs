namespace Orleans.Lattice.Explorer.Tests;

/// <summary>
/// The four palettes the Explorer's token layer can compute, named so a test
/// case can address one, and resolved through the cascade rather than read from
/// a single block.
/// </summary>
/// <remarks>
/// The light and high-contrast blocks restate only the tokens that differ, so a
/// palette is the layering of its blocks in cascade order. Resolving them here
/// keeps that knowledge in one place: a fixture asks for "light high contrast"
/// and gets what a browser would compute, rather than having to know which
/// blocks contribute to it.
/// </remarks>
internal static class DesignTokenPalettes
{
    /// <summary>The default palette.</summary>
    public const string Dark = "dark";

    /// <summary>The light palette, layered over the dark one.</summary>
    public const string Light = "light";

    /// <summary>The dark palette with the high-contrast overlay applied.</summary>
    public const string DarkHighContrast = "dark high contrast";

    /// <summary>The light palette with the high-contrast overlay applied.</summary>
    public const string LightHighContrast = "light high contrast";

    /// <summary>
    /// Resolves a named palette into the tokens a browser would compute for it.
    /// </summary>
    /// <param name="name">One of the palette names declared on this type.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="name"/> is not one of them.
    /// </exception>
    public static IReadOnlyDictionary<string, string> Resolve(string name) => name switch
    {
        Dark => DesignTokens.Palette(
            DesignTokens.DarkSelector),
        Light => DesignTokens.Palette(
            DesignTokens.DarkSelector,
            DesignTokens.LightSelector),
        DarkHighContrast => DesignTokens.Palette(
            DesignTokens.DarkSelector,
            DesignTokens.DarkHighContrastSelector),
        LightHighContrast => DesignTokens.Palette(
            DesignTokens.DarkSelector,
            DesignTokens.LightSelector,
            DesignTokens.LightHighContrastSelector),
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "Unknown palette."),
    };
}
