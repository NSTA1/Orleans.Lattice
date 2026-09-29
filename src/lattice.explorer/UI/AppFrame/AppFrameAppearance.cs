namespace Orleans.Lattice.Explorer.UI.Framing;

/// <summary>
/// The appearance a frame is told about in <c>lattice.bundle</c>, <c>context.read</c> and
/// <c>context.changed</c>. Every value is drawn from a closed set, so nothing the host
/// sends can smuggle other text into the frame.
/// </summary>
/// <param name="Theme"><c>paper</c> or <c>board</c>.</param>
/// <param name="Contrast"><c>standard</c> or <c>more</c>.</param>
/// <param name="Density"><c>comfortable</c> or <c>compact</c>.</param>
/// <param name="ReducedMotion">Whether the reader asked for reduced motion.</param>
internal sealed record AppFrameAppearance(string Theme, string Contrast, string Density, bool ReducedMotion)
{
    /// <summary>The Paper theme.</summary>
    public const string Paper = "paper";

    /// <summary>The Board theme.</summary>
    public const string Board = "board";

    /// <summary>Standard contrast.</summary>
    public const string StandardContrast = "standard";

    /// <summary>More contrast.</summary>
    public const string MoreContrast = "more";

    /// <summary>Comfortable density.</summary>
    public const string Comfortable = "comfortable";

    /// <summary>Compact density.</summary>
    public const string Compact = "compact";

    /// <summary>Paper, standard contrast, comfortable density, full motion.</summary>
    public static AppFrameAppearance Default { get; } = new(Paper, StandardContrast, Comfortable, false);

    /// <summary>Returns a copy whose every value is in its closed set, replacing anything else with the default.</summary>
    /// <returns>The sanitised appearance.</returns>
    public AppFrameAppearance Sanitise()
    {
        var theme = Theme is Paper or Board ? Theme : Paper;
        var contrast = Contrast is StandardContrast or MoreContrast ? Contrast : StandardContrast;
        var density = Density is Comfortable or Compact ? Density : Comfortable;
        return theme == Theme && contrast == Contrast && density == Density
            ? this
            : new AppFrameAppearance(theme, contrast, density, ReducedMotion);
    }
}
