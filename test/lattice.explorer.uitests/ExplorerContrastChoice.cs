namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The contrast choice a scenario applies. A local stand-in for the retired UI's
/// appearance enum, kept so this suite compiles after the cutover (issue #3831)
/// until the browser suite is rewritten against the new Explorer UI (issue #3832).
/// </summary>
public enum ExplorerContrastChoice
{
    /// <summary>Follow the operating system's contrast preference.</summary>
    FollowSystem = 0,

    /// <summary>Standard contrast.</summary>
    Standard = 1,

    /// <summary>More contrast.</summary>
    More = 2,
}

/// <summary>
/// The document attribute values for each contrast choice. A local stand-in for the
/// retired UI's appearance names (see <see cref="ExplorerContrastChoice"/>).
/// </summary>
public static class ExplorerAppearanceNames
{
    /// <summary>The standard-contrast attribute value.</summary>
    public const string StandardName = "standard";

    /// <summary>The high-contrast attribute value.</summary>
    public const string MoreName = "more";

    /// <summary>The attribute value a contrast choice applies.</summary>
    /// <param name="contrast">The choice.</param>
    /// <returns>The value, following the system as standard.</returns>
    public static string ContrastAttribute(ExplorerContrastChoice contrast) =>
        contrast == ExplorerContrastChoice.More ? MoreName : StandardName;
}