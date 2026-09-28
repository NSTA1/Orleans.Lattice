namespace Orleans.Lattice.Explorer.Shell.Design.Tokens;

/// <summary>
/// How a density is applied: an attribute on the document element that
/// <c>lattice-operate.css</c> reads to choose its row and control heights.
/// </summary>
internal static class LtDensities
{
    /// <summary>The document-element attribute that selects the density.</summary>
    public const string AttributeName = "data-lt-density";

    /// <summary>The attribute value for <paramref name="density"/>.</summary>
    /// <param name="density">The density.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="density"/> is not a declared density.</exception>
    public static string AttributeValue(LtDensity density) => density switch
    {
        LtDensity.Comfortable => "comfortable",
        LtDensity.Compact => "compact",
        _ => throw new ArgumentOutOfRangeException(nameof(density), density, "Unknown density."),
    };
}
